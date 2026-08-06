
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Drives row-level TTL: mints sweep runs on one elected node, and processes their spans on every node.
///
/// <para><b>Two roles, deliberately asymmetric.</b> Planning is leader-gated exactly like
/// <see cref="AutoAnalyzeScheduler"/> and <see cref="OrphanReclaimer"/> — one node decides that a table
/// is due, fixes the run's horizon, and publishes the manifest, because minting two runs for one table
/// would double the scan work against two different horizons. Working is <em>not</em> gated: every node
/// claims spans of whatever open runs exist. That split is what makes TTL the first background job in
/// the engine whose work is spread across the cluster rather than done entirely by the leader.</para>
///
/// <para><b>An open run is adopted, never re-minted.</b> When the planner's node loses leadership
/// mid-run, the manifest is already durable and already carries the horizon that makes the run
/// well-defined, so the new leader continues it. Re-minting would re-scan everything the previous run
/// had already checkpointed past, for no gain.</para>
///
/// <para><b>Off by default.</b> With <see cref="CamusDBOptions.TtlEnabled"/> false neither loop starts
/// and behavior is identical to an engine without TTL — an expired row is simply never collected.</para>
/// </summary>
internal sealed class TtlScheduler : IAsyncDisposable
{
    private readonly EmbeddedKahuna sharedNode;
    private readonly string leaderKey;
    private readonly TtlSpanCoordinator coordinator;
    private readonly TtlSpanSweeper sweeper;
    private readonly Func<CancellationToken, Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>>> discoverTtlTables;

    /// <summary>
    /// Every open database, independent of whether any of its tables has TTL configured. The reaper
    /// needs this precisely because a table that was reset or dropped no longer appears in
    /// <c>discoverTtlTables</c> — the metadata to reclaim belongs to tables the sweep has stopped
    /// seeing, so a reaper driven by the sweep's own candidate list could never find it.
    /// </summary>
    private readonly Func<CancellationToken, Task<IReadOnlyList<DatabaseDescriptor>>> discoverDatabases;
    private readonly ILogger<ICamusDB> logger;
    private readonly CamusDBOptions options;
    private readonly CancellationTokenSource cts = new();

    private Task? loop;

    /// <summary>Cumulative counters for this node, surfaced through engine statistics.</summary>
    internal long RowsExpired;
    internal long RowsSkippedByRecheck;

    /// <summary>
    /// Rows whose delete failed or could not be resolved. Kept apart from
    /// <see cref="RowsSkippedByRecheck"/>: a skip means a row was correctly spared, a failure means work
    /// did not happen, and summing them produces a number that looks healthy while data is being
    /// retained.
    /// </summary>
    internal long RowsFailed;

    internal long SpansCompleted;
    internal long RunsPlanned;

    /// <summary>
    /// Spans this node took over from a previous owner whose lease had lapsed — recorded where the
    /// takeover is actually proven (a claim succeeding on a span that already carries progress), not
    /// inferred later from a state that could have several explanations.
    /// </summary>
    internal long SpansReclaimed;

    /// <summary>Runs this node retired as complete, and total time spent sweeping.</summary>
    internal long RunsCompleted;
    internal long SweepMillis;

    /// <summary>
    /// Spans in flight right now, and the most this node has ever had in flight at once. The high-water
    /// mark is how the concurrency limit is confirmed to be doing something: whether span work actually
    /// overlaps cannot be read off elapsed time, because a faster span makes a sweep quicker whether or
    /// not anything ran in parallel.
    /// </summary>
    private int spansInFlight;

    internal int PeakConcurrentSpans;

    /// <summary>
    /// Per-table run state, keyed "{dbId}/{tableId}". Cumulative counters answer "is the sweep working
    /// at all"; this answers "which table is stuck", which is the question an operator actually has when
    /// one table stops shrinking and the totals still look healthy.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, TtlTableStatus> tableStatus =
        new(StringComparer.Ordinal);

    internal IReadOnlyDictionary<string, TtlTableStatus> TableStatus => tableStatus;

    public TtlScheduler(
        EmbeddedKahuna sharedNode,
        string leaderKey,
        TtlSpanCoordinator coordinator,
        TtlSpanSweeper sweeper,
        Func<CancellationToken, Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>>> discoverTtlTables,
        Func<CancellationToken, Task<IReadOnlyList<DatabaseDescriptor>>> discoverDatabases,
        ILogger<ICamusDB> logger,
        CamusDBOptions options)
    {
        this.sharedNode = sharedNode;
        this.leaderKey = leaderKey;
        this.coordinator = coordinator;
        this.sweeper = sweeper;
        this.discoverTtlTables = discoverTtlTables;
        this.discoverDatabases = discoverDatabases;
        this.logger = logger;
        this.options = options;
    }

    /// <summary>Starts the sweep loop. A no-op while row-level TTL is disabled.</summary>
    public void Start()
    {
        if (!options.TtlEnabled)
            return;

        // Serialize concurrent Start calls: an unsynchronized `loop ??=` is check-then-act, and two
        // racing callers would each launch an independent loop for the process lifetime.
        lock (this)
        {
            loop ??= SweepLoopAsync(cts.Token);
        }
    }

    /// <summary>
    /// How often the loop wakes. This is the polling granularity, not a table's sweep cadence: a tick
    /// only ever checks whether a table's own <c>ttl_job_cron</c> interval has elapsed since its last
    /// completed run, so the shortest cadence a table can achieve is one tick and the longest is
    /// whatever it configured.
    /// </summary>
    private const int TickMs = 60_000;

    private async Task SweepLoopAsync(CancellationToken ct)
    {
        // try/catch INSIDE the loop: one failed sweep (a transient scan error, a leadership flip) is
        // logged and retried next tick — it never terminates the loop. Only cancellation ends it.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TickMs, ct).ConfigureAwait(false);
                await RunSweepAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // normal shutdown
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "TTL sweep failed; retrying next tick");
            }
        }
    }

    /// <summary>
    /// Runs one tick: plan on the leader, then work spans on this node. Returns the number of rows
    /// deleted. Exposed to tests so a sweep can be forced without waiting a full tick.
    /// </summary>
    internal async Task<long> RunSweepAsync(CancellationToken ct)
    {
        if (!options.TtlEnabled)
            return 0;

        System.Diagnostics.Stopwatch sweepWatch = System.Diagnostics.Stopwatch.StartNew();

        IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)> candidates =
            await discoverTtlTables(ct).ConfigureAwait(false);

        long deleted = 0;

        // Table ids this tick legitimately sweeps, per database. Anything else with run metadata was
        // paused, reset, or dropped since its run was minted, and nothing else will ever revisit it.
        Dictionary<string, HashSet<string>> liveTableIds = new(StringComparer.Ordinal);
        HashSet<string>? live;

        foreach ((DatabaseDescriptor database, TableDescriptor table) in candidates)
        {
            if (ct.IsCancellationRequested)
                break;

            TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, options);
            if (!ttl.IsActive)
            {
                // A paused table must still report. Dropping it from the status map would make "paused"
                // indistinguishable from "gone", which is exactly the ambiguity this surface removes.
                if (ttl.ExpirationColumn is not null)
                {
                    live = null;
                    await RecordTableStatusAsync(database, table, ttl, manifest: null, 0, 0, ct).ConfigureAwait(false);
                }
                continue;
            }

            if (!liveTableIds.TryGetValue(database.Id, out live))
                liveTableIds[database.Id] = live = new HashSet<string>(StringComparer.Ordinal);
            live.Add(table.Id);

            TtlRunManifest? manifest = await coordinator.ReadManifestAsync(
                database.Id, table.Id, ct).ConfigureAwait(false);

            // Discard a run that belongs to a table id that is no longer this name's table. A run left
            // behind by a DROP must never be driven: its spans would delete rows out of whatever table
            // now carries the name, under the dropped table's configuration.
            if (manifest is not null && !string.Equals(manifest.TableId, table.Id, StringComparison.Ordinal))
            {
                if (await IsLeaderAsync(ct).ConfigureAwait(false))
                    await coordinator.DeleteRunAsync(database.Id, manifest.TableId, manifest.SpanCount, ct).ConfigureAwait(false);
                manifest = null;
            }

            // A run describes one predicate. If the table's expiration column or grace period has since
            // changed, finishing this run would apply the old meaning to its remaining spans while rows
            // it already passed were judged by the old one and never revisited — one run, two answers.
            // Retiring it and re-planning gives the new configuration a clean sweep of the whole table.
            if (manifest is not null &&
                !string.Equals(
                    manifest.PredicateFingerprint,
                    TtlRunManifest.BuildPredicateFingerprint(ttl.ExpirationColumn, ttl.GraceMs),
                    StringComparison.Ordinal))
            {
                if (!await IsLeaderAsync(ct).ConfigureAwait(false))
                    continue; // let the leader retire it; working it would use a stale predicate

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "TTL run {Run} for table {Table} was planned under a different expiry configuration; re-planning",
                        manifest.RunId, manifest.TableName);

                await coordinator.DeleteRunAsync(database.Id, manifest.TableId, manifest.SpanCount, ct).ConfigureAwait(false);
                manifest = null;
            }

            // A completed run is kept as the record of when this table was last swept. Only the leader
            // may replace it, and only once the table's own cadence has elapsed — otherwise every node
            // would re-plan the instant a run finished and the table would sweep continuously.
            if (manifest is not null && manifest.IsComplete)
            {
                if (Now().L - manifest.CompletedPhysical < ttl.JobIntervalMs)
                    continue;

                // Leadership is re-read here rather than reused from the top of the tick: it can be lost
                // between tables, and every planner mutation must be authorized by a fresh check plus a
                // compare-and-set. The check alone is advisory — it can go stale in the gap before the
                // write — so the CAS is what actually makes a lost leadership harmless.
                if (!await IsLeaderAsync(ct).ConfigureAwait(false))
                    continue;

                await coordinator.DeleteRunAsync(database.Id, manifest.TableId, manifest.SpanCount, ct).ConfigureAwait(false);
                manifest = null;
            }

            if (manifest is null)
            {
                if (!await IsLeaderAsync(ct).ConfigureAwait(false))
                    continue; // no open run and this node does not get to mint one

                manifest = await PlanRunAsync(database, table, ttl, ct).ConfigureAwait(false);

                if (manifest is null)
                    continue; // another node published first; leave its run alone this tick
            }

            long spansBefore = Interlocked.Read(ref SpansCompleted);
            long failedBefore = Interlocked.Read(ref RowsFailed);

            deleted += await WorkSpansAsync(database, table, manifest, ttl, ct).ConfigureAwait(false);

            long spansThisTick = Interlocked.Read(ref SpansCompleted) - spansBefore;
            long failedThisTick = Interlocked.Read(ref RowsFailed) - failedBefore;

            // Marking a finished run complete is the leader's job, for the same reason minting is: two
            // nodes racing on the run record would have one overwrite what the other just wrote.
            if (await IsLeaderAsync(ct).ConfigureAwait(false))
                await MarkRunCompleteIfFinishedAsync(database, manifest, ct).ConfigureAwait(false);

            await RecordTableStatusAsync(
                database, table, ttl, manifest, spansThisTick, failedThisTick, ct).ConfigureAwait(false);
        }

        await ReapAbandonedRunMetadataAsync(liveTableIds, ct).ConfigureAwait(false);

        sweepWatch.Stop();
        Interlocked.Add(ref SweepMillis, (long)sweepWatch.Elapsed.TotalMilliseconds);

        return deleted;
    }

    /// <summary>
    /// Records what this tick observed for one table, so an operator can tell a table that has nothing
    /// to do from one that cannot make progress. Derived from what the tick actually saw rather than
    /// re-read afterwards, because the distinction between waiting and stalled is only visible while the
    /// tick is running.
    /// </summary>
    private async Task RecordTableStatusAsync(
        DatabaseDescriptor database, TableDescriptor table, TtlSettings ttl,
        TtlRunManifest? manifest, long spansDoneThisTick, long failedThisTick, CancellationToken ct)
    {
        TtlTableStatus status = new()
        {
            DatabaseName = database.Name ?? "",
            TableName = table.Name ?? "",
            LastObservedPhysical = Now().L,
        };

        if (ttl.Paused)
        {
            status.State = TtlRunState.Paused;
        }
        else if (manifest is null || manifest.IsComplete)
        {
            status.State = TtlRunState.Idle;
            status.RunId = manifest?.RunId ?? "";
            status.HorizonPhysical = manifest?.HorizonPhysical ?? 0;
            status.SpanCount = manifest?.SpanCount ?? 0;
            status.SpansDone = manifest?.SpanCount ?? 0;
        }
        else
        {
            status.RunId = manifest.RunId;
            status.HorizonPhysical = manifest.HorizonPhysical;
            status.SpanCount = manifest.SpanCount;

            int done = 0;
            for (int i = 0; i < manifest.SpanCount && !ct.IsCancellationRequested; i++)
            {
                TtlSpanCheckpoint? checkpoint = await coordinator.ReadCheckpointAsync(
                    database.Id, manifest.TableId, i, manifest.RunId, ct).ConfigureAwait(false);

                if (checkpoint is null)
                    continue;

                if (checkpoint.Done)
                    done++;

                status.RowsDeleted += checkpoint.RowsDeleted;
                status.RowsSkipped += checkpoint.RowsSkipped;
                status.RowsFailed += checkpoint.RowsFailed;
            }

            status.SpansDone = done;

            // Failing outranks the rest: rows are not being removed and the reason is known. Otherwise a
            // tick that completed spans is progressing; one that completed none while spans remain is
            // stalled if the run is already past its cadence, and merely waiting if it is not — the
            // second case is normally another node holding the spans.
            status.State = failedThisTick > 0
                ? TtlRunState.Failing
                : spansDoneThisTick > 0
                    ? TtlRunState.Progressing
                    : Now().L - manifest.StartedPhysical > ttl.JobIntervalMs
                        ? TtlRunState.Stalled
                        : TtlRunState.Waiting;
        }

        tableStatus[$"{database.Id}/{table.Id}"] = status;
    }

    /// <summary>
    /// Deletes run metadata belonging to tables this sweep no longer visits.
    ///
    /// <para>Discovery enumerates tables whose TTL is <em>active</em>, so a table that is paused, reset,
    /// or dropped simply stops appearing — and the manifest, span claims, and checkpoints it left behind
    /// become unreachable. Nothing else in the engine knows those keys exist, so without a pass that
    /// asks what metadata is present rather than what tables are, they accumulate for the life of the
    /// database.</para>
    ///
    /// <para>Leader-only and best-effort. It is safe across partial failure because it only ever deletes
    /// records for a table id the sweep did not touch this tick: re-enabling TTL on that table mints a
    /// fresh run rather than resuming a half-deleted one, and an interrupted reap is simply completed by
    /// the next tick.</para>
    /// </summary>
    private async Task ReapAbandonedRunMetadataAsync(
        Dictionary<string, HashSet<string>> liveTableIds,
        CancellationToken ct)
    {
        if (!await IsLeaderAsync(ct).ConfigureAwait(false))
            return;

        IReadOnlyList<DatabaseDescriptor> databaseList;
        try
        {
            databaseList = await discoverDatabases(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "TTL metadata reap could not enumerate databases");
            return;
        }

        Dictionary<string, DatabaseDescriptor> databases = new(StringComparer.Ordinal);
        foreach (DatabaseDescriptor db in databaseList)
            databases[db.Id] = db;

        foreach (KeyValuePair<string, DatabaseDescriptor> entry in databases)
        {
            if (ct.IsCancellationRequested)
                break;

            liveTableIds.TryGetValue(entry.Key, out HashSet<string>? live);

            IReadOnlyList<string> withMetadata;
            try
            {
                withMetadata = await coordinator.ListRunTableIdsAsync(entry.Key, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "TTL metadata reap could not enumerate runs in database {Db}", entry.Value.Name);
                continue;
            }

            foreach (string tableId in withMetadata)
            {
                if (live is not null && live.Contains(tableId))
                    continue;

                try
                {
                    TtlRunManifest? stale = await coordinator.ReadManifestAsync(entry.Key, tableId, ct).ConfigureAwait(false);

                    // Span count comes from the manifest where one survives; a manifest already gone but
                    // span records left behind still needs its spans cleared, so fall back to the
                    // configured width, which is the most this run could have had.
                    int spanCount = stale?.SpanCount ?? Math.Max(1, options.TtlSpansPerTable);

                    await coordinator.DeleteRunAsync(entry.Key, tableId, spanCount, ct).ConfigureAwait(false);

                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation(
                            "Reclaimed TTL run metadata for table id {TableId} in database {Db}; it is no longer swept",
                            tableId, entry.Value.Name);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "TTL metadata reap failed for table id {TableId}", tableId);
                }
            }
        }
    }

    private ValueTask<bool> IsLeaderAsync(CancellationToken ct) =>
        sharedNode.AmILeaderForKeyAsync(leaderKey, ct);

    /// <summary>
    /// Mints a run for a table that is due, capturing the horizon once for every span in it.
    /// Returns null when the table's cadence has not elapsed since its last run.
    /// </summary>
    private async Task<TtlRunManifest?> PlanRunAsync(
        DatabaseDescriptor database, TableDescriptor table, TtlSettings ttl, CancellationToken ct)
    {
        HLCTimestamp now = Now();

        TtlRunManifest manifest = new()
        {
            RunId = ObjectIdGenerator.Generate().ToString(),
            TableId = table.Id,
            TableName = table.Name,
            HorizonNode = now.N,
            HorizonPhysical = now.L,
            HorizonCounter = now.C,
            SpanCount = Math.Max(1, options.TtlSpansPerTable),
            StartedPhysical = now.L,
            ExpirationColumn = ttl.ExpirationColumn ?? "",
            GraceMs = ttl.GraceMs,
            PredicateFingerprint = TtlRunManifest.BuildPredicateFingerprint(ttl.ExpirationColumn, ttl.GraceMs),
        };

        // Publish only if no run exists. A leadership flip between the read above and this write would
        // otherwise let a former leader overwrite the run its successor already minted, leaving one
        // table with two horizons and spans checkpointed under whichever happened to land last.
        if (!await coordinator.TryWriteManifestAsync(database.Id, expected: null, manifest, ct).ConfigureAwait(false))
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "TTL run for table {Table} was published by another node; leaving it alone this tick", table.Name);
            return null;
        }

        Interlocked.Increment(ref RunsPlanned);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Planned TTL run {Run} for table {Table} in database {Db} across {Spans} spans",
                manifest.RunId, table.Name, database.Name, manifest.SpanCount);

        return manifest;
    }

    /// <summary>
    /// Works every claimable span of an open run, <see cref="CamusDBOptions.TtlMaxConcurrentSpansPerNode"/>
    /// at a time, replenishing each slot as it frees until nothing is left to claim.
    ///
    /// <para><b>The knob is a concurrency limit, not a per-tick quota.</b> Treating it as a quota — one
    /// span per tick at the default — makes an over-split run take one tick per span: with 64 spans and a
    /// one-minute tick, an <em>empty</em> table needs over an hour to confirm it is empty. Since spans
    /// exist precisely to be worked in parallel, the limit has to bound how many run at once and nothing
    /// else.</para>
    ///
    /// <para><b>The limit is node-local.</b> Each node independently runs up to this many spans, so a
    /// cluster of N nodes can have N times as many in flight. That is the intended behaviour — spreading
    /// span work across nodes is the point — but it means the rate limits below are also per node, and
    /// cluster-wide throughput scales with the fleet.</para>
    ///
    /// <para>Spans are visited from a per-node rotating offset rather than always from zero, so several
    /// nodes ticking at the same moment do not all contend for span 0 and then span 1 — they start in
    /// different places and mostly claim disjoint work on the first try.</para>
    /// </summary>
    private async Task<long> WorkSpansAsync(
        DatabaseDescriptor database, TableDescriptor table, TtlRunManifest manifest, TtlSettings ttl, CancellationToken ct)
    {
        int maxConcurrent = Math.Max(1, options.TtlMaxConcurrentSpansPerNode);
        int offset = (int)((uint)sharedNode.Raft.GetLocalNodeId().GetHashCode() % (uint)manifest.SpanCount);

        // One limiter pair for the whole table, shared by every concurrent span. Per-span limiters would
        // multiply the configured rate by the concurrency — a table capped at 100 deletes/second would
        // actually issue 100 × maxConcurrent, which is the opposite of what a rate limit is for.
        TtlRateLimiter selectLimiter = new(ttl.SelectRateLimit);
        TtlRateLimiter deleteLimiter = new(ttl.DeleteRateLimit);

        long deleted = 0;
        int nextSpanOffset = -1;

        using SemaphoreSlim slots = new(maxConcurrent);
        List<Task> running = [];

        while (!ct.IsCancellationRequested)
        {
            int i = Interlocked.Increment(ref nextSpanOffset);
            if (i >= manifest.SpanCount)
                break;

            int spanIndex = (offset + i) % manifest.SpanCount;

            TtlSpanCheckpoint? existing = await coordinator.ReadCheckpointAsync(
                database.Id, manifest.TableId, spanIndex, manifest.RunId, ct).ConfigureAwait(false);

            if (existing?.Done == true)
                continue;

            long? claimToken = await coordinator.TryClaimSpanAsync(
                database.Id, manifest.TableId, spanIndex, ct).ConfigureAwait(false);

            if (claimToken is null)
                continue; // another worker owns it; take the next one rather than waiting

            // Proven, not inferred: winning a claim on a span that already has progress means the
            // previous owner stopped without finishing and its lease lapsed. Deriving this later from
            // stored state could not distinguish it from a resumed span of our own.
            if (existing is not null && existing.LastRowIdHex.Length > 0)
                Interlocked.Increment(ref SpansReclaimed);

            try
            {
                await slots.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await coordinator.ReleaseSpanAsync(database.Id, manifest.TableId, spanIndex, claimToken.Value).ConfigureAwait(false);
                break;
            }

            running.Add(SweepOneSpanAsync(
                database, table, manifest, ttl, spanIndex, claimToken.Value,
                selectLimiter, deleteLimiter, slots, d => Interlocked.Add(ref deleted, d), ct));
        }

        await Task.WhenAll(running).ConfigureAwait(false);
        return Interlocked.Read(ref deleted);
    }

    private async Task SweepOneSpanAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TtlRunManifest manifest,
        TtlSettings ttl,
        int spanIndex,
        long claimToken,
        TtlRateLimiter selectLimiter,
        TtlRateLimiter deleteLimiter,
        SemaphoreSlim slots,
        Action<long> addDeleted,
        CancellationToken ct)
    {
        int inFlight = Interlocked.Increment(ref spansInFlight);

        // Raise the high-water mark without a lock: re-read and retry, since another span may have
        // pushed it higher between the read and the exchange.
        int observedPeak = Volatile.Read(ref PeakConcurrentSpans);
        while (inFlight > observedPeak)
        {
            int previous = Interlocked.CompareExchange(ref PeakConcurrentSpans, inFlight, observedPeak);
            if (previous == observedPeak)
                break;

            observedPeak = previous;
        }

        try
        {
            (long spanDeleted, long spanSkipped, long spanFailed) = await sweeper.SweepSpanAsync(
                database, table, manifest, ttl, spanIndex, claimToken,
                selectLimiter, deleteLimiter, ct).ConfigureAwait(false);

            addDeleted(spanDeleted);
            Interlocked.Add(ref RowsExpired, spanDeleted);
            Interlocked.Add(ref RowsSkippedByRecheck, spanSkipped);
            Interlocked.Add(ref RowsFailed, spanFailed);
            Interlocked.Increment(ref SpansCompleted);
        }
        catch (OperationCanceledException)
        {
            // Shutdown or load-cancel: the checkpoint holds, so the span resumes rather than restarts.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "TTL span {Span} of table {Table} failed", spanIndex, table.Name);
        }
        finally
        {
            // Release explicitly so the next worker can take the span immediately instead of waiting
            // out the lease — a span held by a finished worker is the same stall as one held by a
            // dead worker, just shorter.
            await coordinator.ReleaseSpanAsync(database.Id, manifest.TableId, spanIndex, claimToken).ConfigureAwait(false);
            Interlocked.Decrement(ref spansInFlight);
            slots.Release();
        }
    }

    /// <summary>
    /// Stamps a run whose every span is done as complete. The record stays: it is what paces the next
    /// run to the table's <c>ttl_job_cron</c> cadence, and what reports when the table was last swept.
    /// </summary>
    private async Task MarkRunCompleteIfFinishedAsync(DatabaseDescriptor database, TtlRunManifest manifest, CancellationToken ct)
    {
        if (manifest.IsComplete)
            return;

        for (int i = 0; i < manifest.SpanCount; i++)
        {
            TtlSpanCheckpoint? checkpoint = await coordinator.ReadCheckpointAsync(
                database.Id, manifest.TableId, i, manifest.RunId, ct).ConfigureAwait(false);

            if (checkpoint?.Done != true)
                return; // still work outstanding
        }

        // Complete the run this node has been driving, conditional on it still being the stored one. A
        // node that lost leadership mid-run and whose successor already retired the run and planned a
        // fresh one would otherwise stamp "complete" over that new run — retiring a sweep that had
        // barely started, and pushing the table's next sweep a full cadence into the future.
        TtlRunManifest completed = CloneManifest(manifest);
        completed.CompletedPhysical = Now().L;

        if (!await coordinator.TryWriteManifestAsync(database.Id, manifest, completed, ct).ConfigureAwait(false))
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "TTL run {Run} for table {Table} was superseded before it could be marked complete",
                    manifest.RunId, manifest.TableName);
            return;
        }

        manifest.CompletedPhysical = completed.CompletedPhysical;

        Interlocked.Increment(ref RunsCompleted);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("TTL run {Run} for table {Table} completed", manifest.RunId, manifest.TableName);
    }

    /// <summary>
    /// Copies a manifest so the compare operand of a conditional write stays byte-identical to what was
    /// read. Mutating the original in place would change the very bytes the compare-and-set needs.
    /// </summary>
    private static TtlRunManifest CloneManifest(TtlRunManifest source) => new()
    {
        RunId = source.RunId,
        TableId = source.TableId,
        TableName = source.TableName,
        HorizonNode = source.HorizonNode,
        HorizonPhysical = source.HorizonPhysical,
        HorizonCounter = source.HorizonCounter,
        SpanCount = source.SpanCount,
        StartedPhysical = source.StartedPhysical,
        CompletedPhysical = source.CompletedPhysical,
        ExpirationColumn = source.ExpirationColumn,
        GraceMs = source.GraceMs,
        PredicateFingerprint = source.PredicateFingerprint,
    };

    private HLCTimestamp Now() =>
        sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);

        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* the loop swallows its own errors */ }
        }

        cts.Dispose();
        await coordinator.DisposeAsync().ConfigureAwait(false);
    }
}
