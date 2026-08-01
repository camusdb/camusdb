
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Background driver for automatic table <c>ANALYZE</c>. Periodically finds tables whose statistics
/// have gone stale (too many mutations since the last ANALYZE) and refreshes them cheaply, keeping
/// the optimizer's histograms/NDV current without a user running <c>ANALYZE TABLE</c>.
///
/// <para><b>Once per cluster.</b> Like <see cref="OrphanReclaimer"/> and <see cref="SnapshotHoldRenewer"/>,
/// a sweep runs only while this node leads the database-registry key's partition
/// (<see cref="EmbeddedKahuna.AmILeaderForKeyAsync"/>), re-checked each sweep, between candidates,
/// <em>and inside the scan itself</em>, so N nodes don't each analyze the same table and a former
/// leader can never publish after losing its lease during a failover. Candidate discovery is delegated
/// to <paramref name="discoverStale"/>, which enumerates authoritative cluster metadata (not this
/// node's open-object list) so a table opened only on a follower is still found.</para>
///
/// <para><b>Never spikes, never interferes.</b> The scan is delegated to
/// <see cref="TableAnalyzer.AnalyzeBackgroundAsync"/>, which reads a lock-free snapshot (cannot block
/// or abort foreground work), samples to bound memory, and throttles its row rate. This scheduler adds
/// three protections: it analyzes at most <see cref="CamusDBOptions.AutoAnalyzeMaxConcurrent"/> tables
/// at once; it holds a per-node in-flight claim per table so the loop and the test seam never overlap
/// on one table; and it passes the analyzer a load callback that cancels a running scan at the next
/// batch boundary when foreground load exceeds <see cref="CamusDBOptions.AutoAnalyzeLoadPauseThreshold"/>.
/// Because the reads take no locks, there is no priority inversion — load backoff addresses raw
/// CPU/IO contention only.</para>
///
/// <para><b>Idempotent &amp; resumable.</b> A cancelled analyze (shutdown, load-cancel, or leadership
/// loss) persists nothing and leaves the table marked stale, so it is simply retried on a later sweep —
/// no partial statistics, no double counting.</para>
/// </summary>
internal sealed class AutoAnalyzeScheduler : IAsyncDisposable
{
    private readonly EmbeddedKahuna sharedNode;
    private readonly string leaderKey;
    private readonly TableAnalyzer analyzer;
    private readonly Func<CancellationToken, Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>>> discoverStale;
    private readonly Func<int> foregroundLoad;
    private readonly ILogger<ICamusDB> logger;

    /// <summary>Configuration for the engine this scheduler serves; injected, never ambient.</summary>
    private readonly CamusDBOptions options;
    private readonly int intervalMs;
    private readonly CancellationTokenSource cts = new();

    // Per-node in-flight claim: at most one analyze per table on this node at a time (keyed
    // "{dbId}:{tableId}"). Prevents the timer loop and the forced-sweep test seam from overlapping.
    private readonly ConcurrentDictionary<string, byte> inFlight = new(StringComparer.Ordinal);

    private Task? loop;

    public AutoAnalyzeScheduler(
        EmbeddedKahuna sharedNode,
        string leaderKey,
        TableAnalyzer analyzer,
        Func<CancellationToken, Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>>> discoverStale,
        Func<int> foregroundLoad,
        ILogger<ICamusDB> logger,
        int intervalMs,
        CamusDBOptions options)
    {
        this.options = options;
        this.sharedNode = sharedNode;
        this.leaderKey = leaderKey;
        this.analyzer = analyzer;
        this.discoverStale = discoverStale;
        this.foregroundLoad = foregroundLoad;
        this.logger = logger;
        this.intervalMs = intervalMs;
    }

    /// <summary>Starts the sweep loop. Disabled entirely when auto-analyze is off or the interval is non-positive.</summary>
    public void Start()
    {
        if (!options.AutoAnalyzeEnabled || intervalMs <= 0)
            return;

        // Serialize concurrent Start calls: an unsynchronized `loop ??=` is check-then-act, and
        // two racing callers would each launch an independent sweep loop for the process lifetime.
        lock (this)
        {
            loop ??= SweepLoopAsync(cts.Token);
        }
    }

    private async Task SweepLoopAsync(CancellationToken ct)
    {
        // try/catch INSIDE the loop: a single failed sweep (transient scan error, leadership flip) is
        // logged and retried next interval — it never terminates the loop. Only cancellation ends it.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(intervalMs, ct).ConfigureAwait(false);
                await RunSweepAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // normal shutdown
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Auto-analyze sweep failed; retrying next interval");
            }
        }
    }

    private HLCTimestamp Now() =>
        sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());

    // True when the foreground load probe reports more in-flight work than the pause threshold allows.
    private bool LoadExceeded()
    {
        int threshold = options.AutoAnalyzeLoadPauseThreshold;
        if (threshold <= 0)
            return false; // load-based backoff disabled
        return foregroundLoad() > threshold;
    }

    private ValueTask<bool> AmILeaderAsync(CancellationToken ct) =>
        sharedNode.AmILeaderForKeyAsync(leaderKey, ct);

    /// <summary>
    /// Runs one sweep on the elected node and returns the number of tables analyzed. Exposed to tests
    /// so a sweep can be forced without waiting a full interval.
    /// </summary>
    internal async Task<int> RunSweepAsync(CancellationToken ct)
    {
        if (!options.AutoAnalyzeEnabled)
            return 0;

        if (!await AmILeaderAsync(ct).ConfigureAwait(false))
            return 0;

        // Surge protector: if the node is already busy, don't add background scan load this sweep.
        if (LoadExceeded())
            return 0;

        IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)> candidates =
            await discoverStale(ct).ConfigureAwait(false);
        if (candidates.Count == 0)
            return 0;

        // Jitter: shuffle so that when more tables are stale than we analyze per sweep, the excess is
        // spread across future sweeps instead of always starving the same tail (anti-thundering-herd).
        var ordered = new List<(DatabaseDescriptor db, TableDescriptor table)>(candidates);
        Shuffle(ordered, (ulong)Now().L);

        int maxConcurrent = Math.Max(1, options.AutoAnalyzeMaxConcurrent);
        HLCTimestamp analyzedAt = Now();

        int analyzed = 0;
        using var slots = new SemaphoreSlim(maxConcurrent);
        var running = new List<Task>();

        foreach ((DatabaseDescriptor db, TableDescriptor table) in ordered)
        {
            if (ct.IsCancellationRequested)
                break;

            // Re-check leadership and load between dispatches: leadership can flip mid-sweep, and a
            // load surge should stop new starts (in-flight scans self-cancel via the pause callback).
            if (!await AmILeaderAsync(ct).ConfigureAwait(false) || LoadExceeded())
                break;

            // Per-node exclusion: skip a table already being analyzed on this node (loop vs test seam).
            string claim = ClaimKey(db, table);
            if (!inFlight.TryAdd(claim, 0))
                continue;

            try
            {
                await slots.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                inFlight.TryRemove(claim, out _);
                break;
            }

            running.Add(RunOneAsync(db, table, claim, analyzedAt, ct, slots,
                onSuccess: () => Interlocked.Increment(ref analyzed)));
        }

        await Task.WhenAll(running).ConfigureAwait(false);
        return analyzed;
    }

    private async Task RunOneAsync(
        DatabaseDescriptor db,
        TableDescriptor table,
        string claim,
        HLCTimestamp analyzedAt,
        CancellationToken ct,
        SemaphoreSlim slots,
        Action onSuccess)
    {
        try
        {
            // stillOwner: re-checked at each batch boundary and before publish, so a former leader that
            // loses its lease mid-scan aborts without publishing. shouldPause: cancels the scan when
            // foreground load surges after it started.
            await analyzer.AnalyzeBackgroundAsync(
                db, table, analyzedAt,
                stillOwner: AmILeaderAsync,
                shouldPause: LoadExceeded,
                cancellationToken: ct).ConfigureAwait(false);

            onSuccess();

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Auto-analyzed table {Table} in database {Db}", table.Name, db.Name);
        }
        catch (OperationCanceledException)
        {
            // Shutdown, load-cancel, or leadership loss: nothing persisted, table stays stale, retried.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Auto-analyze of table {Table} in database {Db} failed", table.Name, db.Name);
        }
        finally
        {
            inFlight.TryRemove(claim, out _);
            slots.Release();
        }
    }

    private static string ClaimKey(DatabaseDescriptor db, TableDescriptor table) =>
        string.Concat(db.Id, ":", table.Id);

    // In-place Fisher–Yates using a seeded SplitMix64 stream (avoids the process-shared RNG and keeps
    // the shuffle reproducible for a given sweep timestamp).
    private static void Shuffle<T>(IList<T> items, ulong seed)
    {
        ulong state = seed == 0 ? 0x9E3779B97F4A7C15UL : seed;

        for (int i = items.Count - 1; i > 0; i--)
        {
            state += 0x9E3779B97F4A7C15UL;
            ulong z = state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            z ^= z >> 31;

            int j = (int)(z % (ulong)(i + 1));
            (items[i], items[j]) = (items[j], items[i]);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);
        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* loop swallows its own errors */ }
        }
        cts.Dispose();
    }
}
