
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Ttl;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Maintenance;

/// <summary>
/// Owns the engine's long-running background loops — the branch snapshot-floor renewer, the orphan
/// reclaimer, the auto-analyze scheduler, and row-level TTL — together with the one thing that is
/// easy to get wrong about them: <b>the order they are started in</b>.
///
/// <para>They are grouped rather than started independently because that order is load-bearing.
/// Every loop here issues read-write KV transactions, so all of them must wait for the node to
/// finish electing partition leaders; and startup recovery must run to <em>completion</em> before
/// the reclaimer's first sweep, or the sweep races the stale-marker cleanup it depends on. Started
/// from the executor's constructor, which a hosted service can trigger before the host calls
/// <c>StartAsync</c> — hence the wait rather than an assumption that the node is ready.</para>
///
/// <para>Every loop is leader-elected on the registry partition, so exactly one node in a cluster
/// runs each sweep. The TTL <em>span</em> work is the exception: planning is elected, but the spans
/// themselves run on every node.</para>
/// </summary>
internal sealed class BackgroundSchedulerHost : IAsyncDisposable
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    private readonly TableAnalyzer tableAnalyzer;

    private readonly DatabaseDropper databaseDropper;

    private readonly RowDeleter rowDeleter;

    private readonly StartupRecoveryService recovery;

    private readonly MetadataDiscoveryService discovery;

    /// <summary>
    /// In-flight foreground transaction count, so the analyze and TTL sweeps can back off under load.
    /// A callback rather than a value because the host wires its probe onto the executor after this
    /// object is constructed; resolving it eagerly would freeze it at "no load forever".
    /// </summary>
    private readonly Func<int> foregroundLoad;

    /// <summary>
    /// The deferred start, held so every caller that needs a scheduler can await the same one.
    /// Null until <see cref="Start"/> runs, which happens only when a shared node exists.
    /// </summary>
    private Task? startTask;

    private SnapshotHoldRenewer? snapshotHoldRenewer;

    private OrphanReclaimer? orphanReclaimer;

    private AutoAnalyzeScheduler? autoAnalyzeScheduler;

    private TtlScheduler? ttlScheduler;

    private TtlSpanSweeper? ttlSweeper;

    private TtlSpanCoordinator? ttlCoordinator;

    internal BackgroundSchedulerHost(
        ExecutorContext context,
        CamusDBOptions options,
        TableAnalyzer tableAnalyzer,
        DatabaseDropper databaseDropper,
        RowDeleter rowDeleter,
        StartupRecoveryService recovery,
        MetadataDiscoveryService discovery,
        Func<int> foregroundLoad
    )
    {
        this.context = context;
        this.options = options;
        this.tableAnalyzer = tableAnalyzer;
        this.databaseDropper = databaseDropper;
        this.rowDeleter = rowDeleter;
        this.recovery = recovery;
        this.discovery = discovery;
        this.foregroundLoad = foregroundLoad;
    }

    /// <summary>
    /// Awaits the deferred start, so a caller observes the schedulers (or the start's fault) rather
    /// than a null field. Completes immediately when nothing was started.
    /// </summary>
    private Task WhenStartedAsync() => startTask ?? Task.CompletedTask;

    /// <summary>
    /// Kicks off the deferred start. Called from the executor's constructor, so it must not block:
    /// the returned work waits on node readiness, which is not available yet at construction time.
    /// </summary>
    internal void Start(CommandExecutor executor, EmbeddedKahuna node) => startTask = StartAsync(executor, node);

    /// <summary>
    /// Waits for node readiness, runs startup recovery to completion, then starts each loop.
    /// <paramref name="executor"/> is needed only for the abandoned-refresh takeover, which restarts a
    /// full statement pipeline.
    /// </summary>
    private async Task StartAsync(CommandExecutor executor, EmbeddedKahuna node)
    {
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);

        // The renewer and the orphan scrub both issue read-write KV transactions. This method is
        // kicked off from the constructor, which a hosted service can trigger before Program.cs calls
        // StartAsync, so a transaction routed to a not-yet-created partition would throw "Invalid
        // partition". Wait until the node has elected leaders for every partition first. (The owned
        // registry path is already gated inside DatabaseRegistry.OpenAsync; this also covers the
        // pre-created-registry path, where registryTask completes without that gate.)
        await node.WaitUntilStartedAsync().ConfigureAwait(false);

        SnapshotHoldRenewer renewer = new(node, registry, context.Logger, options.BranchSnapshotHoldLeaseMs);
        renewer.Start();
        snapshotHoldRenewer = renewer;

        // Run startup recovery to COMPLETION before the reclaimer starts. The scrubber clears this
        // node's prior-run stale drop-intent markers (epoch-scoped, so it can never touch a marker this
        // run created) and resumes prior-run interrupted keyspace purges under a freshly-reacquired
        // fence. Doing it first means the reclaimer's loop/immediate-sweep never races the stale-marker
        // cleanup. Errors are logged and swallowed; recovery is advisory and must not block startup.
        await recovery.ScrubOrphanBranchNamespacesAsync(node, registry).ConfigureAwait(false);

        // Physically reclaim deferred-dropped databases/tables past their retention window, on the
        // elected node. Kick one immediate sweep so orphans already expired during downtime are cleaned
        // promptly rather than waiting a full interval.
        OrphanReclaimer reclaimer = new(node, registry, databaseDropper, context.Logger, options)
        {
            // Wired here because reclaiming staging storage needs the database opener and the
            // refresher, neither of which the reclaimer should own.
            ReclaimRefreshJobsForDatabaseAsync = (databaseName, ct) =>
                recovery.ReclaimAbandonedRefreshesAsync(executor, databaseName, ct),
        };
        reclaimer.Start();
        orphanReclaimer = reclaimer;
        _ = recovery.ReclaimExpiredOrphansOnStartupAsync(reclaimer);

        // Keep optimizer statistics fresh in the background. Leader-elected on the same registry key
        // so exactly one node analyzes each table; while AutoAnalyzeEnabled is off the loop idles on a
        // short probe so a runtime enable starts sweeping without a restart.
        AutoAnalyzeScheduler analyzeScheduler = new(
            node,
            registry.RegistryBucket,
            tableAnalyzer,
            discovery.DiscoverStaleTablesAsync,
            foregroundLoad,
            context.Logger,
            options
        );
        analyzeScheduler.Start();
        autoAnalyzeScheduler = analyzeScheduler;

        // Row-level TTL. Planning is elected on the same registry key so exactly one node mints a run
        // per table; the span work itself runs on every node. While TtlEnabled is off each tick no-ops,
        // so a runtime enable starts collection at the next tick without a restart.
        TtlSpanCoordinator ttlCoordinatorInstance = new(
            node.Kahuna,
            $"ttl:{node.Raft.GetLocalNodeId()}",
            options.TtlSpanLeaseMs,
            options.TtlSpanLeaseRenewIntervalMs
        );

        TtlSpanSweeper ttlSweeperInstance = new(
            ttlCoordinatorInstance,
            rowDeleter,
            foregroundLoad,
            options,
            context.Logger
        );

        TtlScheduler scheduler = new(
            node,
            registry.RegistryBucket,
            ttlCoordinatorInstance,
            ttlSweeperInstance,
            discovery.DiscoverTtlTablesAsync,
            discovery.DiscoverRegisteredDatabasesAsync,
            context.Logger,
            options
        );

        scheduler.Start();
        ttlScheduler = scheduler;
        ttlSweeper = ttlSweeperInstance;
        ttlCoordinator = ttlCoordinatorInstance;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. The schedulers are constructed
    /// asynchronously after the registry opens, so they may not exist yet; a swap published before
    /// they start is picked up at construction, because they are built from this host's already-swapped
    /// options field.
    ///
    /// <para>The TTL span coordinator is deliberately skipped: its span-lease duration and renew
    /// cadence are what keeps two nodes from working one span concurrently, and changing lease timing
    /// under an active span would let a claim lapse (or outlive its renewal) mid-sweep — those two
    /// values stay fixed for the coordinator's lifetime.</para>
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        options = next;

        orphanReclaimer?.ApplyOptions(next);
        autoAnalyzeScheduler?.ApplyOptions(next);
        ttlScheduler?.ApplyOptions(next);
        ttlSweeper?.ApplyOptions(next);
    }

    /// <summary>Row-level TTL counters for <c>SHOW ENGINE STATS</c>; null when TTL is not running here.</summary>
    internal IReadOnlyList<EngineMetricRow>? TtlMetricRows() => TtlMetricsReporter.Build(ttlScheduler);

    /// <summary>
    /// Forces one auto-analyze sweep and returns the number of tables analyzed. Waits out the deferred
    /// start first, so a caller cannot observe zero simply because the scheduler does not exist yet.
    /// </summary>
    internal async Task<int> RunAutoAnalyzeSweepAsync()
    {
        await WhenStartedAsync().ConfigureAwait(false);
        return autoAnalyzeScheduler is null ? 0 : await autoAnalyzeScheduler.RunSweepAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Forces one row-level TTL sweep and returns the number of rows deleted.</summary>
    internal async Task<long> RunTtlSweepAsync()
    {
        await WhenStartedAsync().ConfigureAwait(false);
        return ttlScheduler is null ? 0 : await ttlScheduler.RunSweepAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>The table ids that currently have TTL run metadata in a database.</summary>
    internal async Task<IReadOnlyList<string>> ListTtlRunTableIdsAsync(string dbId)
    {
        await WhenStartedAsync().ConfigureAwait(false);

        return ttlCoordinator is null
            ? []
            : await ttlCoordinator.ListRunTableIdsAsync(dbId, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Cumulative TTL counters for this node.</summary>
    internal (long expired, long skipped, long failed, long spans, long runs) TtlCounters()
        => ttlScheduler is null
            ? (0, 0, 0, 0, 0)
            : (ttlScheduler.RowsExpired, ttlScheduler.RowsSkippedByRecheck, ttlScheduler.RowsFailed,
               ttlScheduler.SpansCompleted, ttlScheduler.RunsPlanned);

    /// <summary>The most TTL spans this node has had in flight at once.</summary>
    internal int TtlPeakConcurrentSpans() => ttlScheduler?.PeakConcurrentSpans ?? 0;

    /// <summary>
    /// Fails the TTL delete for any chunk the predicate selects, so a test can observe what the
    /// checkpoint does when a delete does not commit. See <c>TtlSpanSweeper.DeleteChunkFaultInjector</c>
    /// for why this cannot be provoked any other way.
    /// </summary>
    internal async Task SetTtlDeleteFaultInjectorAsync(Func<IReadOnlyList<ObjectIdValue>, bool>? injector)
    {
        await WhenStartedAsync().ConfigureAwait(false);

        if (ttlSweeper is not null)
            ttlSweeper.DeleteChunkFaultInjector = injector;
    }

    /// <summary>
    /// Forces one orphan-reclamation sweep and returns the number of orphans reclaimed.
    /// </summary>
    /// <remarks>
    /// <para><b>Waits for the node to finish starting before sweeping.</b> The sweep is gated on this
    /// node leading the registry partition, and that check answers <c>false</c> <em>immediately</em>
    /// — without its usual wait for an election — while the Raft manager is still initializing. The
    /// sweep then returns 0, which a caller cannot tell apart from "nothing was due".</para>
    /// </remarks>
    internal async Task<int> RunOrphanReclaimAsync()
    {
        await WhenStartedAsync().ConfigureAwait(false);

        if (orphanReclaimer is null)
            return 0;

        if (context.SharedNode is not null)
            await context.SharedNode.WaitUntilStartedAsync().ConfigureAwait(false);

        return await orphanReclaimer.ReclaimDueAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops every loop. Awaits the deferred start first so a scheduler created by an in-flight start
    /// is observed (or its start fault surfaces) before disposal, rather than being left running.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        try { await WhenStartedAsync().ConfigureAwait(false); }
        catch { /* a failed start left nothing to dispose */ }

        if (snapshotHoldRenewer is not null)
            await snapshotHoldRenewer.DisposeAsync().ConfigureAwait(false);

        if (orphanReclaimer is not null)
            await orphanReclaimer.DisposeAsync().ConfigureAwait(false);

        if (autoAnalyzeScheduler is not null)
            await autoAnalyzeScheduler.DisposeAsync().ConfigureAwait(false);

        if (ttlScheduler is not null)
            await ttlScheduler.DisposeAsync().ConfigureAwait(false);
    }
}
