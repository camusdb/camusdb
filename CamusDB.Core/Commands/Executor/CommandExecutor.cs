
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Auth;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Statistics;
using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Facade for executing commands on the database and tables
/// </summary>
public sealed class CommandExecutor : IAsyncDisposable
{
    private readonly ILogger<ICamusDB> logger;

    private readonly CatalogsManager catalogs;

    private readonly DatabaseOpener databaseOpener;

    private readonly DatabaseCreator databaseCreator;

    private readonly DatabaseCloser databaseCloser;

    private readonly DatabaseDropper databaseDroper;

    private readonly DatabaseDescriptors databaseDescriptors;

    // Process-level Kahuna node shared across all databases. Used to route snapshot-floor hold
    // release/renew calls, which auto-forward to the system-partition leader from any node.
    private readonly EmbeddedKahuna? sharedNode;

    // Leader-owned loop that keeps every branch's snapshot-floor hold alive while it exists.
    // Started once the registry is ready; disposed on teardown. Null when no shared node is present.
    private readonly Task? snapshotRenewerStart;
    private SnapshotHoldRenewer? snapshotHoldRenewer;

    // Leader-owned loop that physically reclaims deferred-dropped databases/tables past their retention
    // window. Started alongside the renewer; disposed on teardown.
    private OrphanReclaimer? orphanReclaimer;

    // Leader-owned loop that keeps optimizer statistics fresh via throttled, lock-free background
    // ANALYZE. Started alongside the renewer; disposed on teardown.
    private AutoAnalyzeScheduler? autoAnalyzeScheduler;

    /// <summary>
    /// Optional probe returning the number of in-flight foreground transactions, wired by the host so
    /// the auto-analyze scheduler can back off under load. Null in contexts (tests, standalone) that
    /// don't track foreground load — treated as zero load.
    /// </summary>
    public Func<int>? ForegroundLoadProbe { get; set; }

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableConstraintAlterer tableConstraintAlterer;

    private readonly CommentSetter commentSetter = new();

    private readonly TableDropper tableDropper;

    private readonly RowInserter rowInserter;

    private readonly RowUpdater rowUpdater;

    private readonly RowDeleter rowDeleter;

    private readonly StatisticsManager statisticsManager;

    public StatisticsManager Statistics => statisticsManager;

    internal PlanCache PlanCache => queryExecutor.PlanCache;

    private readonly QueryExecutor queryExecutor;

    private readonly SqlExecutor sqlExecutor;

    private readonly SchemaQuerier schemaQuerier;

    private readonly QueryBinder queryBinder;

    private readonly SubqueryRewriter subqueryRewriter;

    private readonly ExistsSubqueryPreparer existsSubqueryPreparer;

    private readonly ExplainExecutor explainExecutor;

    private readonly TableAnalyzer tableAnalyzer;

    private readonly SelectQueryCreator selectQueryCreator = new();

    private readonly CommandValidator validator;

    private readonly SemiJoinAnalyzer semiJoinAnalyzer;

    private readonly ISchemaDdlForwarder? schemaDdlForwarder;

    private readonly SQLParser.SqlParserCache sqlParserCache;

    // Number of rows indexed per Kahuna transaction during backfill.  Committing in bounded
    // batches keeps transaction size manageable and allows a leader-change resume to skip
    // already-indexed rows via the persisted StartOffset checkpoint.
    private const int BackfillBatchSize = 500;

    /// <summary>
    /// Initializes the commands executor
    /// </summary>
    private readonly Task<DatabaseRegistry> registryTask;

    // Server-level user/grant catalog, opened against the shared node like the database registry. Null
    // only when this executor was constructed without a shared node (no auth surface is reachable).
    private readonly Task<AuthCatalog>? authCatalogTask;

    // Authentication orchestration (login/token/principal). Non-null exactly when authCatalogTask is.
    private readonly AuthService? authService;
    private readonly bool ownsRegistry;
    private readonly bool isClusterMode;

    /// <param name="sharedNode">Process-level Kahuna node shared across all databases; non-null in both standalone and cluster modes.</param>
    /// <param name="schemaDdlForwarder">DDL forwarder for cluster mode; null in standalone.</param>
    /// <param name="registry">Optional pre-created registry; if supplied the executor does not own it and will not dispose it.</param>
    /// <param name="isClusterMode">True when this process is a Raft cluster node; false for standalone.</param>
    /// <param name="cache">Optional query result cache. When non-null, DML commits drive the publish-gate invalidation
    /// protocol and DDL operations call <see cref="IQueryResultCache.InvalidateByTableId"/> after each successful commit.</param>
    public CommandExecutor(
        CommandValidator validator,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        EmbeddedKahuna? sharedNode = null,
        ISchemaDdlForwarder? schemaDdlForwarder = null,
        DatabaseRegistry? registry = null,
        bool isClusterMode = false,
        IQueryResultCache? cache = null)
    {
        this.validator = validator;
        this.catalogs = catalogs;
        this.logger = logger;
        this.schemaDdlForwarder = schemaDdlForwarder;
        this.isClusterMode = isClusterMode;
        this.sharedNode = sharedNode;

        if (registry is not null)
        {
            registryTask = Task.FromResult(registry);
            ownsRegistry = false;
        }
        else
        {
            registryTask = DatabaseRegistry.OpenAsync(sharedNode!, isClusterMode);
            ownsRegistry = true;
        }

        // The auth catalog rides the same shared node and _system/ keyspace as the registry.
        if (sharedNode is not null)
        {
            authCatalogTask = AuthCatalog.OpenAsync(sharedNode, isClusterMode);
            authService = new AuthService(authCatalogTask);
        }

        databaseDescriptors = new();
        databaseOpener = new(this, databaseDescriptors, catalogs, logger, sharedNode, registryTask, isClusterMode, cache);
        databaseCloser = new(databaseDescriptors, logger);
        databaseDroper = new(databaseDescriptors, logger);
        databaseCreator = new(logger);
        tableOpener = new(catalogs, logger);
        tableCreator = new(catalogs, logger);
        tableColumnAlterer = new(catalogs, logger);
        tableIndexAlterer = new(catalogs, logger);
        tableConstraintAlterer = new(logger);
        tableDropper = new(catalogs, logger);
        rowInserter = new(logger);
        rowUpdater = new(logger);
        statisticsManager = new(logger);
        rowDeleter = new(logger, statisticsManager);
        queryExecutor = new(logger, statisticsManager, sharedNode?.Kahuna);
        sqlExecutor = new();
        schemaQuerier = new(catalogs, logger);
        queryBinder = new QueryBinder(tableOpener);
        SubqueryQueryExecutor subqueryQueryExecutor = new(queryBinder, queryExecutor);
        ExistsSubqueryExecutor existsSubqueryExecutor = new(subqueryQueryExecutor);
        subqueryRewriter = new SubqueryRewriter(
            new ScalarSubqueryExecutor(subqueryQueryExecutor),
            new InSubqueryExecutor(subqueryQueryExecutor, statisticsManager),
            existsSubqueryExecutor);
        existsSubqueryPreparer = new ExistsSubqueryPreparer(existsSubqueryExecutor, queryBinder);
        semiJoinAnalyzer = new SemiJoinAnalyzer(tableOpener);
        explainExecutor = new ExplainExecutor(subqueryRewriter, queryBinder, existsSubqueryPreparer, queryExecutor, statisticsManager, semiJoinAnalyzer);
        tableAnalyzer = new TableAnalyzer(statisticsManager);

        sqlParserCache = new SQLParser.SqlParserCache(
            logger,
            CamusDBConfig.SqlParserCacheTtlSeconds,
            CamusDBConfig.SqlParserCacheMaxEntries,
            CamusDBConfig.SqlParserCacheSweepSeconds);

        // Keep every branch's snapshot-floor hold alive for as long as the branch exists. The
        // registry is opened asynchronously, so defer the start until it is ready; the renewer
        // itself elects a single sweeping node by registry-partition leadership.
        if (sharedNode is not null)
            snapshotRenewerStart = StartSnapshotHoldRenewerAsync(sharedNode);
    }

    private async Task StartSnapshotHoldRenewerAsync(EmbeddedKahuna node)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // The renewer and the orphan scrub both issue read-write KV transactions. This method is
        // kicked off from the constructor, which a hosted service can trigger before Program.cs calls
        // StartAsync, so a transaction routed to a not-yet-created partition would throw "Invalid
        // partition". Wait until the node has elected leaders for every partition first. (The owned
        // registry path is already gated inside DatabaseRegistry.OpenAsync; this also covers the
        // pre-created-registry path, where registryTask completes without that gate.)
        await node.WaitUntilStartedAsync().ConfigureAwait(false);

        SnapshotHoldRenewer renewer = new(node, registry, logger, CamusDBConfig.BranchSnapshotHoldLeaseMs);
        renewer.Start();
        snapshotHoldRenewer = renewer;

        // Run startup recovery to COMPLETION before the reclaimer starts. The scrubber clears this
        // node's prior-run stale drop-intent markers (epoch-scoped, so it can never touch a marker this
        // run created) and resumes prior-run interrupted keyspace purges under a freshly-reacquired
        // fence. Doing it first means the reclaimer's loop/immediate-sweep never races the stale-marker
        // cleanup. Errors are logged and swallowed; recovery is advisory and must not block startup.
        await ScrubOrphanBranchNamespacesAsync(node, registry).ConfigureAwait(false);

        // Physically reclaim deferred-dropped databases/tables past their retention window, on the
        // elected node. Kick one immediate sweep so orphans already expired during downtime are cleaned
        // promptly rather than waiting a full interval.
        OrphanReclaimer reclaimer = new(node, registry, databaseDroper, logger, CamusDBConfig.OrphanReclaimIntervalMs);
        reclaimer.Start();
        orphanReclaimer = reclaimer;
        _ = ReclaimExpiredOrphansOnStartupAsync(reclaimer);

        // Keep optimizer statistics fresh in the background. Leader-elected on the same registry key
        // so exactly one node analyzes each table; disabled entirely unless AutoAnalyzeEnabled is set.
        AutoAnalyzeScheduler analyzeScheduler = new(
            node,
            registry.RegistryBucket,
            tableAnalyzer,
            DiscoverStaleTablesAsync,
            () => ForegroundLoadProbe?.Invoke() ?? 0,
            logger,
            CamusDBConfig.AutoAnalyzeCheckIntervalMs);
        analyzeScheduler.Start();
        autoAnalyzeScheduler = analyzeScheduler;
    }

    /// <summary>
    /// Cluster-visible candidate discovery for auto-analyze. Runs on the elected leader and enumerates
    /// <em>authoritative</em> metadata — every database in the registry and every table's per-object
    /// meta key — rather than this node's open-object list, so a hot table opened and mutated only on a
    /// follower is still found. For each table it loads the persisted staleness state (which another
    /// node's flush writes), and opens the descriptor only for tables that are actually stale.
    /// </summary>
    private async Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>> DiscoverStaleTablesAsync(CancellationToken ct)
    {
        var result = new List<(DatabaseDescriptor, TableDescriptor)>();
        if (sharedNode is null)
            return result;

        double fraction = CamusDBConfig.AutoAnalyzeFractionStaleRows;
        long minRows = CamusDBConfig.AutoAnalyzeMinStaleRows;

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        IReadOnlyList<DatabaseRegistryEntry> entries = await registry.ScanAllEntriesAsync().ConfigureAwait(false);

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (ct.IsCancellationRequested)
                break;

            DatabaseDescriptor database;
            try
            {
                database = await databaseOpener.Open(entry.Name).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Auto-analyze discovery: could not open database {Db}", entry.Name);
                continue;
            }

            foreach ((string tableId, string tableName) in await ScanTableMetaAsync(entry.Id, ct).ConfigureAwait(false))
            {
                if (ct.IsCancellationRequested)
                    break;

                // Load persisted staleness so the decision reads cluster-wide state (a follower's
                // flushed mutation count), not this node's possibly-absent cache entry.
                await statisticsManager.LoadByIdAsync(database, tableId).ConfigureAwait(false);
                if (!statisticsManager.IsStale(database, tableId, fraction, minRows))
                    continue;

                try
                {
                    TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);
                    result.Add((database, table));
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Auto-analyze discovery: could not open table {Table} in database {Db}", tableName, entry.Name);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Enumerates a database's live tables by scanning its per-object meta keys
    /// (<c>{dbId}/meta/table:{tableId}</c>) and returns each table's id and current name. Reads the
    /// authoritative KV catalog directly, so it sees tables this node has never opened.
    /// </summary>
    private async Task<List<(string tableId, string tableName)>> ScanTableMetaAsync(string dbId, CancellationToken ct)
    {
        string metaBucket = $"{dbId}/meta";
        string tablePrefix = $"{dbId}/meta/table:";
        var tables = new List<(string, string)>();

        await foreach ((string key, Kahuna.Server.KeyValues.ReadOnlyKeyValueEntry kvEntry) in sharedNode!.Kahuna.LocateAndScanRange(
            Kommander.Time.HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            Kommander.Time.HLCTimestamp.Zero, Kahuna.Shared.KeyValue.KeyValueDurability.Persistent, ct).ConfigureAwait(false))
        {
            if (!key.StartsWith(tablePrefix, StringComparison.Ordinal) || kvEntry.Value is null)
                continue;

            CamusDB.Core.Catalogs.Models.TableSchema schema = CamusDB.Core.Catalogs.MetaJsonSerializer.Deserialize(
                kvEntry.Value, CamusDB.Core.Catalogs.MetaJsonContext.Default.TableSchema);

            // Honor the per-table opt-out (ALTER TABLE ... SET (sql_stats_automatic_collection_enabled
            // = false)) straight from the authoritative meta blob, so a disabled table costs discovery
            // nothing — it is never opened or stats-loaded.
            if (schema.Id is not null && schema.Name is not null && schema.AutoStatsCollectionEnabled)
                tables.Add((schema.Id, schema.Name));
        }

        return tables;
    }

    /// <summary>
    /// Test-only seam: forces one auto-analyze sweep (after the deferred renewer start completes) and
    /// returns the number of tables analyzed, so a test can drive it deterministically instead of
    /// waiting for the timer. Requires <see cref="CamusDBConfig.AutoAnalyzeEnabled"/> to be set.
    /// </summary>
    internal async Task<int> RunAutoAnalyzeForTestsAsync()
    {
        if (snapshotRenewerStart is not null)
            await snapshotRenewerStart.ConfigureAwait(false);
        return autoAnalyzeScheduler is null ? 0 : await autoAnalyzeScheduler.RunSweepAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Test-only seam: runs the throttled background analyzer directly against one table, with an
    /// optional load-pause callback, so a test can drive the mid-scan cancel path without going through
    /// leader election or the sweep's pre-dispatch load gate.
    /// </summary>
    internal Task RunBackgroundAnalyzeForTestsAsync(
        DatabaseDescriptor database, TableDescriptor table, Func<bool>? shouldPause, CancellationToken cancellationToken)
        => tableAnalyzer.AnalyzeBackgroundAsync(
            database, table, default, stillOwner: null, shouldPause: shouldPause, cancellationToken: cancellationToken);

    /// <summary>
    /// Test-only seam: runs the background analyzer against a table with the <b>real</b> registry
    /// leadership ownership check wired in (the same one the scheduler uses), opening the database and
    /// table on this node first. Lets a cluster test start an analyze on the current owner, revoke its
    /// leadership mid-scan, and observe that it aborts without publishing.
    /// </summary>
    internal async Task RunBackgroundAnalyzeWithOwnershipForTestsAsync(
        string databaseName, string tableName, CancellationToken cancellationToken)
    {
        DatabaseDescriptor database = await databaseOpener.Open(databaseName).ConfigureAwait(false);
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        await tableAnalyzer.AnalyzeBackgroundAsync(
            database, table, default,
            stillOwner: c => sharedNode!.AmILeaderForKeyAsync(registry.RegistryBucket, c),
            shouldPause: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Test-only seam: voluntarily steps this node down from leadership of the registry-bucket
    /// partition (the key that gates auto-analyze ownership), so an in-flight owned analyze observes
    /// the loss and aborts. The node stays online as a follower, so snapshot reads keep working.
    /// </summary>
    internal async Task StepDownAutoAnalyzeLeadershipForTestsAsync()
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        await sharedNode!.StepDownForKeyAsync(registry.RegistryBucket).ConfigureAwait(false);
    }

    /// <summary>
    /// Test-only seam: forces one orphan-reclamation sweep after the deferred renewer/reclaimer start
    /// completes, and returns the number of orphans reclaimed. Lets a test drive the GC deterministically
    /// (with a tiny <see cref="CamusDBConfig.OrphanRetentionMs"/>) instead of waiting for the timer.
    /// </summary>
    internal async Task<int> RunOrphanReclaimForTestsAsync()
    {
        if (snapshotRenewerStart is not null)
            await snapshotRenewerStart.ConfigureAwait(false);
        return orphanReclaimer is null ? 0 : await orphanReclaimer.ReclaimDueAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one orphan-reclamation sweep at startup so databases/tables whose retention window elapsed
    /// while the node was down (and any purge interrupted by a crash) are reclaimed promptly instead of
    /// waiting a full <see cref="CamusDBConfig.OrphanReclaimIntervalMs"/>. Best-effort and gated by
    /// leader election inside the reclaimer; failures are logged and swallowed so startup never blocks.
    /// </summary>
    private async Task ReclaimExpiredOrphansOnStartupAsync(OrphanReclaimer reclaimer)
    {
        try
        {
            await reclaimer.ReclaimDueAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Startup orphan reclamation sweep failed");
        }
    }

    /// <summary>
    /// Purges the <c>{branchId}/meta/…</c> namespace written by <c>CopyMetaForBranchAsync</c>
    /// when a branch-creation attempt is abandoned — either by a process crash (startup scrubber
    /// path) or by an in-process abort after the copy but before <c>RegisterAsync</c> commits.
    /// Uses a 3-round retry to absorb transient scan misses; each key is deleted idempotently.
    /// </summary>
    private async Task PurgeBranchMetaNamespaceAsync(string branchId, IKahuna kahuna)
    {
        string metaBucket = $"{branchId}/meta";
        string metaPrefix = $"{branchId}/";

        for (int round = 0; round < 3; round++)
        {
            List<string> keys = [];
            await foreach ((string key, Kahuna.Server.KeyValues.ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith(metaPrefix, StringComparison.Ordinal))
                    keys.Add(key);
            }

            if (keys.Count == 0)
                break;

            foreach (string key in keys)
            {
                try
                {
                    await kahuna.LocateAndTryDeleteKeyValue(
                        HLCTimestamp.Zero, key,
                        KeyValueDurability.Persistent, CancellationToken.None
                    ).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to delete orphan key '{Key}' for branch id {BranchId}", key, branchId);
                }
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Purged {Count} meta key(s) for branch id {BranchId}", keys.Count, branchId);
        }
    }

    /// <summary>
    /// Purges KV namespace entries for branch ids that appear in the pending-create set but
    /// are not registered in the database registry. These orphans are left when a process crash
    /// occurs after metadata is written but before the registry entry is published during
    /// <c>CreateBranchDatabaseAsync</c>.
    ///
    /// <para>Safe to call concurrently with normal operations: orphan ids are not registered,
    /// so no live transaction or table-open path can reference them. The purge is idempotent.</para>
    ///
    /// <para>Also clears <em>this node's own</em> stale drop-intent markers left by a crash during
    /// <c>DropDatabase</c>. A node's own drop-intent can never legitimately survive its restart
    /// (drops do not span restarts), so any own-owned marker at startup is a crash remnant; without
    /// this cleanup the affected database would be permanently undroppable because
    /// <c>AcquireDropIntentAsync</c> uses <c>SetIfNotExists</c> and would always find the key present.
    /// The cleanup is owner-scoped so a restarting node never deletes a drop-intent another live node
    /// currently holds for an in-flight drop (which would reopen the cross-node drop/create race).</para>
    ///
    /// <para>Internal visibility is intentional: tests invoke this directly to verify the
    /// production scrub path rather than reimplementing the same logic inline.</para>
    /// </summary>
    internal async Task ScrubOrphanBranchNamespacesAsync(EmbeddedKahuna node, DatabaseRegistry registry)
    {
        try
        {
            // Clear this node's own stale drop-intent markers first so that a marker left by a crash
            // does not permanently block future drops. Owner-scoped: a restarting node must not delete
            // a drop-intent another live node currently holds for an in-flight drop.
            int intentsCleared = await registry.ClearOwnStaleDropIntentsAsync().ConfigureAwait(false);
            if (intentsCleared > 0 && logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Cleared {Count} stale drop-intent marker(s) on startup", intentsCleared);

            // Resume any DROP DATABASE this node started but did not finish before a crash (prior-epoch
            // dropping markers only — LoadOwnDroppingIdsAsync excludes this run's live markers). The
            // keyspace purge is per-key and non-transactional, so an interrupted drop can leave orphaned
            // row/index/stats data with no other reclaim. Each resume takes the id's fence and rechecks
            // AUTHORITATIVE registry state under it (not the local cache) so a concurrent relink/GC of the
            // same id can never interleave with the resumed purge.
            foreach (string droppingId in await registry.LoadOwnDroppingIdsAsync().ConfigureAwait(false))
            {
                try
                {
                    // Authoritative: if the id is still registered (live), the crash preceded
                    // UnregisterAsync — nothing was purged — so just clear the stale marker.
                    if (await registry.TryResolveNameByIdAsync(droppingId).ConfigureAwait(false) is not null)
                    {
                        await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                        continue;
                    }

                    // Fence the id for the resumed purge. If we cannot take it, another operation holds
                    // it (a relink/GC on this or another node); leave the marker for a later resume.
                    if (!await registry.AcquireDropIntentAsync(droppingId).ConfigureAwait(false))
                        continue;

                    try
                    {
                        // Re-check liveness under the fence before destroying anything.
                        if (await registry.TryResolveNameByIdAsync(droppingId).ConfigureAwait(false) is not null)
                        {
                            await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                            continue;
                        }

                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("Resuming interrupted DROP DATABASE keyspace purge for id {DbId} on startup", droppingId);

                        // Clear the marker only if the resumed purge verifiably completed; otherwise
                        // leave it so the NEXT startup resumes again (never abandon leaked keys).
                        if (await databaseDroper.PurgeKeyspaceByIdAsync(node.Kahuna, droppingId, null).ConfigureAwait(false))
                            await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                        else
                            logger.LogWarning("Resumed purge for id {DbId} is still incomplete; leaving marker for the next startup", droppingId);
                    }
                    finally
                    {
                        await registry.ReleaseDropIntentAsync(droppingId).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to resume interrupted drop for id {DbId}", droppingId);
                }
            }

            List<string> orphanIds = await registry.LoadOrphanBranchIdsAsync().ConfigureAwait(false);
            if (orphanIds.Count == 0)
                return;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Found {Count} orphan branch namespace(s) to scrub on startup", orphanIds.Count);

            IKahuna kahuna = node.Kahuna;
            foreach (string orphanId in orphanIds)
            {
                try
                {
                    await PurgeBranchMetaNamespaceAsync(orphanId, kahuna).ConfigureAwait(false);
                    await registry.ClearPendingBranchAsync(orphanId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to scrub orphan branch namespace for id {BranchId}", orphanId);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Startup orphan-branch scrub failed");
        }
    }

    #region database

    public async Task<DatabaseDescriptor> CreateDatabase(CreateDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string name = ticket.DatabaseName;

        // Resolve the target through the persistent registry (cache-first, live-KV fallback), not the
        // local cache alone. In a cluster the name may be registered on another node but absent from
        // this node's cache; a cache-only check would let CREATE IF NOT EXISTS run the whole create/
        // branch flow (allocating an id, acquiring a snapshot hold, copying metadata) only to fail late
        // at RegisterAsync, instead of returning the existing database here.
        DatabaseRegistryEntry? existing = await registry.TryResolveEntryAsync(name).ConfigureAwait(false);
        if (existing is not null)
        {
            if (ticket.IfNotExists)
                return await databaseOpener.Open(name).ConfigureAwait(false);

            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseAlreadyExists,
                $"Database '{name}' already exists");
        }

        if (ticket.BranchFrom is not null)
            return await CreateBranchDatabaseAsync(ticket, registry, name).ConfigureAwait(false);

        string id = await registry.AllocateIdAsync().ConfigureAwait(false);

        await registry.RegisterAsync(name, id).ConfigureAwait(false);

        try
        {
            await databaseCreator.Create(name, id).ConfigureAwait(false);
            return await databaseOpener.Open(name).ConfigureAwait(false);
        }
        catch (Exception openEx)
        {
            // Roll back the registry entry so the name can be retried.
            try
            {
                await registry.UnregisterAsync(name).ConfigureAwait(false);
            }
            catch (Exception unregEx)
            {
                logger.LogError(unregEx,
                    "Failed to unregister database '{Name}' (id={Id}) after failed create — " +
                    "the name is now wedged: it appears registered but cannot be opened",
                    name, id);
            }

            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(openEx).Throw();
            throw; // unreachable
        }
    }

    /// <summary>
    /// Creates a branch database from <paramref name="ticket"/>.<see cref="CreateDatabaseTicket.BranchFrom"/>.
    /// The source must be schema-stable — <c>HeadSchemaVersion == SchemaVersion</c>, all columns and
    /// indexes in the Public state, and no persisted coordinator jobs — before the branch is taken.
    /// Schema metadata is copied O(schema-size) to the branch namespace under the source's
    /// <c>SchemaDdlSemaphore</c> to keep the copy point consistent.  The registry entry is published
    /// last ("metadata first, registry last") so an orphaned namespace from a crash between the two
    /// steps is cleaned up by the startup orphan-namespace scrubber rather than being served to clients.
    /// </summary>
    private async Task<DatabaseDescriptor> CreateBranchDatabaseAsync(
        CreateDatabaseTicket ticket, DatabaseRegistry registry, string branchName)
    {
        DatabaseDescriptor sourceDescriptor = await databaseOpener.Open(ticket.BranchFrom!).ConfigureAwait(false);

        await sourceDescriptor.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Re-validate the source is still registered under the semaphore. DropDatabase acquires
            // the same semaphore before unregistering, so if Drop won the race it will have already
            // removed the source from the registry by the time we get here.
            if (registry.Get(ticket.BranchFrom!) is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Source database '{ticket.BranchFrom}' was dropped concurrently; branch creation aborted");

            // Schema stability: no committed schema entry may be un-applied.
            if (sourceDescriptor.HeadSchemaVersion != sourceDescriptor.Schema.SchemaVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Database '{ticket.BranchFrom}' has in-flight schema changes; wait for them to complete before branching");

            // Schema stability: every column and index must be in the Public (terminal) state.
            // Non-Public elements indicate an online schema change is mid-sequence and the
            // schema metadata is not yet a consistent snapshot.
            foreach (TableSchema table in sourceDescriptor.Schema.Tables.Values)
            {
                if (table.Columns is not null)
                    foreach (TableColumnSchema col in table.Columns)
                        if (col.State != SchemaElementState.Public)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Column '{col.Name}' in table '{table.Name}' of database '{ticket.BranchFrom}' is in state '{col.State}'; wait for the schema change to reach Public before branching");

                if (table.Indexes is not null)
                    foreach (TableIndexSchema idx in table.Indexes)
                        if (idx.State != SchemaElementState.Public)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Index '{idx.Name}' in table '{table.Name}' of database '{ticket.BranchFrom}' is in state '{idx.State}'; wait for the schema change to reach Public before branching");
            }

            // Schema stability: no coordinator jobs may be in-flight.  A coordinator job
            // can resume after a leader change while element states momentarily read Public;
            // checking persisted jobs under SchemaDdlSemaphore fences that window.
            List<Catalogs.Models.PersistedCoordinatorJob> coordinatorJobs =
                await catalogs.LoadCoordinatorJobsAsync(sourceDescriptor).ConfigureAwait(false);
            if (coordinatorJobs.Count > 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Database '{ticket.BranchFrom}' has {coordinatorJobs.Count} pending coordinator job(s); wait for them to complete before branching");

            // Mint a fork timestamp by going through Kahuna's transaction pipeline rather than
            // reading the Raft node's local HLC directly. The LocateAndStartTransaction call
            // routes through the partition actor's message queue, which serializes with any
            // in-progress partition-actor HLC advances (e.g. from an ongoing TrySet's ReceiveEvent).
            // This is the causal fence that ensures forkT is strictly after every write that was
            // already committed before branch creation, even in a multi-partition deployment where
            // the Raft node's local HLC might lag a partition actor's HLC. The transaction is
            // rolled back immediately — we only needed the causal timestamp.
            KvTransaction forkTx = await sourceDescriptor.Transactions.BeginAsync().ConfigureAwait(false);
            HLCTimestamp forkT = forkTx.TransactionId;
            await sourceDescriptor.Transactions.RollbackIfNotCompletedAsync(forkTx).ConfigureAwait(false);

            string branchId = await registry.AllocateIdAsync().ConfigureAwait(false);

            // Pin the immediate parent's MVCC history at forkT with a Kahuna snapshot-floor hold so
            // the branch's as-of-forkT reads stay correct even under aggressive revision reclamation.
            // The holder id is the branch's own stable id (acquire is idempotent by (holder, forkT)).
            // A hold that is not confirmed granted means durability cannot be guaranteed — fail the
            // create rather than register a branch whose frozen view GC may later reclaim. The hold
            // is renewed while the branch lives (leader-owned renewer) and released on leaf drop.
            IKahuna sourceKahuna = sourceDescriptor.Kahuna.Kahuna;
            (KeyValueResponseType holdType, string holdId, _) = await sourceKahuna
                .LocateAndAcquireSnapshotHold(branchId, forkT, CamusDBConfig.BranchSnapshotHoldLeaseMs, CancellationToken.None)
                .ConfigureAwait(false);

            if (holdType != KeyValueResponseType.Set || string.IsNullOrEmpty(holdId))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Could not acquire a snapshot-floor hold on '{ticket.BranchFrom}' at the fork point (status {holdType}); branch not created because its frozen view could not be guaranteed durable");

            bool metaCopied = false;
            bool childRegistered = false;
            bool leaveMarkerForScrubber = false;
            try
            {
                // Write the pending-create marker FIRST, inside this try block so the catch below
                // releases the snapshot hold if the write fails. The marker must be confirmed
                // durable before CopyMetaForBranchAsync runs: if the process crashes after the
                // metadata copy but before RegisterAsync commits, the startup scrubber finds this
                // id unregistered and purges the orphaned namespace. If the marker write itself
                // fails, we propagate the error and CopyMetaForBranchAsync is never called —
                // so no orphan can exist without a recovery handle (BR15).
                await registry.TrackPendingBranchAsync(branchId).ConfigureAwait(false);

                // Ancestry chain: immediate parent first, then its ancestors (nearest-parent ordering).
                DatabaseRegistryEntry? sourceEntry = await registry.TryResolveEntryAsync(ticket.BranchFrom!).ConfigureAwait(false);
                List<DatabaseBranchAncestor> ancestors =
                [
                    new DatabaseBranchAncestor { DatabaseId = sourceDescriptor.Id, ForkTimestamp = forkT },
                    .. (sourceEntry?.Ancestors ?? [])
                ];

                // Copy schema metadata into the branch namespace before publishing the registry entry.
                // Pass forkT so the scan reads source metadata as-of the fork point: any schema
                // change committed after forkT on another cluster node is not included, keeping the
                // branch schema consistent with the row/index MVCC snapshot it inherits.
                // The pending marker above ensures that if the process crashes here, the orphaned
                // namespace is found by the startup scrubber and purged on next restart.
                await catalogs.CopyMetaForBranchAsync(sourceDescriptor, branchId, forkT).ConfigureAwait(false);
                metaCopied = true;

                await registry.RegisterAsync(branchName, branchId, ancestors, holdId).ConfigureAwait(false);
                childRegistered = true;

                // Cross-node drop-vs-branch-create fence (BR12): check whether DropDatabase set a
                // drop-intent marker on the source after we passed the source-still-registered check
                // above but before RegisterAsync committed.
                //
                // Raft linearizability guarantees: if drop's intent write committed before our
                // RegisterAsync, we observe it here and abort; if our RegisterAsync committed first,
                // drop's subsequent HasLiveDescendantsAsync scan sees our child and drop aborts
                // instead.  One of those two cases always applies, so exactly one of drop or
                // branch-create wins with no orphaned child and no purged-ancestor branch.
                if (await registry.HasDropIntentAsync(sourceDescriptor.Id).ConfigureAwait(false))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseDoesntExist,
                        $"Source database '{ticket.BranchFrom}' is being dropped concurrently; branch creation aborted");
            }
            catch
            {
                // If the child was published (e.g. the drop-intent check threw after RegisterAsync, or
                // returned an indeterminate status), retract it before releasing the hold or purging the
                // metadata. The destructive cleanup below is only safe once the child is CONFIRMED
                // unpublished — otherwise it would leave a durable registry entry pointing at a deleted
                // metadata namespace, with no snapshot floor protecting the inherited rows and no
                // recovery handle.
                if (childRegistered)
                {
                    try
                    {
                        await registry.UnregisterAsync(branchName).ConfigureAwait(false);
                        childRegistered = false;
                    }
                    catch (Exception unregEx)
                    {
                        // Indeterminate create: the child is still registered and we cannot confirm its
                        // removal. Retain the snapshot hold, the copied metadata, AND the pending-create
                        // marker so a startup scrubber / reconciliation can later finish either
                        // unpublication + cleanup or completion of the branch. Do NOT release the hold or
                        // purge the metadata here. Surface a retryable indeterminate error.
                        leaveMarkerForScrubber = true;
                        logger.LogError(unregEx,
                            "Branch '{Branch}' (id={BranchId}) remains registered after an aborted create and could not be unregistered; retaining snapshot hold, metadata, and pending-create marker for recovery",
                            branchName, branchId);
                        throw new CamusDBException(
                            CamusDBErrorCodes.TransactionMustRetry,
                            $"Branch '{branchName}' creation is in an indeterminate state (the published branch could not be retracted after aborting); retry after reconciliation");
                    }
                }

                // Confirmed unpublished (never published, or now retracted): nothing will ever renew or
                // release this hold, so release it best-effort.
                try
                {
                    await sourceKahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception releaseEx)
                {
                    logger.LogWarning(releaseEx, "Failed to release snapshot hold {HoldId} after aborted branch creation of '{Branch}'", holdId, branchName);
                }

                // If metadata was already written before the abort, purge it inline so the orphaned
                // namespace does not linger. If the inline purge fails, leave the pending marker so
                // the startup scrubber can still reclaim the namespace on next restart.
                if (metaCopied)
                {
                    try
                    {
                        await PurgeBranchMetaNamespaceAsync(branchId, sourceKahuna).ConfigureAwait(false);
                    }
                    catch (Exception purgeEx)
                    {
                        logger.LogWarning(purgeEx,
                            "Failed to inline-purge orphaned branch metadata for id {BranchId}; namespace will be reclaimed by startup scrubber",
                            branchId);
                        leaveMarkerForScrubber = true;
                        throw;
                    }
                }
                throw;
            }
            finally
            {
                // Remove the pending marker whether creation succeeded or was cleanly aborted.
                // EXCEPTION: if the inline purge failed the namespace is still present and the
                // marker is the scrubber's only handle — keep it so startup can reclaim it.
                if (!leaveMarkerForScrubber)
                    await registry.ClearPendingBranchAsync(branchId).ConfigureAwait(false);
            }
        }
        finally
        {
            sourceDescriptor.SchemaDdlSemaphore.Release();
        }

        return await databaseOpener.Open(branchName).ConfigureAwait(false);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
    {
        return await databaseOpener.Open(database, recoveryMode).ConfigureAwait(false);
    }

    public async Task CloseDatabase(CloseDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        // Flush tail stats before the descriptor is torn down so debounced deltas survive shutdown.
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        await statisticsManager.FlushAllAsync(database).ConfigureAwait(false);

        await databaseCloser.Close(database.Id).ConfigureAwait(false);
    }

    public async Task DropDatabase(DropDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        DatabaseRegistryEntry? entry = registry.Get(ticket.DatabaseName);
        if (entry is null)
        {
            if (ticket.IfExists)
                return;
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseDoesntExist,
                $"Database '{ticket.DatabaseName}' does not exist");
        }

        // Acquire the target's SchemaDdlSemaphore before the descendant check so that a concurrent
        // CreateBranchDatabaseAsync — which holds the source's SchemaDdlSemaphore through
        // RegisterAsync — cannot slip between the check and the Unregister on this node.
        // The semaphore is released before the heavier Drop/drain step so long-running DML on the
        // database can drain while the schema lock is no longer held.
        //
        // If the descriptor is not yet cached (database was never opened after server start) we open
        // it to obtain the semaphore. If the open itself fails the descriptor is unusable and no
        // concurrent branch-create can be in flight against it, so we fall through without the lock.
        //
        // Cross-node limitation (documented): a branch-create racing on a different cluster node
        // can still interleave because the registry KV scan and the remote Open are not atomic.
        // A stronger cross-node guard (e.g. a replicated drop-lock key) is deferred.
        DatabaseDescriptor? targetDescriptor = null;
        try
        {
            targetDescriptor = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        }
        catch
        {
            // Descriptor unavailable (faulted or being torn down). No concurrent branch-create can
            // be in flight against a descriptor that cannot be opened; proceed without the semaphore.
        }

        // Acquire a persistent drop-intent marker for this database id before the descendant scan.
        // CreateBranchDatabaseAsync checks this marker after registering a new child: if the marker
        // is set, branch-create unregisters the child and aborts. Together with HasLiveDescendantsAsync
        // this closes the cross-node race where a branch-create slips between the scan and UnregisterAsync
        // on a different cluster node. Raft linearizability ensures either:
        //   (a) intent committed before branch-create's RegisterAsync: branch-create sees it and aborts, or
        //   (b) branch-create's RegisterAsync committed first: HasLiveDescendantsAsync below sees the child
        //       and this drop aborts with DatabaseHasLiveDescendants.
        // The intent is held through purge and released on every exit path (success, descendant check
        // failure, or Drop error) via the outer finally below.
        bool dropIntentAcquired = false;
        try
        {
            dropIntentAcquired = await registry.AcquireDropIntentAsync(entry.Id).ConfigureAwait(false);
            if (!dropIntentAcquired)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"A concurrent drop of '{ticket.DatabaseName}' is already in progress; retry later");
        }
        catch (CamusDBException) { throw; }
        catch (Exception intentEx)
        {
            // In cluster mode the drop-intent marker is the ONLY cross-node fence against a branch-create
            // racing on another node. If it cannot be acquired or confirmed, fail the drop CLOSED before
            // any destructive step (no orphan/drop marker, no unregister, no purge): the local
            // SchemaDdlSemaphore does not fence other nodes, so proceeding could unregister and purge the
            // parent while a remote branch-create publishes a child against it. Surface a retryable error.
            if (isClusterMode)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"Could not acquire the cross-node drop fence for '{ticket.DatabaseName}' ({intentEx.Message}); retry the drop");

            // Standalone: no other node can race, so the single-node semaphore guard is sufficient and the
            // drop may proceed without the distributed fence.
            logger.LogWarning(intentEx,
                "Could not acquire drop-intent marker for '{Database}' (standalone mode); proceeding under the single-node semaphore guard",
                ticket.DatabaseName);
        }

        // A root database dropped without FORCE is retained as a recoverable orphan: its keyspace is
        // kept on disk and the id becomes relinkable until the garbage collector reclaims it after the
        // retention window. Branch databases (and any FORCE drop) take the immediate-purge path — a
        // branch's recovery would require holding its parent's snapshot floor for the whole window, so
        // deferred drop is out of scope for branches.
        bool deferred = !ticket.Force && entry.Ancestors.Count == 0;

        // Single outer try/finally ensures the intent marker is released on every exit path:
        // successful drop, failed descendant check, or error during Drop/hold-release.
        try
        {
            if (targetDescriptor is not null)
                await targetDescriptor.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                // Re-check descendants under the semaphore (single-node guard from BR8) and after the
                // intent marker is set (cross-node guard from BR12). A branch-create on another node
                // that registered its child before we set the intent will be visible here; one that
                // registered after we set the intent will see the intent flag and abort itself.
                if (await registry.HasLiveDescendantsAsync(entry.Id).ConfigureAwait(false))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseHasLiveDescendants,
                        $"Database '{ticket.DatabaseName}' cannot be dropped because it has live branch descendants. Drop all descendant branches first.");

                if (deferred)
                {
                    // Write the orphan record BEFORE unregistering so a crash between the two leaves the
                    // database still live (stale record, harmless) rather than data stranded with no
                    // recovery path. DroppedAt is minted from the HLC so the GC's eligibility decision is
                    // consistent across nodes.
                    HLCTimestamp droppedAt = sharedNode!.Raft.HybridLogicalClock
                        .SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
                    await registry.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord
                    {
                        Id = entry.Id,
                        FormerName = entry.Name,
                        DroppedAt = droppedAt,
                    }).ConfigureAwait(false);
                }
                else
                {
                    // Mark the drop in progress before unregistering so a crash during the (non-atomic,
                    // per-key) keyspace purge below can be resumed at startup. The marker is owner-scoped
                    // and cleared only after the purge fully completes.
                    await registry.MarkDroppingAsync(entry.Id).ConfigureAwait(false);
                }

                // Unregister first: once the registry KV entry is deleted and the in-memory cache
                // cleared, any concurrent Open(name) gets DatabaseDoesntExist immediately — the name
                // is unreachable before the descriptor is removed from the descriptor cache.
                // If Drop throws after this point, the name is already freed for recreate rather
                // than wedged (better failure mode than registering after Drop).
                await registry.UnregisterAsync(ticket.DatabaseName).ConfigureAwait(false);
            }
            finally
            {
                if (targetDescriptor is not null)
                    targetDescriptor.SchemaDdlSemaphore.Release();
            }

            // Deferred drop closes the database but leaves its keyspace intact for recovery.
            // Pass the shared node so a missing/faulted descriptor still purges the keyspace by id
            // (headless) instead of reporting a phantom success and leaking the keyspace.
            bool purged = await databaseDroper
                .Drop(entry.Id, purge: !deferred, headlessKahuna: sharedNode?.Kahuna)
                .ConfigureAwait(false);

            if (!deferred)
            {
                // Release the snapshot-floor hold this branch owned on its immediate parent so the
                // parent's pinned MVCC history can be reclaimed. Best-effort.
                if (!string.IsNullOrEmpty(entry.ImmediateParentHoldId) && sharedNode is not null)
                {
                    try
                    {
                        await sharedNode.Kahuna
                            .LocateAndReleaseSnapshotHold(entry.ImmediateParentHoldId, CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Failed to release snapshot hold {HoldId} for dropped branch '{Database}'", entry.ImmediateParentHoldId, ticket.DatabaseName);
                    }
                }

                // Clear the drop-in-progress marker ONLY if the purge verifiably completed. If it did
                // not (a delete/scan failure), leave the marker so the startup resume finishes the purge
                // — clearing it now would abandon leaked row/index/meta keys with no reclaim.
                if (purged)
                    await registry.ClearDroppingAsync(entry.Id).ConfigureAwait(false);
                else
                    logger.LogWarning(
                        "DROP DATABASE FORCE for '{Database}' (id={Id}) did not fully purge; leaving drop-in-progress marker for startup resume",
                        ticket.DatabaseName, entry.Id);

                // A FORCE drop destroys the keyspace for good, so any stale orphan record for this id
                // (left by a crashed relink) must go too — otherwise the id stays "relinkable" to an
                // empty/partial keyspace.
                await registry.DeleteDatabaseOrphanAsync(entry.Id).ConfigureAwait(false);
            }

            // Evict every cache entry for this database. The descriptor may no longer be available
            // after the drop, so use the entry id directly rather than targetDescriptor?.Cache.
            targetDescriptor?.Cache?.InvalidateDatabase(entry.Id);
        }
        finally
        {
            // Release the intent marker now that the keyspace is purged (or on any failure path).
            // Branch-creates that were blocked by this intent now see the id gone; any that already
            // checked and are in-flight will either unregister (if they saw the intent) or will be
            // caught by a re-read of the now-unregistered source.
            if (dropIntentAcquired)
                await registry.ReleaseDropIntentAsync(entry.Id).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) root database: re-attaches <see cref="RelinkDatabaseTicket.NewName"/>
    /// to the orphan's preserved id and opens it against the retained keyspace. The orphan's id, rows,
    /// indexes, and schema are all still on disk, so the reopened database is immediately populated.
    ///
    /// <para>Serialized against a concurrent GC purge (and another relink) of the same id via the same
    /// per-id drop-intent fence the GC takes. Ordering is register-then-delete-orphan so a crash between
    /// them leaves the database live with a stale orphan record (the GC skips ids that are registered)
    /// rather than data stranded with no name.</para>
    /// </summary>
    public async Task<DatabaseDescriptor> RelinkDatabase(RelinkDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // Fence the id so a concurrent GC purge or a second relink cannot race this recovery. All the
        // authoritative state decisions below happen under this fence.
        bool fenced = await registry.AcquireDropIntentAsync(ticket.OrphanId).ConfigureAwait(false);
        if (!fenced)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A concurrent operation on orphan id '{ticket.OrphanId}' is in progress; retry later");

        try
        {
            // Idempotency by id (not just by name): if the id is already live — a relink that committed
            // its registration but crashed before deleting the orphan record — do not mint a second
            // alias. Re-check authoritatively (cross-node) under the fence.
            string? existingName = await registry.TryResolveNameByIdAsync(ticket.OrphanId).ConfigureAwait(false);
            if (existingName is not null)
            {
                // Already relinked. If under the requested name, finish idempotently (clean any stale
                // orphan record and return the live database); otherwise it is a conflicting second name.
                // Database names are case-insensitive, so compare the stored name to the requested one
                // case-insensitively rather than assuming either was folded.
                if (!string.Equals(existingName, ticket.NewName, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database id '{ticket.OrphanId}' is already live under name '{existingName}'");

                await registry.DeleteDatabaseOrphanAsync(ticket.OrphanId).ConfigureAwait(false);
                return await databaseOpener.Open(existingName).ConfigureAwait(false);
            }

            // The target name must be free (cache + live-KV check for cross-node visibility).
            if (await registry.TryResolveEntryAsync(ticket.NewName).ConfigureAwait(false) is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{ticket.NewName}' already exists");

            OrphanDatabaseRecord? orphan = await registry.TryGetDatabaseOrphanAsync(ticket.OrphanId).ConfigureAwait(false);
            if (orphan is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.OrphanNotFound,
                    $"No orphaned database with id '{ticket.OrphanId}' is available to relink (never dropped, or already reclaimed)");

            // Re-register the name to the preserved id; the keyspace is already on disk.
            await registry.RegisterAsync(ticket.NewName, orphan.Id).ConfigureAwait(false);

            // The database is live again — remove the orphan record so the GC leaves it alone.
            await registry.DeleteDatabaseOrphanAsync(orphan.Id).ConfigureAwait(false);

            return await databaseOpener.Open(ticket.NewName).ConfigureAwait(false);
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(ticket.OrphanId).ConfigureAwait(false);
        }
    }

    public async Task RenameDatabase(RenameDatabaseTicket ticket)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // Delegates all validation (old-exists, new-free, reserved-name check) to the registry.
        await registry.RenameAsync(ticket.OldName, ticket.NewName).ConfigureAwait(false);

        // The descriptor cache is keyed by id (which is unchanged after rename), so the existing
        // cached descriptor continues to serve Open(newName) via the same live Kahuna node.
        // Do NOT evict the descriptor: evicting without flushing/disposing the node would orphan
        // the running SQLite+Raft process (standalone) or leak the schema-replication subscription
        // (cluster), and a second Load would open a second node on the same storage (double-writer).
        //
        // Instead refresh the descriptor's display name in place. It was previously left stale on the
        // theory that Name is display-only — but any path that fed it back into a by-name resolution
        // then resolved a name the registry no longer knows, which is exactly how INSERT broke after a
        // rename. Request-scoped code must still prefer ticket.DatabaseName; this keeps the fallback
        // honest rather than relying on every caller getting it right.
        await RefreshCachedDescriptorNameAsync(registry, ticket.NewName).ConfigureAwait(false);
    }

    /// <summary>
    /// Points the cached descriptor's display name at <paramref name="newName"/> after a committed
    /// rename.
    ///
    /// <para>Resolved by id rather than by matching the old name: the descriptor cache is keyed by id,
    /// the id is what a rename preserves, and the registry has already been updated so the new name
    /// resolves to it.</para>
    ///
    /// <para>Best-effort by design — the registry swap is already durable when this runs, so a failure
    /// here must not fail the statement; the worst case is a stale name in log output until the
    /// descriptor is recycled. Only a descriptor that is already materialized is touched: forcing an
    /// unstarted cache entry would load a database nobody asked for, and a later load already picks up
    /// the new name.</para>
    /// </summary>
    private async Task RefreshCachedDescriptorNameAsync(DatabaseRegistry registry, string newName)
    {
        try
        {
            string? id = await registry.TryResolveIdAsync(newName).ConfigureAwait(false);
            if (id is null)
                return;

            if (!databaseDescriptors.Descriptors.TryGetValue(id, out Nito.AsyncEx.AsyncLazy<DatabaseDescriptor>? lazy))
                return;

            if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                return;

            lazy.Task.Result.SetName(newName);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to refresh the cached descriptor name after renaming to {NewName}", newName);
        }
    }

    #endregion

    #region DDL

    // DDL transaction commits must be bounded — LocateAndCommitTransaction with
    // CancellationToken.None can hang indefinitely if the schema partition Raft actor
    // is stalled. 10 s covers leader-election time in a healthy cluster while still
    // converting permanent stalls into a recoverable CamusDBException.
    private static readonly TimeSpan DdlCommitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Executes a DDL action in a self-managed Kahuna transaction.
    /// Begins a transaction, runs <paramref name="action"/>, commits on success,
    /// or rolls back (and re-throws) on any exception.
    /// </summary>
    /// <summary>
    /// Runs a DDL action inside a <see cref="KvTransaction"/> and commits it. Calls
    /// <paramref name="postCommitInvalidate"/> after a successful commit so the query-result cache
    /// evicts any entries whose schema deps reference the affected table. Schema meta keys use the
    /// <c>{db}/meta/…</c> keyspace and do not match the row/index bucket patterns tracked by
    /// <see cref="KvTransactionsManager"/>'s automatic key-based invalidation, so DDL paths must
    /// supply an explicit invalidation action here rather than relying on the DML commit hook.
    /// </summary>
    /// <remarks>
    /// <b>Intentional asymmetry vs DML invalidation:</b> DML commits drive
    /// <c>CachePublishGate.MarkWriteInFlight / CommitWrite</c> so the invalidation runs inside a
    /// critical section that also bumps the generation counter, atomically fencing concurrent
    /// TryPublishUnderGeneration calls. DDL invalidation (<paramref name="postCommitInvalidate"/>)
    /// runs after <c>CommitAsync</c> returns, outside that critical section — there is a narrow
    /// window in which a concurrent SELECT could publish a stale-schema entry.
    ///
    /// This asymmetry is acceptable: schema meta keys cannot participate in the key-based gate
    /// (they do not map to a table bucket). However, the window is closed at the read side:
    /// <see cref="QueryExecutor"/> performs an in-memory schema dep re-check on every hit
    /// (non-strict and strict alike). An entry that manages to publish in this narrow window
    /// will be evicted on the very first subsequent probe when its schema version is found stale,
    /// so it can never be served.
    /// </remarks>
    private async Task<T> ExecuteDdlInTransaction<T>(
        DatabaseDescriptor database,
        Func<KvTransaction, Task<T>> action,
        Func<Task>? onAbort = null,
        Action? postCommitInvalidate = null
    )
    {
        // Acquired in BOTH modes. It used to be cluster-only, on the reasoning that a standalone node
        // has no replicated schema log to order proposals against — but the gate is also what makes
        // "resolve the target, then mutate it" atomic against a concurrent drop/recreate of the same
        // object. Skipping it standalone meant metadata-only DDL that *does* take the gate (COMMENT ON,
        // ALTER TABLE SET) serialized against nothing there, and could persist a blob for a table that
        // had just been dropped. One discipline in both modes; DDL is rare enough that the added
        // serialization costs nothing that matters.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);
        try
        {
            T result = await action(tx).ConfigureAwait(false);
            using CancellationTokenSource cts = new(DdlCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);
            postCommitInvalidate?.Invoke();
            return result;
        }
        catch (Exception ex)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);

            if (onAbort is not null)
            {
                try
                {
                    await onAbort().ConfigureAwait(false);
                }
                catch (Exception cleanupEx)
                {
                    logger.LogWarning(
                        cleanupEx,
                        "Failed to run DDL abort compensation for database {DatabaseName} after {ErrorType}",
                        database.Name,
                        ex.GetType().Name
                    );
                }
            }

            throw;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            // Fire after CommitAsync (or RollbackIfNotCompletedAsync on error) so the
            // KV transaction is settled before schema-partition leadership changes.
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    public async Task<CreateTableResult> CreateTable(CreateTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardCreateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new CreateTableResult(database, forwarded.Value);

        // Allocate the table id before the DDL transaction — only the proposer/leader allocates;
        // the id is carried in the replicated payload so every follower applies the same id.
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string tableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database, async tx =>
        {
            bool result = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, ticket, tx, tableId).ConfigureAwait(false);
            return new CreateTableResult(database, result);
        }).ConfigureAwait(false);
    }

    public async Task<bool> AlterTable(AlterTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardAlterTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        if (isClusterMode && ticket.Operation == AlterTableOperation.AddColumn)
            return await ExecuteClusterAddColumnAsync(database, table, ticket).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database,
            tx => tableColumnAlterer.Alter(queryExecutor, database, table, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Cluster path for ADD COLUMN: drives the column through the staged
    /// Absent → DeleteOnly → WriteOnly → Public sequence via <see cref="SchemaChangeCoordinator"/>,
    /// backfilling row defaults once the column reaches <c>WriteOnly</c> (the first state at
    /// which <see cref="RowEncoder.Encode"/> includes the column in encoded bytes).
    ///
    /// The <c>SchemaDdlSemaphore</c> is held across the entire coordinator sequence so
    /// concurrent DDL on this node cannot observe an intermediate schema version.
    /// </summary>
    private async Task<bool> ExecuteClusterAddColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterTableTicket ticket
    )
    {
        ColumnInfo columnInfo = ticket.Column;

        // Eagerly reject duplicates before acquiring the semaphore — the coordinator
        // would silently no-op (already-at-Public = empty path) instead of throwing.
        if (table.Schema.Columns?.Any(c => string.Equals(c.Name, columnInfo.Name, StringComparison.OrdinalIgnoreCase)) == true)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate column '{columnInfo.Name}'");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, logger);
            coordinator.BackfillAsync = (db, tableName, column) => BackfillColumnDefaultsAsync(db, tableName, column);

            await coordinator.RunJobAsync(
                database,
                new SchemaChangeJob(database.Name, ticket.TableName, table.Id, columnInfo.Name, SchemaElementState.Public),
                columnDefinition: columnInfo
            ).ConfigureAwait(false);

            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            return true;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Re-encodes every existing row in <paramref name="tableName"/> so that the newly added
    /// <paramref name="column"/> (in <c>WriteOnly</c> state) is stored with its default value.
    /// Used by both the command-path coordinator and the leader-change resume coordinator so
    /// backfill is always part of the resumable sequence.
    /// </summary>
    internal async Task BackfillColumnDefaultsAsync(DatabaseDescriptor database, string tableName, ColumnInfo column)
    {
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);

        AlterColumnTicket alterTicket = new(
            databaseName: database.Name,
            tableName: tableName,
            column: column,
            operation: AlterTableOperation.AddColumn
        );

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);
        try
        {
            await tableColumnAlterer.BackfillColumnDefaultsAsync(queryExecutor, database, table, alterTicket, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }    

    /// <summary>
    /// Test-only hook: invoked after each intermediate batch checkpoint is persisted
    /// (i.e., after batch N commits and before batch N+1 starts). Allows tests to inject
    /// a pause or forced leader change between batches without relying on timing.
    /// Set to null in production; cleared in test TearDown.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal Func<Task>? TestInterceptAfterBackfillCheckpoint;

    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal CatalogsManager Catalogs => catalogs;

    /// <summary>
    /// Scans every existing row in <paramref name="tableName"/> and writes an index entry
    /// for each, using backfill mode (idempotent — <c>Set</c> rather than <c>SetIfNotExists</c>
    /// for unique indexes). Processes rows in <see cref="BackfillBatchSize"/>-row transactions;
    /// after each committed batch invokes <paramref name="onCheckpoint"/> with the last rowId so
    /// the coordinator can persist a resume offset. Called by <see cref="SchemaChangeCoordinator"/>
    /// just before the index transitions from <c>WriteOnly</c> to <c>Public</c>.
    /// </summary>
    internal async Task BackfillIndexEntriesAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexInfo,
        string? startOffset,
        Func<string, Task>? onCheckpoint = null
    )
    {
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);
        bool unique = indexInfo.IndexType == IndexType.Unique;

        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(startOffset)
            ? null
            : ObjectId.ToValue(startOffset!);

        int totalRows = 0;

        while (true)
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            int batchRows = 0;
            ObjectIdValue lastRowId = default;

            try
            {
                // Each scanned row produces an index entry in this same transaction, so the rows read
                // are a genuine commit dependency and must stay in the read set.
                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    tx, afterRowId: afterRowId, trackReadSet: true).ConfigureAwait(false))
                {
                    Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                        table.Schema, tx.TransactionId, rowId, data,
                        visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);

                    // NULLs are distinct: a unique index omits entries for rows with a NULL (or absent)
                    // value in any indexed column, so multiple such rows can coexist. This must match
                    // the incremental insert path so a backfilled index equals one built row-by-row.
                    if (!unique || !HasNullIndexColumn(row, indexInfo.ColumnNames))
                    {
                        int i = 0;
                        ColumnValue[] columnValues = unique
                            ? new ColumnValue[indexInfo.ColumnNames.Length]
                            : new ColumnValue[indexInfo.ColumnNames.Length + 1];

                        foreach (string columnName in indexInfo.ColumnNames)
                        {
                            ColumnValue? keyValue = row.GetValueOrDefault(columnName);
                            if (keyValue is null)
                                throw new CamusDBException(
                                    CamusDBErrorCodes.InvalidInternalOperation,
                                    $"A null value was found for index key field '{columnName}'"
                                );
                            columnValues[i++] = keyValue;
                        }

                        if (!unique)
                            columnValues[i] = new(ColumnType.Id, rowId.ToString());

                        CompositeColumnValue compositeKey = new(columnValues);

                        // Materialize stored/payload (INCLUDE) values (NULL-tolerant) for a covering index.
                        // EncodeTupleChecked enforces the per-entry byte ceiling before the KV write.
                        byte[]? includeTuple = indexInfo.IncludeColumnNames is { Length: > 0 } includeNames
                            ? Storage.Kv.IndexIncludeValueCodec.EncodeTupleChecked(includeNames, row, indexInfo.IndexName)
                            : null;

                        await table.Store.PutIndexEntry(tx, indexInfo.IndexId, compositeKey, rowId, unique, backfillMode: true, includeTuple: includeTuple).ConfigureAwait(false);
                    }

                    lastRowId = rowId;
                    batchRows++;

                    if (batchRows >= BackfillBatchSize)
                        break;
                }

                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            totalRows += batchRows;

            if (batchRows < BackfillBatchSize)
                break;

            // More rows may remain — checkpoint and advance the cursor.
            afterRowId = lastRowId;
            if (onCheckpoint is not null)
                await onCheckpoint(lastRowId.ToString()).ConfigureAwait(false);

            if (TestInterceptAfterBackfillCheckpoint is not null)
                await TestInterceptAfterBackfillCheckpoint().ConfigureAwait(false);
        }

        Log.LogIndexBackfillComplete(logger, totalRows, indexInfo.IndexName);
    }

    /// <summary>
    /// Returns true when any of the index's columns is absent from the row or holds a NULL value.
    /// Such a row is exempt from a unique index (NULLs are distinct) and is skipped during backfill.
    /// </summary>
    private static bool HasNullIndexColumn(Dictionary<string, ColumnValue> row, string[] columnNames)
    {
        foreach (string columnName in columnNames)
        {
            ColumnValue? value = row.GetValueOrDefault(columnName);
            if (value is null || value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Drives the add-index sequence through the coordinator-owned staged path
    /// (<c>Absent → DeleteOnly → WriteOnly → [backfill] → Public</c>).
    /// Only called in cluster mode.
    /// </summary>
    private async Task<bool> ExecuteClusterAddIndexAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        if (table.Indexes.ContainsKey(ticket.IndexName))
        {
            if (ticket.IfNotExists)
                return false;
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{ticket.IndexName}' already exists on table '{table.Name}'");
        }

        IndexType indexType = ticket.Operation is AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey
            ? IndexType.Unique
            : IndexType.Multi;

        IndexColumnOrder.RejectDescendingOnUnsupportedType(
            ticket.Columns,
            ticket.IndexName,
            name => table.Schema.Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))?.Type);

        TableIndexAdder.ValidateIncludeColumns(table, ticket);

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = GetColumnIdsForIndex(table, ticket.Columns);
        string[] columnNames = ticket.Columns.Select(c => c.Name).ToArray();
        string[]? includeColumnIds = ticket.IncludeColumns.Length > 0
            ? GetColumnIdsForIndex(table, ticket.IncludeColumns.Select(n => new ColumnIndexInfo(n, OrderType.Ascending)).ToArray())
            : null;
        string[]? includeColumnNames = ticket.IncludeColumns.Length > 0 ? ticket.IncludeColumns : null;

        IndexBuildInfo indexInfo = new(indexId, ticket.IndexName, columnIds, columnNames, indexType, IndexColumnOrder.Extract(ticket.Columns), IncludeColumnIds: includeColumnIds, IncludeColumnNames: includeColumnNames);

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, logger);
            coordinator.IndexBackfillAsync = (db, tbl, info, start, checkpoint) => BackfillIndexEntriesAsync(db, tbl, info, start, checkpoint);

            try
            {
                await coordinator.RunJobAsync(
                    database,
                    new SchemaChangeJob(database.Name, ticket.TableName, table.Id, ticket.IndexName, SchemaElementState.Public, SchemaElementKind.Index),
                    indexBuildInfo: indexInfo
                ).ConfigureAwait(false);

                database.Cache?.InvalidateByTableId(database.Id, table.Id);

                return true;
            }
            catch
            {
                // Compensate: if the index was partially committed to the schema (in DeleteOnly
                // or WriteOnly state) but did not reach Public, emit DropIndex on all nodes and
                // delete the persisted coordinator job, leaving the cluster in a clean state.
                // Note: if this node is now degraded, compensation may be skipped by the
                // degraded gate in ReplicateDropIndexAsync; a healthy peer's ResumeJobsAsync
                // will reconcile the state after the step-down below.
                await CompensateClusterAddIndexAsync(database, table.Id, ticket.TableName, ticket.IndexName).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    // Shared helper — delegates to DatabaseDescriptor.FireDeferredSchemaStepDownAsync,
    // adding the caller's logger for the step-down failure case. Called from the finally blocks
    // of ExecuteDdlInTransaction, ExecuteClusterAddColumnAsync, ExecuteClusterAddIndexAsync, and
    // ExecuteClusteredIndexDdlAsync so all DDL paths release leadership on degradation.
    private async Task FireDeferredStepDownIfRequestedAsync(DatabaseDescriptor database)
    {
        try
        {
            await database.FireDeferredSchemaStepDownAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Schema partition step-down after persist exhaustion failed for database {DatabaseName}",
                database.Name
            );
        }
    }

    private async Task CompensateClusterAddIndexAsync(DatabaseDescriptor database, string tableId, string tableName, string indexName)
    {
        try
        {
            if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema) &&
                tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase)) == true)
            {
                await catalogs.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to compensate partial index add for {IndexName} on {TableName}", indexName, tableName);
        }

        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, tableId, indexName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to delete coordinator job for index {IndexName} on {TableName}", indexName, tableName);
        }
    }

    private static string[] GetColumnIdsForIndex(TableDescriptor table, ReadOnlySpan<ColumnIndexInfo> columns)
    {
        string[] columnIds = new string[columns.Length];
        int i = 0;

        foreach (ColumnIndexInfo columnIndex in columns)
        {
            bool found = false;
            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name != columnIndex.Name)
                    continue;

                if (!SchemaElementStateRules.IsReadable(column) || !SchemaElementStateRules.IsWritable(column))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnIndex.Name}' is not public on table '{table.Name}'"
                    );

                columnIds[i++] = column.Id;
                found = true;
                break;
            }

            if (!found)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{columnIndex.Name}' does not exist on table '{table.Name}'"
                );
        }

        return columnIds;
    }

    public async Task<bool> AlterIndex(AlterIndexTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardAlterIndexAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        // RenameIndex is metadata-only (no KV data changes); use single-phase DDL on all paths.
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
            return await ExecuteDdlInTransaction(database,
                tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
            ).ConfigureAwait(false);

        bool addIndexOperation = ticket.Operation is
            AlterIndexOperation.AddIndex or
            AlterIndexOperation.AddUniqueIndex or
            AlterIndexOperation.AddPrimaryKey;

        if (isClusterMode && addIndexOperation)
            return await ExecuteClusterAddIndexAsync(database, table, ticket).ConfigureAwait(false);

        bool indexExistedBefore = table.Indexes.ContainsKey(ticket.IndexName);
        bool compensateOnAbort = addIndexOperation && !indexExistedBefore;

        // Both cluster (non-add) and standalone paths use two-phase DDL: local work + schema
        // replication. The old standalone-only ExecuteDdlInTransaction path omitted Phase 2
        // (ReplicateIndexChangeAsync), leaving the schema unpersisted across close/reopen.
        return await ExecuteClusteredIndexDdlAsync(
            database, table, ticket, compensateOnAbort,
            tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx)
        ).ConfigureAwait(false);
    }

    private static async Task CompensateAbortedAddIndexAsync(DatabaseDescriptor database, TableDescriptor table, string indexName)
    {
        table.Indexes.Remove(indexName);

        await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            table.Schema.Indexes?.RemoveAll(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }
    }

    /// <summary>
    /// Two-phase execution for cluster index DDL. Phase 1 commits the backfill data
    /// (tx1) so the index KV entries are visible before Phase 2 replicates the schema
    /// delta. Both phases run under <c>SchemaDdlSemaphore</c> so <c>SchemaVersion</c>
    /// stays stable across the pair. Only called in cluster mode.
    /// </summary>
    private async Task<bool> ExecuteClusteredIndexDdlAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        bool compensateOnAbort,
        Func<KvTransaction, Task<bool>> localWork
    )
    {
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Phase 1: run local DDL (including backfill) and commit so the index KV
            // entries are durable and visible before the schema delta is published.
            KvTransaction tx1 = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            bool result;
            try
            {
                result = await localWork(tx1).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx1).ConfigureAwait(false);
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx1).ConfigureAwait(false);
                if (compensateOnAbort)
                    await CompensateAbortedAddIndexAsync(database, table, ticket.IndexName).ConfigureAwait(false);
                throw;
            }

            if (!result) return result;

            // Phase 2: index data is committed — replicate the schema change so every
            // node updates its TableSchema.Indexes and evicts its TableDescriptor cache.
            // A fresh transaction supplies the HLC timestamp for the schema-log entry;
            // no KV writes happen under it (ReplicateIndexChangeAsync creates its own
            // internal checkpoint transaction via PersistSchemaCheckpointAsync).
            KvTransaction tx2 = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            try
            {
                await catalogs.ReplicateIndexChangeAsync(database, ticket, table, tx2).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx2).ConfigureAwait(false);
            }

            // Schema has been replicated; evict stale cache entries for this table. Phase 1's
            // row/index KV writes are already handled by the CommitAsync invalidation hook, but
            // schema-dep entries (keyed by tableId, not by KV key) need an explicit call here.
            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            // Re-populate the descriptor cache: ReplicateIndexChangeAsync fires
            // InvalidateAppliedTableDescriptor which evicts the table. Re-opening here
            // ensures callers that rely on TableDescriptors find it immediately after DDL.
            await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            return result;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    public async Task<bool> DropTable(DropTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardDropTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        if (ticket.IfExists && !catalogs.TableExists(database, ticket.TableName))
            return false;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database,
            tx => tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) table by reattaching it to the schema under a new name,
    /// reusing its preserved id and retained row/index data. Like other table DDL it forwards to the
    /// schema leader in cluster mode. Fails with <see cref="CamusDBErrorCodes.OrphanNotFound"/> if no
    /// orphan record exists for the id, or <see cref="CamusDBErrorCodes.TableAlreadyExists"/> if the new
    /// name is taken.
    /// </summary>
    public async Task<bool> RelinkTable(RelinkTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardRelinkTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        // Fence this table id against a concurrent GC reclamation (and a second relink). The reclaimer
        // takes the same per-table drop-intent key before purging, so the two never interleave. All the
        // state decisions below happen under this fence.
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string fenceId = DatabaseRegistry.TableFenceId(database.Id, ticket.OrphanTableId);
        bool fenced = await registry.AcquireDropIntentAsync(fenceId).ConfigureAwait(false);
        if (!fenced)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A concurrent operation on orphan table id '{ticket.OrphanTableId}' is in progress; retry later");

        try
        {
            // Idempotency by id: if a live table already has this id — a relink that committed its apply
            // but crashed before deleting the orphan record — don't reattach a second alias.
            TableSchema? liveWithId = null;
            foreach (TableSchema t in database.Schema.Tables.Values)
                if (string.Equals(t.Id, ticket.OrphanTableId, StringComparison.Ordinal)) { liveWithId = t; break; }

            if (liveWithId is not null)
            {
                if (!string.Equals(liveWithId.Name, ticket.NewTableName, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TableAlreadyExists,
                        $"Table id '{ticket.OrphanTableId}' is already live under name '{liveWithId.Name}'");

                // Already relinked to this exact name — finish idempotently by cleaning any stale record.
                await DeleteTableOrphanRecordAsync(database, ticket.OrphanTableId).ConfigureAwait(false);
                return true;
            }

            if (catalogs.TableExists(database, ticket.NewTableName))
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"Table '{ticket.NewTableName}' already exists");

            OrphanTableRecord? orphan = await catalogs.TryGetTableOrphanAsync(database, ticket.OrphanTableId).ConfigureAwait(false);
            if (orphan is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.OrphanNotFound,
                    $"No orphaned table with id '{ticket.OrphanTableId}' is available to relink in database '{ticket.DatabaseName}'");

            // Relink adds a live table, so it counts against the per-database table limit like CREATE.
            int maxTables = CamusDBConfig.MaxTablesPerDatabase;
            if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
                throw new CamusDBException(
                    CamusDBErrorCodes.SchemaLimitExceeded,
                    $"Database '{ticket.DatabaseName}' would exceed the maximum of {maxTables} tables per database");

            await ExecuteDdlInTransaction(database,
                tx => catalogs.RelinkTable(database, orphan, ticket.NewTableName, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, orphan.TableId)
            ).ConfigureAwait(false);
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Deletes a stale table orphan record in its own committed transaction (idempotent cleanup).</summary>
    private async Task DeleteTableOrphanRecordAsync(DatabaseDescriptor database, string tableId)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await catalogs.DeleteTableOrphanAsync(database, tableId, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    public async Task<bool> RenameTable(RenameTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardRenameTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor renameTable = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database,
            tx => catalogs.RenameTable(database, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTable.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table by <b>database name</b>, resolving the database through the registry first.
    ///
    /// <para>For request-scoped callers that only have a name. Code that already holds a
    /// <see cref="DatabaseDescriptor"/> must call <see cref="OpenTableWithDescriptor"/> instead —
    /// passing <c>descriptor.Name</c> back into this method re-resolves a cached display name, which
    /// after a RENAME DATABASE was the pre-rename name and made every INSERT fail with
    /// "Database '&lt;old&gt;' does not exist". It is also two redundant lookups on a hot path.</para>
    /// </summary>
    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = descriptor.Use();
        return await tableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table against an already-resolved database. The preferred entry point whenever the
    /// caller holds a descriptor: it skips the registry round-trip and cannot be affected by a rename
    /// that happened after the descriptor was cached. <c>ticket.DatabaseName</c> is carried for
    /// diagnostics only — the descriptor decides which database is used.
    /// </summary>
    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await tableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardCreateTableAsync(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardCreateTableAsync(leader, ticket, opId, ct),
            () => ForwardedCreateTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardAlterTableAsync(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterTableAsync(leader, ticket, opId, ct),
            () => ForwardedAlterTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardAlterIndexAsync(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterIndexAsync(leader, ticket, opId, ct),
            () => ForwardedAlterIndexApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardDropTableAsync(DatabaseDescriptor database, DropTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardDropTableAsync(leader, ticket, opId, ct),
            () => ForwardedDropTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardRenameTableAsync(DatabaseDescriptor database, RenameTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardRenameTableAsync(leader, ticket, opId, ct),
            () => ForwardedRenameTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardRelinkTableAsync(DatabaseDescriptor database, RelinkTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardRelinkTableAsync(leader, ticket, opId, ct),
            () => ForwardedRelinkTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardDdlAsync(
        DatabaseDescriptor database,
        Func<string, string, CancellationToken, Task<bool?>> forward,
        Func<bool> wasApplied
    )
    {
        if (!isClusterMode)
            return null;

        // Degraded nodes must not propose or forward DDL — reject immediately so the
        // caller gets a typed "degraded" error rather than a generic "not leader" error.
        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

        if (await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            return null;

        if (schemaDdlForwarder is null)
        {
            string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"DDL must be executed by schema leader '{leader}' for database '{database.Name}'"
            );
        }

        // One stable id for all retry attempts so a dedup receiver can
        // recognise retransmissions of the same logical operation.
        string operationId = Guid.NewGuid().ToString("N");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            long fromVersion = database.Schema.SchemaVersion;

            for (int attempt = 0; attempt < 3; attempt++)
            {
                string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
                bool? result = await forward(leader, operationId, CancellationToken.None).ConfigureAwait(false);
                if (result is not null)
                {
                    if (result.Value)
                        await WaitForForwardedSchemaApplyAsync(database, fromVersion, wasApplied).ConfigureAwait(false);

                    return result;
                }
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Failed to forward DDL to schema leader for database '{database.Name}'"
        );
    }

    private static async Task WaitForForwardedSchemaApplyAsync(DatabaseDescriptor database, long fromVersion, Func<bool> wasApplied)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            if (database.Schema.SchemaVersion > fromVersion && wasApplied())
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for forwarded schema apply for database '{database.Name}' after version {fromVersion}"
        );
    }

    private static bool ForwardedCreateTableApplied(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        // Also wait for all index constraints to be replicated via the schema log.
        // ApplyAddIndex runs after ApplyCreateTable (separate Raft entries), so the table
        // may exist before all its constraints are visible.
        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            if (constraint.Type is ConstraintType.PrimaryKey or ConstraintType.IndexUnique or ConstraintType.IndexMulti)
            {
                if (tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, constraint.Name, StringComparison.OrdinalIgnoreCase)) != true)
                    return false;
            }
        }

        return true;
    }

    private static bool ForwardedAlterTableApplied(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        if (ticket.Operation == AlterTableOperation.RenameColumn)
        {
            // After rename, new name present in any table (table name unchanged).
            return database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts) &&
                   ts.Columns?.Any(c => string.Equals(c.Name, ticket.NewName, StringComparison.OrdinalIgnoreCase)) == true;
        }

        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        return ticket.Operation switch
        {
            // A forwarded AddColumn is complete only when the column is Public — intermediate
            // staged states (DeleteOnly, WriteOnly) are not yet visible to queries.
            AlterTableOperation.AddColumn =>
                tableSchema.Columns?.Any(c => string.Equals(c.Name, ticket.Column.Name, StringComparison.OrdinalIgnoreCase) && c.State == SchemaElementState.Public) == true,
            AlterTableOperation.DropColumn =>
                tableSchema.Columns?.Any(c => string.Equals(c.Name, ticket.Column.Name, StringComparison.OrdinalIgnoreCase)) != true,
            _ => false
        };
    }

    private static bool ForwardedAlterIndexApplied(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
        {
            return database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts) &&
                   ts.Indexes?.Any(ix => string.Equals(ix.Name, ticket.NewName, StringComparison.OrdinalIgnoreCase)) == true;
        }

        // Check TableSchema.Indexes (the source of truth). Fall back to SystemSchema
        // for nodes that haven't yet applied the migration (legacy path).
        bool existsInSchema = database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema) &&
                              tableSchema.Indexes is not null &&
                              tableSchema.Indexes.Any(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.OrdinalIgnoreCase));

        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey => existsInSchema,
            AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey => !existsInSchema,
            _ => false
        };
    }

    private static bool ForwardedDropTableApplied(DatabaseDescriptor database, DropTableTicket ticket)
    {
        return !database.Schema.Tables.ContainsKey(ticket.TableName)
            && !database.TableDescriptors.ContainsKey(ticket.TableName);
    }

    private static bool ForwardedRenameTableApplied(DatabaseDescriptor database, RenameTableTicket ticket)
    {
        return database.Schema.Tables.ContainsKey(ticket.NewName)
            && !database.Schema.Tables.ContainsKey(ticket.TableName);
    }

    private static bool ForwardedRelinkTableApplied(DatabaseDescriptor database, RelinkTableTicket ticket)
    {
        return database.Schema.Tables.ContainsKey(ticket.NewTableName);
    }

    private static bool ForwardedAlterConstraintApplied(DatabaseDescriptor database, AlterConstraintTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts))
            return false;

        if (ticket.Operation == AlterConstraintOperation.SetNotNull)
        {
            TableColumnSchema? col = ts.Columns?.FirstOrDefault(c => string.Equals(c.Name, ticket.ColumnName, StringComparison.OrdinalIgnoreCase));
            return col?.NotNull == true;
        }

        if (ticket.Operation == AlterConstraintOperation.DropNotNull)
        {
            TableColumnSchema? col = ts.Columns?.FirstOrDefault(c => string.Equals(c.Name, ticket.ColumnName, StringComparison.OrdinalIgnoreCase));
            return col?.NotNull == false;
        }

        bool constraintExists = ts.CheckConstraints?.Any(c => string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase)) == true;
        return ticket.Operation == AlterConstraintOperation.AddCheck ? constraintExists : !constraintExists;
    }

    private async Task<bool?> TryForwardAlterConstraintAsync(DatabaseDescriptor database, AlterConstraintTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterConstraintAsync(leader, ticket, opId, ct),
            () => ForwardedAlterConstraintApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Whether a forwarded <c>COMMENT ON</c> is visible in this node's in-memory schema yet.
    /// Comparison is ordinal and null-aware on purpose: <c>IS NULL</c> (null) and <c>IS ''</c> (empty)
    /// are different outcomes, so treating them as equal would report "applied" for the wrong one.
    /// </summary>
    private static bool ForwardedCommentApplied(DatabaseDescriptor database, CommentTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName ?? "", out TableSchema? ts))
            return false;

        string? current = ticket.Target switch
        {
            CommentTarget.Table => ts.Comment,
            CommentTarget.Column => ts.Columns?.FirstOrDefault(
                c => string.Equals(c.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase))?.Comment,
            CommentTarget.Index => ts.Indexes?.FirstOrDefault(
                ix => string.Equals(ix.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase))?.Comment,
            _ => null
        };

        return string.Equals(current, ticket.Comment, StringComparison.Ordinal);
    }

    private async Task<bool?> TryForwardCommentAsync(DatabaseDescriptor database, CommentTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardCommentAsync(leader, ticket, opId, ct),
            () => ForwardedCommentApplied(database, ticket)
        ).ConfigureAwait(false);
    }


    /// <summary>
    /// Applies <c>ALTER TABLE t SET (key = value)</c> table storage parameters version-neutrally (rides
    /// the table blob, no <see cref="TableSchema.Version"/> bump). Replicates in cluster mode (schema
    /// leader only); applies directly in standalone mode. Public so a ticket caller can invoke it
    /// without the SQL path.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> AlterTableSettings(AlterTableSettingsTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await AlterTableSettings(database, ticket).ConfigureAwait(false);
    }

    private async Task<ExecuteDDLSQLResult> AlterTableSettings(DatabaseDescriptor database, AlterTableSettingsTicket ticket)
    {
        // Canonical validation for every entry point (SQL and direct ticket): unknown key, non-boolean
        // value, empty set, or duplicate key are rejected here, and keys/values are lowercased.
        Dictionary<string, string> settings = CamusDB.Core.Catalogs.Models.TableSettings.Canonicalize([.. ticket.Settings]);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        // Serialize with all other schema changes for this database, exactly like the established DDL
        // paths: the version capture, proposal (cluster), and blob rewrite must not race another DDL.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (isClusterMode)
            {
                // Settings DDL replicates through the schema log, which only the schema leader may
                // propose. There is no HTTP forwarder for it yet, so a non-leader rejects with a clear
                // error and the client retargets the leader (the in-process path issues it on the leader).
                if (!await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
                {
                    string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"ALTER TABLE SET must be executed by schema leader '{leader}' for database '{database.Name}'");
                }

                await catalogs.ReplicateSetTableSettingsAsync(database, ticket.TableName, settings).ConfigureAwait(false);
            }
            else
            {
                await catalogs.AlterTableSettingsAsync(database, table, settings).ConfigureAwait(false);
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        database.Cache?.InvalidateByTableId(database.Id, table.Id);
        return new ExecuteDDLSQLResult(database, true);
    }

    /// <summary>
    /// Adds or drops a CHECK (or named NOT NULL) constraint on an existing table.
    /// For ADD CHECK, scans all existing rows and rejects if any row violates the expression.
    /// Replicates in cluster mode; applies directly in standalone mode.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> AlterConstraint(AlterConstraintTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await TryForwardAlterConstraintAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        bool ok = await tableConstraintAlterer.Alter(
            catalogs, database, table, ticket, isClusterMode
        ).ConfigureAwait(false);
        database.Cache?.InvalidateByTableId(database.Id, table.Id);
        return new ExecuteDDLSQLResult(database, ok);
    }

    /// <summary>
    /// Attaches or removes a comment on a table, column, index, or database. Public so a ticket caller
    /// can invoke it without going through SQL. The database target is routed to the registry; the
    /// other three open the table and go through <see cref="CommentSetter"/>.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> Comment(CommentTicket ticket)
    {
        validator.Validate(ticket);

        if (ticket.Target == CommentTarget.Database)
        {
            await CommentDatabase(ticket).ConfigureAwait(false);

            // No descriptor to hand back — the registry write needs no open database — but the
            // operation did succeed, so report that rather than a defaulted (false) result.
            return new ExecuteDDLSQLResult(null!, true);
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await Comment(database, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Applies a table/column/index comment against an already-opened database, forwarding to the
    /// schema leader first when this node is a follower. Serialized against other DDL on this
    /// database by <c>SchemaDdlSemaphore</c>, exactly like <c>ALTER TABLE … SET</c>.
    /// </summary>
    private async Task<ExecuteDDLSQLResult> Comment(DatabaseDescriptor database, CommentTicket ticket)
    {
        bool? forwarded = await TryForwardCommentAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        // The table is opened INSIDE the gate, not before it. Resolving first and validating later
        // is a check-then-act: a drop-and-recreate of the same name in between leaves the validation
        // looking at the old object while the delta — which names its target by table name, not id —
        // lands on the replacement. The apply deliberately no-ops on a missing column/index (for
        // replay safety), so that mismatch would report success while doing nothing at all.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            TableDescriptor table = await tableOpener.Open(database, ticket.TableName!).ConfigureAwait(false);

            bool ok = await commentSetter.Set(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false);
            return new ExecuteDDLSQLResult(database, ok);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    /// <summary>
    /// Sets a database's comment on the cross-database registry. No schema-leader forwarding: the
    /// registry write is a plain replicated KV write, exactly like RENAME DATABASE.
    /// </summary>
    private async Task CommentDatabase(CommentTicket ticket)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        await registry.SetCommentAsync(ticket.DatabaseName, ticket.Comment).ConfigureAwait(false);
    }

    private async Task<AuthCatalog> GetAuthCatalogAsync()
    {
        if (authCatalogTask is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication catalog is unavailable (no shared node was configured)");

        return await authCatalogTask.ConfigureAwait(false);
    }

    /// <summary>
    /// The authorization gate. A no-op when <see cref="CamusDBConfig.AuthenticationEnabled"/> is off
    /// (the default). When on, it rejects an unauthenticated request and then checks the parsed
    /// statement against the caller's privileges before any lock or mutation:
    /// <list type="bullet">
    ///   <item>user/grant administration and database lifecycle DDL require the superuser attribute;</item>
    ///   <item>server-level <c>SHOW</c> statements are allowed to any authenticated caller;</item>
    ///   <item>an in-database statement requires its mapped privilege at the context database's scope
    ///     (a <c>db.*</c> or global grant satisfies it).</item>
    /// </list>
    ///
    /// <para><b>Scope note:</b> enforcement is at the database scope. Table-scoped grants
    /// (<c>db.table</c>) and per-object checks for join / subquery / <c>INSERT … SELECT</c> sources are
    /// a deliberate follow-up; the current gate fails <em>closed</em> (a table-only grant is denied a
    /// database-wide check, never over-permitted).</para>
    /// </summary>
    private async Task EnforceAsync(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!CamusDBConfig.AuthenticationEnabled)
            return;

        Principal? principal = ticket.Principal;
        if (principal is null)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication required");

        // ALTER USER: a caller may always change THEIR OWN password; changing another user's requires
        // superuser. (Principal.UserName is normalized; normalize the AST target to compare.)
        if (ast.nodeType is NodeType.AlterUser)
        {
            string target = ast.leftAst!.yytext!.ToLowerInvariant();
            if (principal.IsSuperuser || string.Equals(target, principal.UserName, StringComparison.Ordinal))
                return;
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege, "Changing another user's password requires a superuser");
        }

        // Other server-level user/grant administration: superuser only.
        if (ast.nodeType is NodeType.CreateUser or NodeType.CreateUserIfNotExists
            or NodeType.DropUser or NodeType.DropUserIfExists or NodeType.Grant or NodeType.Revoke)
        {
            if (!principal.CanAdministerUsers)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "User administration requires a superuser");
            return;
        }

        // Database lifecycle DDL: superuser only (finer database-create privileges are future work).
        if (ast.nodeType is NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists
            or NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists
            or NodeType.CreateDatabaseRelink or NodeType.DropDatabase or NodeType.DropDatabaseIfExists
            or NodeType.RenameDatabase or NodeType.CommentOnDatabase)
        {
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Database administration requires a superuser");
            return;
        }

        // Server-level introspection: any authenticated caller may run these.
        if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors
            or NodeType.ShowOrphanDatabases or NodeType.ShowGrants)
            return;

        // CREATE TABLE is checked at DATABASE scope here — the table does not exist yet, so it cannot
        // be a per-table grant target, and the check must happen before the table is created (not at
        // the post-create re-open). A db.* / global CreateTable grant (or superuser) passes.
        if (ast.nodeType is NodeType.CreateTable or NodeType.CreateTableIfNotExists or NodeType.CreateTableRelink)
        {
            DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
            if (registry.TryResolveId(ticket.DatabaseName, out string createDbId)
                && !principal.HasPrivilege(Privilege.CreateTable, createDbId, tableId: null))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    $"Missing CreateTable privilege on database '{ticket.DatabaseName}'");
            }
            return;
        }

        // Every other in-database statement is enforced PER TABLE at the resolution chokepoint
        // (TableOpener.Open), which sees every referenced table — including join and subquery sources
        // that never reach this statement-level gate. The ambient AuthorizationContext set by the
        // entry point carries the principal and the statement's required privilege down to it.
    }

    /// <summary>
    /// Publishes the request's principal and the statement's required privilege to the ambient
    /// <see cref="AuthorizationContext"/> so the per-table check in <c>TableOpener.Open</c> can consult
    /// them. Must be called <b>synchronously</b> from the entry method (a synchronous
    /// <see cref="AsyncLocal{T}"/> write flows to the caller's execution context and thus down to the
    /// table-open callees; a write inside an awaited method would not). Cleared to defaults when auth
    /// is off so no stale scope from a pooled context leaks in.
    /// </summary>
    private void SetAuthorizationScope(ExecuteSQLTicket ticket, NodeAst ast)
    {
        AuthorizationContext.Current = CamusDBConfig.AuthenticationEnabled
            ? new AuthorizationScope(ticket.Principal, MapRequiredPrivilege(ast.nodeType))
            : default;
    }

    /// <summary>Maps an in-database statement to the privilege it requires, or null when it needs none.</summary>
    private static Privilege? MapRequiredPrivilege(NodeType nodeType) => nodeType switch
    {
        NodeType.Select => Privilege.Select,
        NodeType.Insert => Privilege.Insert,
        NodeType.Update => Privilege.Update,
        NodeType.Delete => Privilege.Delete,
        NodeType.CreateTable or NodeType.CreateTableIfNotExists or NodeType.CreateTableRelink => Privilege.CreateTable,
        NodeType.DropTable or NodeType.DropTableIfExists => Privilege.Drop,
        NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists
            or NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists
            or NodeType.AlterTableDropIndex => Privilege.Index,
        NodeType.AlterTableAddColumn or NodeType.AlterTableDropColumn or NodeType.AlterTableRenameTo
            or NodeType.AlterTableRenameColumn or NodeType.AlterTableRenameIndex
            or NodeType.AlterTableAddConstraintCheck or NodeType.AlterTableDropConstraint
            or NodeType.AlterTableSetNotNull or NodeType.AlterTableDropNotNull
            or NodeType.AlterTableAddPrimaryKey or NodeType.AlterTableDropPrimaryKey
            or NodeType.AlterTableSetSetting or NodeType.AnalyzeTable
            or NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex => Privilege.Alter,
        NodeType.ShowTables or NodeType.ShowColumns or NodeType.ShowIndexes or NodeType.ShowCreateTable
            or NodeType.ShowDatabase or NodeType.ShowOrphanTables => Privilege.Select,
        _ => null,
    };

    private AuthService RequireAuthService()
    {
        if (authService is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication service is unavailable (no shared node was configured)");
        return authService;
    }

    /// <summary>Verifies credentials and returns an opaque bearer token (see <see cref="AuthService.LoginAsync"/>).
    /// <paramref name="source"/> is the caller's origin (e.g. remote IP) for per-source rate limiting.</summary>
    public Task<string> LoginAsync(string user, string password, string source = "") => RequireAuthService().LoginAsync(user, password, source);

    /// <summary>Resolves a bearer token to a <see cref="Principal"/>, or throws AuthenticationFailed.</summary>
    public Task<Principal> ResolvePrincipalAsync(string? bearer) => RequireAuthService().ResolvePrincipalAsync(bearer);

    /// <summary>Revokes the presented token (logout).</summary>
    public Task LogoutAsync(string? bearer) => RequireAuthService().LogoutAsync(bearer);

    /// <summary>
    /// If <see cref="CamusDBConfig.AuthenticationEnabled"/> is on, ensures the catalog has at least one
    /// user by seeding the configured bootstrap superuser when it is empty. Fails startup (fail-closed)
    /// when auth is enabled, the catalog is empty, and no bootstrap secret was supplied — never opens an
    /// unauthenticated administration window. A no-op when auth is disabled or a user already exists.
    /// </summary>
    public async Task EnsureBootstrapSuperuserAsync()
    {
        if (!CamusDBConfig.AuthenticationEnabled || authCatalogTask is null)
            return;

        AuthCatalog catalog = await GetAuthCatalogAsync().ConfigureAwait(false);
        if (await catalog.UserCountAsync().ConfigureAwait(false) > 0)
            return;

        if (string.IsNullOrEmpty(CamusDBConfig.BootstrapSuperuser) || string.IsNullOrEmpty(CamusDBConfig.BootstrapSuperuserPassword))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                "Authentication is enabled with an empty user catalog but no bootstrap superuser is configured; " +
                "refusing to start without an administrator (set the bootstrap superuser secret).");

        bool created = await catalog.TryBootstrapSuperuserAsync(
            CamusDBConfig.BootstrapSuperuser,
            PasswordHasher.Hash(CamusDBConfig.BootstrapSuperuserPassword)).ConfigureAwait(false);

        if (created && logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Bootstrap superuser '{User}' created", CamusDBConfig.BootstrapSuperuser);
    }

    /// <summary>
    /// Creates a server-level user in the shared auth catalog. The cleartext password (if any) is hashed
    /// here and never persisted or logged; the ticket carries it no further. Server-level — returns no
    /// descriptor.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> CreateUser(CreateUserTicket ticket)
    {
        validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        Credential? credential = ticket.Password is null ? null : PasswordHasher.Hash(ticket.Password);
        await auth.CreateUserAsync(ticket.UserName, credential, ticket.IfNotExists).ConfigureAwait(false);

        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>Rotates a user's password verifier and advances its credential epoch.</summary>
    public async Task<ExecuteDDLSQLResult> AlterUser(AlterUserTicket ticket)
    {
        validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        await auth.SetPasswordAsync(ticket.UserName, PasswordHasher.Hash(ticket.Password)).ConfigureAwait(false);

        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>Drops a user and all its grants in one catalog transaction.</summary>
    public async Task<ExecuteDDLSQLResult> DropUser(DropUserTicket ticket)
    {
        validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        await auth.DropUserAsync(ticket.UserName, ticket.IfExists).ConfigureAwait(false);

        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Applies a <c>GRANT</c>/<c>REVOKE</c>. Resolves the grant object's name(s) to immutable ids first
    /// (a database via the registry; a table by opening the target database's catalog) so the grant is
    /// bound to the id, not the name, and never resurrects on a dropped-and-recreated object.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> Grant(GrantTicket ticket)
    {
        validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        GrantScope scope = await ResolveGrantScopeAsync(ticket).ConfigureAwait(false);
        await auth.GrantAsync(ticket.UserName, scope, ticket.Privileges, ticket.Revoke).ConfigureAwait(false);

        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Turns a grant ticket's scope names into an id-bound <see cref="GrantScope"/>. The database must
    /// exist (resolved through the registry); a table scope additionally opens the target database and
    /// resolves the table's id. Global scope needs no resolution.
    /// </summary>
    private async Task<GrantScope> ResolveGrantScopeAsync(GrantTicket ticket)
    {
        switch (ticket.ScopeKind)
        {
            case GrantScopeKind.Global:
                return new GrantScope { Kind = GrantScopeKind.Global };

            case GrantScopeKind.Database:
                {
                    DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
                    DatabaseRegistryEntry? entry = await registry.TryResolveEntryAsync(ticket.DatabaseName).ConfigureAwait(false);
                    if (entry is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.DatabaseDoesntExist,
                            $"Database '{ticket.DatabaseName}' does not exist");

                    return new GrantScope
                    {
                        Kind = GrantScopeKind.Database,
                        DatabaseId = entry.Id,
                        DatabaseName = entry.Name,
                    };
                }

            case GrantScopeKind.Table:
                {
                    DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
                    DatabaseRegistryEntry? entry = await registry.TryResolveEntryAsync(ticket.DatabaseName).ConfigureAwait(false);
                    if (entry is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.DatabaseDoesntExist,
                            $"Database '{ticket.DatabaseName}' does not exist");

                    // Open the TARGET database (not the empty context database) to resolve the table id.
                    DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
                    using DatabaseUseHandle _ = database.Use();
                    TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

                    return new GrantScope
                    {
                        Kind = GrantScopeKind.Table,
                        DatabaseId = entry.Id,
                        DatabaseName = entry.Name,
                        TableId = table.Id,
                        TableName = table.Name,
                    };
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown grant scope kind {ticket.ScopeKind}");
        }
    }

    /// <summary>
    /// Returns the grants for <paramref name="userName"/> as rows for <c>SHOW GRANTS</c>. Server-level:
    /// reads the auth catalog and needs no open database.
    /// </summary>
    internal async Task<(IReadOnlyList<GrantRecord> Grants, bool UserExists)> ListGrantsForShowAsync(string userName)
    {
        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        UserRecord? user = await auth.TryGetUserAsync(userName).ConfigureAwait(false);
        if (user is null)
            return ([], false);

        return (await auth.ListGrantsAsync(userName).ConfigureAwait(false), true);
    }

    /// <summary>
    /// Test-only: resolves a registry entry through <b>this executor's own</b> registry instance,
    /// exercising its cache-coherence path. Exists because cross-node registry behavior is otherwise
    /// unobservable — each node owns a private <see cref="DatabaseRegistry"/>, and asserting that one
    /// node sees another's write is the whole point of those tests. Not a production seam: callers
    /// should go through <c>OpenDatabase</c> or <c>SHOW DATABASE</c>.
    /// </summary>
    internal async Task<DatabaseRegistryEntry?> ResolveRegistryEntryForTestingAsync(string databaseName)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        return await registry.TryResolveEntryAsync(databaseName).ConfigureAwait(false);
    }

    /// <summary>Maps a parsed statement's node type to the bounded <c>statement</c> metric family tag.</summary>
    private static string MapStatementFamily(NodeType nodeType) => nodeType switch
    {
        NodeType.Select => ServerDiagnostics.Tags.Statement.Select,
        NodeType.Insert => ServerDiagnostics.Tags.Statement.Insert,
        NodeType.Update => ServerDiagnostics.Tags.Statement.Update,
        NodeType.Delete => ServerDiagnostics.Tags.Statement.Delete,
        _ => ServerDiagnostics.Tags.Statement.Other,
    };

    public async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        SetAuthorizationScope(ticket, ast);
        await EnforceAsync(ticket, ast).ConfigureAwait(false);

        using ServerDiagnostics.ExecuteScope executeScope = ServerDiagnostics.MeasureExecute(
            ServerDiagnostics.Tags.Operation.Ddl, ServerDiagnostics.Tags.Statement.Other);

        // CREATE/DROP/RENAME DATABASE do not require an open database context.
        if (ast.nodeType is NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists
                           or NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists)
        {
            bool isBranch = ast.nodeType is NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists;
            bool ifNotExists = ast.nodeType is NodeType.CreateDatabaseIfNotExists or NodeType.CreateDatabaseBranchIfNotExists;
            string dbName = ast.leftAst!.yytext!;
            string? branchFrom = isBranch ? ast.rightAst!.yytext! : null;
            DatabaseDescriptor created = await CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists, branchFrom)).ConfigureAwait(false);
            return new ExecuteDDLSQLResult(created, true);
        }

        if (ast.nodeType is NodeType.CreateDatabaseRelink)
        {
            string dbName = ast.leftAst!.yytext!;
            string orphanId = Controllers.DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst!.yytext!);
            DatabaseDescriptor relinked = await RelinkDatabase(new RelinkDatabaseTicket(dbName, orphanId)).ConfigureAwait(false);
            return new ExecuteDDLSQLResult(relinked, true);
        }

        if (ast.nodeType is NodeType.DropDatabase or NodeType.DropDatabaseIfExists)
        {
            string dbName = ast.leftAst!.yytext!;
            bool ifExists = ast.nodeType == NodeType.DropDatabaseIfExists;
            bool force = ast.yytext == "force";
            await DropDatabase(new DropDatabaseTicket(dbName, ifExists, force)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.RenameDatabase)
        {
            string oldName = ast.leftAst!.yytext!;
            string newName = ast.rightAst!.yytext!;
            await RenameDatabase(new RenameDatabaseTicket(oldName, newName)).ConfigureAwait(false);
            return default;
        }

        // A database comment lives on the cross-database registry entry, so it is handled here —
        // before any database is opened — alongside the other database-scoped DDL.
        if (ast.nodeType is NodeType.CommentOnDatabase)
        {
            CommentTicket databaseCommentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
            validator.Validate(databaseCommentTicket);
            await CommentDatabase(databaseCommentTicket).ConfigureAwait(false);
            return default;
        }

        // User and grant DDL are server-level: they live in the shared _system/auth keyspace and open
        // no database of their own (a table-scoped GRANT opens its target database itself, inside Grant).
        if (ast.nodeType is NodeType.CreateUser or NodeType.CreateUserIfNotExists)
            return await CreateUser(sqlExecutor.CreateCreateUserTicket(ticket, ast)).ConfigureAwait(false);

        if (ast.nodeType is NodeType.AlterUser)
            return await AlterUser(sqlExecutor.CreateAlterUserTicket(ticket, ast)).ConfigureAwait(false);

        if (ast.nodeType is NodeType.DropUser or NodeType.DropUserIfExists)
            return await DropUser(sqlExecutor.CreateDropUserTicket(ast)).ConfigureAwait(false);

        if (ast.nodeType is NodeType.Grant or NodeType.Revoke)
            return await Grant(sqlExecutor.CreateGrantTicket(ast)).ConfigureAwait(false);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        switch (ast.nodeType)
        {
            case NodeType.CommentOnTable:
            case NodeType.CommentOnColumn:
            case NodeType.CommentOnIndex:
                {
                    CommentTicket commentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
                    validator.Validate(commentTicket);
                    return await Comment(database, commentTicket).ConfigureAwait(false);
                }

            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = sqlExecutor.CreateCreateTableTicket(ticket, ast);
                    validator.Validate(createTableTicket);

                    bool? forwarded = await TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    // Allocate before the DDL transaction — only the proposer/leader allocates;
                    // the id is carried in the replicated payload so every follower applies the same id.
                    DatabaseRegistry sqlRegistry = await registryTask.ConfigureAwait(false);
                    string sqlTableId = await sqlRegistry.AllocateTableIdAsync().ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, createTableTicket, tx, sqlTableId).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            case NodeType.CreateTableRelink:
                {
                    RelinkTableTicket relinkTicket = new(
                        ticket.DatabaseName,
                        ast.leftAst!.yytext!,
                        Controllers.DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst!.yytext!));

                    // Delegate to the executor method so fencing, forwarding, and orphan-load live in one place.
                    bool relinked = await RelinkTable(relinkTicket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, relinked);
                }

            case NodeType.AlterTableAddColumn:
            case NodeType.AlterTableDropColumn:
                {
                    AlterTableTicket alterTableTicket = sqlExecutor.CreateAlterTableTicket(ticket, ast);
                    validator.Validate(alterTableTicket);

                    bool? forwarded = await TryForwardAlterTableAsync(database, alterTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await tableOpener.Open(database, alterTableTicket.TableName).ConfigureAwait(false);

                    if (isClusterMode && alterTableTicket.Operation == AlterTableOperation.AddColumn)
                    {
                        bool ok = await ExecuteClusterAddColumnAsync(database, table, alterTableTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableColumnAlterer.Alter(queryExecutor, database, table, alterTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddIndex:
            case NodeType.AlterTableAddIndexIfNotExists:
            case NodeType.AlterTableAddUniqueIndex:
            case NodeType.AlterTableAddUniqueIndexIfNotExists:
            case NodeType.AlterTableDropIndex:
            case NodeType.AlterTableAddPrimaryKey:
            case NodeType.AlterTableDropPrimaryKey:
                {
                    AlterIndexTicket alterIndexTicket = sqlExecutor.CreateAlterIndexTicket(ticket, ast);
                    validator.Validate(alterIndexTicket);

                    bool? forwarded = await TryForwardAlterIndexAsync(database, alterIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await tableOpener.Open(database, alterIndexTicket.TableName).ConfigureAwait(false);

                    bool sqlAddIndex = alterIndexTicket.Operation is
                        AlterIndexOperation.AddIndex or
                        AlterIndexOperation.AddUniqueIndex or
                        AlterIndexOperation.AddPrimaryKey;

                    if (isClusterMode && sqlAddIndex)
                    {
                        bool ok = await ExecuteClusterAddIndexAsync(database, table, alterIndexTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    // Both cluster (non-add) and standalone paths require the two-phase DDL sequence
                    // so Phase 2 (ReplicateIndexChangeAsync) persists the schema change across
                    // close/reopen. ExecuteDdlInTransaction is single-phase and skips that step.
                    bool indexExistedBefore = table.Indexes.ContainsKey(alterIndexTicket.IndexName);
                    bool compensateOnAbort = sqlAddIndex && !indexExistedBefore;
                    {
                        bool ok = await ExecuteClusteredIndexDdlAsync(
                            database, table, alterIndexTicket, compensateOnAbort,
                            tx => tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx)
                        ).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }
                }

            case NodeType.AlterTableRenameColumn:
                {
                    AlterTableTicket renameColumnTicket = sqlExecutor.CreateAlterTableTicket(ticket, ast);
                    validator.Validate(renameColumnTicket);

                    bool? forwarded = await TryForwardAlterTableAsync(database, renameColumnTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor tableForRenameCol = await tableOpener.Open(database, renameColumnTicket.TableName).ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableColumnAlterer.Alter(queryExecutor, database, tableForRenameCol, renameColumnTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameCol.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableRenameIndex:
                {
                    AlterIndexTicket renameIndexTicket = sqlExecutor.CreateAlterIndexTicket(ticket, ast);
                    validator.Validate(renameIndexTicket);

                    bool? forwarded = await TryForwardAlterIndexAsync(database, renameIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor tableForRenameIdx = await tableOpener.Open(database, renameIndexTicket.TableName).ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableIndexAlterer.Alter(queryExecutor, database, tableForRenameIdx, renameIndexTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameIdx.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddConstraintCheck:
            case NodeType.AlterTableDropConstraint:
            case NodeType.AlterTableSetNotNull:
            case NodeType.AlterTableDropNotNull:
                {
                    TableDescriptor tableForConstraint = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);

                    AlterConstraintTicket alterConstraintTicket = sqlExecutor.CreateAlterConstraintTicket(ticket, ast, tableForConstraint.Schema);
                    validator.Validate(alterConstraintTicket);

                    bool? constraintForwarded = await TryForwardAlterConstraintAsync(database, alterConstraintTicket).ConfigureAwait(false);
                    if (constraintForwarded is not null)
                        return new ExecuteDDLSQLResult(database, constraintForwarded.Value);

                    bool constraintOk = await tableConstraintAlterer.Alter(
                        catalogs, database, tableForConstraint, alterConstraintTicket, isClusterMode
                    ).ConfigureAwait(false);
                    database.Cache?.InvalidateByTableId(database.Id, tableForConstraint.Id);
                    return new ExecuteDDLSQLResult(database, constraintOk);
                }

            case NodeType.AlterTableSetSetting:
                {
                    // Validation (recognized key, boolean value) happens at ticket creation.
                    AlterTableSettingsTicket settingsTicket = sqlExecutor.CreateAlterTableSettingsTicket(ticket, ast);
                    return await AlterTableSettings(database, settingsTicket).ConfigureAwait(false);
                }

            case NodeType.AlterTableRenameTo:
                {
                    string oldTableName = ast.leftAst!.yytext!;
                    string newTableName = ast.rightAst!.yytext!;
                    RenameTableTicket renameTableTicket = new(ticket.DatabaseName, oldTableName, newTableName);
                    validator.Validate(renameTableTicket);

                    bool? forwarded = await TryForwardRenameTableAsync(database, renameTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor renameTableDesc = await tableOpener.Open(database, oldTableName).ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await catalogs.RenameTable(database, renameTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTableDesc.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.DropTable:
            case NodeType.DropTableIfExists:
                {
                    DropTableTicket dropTableTicket = sqlExecutor.CreateDropTableTicket(ticket, ast);
                    validator.Validate(dropTableTicket);

                    bool? forwarded = await TryForwardDropTableAsync(database, dropTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    if (dropTableTicket.IfExists && !catalogs.TableExists(database, dropTableTicket.TableName))
                        return new(database, false);

                    TableDescriptor table = await tableOpener.Open(database, dropTableTicket.TableName).ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, dropTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
                    ).ConfigureAwait(false);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown DDL AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    #region DML

    public async Task<InsertResult> Insert(InsertTicket ticket)
    {
        validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int inserted = await rowInserter.Insert(database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackInsert(database, table, inserted, ticket.Values);
        return new(database, table, inserted);
    }

    /// <summary>
    /// Updates rows specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<UpdateResult> Update(UpdateTicket ticket)
    {
        validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int updated = await rowUpdater.Update(queryExecutor, database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackUpdate(database, table, updated, ticket.PlainValues);
        return new(database, table, updated);
    }

    /// <summary>
    /// Deletes rows specifying a filter criteria
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<DeleteResult> Delete(DeleteTicket ticket)
    {
        validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int deleted = await rowDeleter.Delete(queryExecutor, database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackDelete(database, table, deleted);
        return new(database, table, deleted);
    }

    /// <summary>
    /// Queries table data specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)> Query(QueryTicket ticket)
    {
        validator.Validate(ticket);
        ticket.TxnState.MarkStatementExecuted();

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        return (database, queryExecutor.Query(database, table, ticket));
    }

    /// <summary>
    /// Queries a table by the row's id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        return queryExecutor.QueryById(database, table, ticket);
    }

    /// <summary>
    /// Execute a SQL statement that doesn't return rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of inserted/modified/deleted rows</returns>
    public async Task<ExecuteNonSQLResult> ExecuteNonSQLQuery(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        // Executor stage timing for the write path (parse+plan+stage, exclusive of transport/commit
        // transport). Covers every return path of this method; a no-op when diagnostics are disabled.
        using ServerDiagnostics.ExecuteScope executeScope = ServerDiagnostics.MeasureExecute(
            ServerDiagnostics.Tags.Operation.NonQuery, MapStatementFamily(ast.nodeType));
        using System.Diagnostics.Activity? executeSpan = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Execute);
        executeSpan?.SetTag("statement", MapStatementFamily(ast.nodeType));

        SetAuthorizationScope(ticket, ast);
        await EnforceAsync(ticket, ast).ConfigureAwait(false);

        // DROP/RENAME DATABASE do not require an open database context — dispatch before Open so
        // we don't accidentally load the descriptor we're about to destroy or rename.
        if (ast.nodeType is NodeType.DropDatabase or NodeType.DropDatabaseIfExists)
        {
            string targetName = ast.leftAst!.yytext!;
            bool ifExists = ast.nodeType == NodeType.DropDatabaseIfExists;
            bool force = ast.yytext == "force";
            await DropDatabase(new DropDatabaseTicket(targetName, ifExists, force)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.RenameDatabase)
        {
            string oldName = ast.leftAst!.yytext!;
            string newName = ast.rightAst!.yytext!;
            await RenameDatabase(new RenameDatabaseTicket(oldName, newName)).ConfigureAwait(false);
            return default;
        }

        // COMMENT ON DATABASE is database-scoped like the two above: it names its target in the SQL
        // and touches only the cross-database registry, so it must not open a descriptor. Clients
        // route no-rows statements to whichever endpoint they use for non-SELECT SQL, so every
        // statement reachable through ExecuteDDLSQL's pre-open block has to be reachable here too.
        if (ast.nodeType is NodeType.CommentOnDatabase)
        {
            CommentTicket databaseCommentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
            validator.Validate(databaseCommentTicket);
            await CommentDatabase(databaseCommentTicket).ConfigureAwait(false);
            return default;
        }

        // User/grant DDL is server-level, and a client may route it to this no-rows endpoint just as
        // easily as to /execute-sql-ddl, so it must be reachable here too (mirrors ExecuteDDLSQL).
        if (ast.nodeType is NodeType.CreateUser or NodeType.CreateUserIfNotExists)
        {
            await CreateUser(sqlExecutor.CreateCreateUserTicket(ticket, ast)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.AlterUser)
        {
            await AlterUser(sqlExecutor.CreateAlterUserTicket(ticket, ast)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.DropUser or NodeType.DropUserIfExists)
        {
            await DropUser(sqlExecutor.CreateDropUserTicket(ast)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.Grant or NodeType.Revoke)
        {
            await Grant(sqlExecutor.CreateGrantTicket(ast)).ConfigureAwait(false);
            return default;
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Mark the transaction as having executed a statement for every statement type except
        // SET TRANSACTION / SET TRANSACTION LOCKING — those must be the first statement per standard
        // SQL semantics, so they are exempt from the gate (mirrors ExecuteSQLQuery). A client routes
        // these no-rows statements to whichever endpoint it uses for non-SELECT SQL, which is often
        // this one.
        if (ast.nodeType != NodeType.SetTransaction && ast.nodeType != NodeType.SetTransactionLocking)
            ticket.TxnState.MarkStatementExecuted();

        // Retry boundary: two transient errors, two different retry owners.
        //   CADB0503 SchemaCatchingUp — retried HERE, inside ExecuteNonSQLQuery. The fence fires
        //     in TableOpener.Open before any write or schema-pin, so the in-flight transaction is
        //     unmodified and the same tx is safely reused on each attempt.
        //   CADB0502/CADB0504/CADB0505 serialization failures — retried by the HTTP controller for
        //     autocommit statements (via SerializableRetryHelper). The controller replays from a
        //     fresh BeginAsync each time. Explicit multi-statement transactions surface these codes
        //     to the caller, which owns the retry loop.
        const int MaxFenceRetries = 3;

        switch (ast.nodeType)
        {
            case NodeType.Insert:
                {
                    InsertTicket insertTicket = await sqlExecutor.CreateInsertTicket(this, database, ticket, ast).ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await tableOpener.Open(database, insertTicket.TableName).ConfigureAwait(false);
                            PinSchemaVersion(database, table, ticket.TxnState);
                            int inserted = await rowInserter.Insert(database, table, insertTicket).ConfigureAwait(false);
                            // Track statistics on the SQL path too, mirroring the ticket-based Insert()
                            // wrapper — otherwise SQL DML never updates row/mutation counts and auto-analyze
                            // never triggers for the common SQL workload.
                            statisticsManager.TrackInsert(database, table, inserted, insertTicket.Values);
                            return new(database, table, inserted);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Update:
                {
                    UpdateTicket updateTicket = sqlExecutor.CreateUpdateTicket(ticket, ast);
                    updateTicket = await RewriteUpdateSubqueriesAsync(database, updateTicket, ticket).ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await tableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);
                            PinSchemaVersion(database, table, ticket.TxnState);
                            int updated = await rowUpdater.Update(queryExecutor, database, table, updateTicket).ConfigureAwait(false);
                            statisticsManager.TrackUpdate(database, table, updated, updateTicket.PlainValues);
                            return new(database, table, updated);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Delete:
                {
                    DeleteTicket deleteTicket = sqlExecutor.CreateDeleteTicket(ticket, ast);

                    // Resolve any scalar / IN / NOT IN subquery in the WHERE clause to a literal
                    // before evaluation (DELETE has no subquery rewrite of its own, unlike SELECT).
                    // Done once here, outside the fence-retry loop, so the inner query is not
                    // re-executed on each SchemaCatchingUp retry.
                    if (deleteTicket.Where is not null)
                    {
                        NodeAst rewrittenWhere = await subqueryRewriter
                            .RewriteWhereExpressionAsync(database, deleteTicket.Where, ticket)
                            .ConfigureAwait(false);

                        if (!ReferenceEquals(rewrittenWhere, deleteTicket.Where))
                            deleteTicket = new DeleteTicket(
                                txnState: deleteTicket.TxnState,
                                databaseName: deleteTicket.DatabaseName,
                                tableName: deleteTicket.TableName,
                                where: rewrittenWhere,
                                filters: deleteTicket.Filters,
                                parameters: deleteTicket.Parameters,
                                limit: deleteTicket.Limit);
                    }

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await tableOpener.Open(database, deleteTicket.TableName).ConfigureAwait(false);
                            PinSchemaVersion(database, table, ticket.TxnState);
                            int deleted = await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false);
                            statisticsManager.TrackDelete(database, table, deleted);
                            return new(database, table, deleted);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.EvictCache:
                {
                    string rawName = ast.yytext ?? string.Empty;
                    string cacheName = rawName.Length >= 2 && rawName[0] == rawName[^1] && (rawName[0] == '\'' || rawName[0] == '"')
                        ? rawName[1..^1]
                        : rawName;
                    // Normalize to lowercase to match the hint grammar's ToLowerInvariant on identifier tokens.
                    database.Cache?.InvalidateCacheName(database.Id, cacheName.ToLowerInvariant());
                    return new(database, null!, 0);
                }

            case NodeType.EvictCacheAll:
                {
                    database.Cache?.InvalidateDatabase(database.Id);
                    return new(database, null!, 0);
                }

            case NodeType.CommentOnTable:
            case NodeType.CommentOnColumn:
            case NodeType.CommentOnIndex:
                {
                    CommentTicket commentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
                    validator.Validate(commentTicket);
                    await Comment(database, commentTicket).ConfigureAwait(false);
                    return new(database, null!, 0);
                }

            case NodeType.SetTransaction:
            case NodeType.SetTransactionLocking:
                ApplySetTransactionStatement(ast, ticket);
                return new(database, null!, 0);

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Applies an <c>AS OF SYSTEM TIME</c> clause (carried on the SELECT node's
    /// <see cref="NodeAst.extendedSeven"/> slot) by returning a ticket whose transaction is a cheap
    /// read-only snapshot pinned to the resolved historical timestamp. Returns the ticket unchanged
    /// when the SELECT has no time-travel clause.
    /// <para>
    /// Time-travel is only supported for an autocommit read-only SELECT: the incoming transaction must
    /// be the lock-free zero-identity snapshot the autocommit read path creates
    /// (<see cref="KvTransaction.CreateReadOnly"/> / <see cref="KvTransaction.CreateSnapshotReadOnly"/>).
    /// A transaction that already holds a live Kahuna session — an explicit multi-statement transaction
    /// or a promoted key-range-sharded read — is pinned to its own read snapshot and cannot be
    /// retroactively moved to an arbitrary past point, so the clause is rejected there rather than
    /// silently ignored.
    /// </para>
    /// </summary>
    private ExecuteSQLTicket ApplyAsOfSystemTime(NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (ast.extendedSeven is null)
            return ticket;

        KvTransaction current = ticket.TxnState;

        if (!current.IsReadOnly || current.TransactionId != HLCTimestamp.Zero)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidAsOfSystemTime,
                "AS OF SYSTEM TIME is only supported for an autocommit read-only SELECT, not inside an " +
                "explicit or promoted transaction.");

        if (sharedNode is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "AS OF SYSTEM TIME requires a storage node to resolve the snapshot timestamp.");

        HLCTimestamp now = sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        HLCTimestamp snapshotT = AsOfSystemTimeResolver.Resolve(ast.extendedSeven, ticket.Parameters, now);

        KvTransaction snapshotTx = KvTransaction.CreateSnapshotReadOnly(snapshotT);
        return new ExecuteSQLTicket(snapshotTx, ticket.DatabaseName, ticket.Sql, ticket.Parameters);
    }

    /// <summary>
    /// Execute a SQL statement that returns rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<(DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)> ExecuteSQLQuery(ExecuteSQLTicket ticket, CacheMetadataHolder? metaOut = null, QuerySchemaHolder? schemaOut = null)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        SetAuthorizationScope(ticket, ast);
        await EnforceAsync(ticket, ast).ConfigureAwait(false);

        // SHOW DATABASES does not require a database context — resolve the registry and return.
        if (ast.nodeType == NodeType.ShowDatabases)
        {
            DatabaseRegistry reg = await registryTask.ConfigureAwait(false);
            string? dbPattern = UnquoteLikePattern(ast.leftAst?.yytext);
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowDatabasesSchema;
            return (null!, schemaQuerier.ShowDatabases(reg.List(), dbPattern));
        }

        // SHOW ORPHAN DATABASES lists recoverable dropped databases from the registry — no db context.
        if (ast.nodeType == NodeType.ShowOrphanDatabases)
        {
            DatabaseRegistry reg = await registryTask.ConfigureAwait(false);
            List<OrphanDatabaseRecord> orphans = await reg.LoadDatabaseOrphansAsync().ConfigureAwait(false);
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowOrphanDatabasesSchema;
            return (null!, schemaQuerier.ShowOrphanDatabases(orphans));
        }

        // SHOW BRANCHES and SHOW ANCESTORS operate on the registry directly.
        if (ast.nodeType is NodeType.ShowBranches or NodeType.ShowAncestors)
        {
            string targetName = ast.leftAst!.yytext!;
            DatabaseRegistry reg = await registryTask.ConfigureAwait(false);
            DatabaseRegistryEntry? target = await reg.TryResolveEntryAsync(targetName).ConfigureAwait(false);
            if (target is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{targetName}' does not exist");
            IReadOnlyList<DatabaseRegistryEntry> allEntries = await reg.ScanAllEntriesAsync().ConfigureAwait(false);
            if (ast.nodeType == NodeType.ShowBranches)
            {
                if (schemaOut is not null)
                    schemaOut.Schema = DerivedTableSchemaBuilder.ShowBranchesSchema;
                return (null!, schemaQuerier.ShowBranches(allEntries, target));
            }
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowAncestorsSchema;
            return (null!, schemaQuerier.ShowAncestors(target, allEntries));
        }

        // SHOW GRANTS reads the server-level auth catalog — no database context.
        if (ast.nodeType == NodeType.ShowGrants)
        {
            // `SHOW GRANTS` (no FOR) defaults to the authenticated caller. Without a principal (auth
            // disabled) there is no "current user", so the bare form needs an explicit FOR.
            string? grantUser = ast.leftAst?.yytext ?? ticket.Principal?.UserName;
            if (grantUser is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SHOW GRANTS without FOR requires an authenticated session; use SHOW GRANTS FOR <user>");

            (IReadOnlyList<GrantRecord> grants, bool userExists) = await ListGrantsForShowAsync(grantUser).ConfigureAwait(false);
            if (!userExists)
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{grantUser}' does not exist");

            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowGrantsSchema;
            return (null!, schemaQuerier.ShowGrants(grantUser, grants));
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);

        // Mark the transaction as having executed a statement for every statement type except
        // SET TRANSACTION — that one must be the first statement per standard SQL semantics.
        if (ast.nodeType != NodeType.SetTransaction && ast.nodeType != NodeType.SetTransactionLocking)
            ticket.TxnState.MarkStatementExecuted();

        switch (ast.nodeType)
        {
            case NodeType.Select:
                {
                    // FROM-less SELECT (no table node): project scalar expressions against a single
                    // synthetic row. The grammar only admits a projection list plus optional
                    // LIMIT/OFFSET here, so there is no scan, join, filter, or grouping to plan.
                    if (ast.rightAst is null)
                        return (database, await ExecuteFromlessSelectAsync(database, ast, ticket, schemaOut).ConfigureAwait(false));

                    // AS OF SYSTEM TIME: rebind the whole statement onto a read-only snapshot pinned
                    // to the requested past timestamp, so every scan/join/subquery below reads that
                    // one historical point. No-op when the SELECT carries no time-travel clause.
                    ticket = ApplyAsOfSystemTime(ast, ticket);

                    SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(ast);

                    // Detect an inner subquery that carries a {cache=name} hint when the outer
                    // SELECT has none. SubqueryRewriter executes all inner subqueries live and
                    // discards inner cache hints — they are inert. Surface the bypass in the
                    // outer response so the caller sees an explicit "inner-hint" bypass rather
                    // than silence (which looks identical to an un-hinted outer query).
                    if (selectQuery.CacheHint is null && metaOut is not null)
                    {
                        CacheHintOptions? innerHint = FindInnerSubqueryCacheHint(selectQuery.Where?.Expression);
                        if (innerHint is not null)
                        {
                            metaOut.CacheName = innerHint.CacheName;
                            metaOut.Status = QueryCacheStatus.Bypass;
                            metaOut.BypassReason = QueryCacheBypassReason.InnerHint;
                        }
                    }

                    // Extract eligible IN / NOT IN subqueries as semi/anti-join specs
                    // before SubqueryRewriter materialises them.
                    (selectQuery, List<SemiJoinSpec> semiJoinSpecs) = await semiJoinAnalyzer
                        .AnalyzeAsync(database, selectQuery, ticket)
                        .ConfigureAwait(false);

                    selectQuery = await subqueryRewriter
                        .RewriteSelectQueryAsync(database, selectQuery, ticket)
                        .ConfigureAwait(false);
                    BoundSelectQuery boundQuery = await queryBinder.BindAsync(database, selectQuery).ConfigureAwait(false);
                    (selectQuery, ExistsSubqueryRegistry? existsRegistry) = await existsSubqueryPreparer
                        .PrepareAsync(
                            database,
                            selectQuery,
                            boundQuery.Sources,
                            boundQuery.DerivedSources,
                            ticket)
                        .ConfigureAwait(false);
                    boundQuery = new BoundSelectQuery(
                        selectQuery,
                        boundQuery.Sources,
                        boundQuery.RowNames,
                        boundQuery.DerivedSources);
                    IReadOnlyList<SemiJoinSpec>? specs = semiJoinSpecs.Count > 0 ? semiJoinSpecs : null;
                    QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(boundQuery, ticket, existsRegistry, specs);
                    PinSchemaVersions(database, boundQuery.Sources, ticket.TxnState);

                    // Join queries bypass the result cache: caching a multi-table result
                    // requires fencing ALL involved tables' row keyspaces, not just one.
                    // Until the multi-keyspace fence is implemented, any {cache=name} hint
                    // on a join executes live every time. Surface the bypass so the response
                    // is not silently identical to an unhinted query.
                    if (boundQuery.IsMultiSource)
                    {
                        if (queryTicket.CacheHint is { } joinHint && metaOut is not null)
                        {
                            metaOut.CacheName = joinHint.CacheName;
                            metaOut.Status = QueryCacheStatus.Bypass;
                            metaOut.BypassReason = QueryCacheBypassReason.Join;
                        }
                        if (schemaOut is not null)
                            schemaOut.Schema = DerivedTableSchemaBuilder.Build(selectQuery, boundQuery);
                        return (database, queryExecutor.ExecuteJoinQuery(database, boundQuery, queryTicket));
                    }

                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.Build(selectQuery, boundQuery);

                    TableDescriptor table = boundQuery.PrimaryTable;

                    return (database, queryExecutor.Query(database, table, queryTicket, metaOut));
                }

            case NodeType.ShowTables:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowTablesSchema;
                    string? tablePattern = UnquoteLikePattern(ast.leftAst?.yytext);
                    return (database, schemaQuerier.ShowTables(database, tablePattern));
                }

            case NodeType.ShowOrphanTables:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowOrphanTablesSchema;
                    return (database, schemaQuerier.ShowOrphanTables(database));
                }

            case NodeType.ShowColumns:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowColumnsSchema;
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowColumns(table));
                }

            case NodeType.ShowIndexes:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowIndexesSchema;
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowIndexes(table));
                }

            case NodeType.ShowCreateTable:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowCreateTableSchema;
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowCreateTable(table));
                }

            case NodeType.ShowDatabase:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowDatabaseSchema;

                    // The comment lives on the cross-database registry entry, not on the descriptor,
                    // so it is resolved here. A cache miss (another node set it and this node has not
                    // reconciled yet) simply renders empty rather than failing the statement.
                    DatabaseRegistry showRegistry = await registryTask.ConfigureAwait(false);
                    string? databaseComment = showRegistry.GetById(database.Id)?.Comment;

                    return (database, schemaQuerier.ShowDatabase(database, databaseComment));
                }

            case NodeType.Explain:
            case NodeType.ExplainPhysical:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ExplainSchema;
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "physical"));
                }

            case NodeType.ExplainLogical:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ExplainSchema;
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "logical"));
                }

            case NodeType.ExplainAnalyze:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ExplainAnalyzeSchema;
                    return (database, explainExecutor.ExplainAnalyzeQuery(database, ast.leftAst!, ticket));
                }

            case NodeType.AnalyzeTable:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.AnalyzeTableSchema;
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    QueryResultRow result = await tableAnalyzer.AnalyzeAsync(database, table, ticket.TxnState).ConfigureAwait(false);
                    return (database, ToAsyncEnumerable(result));
                }

            case NodeType.SetTransaction:
            case NodeType.SetTransactionLocking:
                ApplySetTransactionStatement(ast, ticket);
                return (database, AsyncEnumerable.Empty<QueryResultRow>());

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    /// <summary>
    /// Applies a <c>SET TRANSACTION</c> / <c>SET TRANSACTION LOCKING</c> statement to the in-flight
    /// transaction state. Shared by both SQL entry points: these statements return no rows, so a
    /// client may route them to either <see cref="ExecuteSQLQuery"/> (the row-returning endpoint) or
    /// <see cref="ExecuteNonSQLQuery"/> (the "rows affected" endpoint). Keeping the parse-and-apply
    /// logic in one place means the two dispatchers cannot drift — a missing case here is why
    /// <c>SET TRANSACTION LOCKING</c> previously threw "Unknown non-query AST stmt" when a client
    /// sent it to the non-query endpoint. Callers must exempt these node types from the
    /// "must be the first statement" gate before invoking this.
    /// </summary>
    private static void ApplySetTransactionStatement(NodeAst ast, ExecuteSQLTicket ticket)
    {
        switch (ast.nodeType)
        {
            case NodeType.SetTransaction:
                {
                    // yytext holds the isolation level ("Serializable"); leftAst.yytext holds the mode
                    // ("ReadOnly" or "ReadWrite"). Both are set by the grammar.
                    if (!Enum.TryParse(ast.yytext, out CamusIsolationLevel level))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown isolation level '{ast.yytext}' in SET TRANSACTION");

                    string modeStr = ast.leftAst?.yytext ?? "ReadWrite";
                    if (!Enum.TryParse(modeStr, out CamusTransactionMode mode))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown transaction mode '{modeStr}' in SET TRANSACTION");

                    // ApplyIsolationLevel throws if locks are already held, ensuring the level
                    // change cannot silently skip required read-locks already missed.
                    ticket.TxnState.ApplyIsolationLevel(level, mode);
                    return;
                }

            case NodeType.SetTransactionLocking:
                {
                    // yytext carries the resolved enum name ("Pessimistic" or "Optimistic") set by the grammar.
                    if (!Enum.TryParse(ast.yytext, out Kahuna.Shared.KeyValue.KeyValueTransactionLocking locking))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown locking mode '{ast.yytext}' in SET TRANSACTION LOCKING");

                    ticket.TxnState.ApplyLocking(locking);
                    return;
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt,
                    "ApplySetTransactionStatement called with non-SET node: " + ast.nodeType);
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> ToAsyncEnumerable(QueryResultRow row)
    {
        await Task.CompletedTask;
        yield return row;
    }

    private static async IAsyncEnumerable<QueryResultRow> EmptyResultset()
    {
        await Task.CompletedTask;
        yield break;
    }

    /// <summary>
    /// Pre-materializes uncorrelated scalar / <c>IN</c> / <c>NOT IN</c> subqueries in an
    /// <c>UPDATE</c>'s <c>WHERE</c> clause and each <c>SET</c> value into literals, mirroring the
    /// subquery rewrite that <c>SELECT</c> already performs, so the synchronous evaluator in
    /// <see cref="RowUpdater"/> never sees an unresolved subquery node. Runs once before the
    /// fence-retry loop so inner queries are not re-executed per retry. Rebuilds the ticket only
    /// when something actually changed; <c>EXISTS</c> is left intact (see
    /// <see cref="SubqueryRewriter.RewriteWhereExpressionAsync"/>).
    /// </summary>
    private async Task<UpdateTicket> RewriteUpdateSubqueriesAsync(
        DatabaseDescriptor database, UpdateTicket ticket, ExecuteSQLTicket sqlTicket)
    {
        bool changed = false;

        NodeAst? newWhere = ticket.Where;
        if (newWhere is not null)
        {
            newWhere = await subqueryRewriter
                .RewriteWhereExpressionAsync(database, newWhere, sqlTicket)
                .ConfigureAwait(false);
            changed |= !ReferenceEquals(newWhere, ticket.Where);
        }

        Dictionary<string, NodeAst>? newExprValues = ticket.ExprValues;
        if (ticket.ExprValues is not null)
        {
            Dictionary<string, NodeAst> rewritten = new(ticket.ExprValues.Count);
            foreach (KeyValuePair<string, NodeAst> entry in ticket.ExprValues)
            {
                NodeAst resolved = await subqueryRewriter
                    .RewriteWhereExpressionAsync(database, entry.Value, sqlTicket)
                    .ConfigureAwait(false);
                rewritten[entry.Key] = resolved;
                changed |= !ReferenceEquals(resolved, entry.Value);
            }

            if (changed)
                newExprValues = rewritten;
        }

        if (!changed)
            return ticket;

        return new UpdateTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            plainValues: ticket.PlainValues,
            exprValues: newExprValues,
            where: newWhere,
            filters: ticket.Filters,
            parameters: ticket.Parameters,
            limit: ticket.Limit);
    }

    /// <summary>
    /// Executes a FROM-less <c>SELECT &lt;expr, …&gt; [LIMIT n] [OFFSET n]</c>: evaluates each
    /// projection as a scalar expression against a single synthetic (empty) row and returns exactly
    /// one row, subject to LIMIT/OFFSET. Uncorrelated subqueries in a projection (the existence-check
    /// idiom, e.g. <c>SELECT EXISTS(…)</c> / <c>SELECT (SELECT COUNT(*) …) &gt; 0</c>) are
    /// pre-materialized into literals via <see cref="SubqueryRewriter"/> before evaluation. There is
    /// no table, so a projection may not reference columns (a bare identifier surfaces
    /// <see cref="CamusDBErrorCodes.UnknownColumn"/> at evaluation), use <c>*</c>, or aggregate —
    /// those are rejected with a clear <see cref="CamusDBErrorCodes.InvalidInput"/>.
    /// </summary>
    private async Task<IAsyncEnumerable<QueryResultRow>> ExecuteFromlessSelectAsync(
        DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket, QuerySchemaHolder? schemaOut = null)
    {
        List<NodeAst> projections = new();
        FlattenProjectionList(ast.leftAst!, projections);

        Dictionary<string, ColumnValue> emptyRow = new();
        Dictionary<string, ColumnValue> projected = new(projections.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < projections.Count; i++)
        {
            NodeAst projection = projections[i];
            NodeAst valueExpr = projection.nodeType == NodeType.ExprAlias ? projection.leftAst! : projection;

            if (valueExpr.nodeType == NodeType.ExprAllFields)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "SELECT * requires a FROM clause");

            if (QueryExpressionClassifier.IsAggregateProjection(valueExpr)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(valueExpr))
            {
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Aggregate functions require a FROM clause");
            }

            // Pre-materialize any uncorrelated projection subquery (EXISTS / scalar / IN) into a
            // literal so the synchronous evaluator only ever sees literals.
            NodeAst resolved = await subqueryRewriter
                .RewriteProjectionExpressionAsync(database, valueExpr, ticket)
                .ConfigureAwait(false);

            string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(projection, i);
            projected[name] = SQLExecutorBaseCreator.EvalExpr(resolved, emptyRow, ticket.Parameters);
        }

        if (schemaOut is not null)
            schemaOut.Schema = DerivedTableSchemaBuilder.BuildFromless(projections, projected);

        // A single constant row: apply OFFSET (any offset >= 1 skips it) then LIMIT (0 drops it).
        long offset = EvalRowCount(ast.extendedFour, ticket.Parameters, "OFFSET");
        long limit = ast.extendedThree is null ? long.MaxValue : EvalRowCount(ast.extendedThree, ticket.Parameters, "LIMIT");

        if (offset >= 1 || limit <= 0)
            return EmptyResultset();

        return ToAsyncEnumerable(new QueryResultRow(new ObjectIdValue(), projected));
    }

    /// <summary>Flattens the left-recursive <see cref="NodeType.IdentifierList"/> projection chain into order.</summary>
    private static void FlattenProjectionList(NodeAst ast, List<NodeAst> projections)
    {
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                FlattenProjectionList(ast.leftAst, projections);
            if (ast.rightAst is not null)
                FlattenProjectionList(ast.rightAst, projections);
            return;
        }

        projections.Add(ast);
    }

    /// <summary>Evaluates a LIMIT/OFFSET count node to a non-negative long; a null node means "none".</summary>
    private static long EvalRowCount(NodeAst? node, Dictionary<string, ColumnValue>? parameters, string clause)
    {
        if (node is null)
            return 0;

        ColumnValue value = SQLExecutorBaseCreator.EvalExpr(node, new Dictionary<string, ColumnValue>(), parameters);
        if (value.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{clause} must be an integer");

        return value.LongValue;
    }

    private static string? UnquoteLikePattern(string? raw)
    {
        if (raw is null) return null;
        if (raw.Length >= 2 && raw[0] == raw[^1] && (raw[0] == '\'' || raw[0] == '"'))
            return raw[1..^1];
        return raw;
    }

    private static void PinSchemaVersions(
        DatabaseDescriptor database,
        IEnumerable<BoundTableSource> sources,
        KvTransaction tx
    )
    {
        foreach (BoundTableSource source in sources)
            PinSchemaVersion(database, source.Table, tx);
    }

    private static void PinSchemaVersion(DatabaseDescriptor database, TableDescriptor table, KvTransaction tx)
    {
        string resource = $"{database.Id}/{table.Id}";
        tx.PinSchemaVersion(
            resource,
            table.Schema.Version,
            () => table.Schema.Version,
            () => database.Schema.Tables.TryGetValue(table.Name, out TableSchema? current)
                  && current.Id == table.Id
        );
    }

    /// <summary>
    /// Walks a WHERE-clause predicate AST looking for an IN/NOT IN/scalar subquery node whose
    /// inner SELECT carries a <c>{cache=name}</c> table-reference hint.
    /// Returns the first such <see cref="CacheHintOptions"/> found, or <c>null</c> if none.
    ///
    /// <para>Used to detect the "inner subquery hint" case, where the outer SELECT has no
    /// cache hint but a subquery in its WHERE clause does. SubqueryRewriter executes such
    /// subqueries live and silently discards the inner hint; calling this method before
    /// SubqueryRewriter runs lets <c>ExecuteSQLQuery</c> surface an explicit
    /// <see cref="QueryCacheBypassReason.InnerHint"/> bypass in the response metadata.</para>
    ///
    /// <para><b>Scope limitation:</b> this walks only the WHERE-clause predicate. A
    /// <c>{cache=name}</c> hint on a subquery in the projection list, a FROM-derived table,
    /// or a HAVING clause is <em>not</em> detected here and still produces a silent bypass
    /// (the hint is discarded by SubqueryRewriter with no <c>InnerHint</c> metadata). WHERE is
    /// the common case and the one the surfacing contract currently covers; extend this walk
    /// if inner hints in those other positions need to be surfaced too.</para>
    /// </summary>
    private static CacheHintOptions? FindInnerSubqueryCacheHint(NodeAst? node)
    {
        if (node is null)
            return null;

        // ExprInSubquery / ExprNotInSubquery: rightAst is the inner SELECT.
        // ExprScalarSubquery: leftAst is the inner SELECT.
        NodeAst? subSelect = node.nodeType switch
        {
            NodeType.ExprInSubquery or NodeType.ExprNotInSubquery => node.rightAst,
            NodeType.ExprScalarSubquery                           => node.leftAst,
            _                                                     => null,
        };

        if (subSelect is not null)
        {
            SelectQuery inner = new SelectQueryCreator().CreateSelectQuery(subSelect);
            if (inner.CacheHint is not null)
                return inner.CacheHint;
        }

        return FindInnerSubqueryCacheHint(node.leftAst)
            ?? FindInnerSubqueryCacheHint(node.rightAst);
    }

    public async ValueTask DisposeAsync()
    {
        // Stop the snapshot-hold renewer first. Await its deferred start so the renewer instance is
        // observed (or its start fault surfaces) before disposal, avoiding a lost background loop.
        if (snapshotRenewerStart is not null)
        {
            try { await snapshotRenewerStart.ConfigureAwait(false); }
            catch { /* a failed start left nothing to dispose */ }
        }
        if (snapshotHoldRenewer is not null)
            await snapshotHoldRenewer.DisposeAsync().ConfigureAwait(false);

        if (orphanReclaimer is not null)
            await orphanReclaimer.DisposeAsync().ConfigureAwait(false);

        if (autoAnalyzeScheduler is not null)
            await autoAnalyzeScheduler.DisposeAsync().ConfigureAwait(false);

        await databaseCloser.DisposeAsync();
        await sqlParserCache.DisposeAsync().ConfigureAwait(false);

        if (ownsRegistry)
        {
            // Disposal must never crash graceful shutdown. Awaiting registryTask here can re-throw a
            // fault the registry-open captured earlier — e.g. its startup scan's rollback tried to
            // reach a Raft partition that was not yet ready at boot or is already torn down at
            // shutdown (RaftException: Invalid partition). A registry that never opened successfully
            // has nothing to clean up, and a rollback against a vanishing node is moot, so log and
            // swallow rather than aborting the rest of the shutdown sequence.
            try
            {
                DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
                await registry.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Registry cleanup during shutdown failed; continuing teardown");
            }
        }
    }
}
