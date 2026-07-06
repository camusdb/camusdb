
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
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

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

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
            registryTask = DatabaseRegistry.OpenAsync(sharedNode!);
            ownsRegistry = true;
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
            new InSubqueryExecutor(subqueryQueryExecutor, statisticsManager));
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
        SnapshotHoldRenewer renewer = new(node, registry, logger, CamusDBConfig.BranchSnapshotHoldLeaseMs);
        renewer.Start();
        snapshotHoldRenewer = renewer;

        // On startup, find any branch ids that were allocated and partially written (metadata
        // copied) but never registered — left by a crash between CopyMetaForBranchAsync and
        // RegisterAsync during a previous branch-creation attempt. Purge their namespaces so
        // wasted KV space is reclaimed.  Errors are logged and swallowed; orphan cleanup is
        // advisory and must not block normal startup.
        _ = ScrubOrphanBranchNamespacesAsync(node, registry);
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

            // Resume any DROP DATABASE this node started but did not finish before a crash. The
            // keyspace purge is per-key and non-transactional, so an interrupted drop can leave
            // orphaned row/index/stats data with no other reclaim. Owner-scoped markers mean we only
            // resume our own drops; a marker whose id is still registered means the crash preceded
            // UnregisterAsync (nothing was purged) so we just clear the stale marker.
            foreach (string droppingId in await registry.LoadOwnDroppingIdsAsync().ConfigureAwait(false))
            {
                try
                {
                    if (registry.GetById(droppingId) is null)
                    {
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("Resuming interrupted DROP DATABASE keyspace purge for id {DbId} on startup", droppingId);
                        await databaseDroper.PurgeKeyspaceByIdAsync(node.Kahuna, droppingId, null).ConfigureAwait(false);
                    }
                    await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
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
                // If the child was published (drop-intent check threw after RegisterAsync), retract
                // it so no stale entry remains. Hold release and meta purge follow regardless.
                if (childRegistered)
                {
                    try
                    {
                        await registry.UnregisterAsync(branchName).ConfigureAwait(false);
                        childRegistered = false;
                    }
                    catch (Exception unregEx)
                    {
                        logger.LogWarning(unregEx,
                            "Failed to unregister branch '{Branch}' after aborting due to concurrent parent drop; entry may be stale",
                            branchName);
                    }
                }

                // The branch was never published (or has now been retracted), so nothing will ever
                // renew or release this hold. Release it best-effort.
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
            // Transient Kahuna error; fall through with only the single-node semaphore guard (BR8).
            logger.LogWarning(intentEx,
                "Could not acquire drop-intent marker for '{Database}'; cross-node branch-create race is not fully fenced",
                ticket.DatabaseName);
        }

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

                // Mark the drop in progress before unregistering so a crash during the (non-atomic,
                // per-key) keyspace purge below can be resumed at startup. The marker is owner-scoped
                // and cleared only after the purge fully completes.
                await registry.MarkDroppingAsync(entry.Id).ConfigureAwait(false);

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

            await databaseDroper.Drop(entry.Id).ConfigureAwait(false);

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

            // Purge completed: clear the drop-in-progress marker. Left in place only if a step above
            // threw (crash/failure), so the startup scrub resumes the interrupted purge.
            await registry.ClearDroppingAsync(entry.Id).ConfigureAwait(false);

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
        // Name is display-only (logs, error messages) and never a storage or routing key,
        // so a stale Name on the cached descriptor is functionally harmless.

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
        if (isClusterMode)
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
            if (isClusterMode)
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

        return await ExecuteDdlInTransaction(database, async tx =>
        {
            bool result = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, ticket, tx).ConfigureAwait(false);
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
        if (table.Schema.Columns?.Any(c => c.Name == columnInfo.Name) == true)
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
                await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(
                    tx, afterRowId: afterRowId).ConfigureAwait(false))
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
                        await table.Store.PutIndexEntry(tx, indexInfo.IndexId, compositeKey, rowId, unique, backfillMode: true).ConfigureAwait(false);
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

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = GetColumnIdsForIndex(table, ticket.Columns);
        string[] columnNames = ticket.Columns.Select(c => c.Name).ToArray();

        IndexBuildInfo indexInfo = new(indexId, ticket.IndexName, columnIds, columnNames, indexType);

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
                tableSchema.Indexes?.Any(ix => ix.Name == indexName) == true)
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
            table.Schema.Indexes?.RemoveAll(ix => ix.Name == indexName);
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

    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = descriptor.Use();
        return await tableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

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
                if (tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, constraint.Name, StringComparison.Ordinal)) != true)
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
                   ts.Columns?.Any(c => c.Name == ticket.NewName) == true;
        }

        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        return ticket.Operation switch
        {
            // A forwarded AddColumn is complete only when the column is Public — intermediate
            // staged states (DeleteOnly, WriteOnly) are not yet visible to queries.
            AlterTableOperation.AddColumn =>
                tableSchema.Columns?.Any(c => c.Name == ticket.Column.Name && c.State == SchemaElementState.Public) == true,
            AlterTableOperation.DropColumn =>
                tableSchema.Columns?.Any(c => c.Name == ticket.Column.Name) != true,
            _ => false
        };
    }

    private static bool ForwardedAlterIndexApplied(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
        {
            return database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts) &&
                   ts.Indexes?.Any(ix => string.Equals(ix.Name, ticket.NewName, StringComparison.Ordinal)) == true;
        }

        // Check TableSchema.Indexes (the source of truth). Fall back to SystemSchema
        // for nodes that haven't yet applied the migration (legacy path).
        bool existsInSchema = database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema) &&
                              tableSchema.Indexes is not null &&
                              tableSchema.Indexes.Any(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.Ordinal));

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

    public async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

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

        if (ast.nodeType is NodeType.DropDatabase or NodeType.DropDatabaseIfExists)
        {
            string dbName = ast.leftAst!.yytext!;
            bool ifExists = ast.nodeType == NodeType.DropDatabaseIfExists;
            await DropDatabase(new DropDatabaseTicket(dbName, ifExists)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.RenameDatabase)
        {
            string oldName = ast.leftAst!.yytext!;
            string newName = ast.rightAst!.yytext!;
            await RenameDatabase(new RenameDatabaseTicket(oldName, newName)).ConfigureAwait(false);
            return default;
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        switch (ast.nodeType)
        {
            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = sqlExecutor.CreateCreateTableTicket(ticket, ast);
                    validator.Validate(createTableTicket);

                    bool? forwarded = await TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, createTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
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

        // DROP/RENAME DATABASE do not require an open database context — dispatch before Open so
        // we don't accidentally load the descriptor we're about to destroy or rename.
        if (ast.nodeType is NodeType.DropDatabase or NodeType.DropDatabaseIfExists)
        {
            string targetName = ast.leftAst!.yytext!;
            bool ifExists = ast.nodeType == NodeType.DropDatabaseIfExists;
            await DropDatabase(new DropDatabaseTicket(targetName, ifExists)).ConfigureAwait(false);
            return default;
        }

        if (ast.nodeType is NodeType.RenameDatabase)
        {
            string oldName = ast.leftAst!.yytext!;
            string newName = ast.rightAst!.yytext!;
            await RenameDatabase(new RenameDatabaseTicket(oldName, newName)).ConfigureAwait(false);
            return default;
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Mark the transaction as having executed a statement (all DML is non-SET-TRANSACTION).
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
                            return new(database, table, await rowInserter.Insert(database, table, insertTicket).ConfigureAwait(false));
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

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await tableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);
                            PinSchemaVersion(database, table, ticket.TxnState);
                            return new(database, table, await rowUpdater.Update(queryExecutor, database, table, updateTicket));
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

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await tableOpener.Open(database, deleteTicket.TableName).ConfigureAwait(false);
                            PinSchemaVersion(database, table, ticket.TxnState);
                            return new(database, table, await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false));
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
                    return default;
                }

            case NodeType.EvictCacheAll:
                {
                    database.Cache?.InvalidateDatabase(database.Id);
                    return default;
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Execute a SQL statement that returns rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<(DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)> ExecuteSQLQuery(ExecuteSQLTicket ticket, CacheMetadataHolder? metaOut = null)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        // SHOW DATABASES does not require a database context — resolve the registry and return.
        if (ast.nodeType == NodeType.ShowDatabases)
        {
            DatabaseRegistry reg = await registryTask.ConfigureAwait(false);
            string? dbPattern = UnquoteLikePattern(ast.leftAst?.yytext);
            return (null!, schemaQuerier.ShowDatabases(reg.List(), dbPattern));
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
                return (null!, schemaQuerier.ShowBranches(allEntries, target));
            return (null!, schemaQuerier.ShowAncestors(target, allEntries));
        }

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);

        // Mark the transaction as having executed a statement for every statement type except
        // SET TRANSACTION — that one must be the first statement per standard SQL semantics.
        if (ast.nodeType != NodeType.SetTransaction)
            ticket.TxnState.MarkStatementExecuted();

        switch (ast.nodeType)
        {
            case NodeType.Select:
                {
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
                        return (database, queryExecutor.ExecuteJoinQuery(database, boundQuery, queryTicket));
                    }

                    TableDescriptor table = boundQuery.PrimaryTable;

                    return (database, queryExecutor.Query(database, table, queryTicket, metaOut));
                }

            case NodeType.ShowTables:
                {
                    string? tablePattern = UnquoteLikePattern(ast.leftAst?.yytext);
                    return (database, schemaQuerier.ShowTables(database, tablePattern));
                }

            case NodeType.ShowColumns:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowColumns(table));
                }

            case NodeType.ShowIndexes:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowIndexes(table));
                }

            case NodeType.ShowCreateTable:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowCreateTable(table));
                }

            case NodeType.ShowDatabase:
                {
                    return (database, schemaQuerier.ShowDatabase(database));
                }

            case NodeType.Explain:
            case NodeType.ExplainPhysical:
                {
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "physical"));
                }

            case NodeType.ExplainLogical:
                {
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "logical"));
                }

            case NodeType.ExplainAnalyze:
                {
                    return (database, explainExecutor.ExplainAnalyzeQuery(database, ast.leftAst!, ticket));
                }

            case NodeType.AnalyzeTable:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    QueryResultRow result = await tableAnalyzer.AnalyzeAsync(database, table, ticket.TxnState).ConfigureAwait(false);
                    return (database, ToAsyncEnumerable(result));
                }

            case NodeType.SetTransaction:
                {
                    // yytext holds the isolation level ("Serializable"); leftAst.yytext holds the mode
                    // ("ReadOnly" or "ReadWrite"). Both are set by the grammar (grammar cases 48/49).
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

                    return (database, AsyncEnumerable.Empty<QueryResultRow>());
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    private static async IAsyncEnumerable<QueryResultRow> ToAsyncEnumerable(QueryResultRow row)
    {
        await Task.CompletedTask;
        yield return row;
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
