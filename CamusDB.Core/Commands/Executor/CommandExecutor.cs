
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Auth;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsValidator;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Ttl;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
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

    private readonly DatabaseEvictor databaseEvictor;

    private readonly SchemaFreshnessSweeper schemaFreshnessSweeper;

    private readonly DatabaseDropper databaseDroper;

    private readonly DatabaseDescriptors databaseDescriptors;

    // Process-level Kahuna node shared across all databases. Used to route snapshot-floor hold
    // release/renew calls, which auto-forward to the system-partition leader from any node.
    private readonly EmbeddedKahuna? sharedNode;

    // The leader-owned background loops — snapshot-floor renewal, orphan reclamation, auto-analyze and
    // row-level TTL — with their start ordering and their teardown. Null when no shared node is present,
    // in which case none of them has anything to run against.
    private readonly Controllers.Maintenance.BackgroundSchedulerHost? backgroundSchedulers;

    // Reclaims what a crashed or dead run left behind: stale drop-intent markers, interrupted keyspace
    // purges, orphan branch namespaces, and abandoned materialized-view refreshes.
    private readonly Controllers.Maintenance.StartupRecoveryService startupRecovery;

    // Whole-database operations — create, branch, open, close, drop, relink, rename — and the fences
    // that keep them correct against each other.
    private readonly DatabaseLifecycleService databaseLifecycle;

    // Routes DDL that arrived on a follower to the database's schema leader, and waits until the
    // committed change is visible in this node's own schema before returning.
    private readonly Controllers.DDL.DdlForwardingCoordinator ddlForwarding;

    // Schema DDL against one database: create/drop/rename/relink table, columns, indexes,
    // constraints and comments, with the transaction and staging discipline each one needs.
    private readonly Controllers.DDL.SchemaDdlService schemaDdl;

    // Version-neutral ALTER TABLE SET/RESET storage parameters.
    private readonly Controllers.DDL.TableSettingsService tableSettings;

    // The statement-level authorization gate, and the ambient scope the per-table check reads.
    private readonly Controllers.Auth.StatementAuthorizer statementAuthorizer;

    // Server-level user/grant administration against the shared auth catalog.
    private readonly Controllers.Auth.UserAdminService userAdmin;

    // The read path: SELECT in all its forms, the SHOW family, and distributed query fragments.
    private readonly Controllers.Queries.SelectStatementExecutor selectExecutor;

    // CREATE TABLE ... AS SELECT, plus the relation-staging primitives a matview refresh builds on.
    private readonly Controllers.DDL.CreateTableAsSelectExecutor ctasExecutor;

    // Ticket-based row operations for callers that do not go through SQL.
    private readonly Controllers.DML.RowCommandService rowCommands;

    // The statements dispatched before any database is opened, shared by both SQL entry points.
    private readonly Controllers.ServerLevelStatementDispatcher serverLevelDispatcher;

    // Routes a parsed DDL statement to the service that executes it.
    private readonly Controllers.DDL.DdlStatementDispatcher ddlDispatcher;

    // Executes a SQL statement that returns no rows, and accepts schema DDL by forwarding it.
    private readonly Controllers.DML.NonQueryStatementDispatcher nonQueryDispatcher;

    // Server-level backup / point-in-time-recovery controller over the shared node. Null only when this
    // executor was constructed without a shared node (no backup surface is reachable).
    private readonly BackupManager? backupManager;

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

    private readonly RowInsertSelector rowInsertSelector = new();

    private readonly Controllers.DDL.ViewCreator viewCreator = new();

    private readonly Controllers.DDL.MaterializedViewCreator matViewCreator = new();

    /// <summary>
    /// Owned by the executor rather than created per statement because it carries the in-flight
    /// refresh set: a gate that was rebuilt for every statement would gate nothing.
    /// </summary>
    private readonly Controllers.DDL.MaterializedViewRefresher matViewRefresher = new();

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

    /// <summary>
    /// Configuration for this engine. Held here and handed to everything this executor constructs, so
    /// a component never reaches for a process-wide value and two executors can be configured
    /// independently in one process.
    ///
    /// <para>When the engine was built with a <see cref="CamusDBOptionsHolder"/>, a published swap
    /// replaces this reference and fans out to the controllers, open databases, and table stores via
    /// <see cref="ApplyOptions"/>; each read site pins the field once per operation, so an in-flight
    /// statement keeps the snapshot it started with. Without a holder the field never changes and the
    /// engine is configured for life — the behavior every existing test relies on.</para>
    /// </summary>
    private CamusDBOptions options;

    /// <summary>Configuration for this engine, for the controllers and stores it owns.</summary>
    internal CamusDBOptions Options => options;

    /// <summary>Unhooks this executor from the options holder on dispose; null when none was given.</summary>
    private readonly IDisposable? optionsSubscription;

    /// <summary>
    /// The runtime cluster-settings pipeline behind <c>SET/RESET CLUSTER SETTING</c> and
    /// <c>SHOW CLUSTER SETTINGS</c>. Null on engines composed without one (most tests), where the
    /// statements are rejected with a clear error rather than silently applying to nothing.
    /// </summary>
    private readonly ClusterSettingsService? clusterSettings;

    /// <summary>
    /// Parses SQL through this executor's shared parser cache. Exists for transports (HTTP/gRPC)
    /// that must inspect a statement's root node to route it before building a ticket: without
    /// this, an inline request pays a full, uncached lex+parse per request just to read the root
    /// node type, and then the executor parses the same text again (cached) during execution.
    /// The returned AST is the same cached, immutable, share-safe instance execution will use.
    /// </summary>
    public NodeAst ParseSql(string sql) => SQLParserProcessor.Parse(sql, sqlParserCache);

    private readonly SemiJoinAnalyzer semiJoinAnalyzer;

    private readonly ISchemaDdlForwarder? schemaDdlForwarder;

    private readonly SQLParser.SqlParserCache sqlParserCache;

    /// <summary>
    /// Backs <c>SHOW ENGINE STATS</c>. Null when <see cref="CamusDBOptions.EngineMetricsEnabled"/> is
    /// off, in which case the statement reports no rows rather than failing.
    /// </summary>
    private readonly Diagnostics.EngineMetricsCollector? engineMetrics;

    /// <summary>
    /// Times statements into the bounded slow query log, or null when the log was off when this
    /// engine was built. Null rather than an always-present recorder with a flag, because the ring
    /// allocates its backing array in its constructor: an engine that never enables the log should
    /// not hold that array at all.
    ///
    /// <para>Turning the log off at runtime therefore stops recording without discarding what was
    /// already recorded, and turning it on at runtime works only if the engine started with it on.
    /// That asymmetry is what <see cref="CamusDBOptions.SlowQueryLogMaxEntries"/> being
    /// restart-class means in practice.</para>
    /// </summary>
    private readonly Diagnostics.SlowQueryRecorder? slowQueries;

    // Number of rows indexed per Kahuna transaction during backfill.  Committing in bounded
    // batches keeps transaction size manageable and allows a leader-change resume to skip
    // already-indexed rows via the persisted StartOffset checkpoint. Shared with the standalone
    // flux backfill so both paths batch identically.
    private const int BackfillBatchSize = TableIndexAdder.IndexBackfillBatchSize;

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

    /// <summary>
    /// The engine-level collaborators handed to components extracted out of this facade. Built near
    /// the end of the constructor, once every controller it names exists.
    /// </summary>
    private readonly ExecutorContext executorContext;

    /// <summary>
    /// Finds the databases and tables background maintenance has work to do on, by reading
    /// authoritative KV metadata. Owned here because it holds the discovery memos that keep a
    /// steady-state tick from re-scanning every database's metadata bucket.
    /// </summary>
    private readonly Controllers.Maintenance.MetadataDiscoveryService metadataDiscovery;

    /// <summary>
    /// How many database descriptors this engine currently holds open. A read-only count, exposed so a
    /// test can assert that background work left the set alone: the background schedulers must open a
    /// database only when its metadata has already shown there is work to do there, and the only way to
    /// pin that down is to watch the count across a sweep.
    /// </summary>
    internal int OpenDatabaseCount => databaseDescriptors.Descriptors.Count;

    /// <summary>
    /// Releases, across every database this node holds open, the key mirrors of abandoned transactions
    /// that have reached the age at which no coordinator session can still own their holdings.
    /// Returns how many keys were released.
    ///
    /// <para>A rollback that met the coordinator-unknown outcome while its transaction was still too
    /// young to release parks the transaction's key mirror instead of dropping it — see
    /// <see cref="Transactions.KvTransactionsManager.ReleaseDueMirroredHoldingsAsync"/>. Nothing else
    /// will ever come back for those keys: the transaction is terminal and untracked, and the mirror is
    /// the only remaining record of what it planted. This is the sweep that finishes the job, driven by
    /// the abandoned-transaction reaper on its ordinary tick.</para>
    ///
    /// <para>Only databases that are already open are visited, and a descriptor still opening is
    /// skipped: a background sweep must never be the thing that opens a database (or blocks on one
    /// opening), and a database that is not open holds no mirrors.</para>
    /// </summary>
    public async Task<int> ReleaseDueMirroredHoldingsAsync(CancellationToken cancellationToken = default)
    {
        int released = 0;

        foreach (KeyValuePair<string, Nito.AsyncEx.AsyncLazy<DatabaseDescriptor>> database in databaseDescriptors.Descriptors)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            if (!database.Value.IsStarted || !database.Value.Task.IsCompletedSuccessfully)
                continue;

            released += await database.Value.Task.Result.Transactions
                .ReleaseDueMirroredHoldingsAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        return released;
    }

    /// <summary>
    /// Whether this node currently holds an open descriptor for <paramref name="databaseId"/>.
    ///
    /// <para>The key is the database <b>id</b>, not its name: the descriptor cache is keyed by id so
    /// a rename cannot orphan an entry. It is a plain lookup that opens nothing, which is the whole
    /// point — a caller reporting on residency must not create the residency it reports.</para>
    /// </summary>
    internal bool IsDatabaseResident(string databaseId) =>
        databaseDescriptors.Descriptors.ContainsKey(databaseId);

    /// <summary>
    /// How many times background discovery has range-scanned a database's metadata bucket. The count a
    /// test needs to tell "the memo was used" from "the memo happened to produce the same answer" —
    /// the two are indistinguishable from the discovery result alone.
    /// </summary>
    internal int MetaDiscoveryScanCount => metadataDiscovery.MetaDiscoveryScanCount;

    /// <param name="sharedNode">Process-level Kahuna node shared across all databases; non-null in both standalone and cluster modes.</param>
    /// <param name="schemaDdlForwarder">DDL forwarder for cluster mode; null in standalone.</param>
    /// <param name="registry">Optional pre-created registry; if supplied the executor does not own it and will not dispose it.</param>
    /// <param name="isClusterMode">True when this process is a Raft cluster node; false for standalone.</param>
    /// <param name="cache">Optional query result cache. When non-null, DML commits drive the publish-gate invalidation
    /// protocol and DDL operations call <see cref="IQueryResultCache.InvalidateByTableId"/> after each successful commit.</param>
    /// <param name="optionsHolder">Optional swappable source of configuration snapshots. When
    /// supplied, its current snapshot overrides <paramref name="options"/> and every subsequent
    /// <see cref="CamusDBOptionsHolder.Publish"/> is fanned out to this engine's controllers, open
    /// databases, and table stores. When null the engine keeps <paramref name="options"/> for life.</param>
    public CommandExecutor(
        CommandValidator validator,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        CamusDBOptions options,
        EmbeddedKahuna? sharedNode = null,
        ISchemaDdlForwarder? schemaDdlForwarder = null,
        DatabaseRegistry? registry = null,
        bool isClusterMode = false,
        IQueryResultCache? cache = null,
        CamusDBOptionsHolder? optionsHolder = null,
        ClusterSettingsService? clusterSettings = null,
        IQueryFragmentTransport? fragmentTransport = null)
    {
        this.validator = validator;
        this.catalogs = catalogs;
        this.logger = logger;
        this.options = optionsHolder?.Current ?? options;
        options = this.options;
        this.schemaDdlForwarder = schemaDdlForwarder;
        this.isClusterMode = isClusterMode;
        this.sharedNode = sharedNode;
        this.clusterSettings = clusterSettings;

        if (registry is not null)
        {
            registryTask = Task.FromResult(registry);
            ownsRegistry = false;
        }
        else
        {
            registryTask = DatabaseRegistry.OpenAsync(sharedNode!, options, isClusterMode);
            ownsRegistry = true;
        }

        // The auth catalog rides the same shared node and _system/ keyspace as the registry.
        if (sharedNode is not null)
        {
            authCatalogTask = AuthCatalog.OpenAsync(sharedNode, options, isClusterMode);
            authService = new AuthService(authCatalogTask, options);
            backupManager = new BackupManager(sharedNode, logger);
        }

        databaseDescriptors = new();
        statisticsManager = new(logger);
        databaseOpener = new(
            this, 
            databaseDescriptors, 
            catalogs, 
            logger, 
            options, 
            sharedNode, 
            registryTask, 
            isClusterMode, 
            cache,
            statisticsManager.EvictTableStats
        );
        databaseCloser = new(databaseDescriptors, logger);
        databaseEvictor = new(databaseDescriptors, statisticsManager, logger, options.DatabaseIdleEvictionMs);

        // Started here rather than alongside the KV-backed schedulers: eviction reads nothing but this
        // engine's own descriptor cache, so it has no registry or partition-readiness to wait for.
        databaseEvictor.Start();

        // Repairs a node whose in-memory schema silently fell behind the durable checkpoint —
        // committed schema deltas are delivered exactly once, so one that commits while a database
        // is unopened (or inside the open-time load-to-register gap) never reaches this node's
        // catalog. Cluster mode only: a standalone node applies its own deltas in-process and
        // cannot fall behind its own checkpoint.
        schemaFreshnessSweeper = new(databaseDescriptors, catalogs, logger, options.SchemaFreshnessCheckIntervalMs);
        if (isClusterMode)
            schemaFreshnessSweeper.Start();
        databaseDroper = new(databaseDescriptors, logger, options);
        databaseCreator = new(logger);
        tableOpener = new(catalogs, logger);
        tableCreator = new(catalogs, logger, options);
        tableColumnAlterer = new(catalogs, logger);
        tableIndexAlterer = new(catalogs, logger);
        tableConstraintAlterer = new(logger);
        rowInserter = new(logger);
        rowUpdater = new(logger);
        tableDropper = new(catalogs, statisticsManager, logger);
        rowDeleter = new(logger, statisticsManager);
        queryExecutor = new(logger, options, statisticsManager, sharedNode?.Kahuna, fragmentTransport);
        sqlExecutor = new();
        schemaQuerier = new(catalogs, logger, options);
        // The owner resolver is what turns a recorded owner into enforceable definer's rights. Null
        // when the engine has no auth service (no shared node), in which case there is no principal to
        // swap to and every view binds as the caller — the same as authentication being off.
        queryBinder = new QueryBinder(
            tableOpener,
            authService is null
                ? null
                : (ownerName, ownerId) => authService.TryLoadOwnerPrincipalAsync(ownerName, ownerId));
        SubqueryQueryExecutor subqueryQueryExecutor = new(queryBinder, queryExecutor);
        ExistsSubqueryExecutor existsSubqueryExecutor = new(subqueryQueryExecutor);
        subqueryRewriter = new SubqueryRewriter(
            new ScalarSubqueryExecutor(subqueryQueryExecutor),
            new InSubqueryExecutor(subqueryQueryExecutor, statisticsManager),
            existsSubqueryExecutor
        );
        existsSubqueryPreparer = new ExistsSubqueryPreparer(existsSubqueryExecutor, queryBinder);
        semiJoinAnalyzer = new SemiJoinAnalyzer(tableOpener);
        explainExecutor = new ExplainExecutor(subqueryRewriter, queryBinder, existsSubqueryPreparer, queryExecutor, options, statisticsManager, semiJoinAnalyzer);
        tableAnalyzer = new TableAnalyzer(statisticsManager, options);

        // Observing the embedded Kommander/Kahuna meters costs nothing while no instrument fires, and
        // the listener replays instruments published before it started, so it can be built here
        // regardless of how far along the shared node is.
        if (options.EngineMetricsEnabled)
            engineMetrics = new Diagnostics.EngineMetricsCollector();

        if (options.SlowQueryLogEnabled)
            slowQueries = new Diagnostics.SlowQueryRecorder(options);

        sqlParserCache = new SQLParser.SqlParserCache(
            logger,
            options.SqlParserCacheTtlSeconds,
            options.SqlParserCacheMaxEntries,
            options.SqlParserCacheSweepSeconds);

        // Built here rather than earlier because it names the openers, and DatabaseOpener is itself
        // constructed with a back-reference to this half-built executor — so the collaborators it
        // carries only all exist by this point.
        executorContext = new ExecutorContext(
            logger,
            sharedNode,
            registryTask,
            databaseOpener,
            tableOpener,
            statisticsManager,
            validator,
            isClusterMode
        );

        metadataDiscovery = new Controllers.Maintenance.MetadataDiscoveryService(executorContext, options);
        startupRecovery = new Controllers.Maintenance.StartupRecoveryService(executorContext, catalogs, databaseDroper);
        // Constructed here, ahead of every component that captures it, even though its loops do not
        // start until the end of this constructor. A collaborator captured before it is assigned would
        // be captured as null and fail much later, far from the mistake.
        if (sharedNode is not null)
            backgroundSchedulers = new Controllers.Maintenance.BackgroundSchedulerHost(
                executorContext,
                options,
                tableAnalyzer,
                databaseDroper,
                rowDeleter,
                startupRecovery,
                metadataDiscovery,
                // Resolved per call, not captured: the host wires its probe onto this executor after
                // construction, so reading it now would freeze the answer at "no load forever".
                () => ForegroundLoadProbe?.Invoke() ?? 0
            );

        databaseLifecycle = new DatabaseLifecycleService(
            executorContext,
            options,
            catalogs,
            databaseCreator,
            databaseCloser,
            databaseDroper,
            databaseDescriptors,
            startupRecovery
        );
        ddlForwarding = new Controllers.DDL.DdlForwardingCoordinator(schemaDdlForwarder, isClusterMode);
        schemaDdl = new Controllers.DDL.SchemaDdlService(
            executorContext,
            options,
            catalogs,
            ddlForwarding,
            tableCreator,
            tableColumnAlterer,
            tableIndexAlterer,
            tableConstraintAlterer,
            commentSetter,
            tableDropper,
            rowDeleter,
            queryExecutor,
            sqlParserCache
        );
        tableSettings = new Controllers.DDL.TableSettingsService(executorContext, options, catalogs);
        statementAuthorizer = new Controllers.Auth.StatementAuthorizer(executorContext, options);
        userAdmin = new Controllers.Auth.UserAdminService(executorContext, options, authCatalogTask);
        selectExecutor = new Controllers.Queries.SelectStatementExecutor(
            executorContext,
            options,
            catalogs,
            queryExecutor,
            schemaQuerier,
            queryBinder,
            subqueryRewriter,
            existsSubqueryPreparer,
            explainExecutor,
            tableAnalyzer,
            semiJoinAnalyzer,
            selectQueryCreator,
            sqlExecutor,
            sqlParserCache,
            statementAuthorizer,
            userAdmin,
            backgroundSchedulers,
            clusterSettings,
            engineMetrics,
            slowQueries
        );
        ctasExecutor = new Controllers.DDL.CreateTableAsSelectExecutor(
            executorContext,
            catalogs,
            schemaDdl,
            ddlForwarding,
            selectExecutor,
            tableCreator,
            tableIndexAlterer,
            queryExecutor,
            rowInserter,
            rowInsertSelector
        );
        rowCommands = new Controllers.DML.RowCommandService(
            executorContext, rowInserter, rowUpdater, rowDeleter, queryExecutor);
        serverLevelDispatcher = new Controllers.ServerLevelStatementDispatcher(
            executorContext, sqlExecutor, databaseLifecycle, schemaDdl, userAdmin, clusterSettings);
        ddlDispatcher = new Controllers.DDL.DdlStatementDispatcher(
            executorContext,
            catalogs,
            sqlExecutor,
            sqlParserCache,
            statementAuthorizer,
            userAdmin,
            serverLevelDispatcher,
            databaseLifecycle,
            schemaDdl,
            tableSettings,
            ddlForwarding,
            ctasExecutor,
            queryExecutor,
            tableCreator,
            tableColumnAlterer,
            tableIndexAlterer,
            tableDropper,
            rowDeleter,
            tableConstraintAlterer,
            viewCreator,
            matViewCreator,
            matViewRefresher,
            authService,
            clusterSettings
        );
        nonQueryDispatcher = new Controllers.DML.NonQueryStatementDispatcher(
            executorContext,
            catalogs,
            sqlExecutor,
            sqlParserCache,
            statementAuthorizer,
            ddlDispatcher,
            serverLevelDispatcher,
            selectExecutor,
            ctasExecutor,
            schemaDdl,
            databaseLifecycle,
            userAdmin,
            rowInserter,
            rowUpdater,
            rowDeleter,
            rowInsertSelector,
            queryExecutor,
            subqueryRewriter,
            matViewRefresher
        );

        // Keep every branch's snapshot-floor hold alive for as long as the branch exists. The
        // registry is opened asynchronously, so defer the start until it is ready; the loops
        // themselves elect a single sweeping node by registry-partition leadership.
        if (backgroundSchedulers is not null && sharedNode is not null)
        {
            backgroundSchedulers.Start(this, sharedNode);
        }

        // Subscribed last, after every component the fan-out touches exists. The callback runs
        // inline under the holder's publish lock, so it must stay cheap: swap references, nothing
        // more.
        if (optionsHolder is not null)
            optionsSubscription = optionsHolder.Subscribe(ApplyOptions);
    }

    /// <summary>
    /// Fans a newly published configuration snapshot out to this engine: the executor's own field,
    /// the controllers that captured the record at construction, and every open database (which
    /// forwards to its transactions manager and opened table stores). Reference assignment is
    /// atomic and each read site pins its snapshot once per operation, so an in-flight statement
    /// keeps the configuration it started with and the change takes effect at the next boundary.
    ///
    /// <para>Components deliberately not reached here hold restart-class settings only — their
    /// values are baked into something built once (the embedded node, the auth service's KDF
    /// throttle, the metrics collector) and honestly require a restart, which is what their
    /// <see cref="Config.ConfigSettingAttribute"/> classification declares. The background loops
    /// are fanned out through
    /// <see cref="Controllers.Maintenance.BackgroundSchedulerHost.ApplyOptions"/>, which documents
    /// the one loop it deliberately does not re-tune.</para>
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        options = next;

        validator.ApplyOptions(next);
        queryExecutor.ApplyOptions(next);
        schemaQuerier.ApplyOptions(next);
        tableCreator.ApplyOptions(next);
        tableAnalyzer.ApplyOptions(next);
        databaseOpener.ApplyOptions(next);
        databaseDroper.ApplyOptions(next);
        metadataDiscovery.ApplyOptions(next);
        databaseLifecycle.ApplyOptions(next);
        schemaDdl.ApplyOptions(next);
        tableSettings.ApplyOptions(next);
        statementAuthorizer.ApplyOptions(next);
        userAdmin.ApplyOptions(next);
        selectExecutor.ApplyOptions(next);
        slowQueries?.ApplyOptions(next);

        backgroundSchedulers?.ApplyOptions(next);

        // The parser cache latched its TTL/cap/sweep cadence at construction; retune swaps them and
        // trims an over-cap population immediately.
        sqlParserCache.Retune(
            next.SqlParserCacheTtlSeconds,
            next.SqlParserCacheMaxEntries,
            next.SqlParserCacheSweepSeconds
        );

        // The compiled-regex cache keys entries by match timeout, so a timeout change strands the
        // old entries as unreachable garbage that still counts against the cap — drop them.
        RegexMatcher.EvictEntriesCompiledUnderOtherTimeouts(next.RegexMatchTimeoutMs);

        foreach (KeyValuePair<string, Nito.AsyncEx.AsyncLazy<DatabaseDescriptor>> database in databaseDescriptors.Descriptors)
        {
            // IsStarted first: reading AsyncLazy.Task starts the factory, and a configuration swap
            // must never force-open a database nothing has asked for.
            if (database.Value.IsStarted && database.Value.Task.IsCompletedSuccessfully)
                database.Value.Task.Result.ApplyOptions(next);
        }
    }



    /// <summary>
    /// Test-only seam: attempts to release every descriptor idle for at least
    /// <paramref name="idleWindowMs"/>, returning how many were released. Drives the eviction path
    /// deterministically; the periodic sweep that will call it on a timer is configuration-driven.
    /// </summary>
    internal int EvictIdleDatabasesForTests(long idleWindowMs) => databaseEvictor.EvictIdle(idleWindowMs);

    /// <summary>
    /// Test-only seam: runs one schema freshness sweep over every open database and returns how
    /// many stale schemas were repaired. Drives the sweep deterministically instead of waiting out
    /// the configured timer tick.
    /// </summary>
    internal Task<int> SweepSchemaFreshnessForTests() => schemaFreshnessSweeper.SweepOnceAsync();

    /// <summary>
    /// Test-only seam: attempts to release one database and reports why it was or was not released, so
    /// a test can assert on the reason a busy database was spared rather than only on the count.
    /// </summary>
    internal DatabaseEvictionOutcome TryEvictDatabaseForTests(string id, long idleWindowMs)
        => databaseEvictor.TryEvict(id, idleWindowMs);

    /// <summary>
    /// Test-only seam: forces one auto-analyze sweep (after the deferred renewer start completes) and
    /// returns the number of tables analyzed, so a test can drive it deterministically instead of
    /// waiting for the timer. Requires <see cref="CamusDBOptions.AutoAnalyzeEnabled"/> to be set.
    /// </summary>
    internal Task<int> RunAutoAnalyzeForTestsAsync()
        => backgroundSchedulers is null ? Task.FromResult(0) : backgroundSchedulers.RunAutoAnalyzeSweepAsync();

    /// <summary>
    /// Test-only seam: forces one row-level TTL sweep and returns the number of rows deleted, so a test
    /// can drive it deterministically instead of waiting out a tick and a table's cron cadence.
    /// Requires <see cref="CamusDBOptions.TtlEnabled"/> to be set.
    /// </summary>
    internal Task<long> RunTtlSweepForTestsAsync()
        => backgroundSchedulers is null ? Task.FromResult(0L) : backgroundSchedulers.RunTtlSweepAsync();

    /// <summary>
    /// Test-only seam: runs the TTL delete path against an explicit candidate list, so a test can
    /// present the exact state the sweep is in between its scan and its delete — including a candidate
    /// whose expiry was extended in that window — without depending on timing.
    /// </summary>
    internal async Task<(int deleted, int skipped)> DeleteExpiredRowsForTestsAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        IReadOnlyList<ObjectIdValue> rowIds,
        string expirationColumn,
        long cutoffEpochMs)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);

        try
        {
            (int deleted, int skipped) = await rowDeleter.DeleteExpiredRowsAsync(
                table, tx, rowIds, expirationColumn, cutoffEpochMs).ConfigureAwait(false);

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            return (deleted, skipped);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Test-only seam: the table ids that currently have TTL run metadata in a database, so a test can
    /// assert that abandoned records are actually reclaimed rather than merely unreferenced.
    /// </summary>
    internal Task<IReadOnlyList<string>> ListTtlRunTableIdsForTestsAsync(string dbId)
        => backgroundSchedulers is null
            ? Task.FromResult<IReadOnlyList<string>>([])
            : backgroundSchedulers.ListTtlRunTableIdsAsync(dbId);

    /// <summary>Test-only seam: cumulative TTL counters for this node.</summary>
    internal (long expired, long skipped, long failed, long spans, long runs) TtlCountersForTests()
        => backgroundSchedulers?.TtlCounters() ?? (0, 0, 0, 0, 0);

    /// <summary>Test-only seam: the most TTL spans this node has had in flight at once.</summary>
    internal int TtlPeakConcurrentSpansForTests() => backgroundSchedulers?.TtlPeakConcurrentSpans() ?? 0;

    /// <summary>
    /// Test-only seam: fails the TTL delete for any chunk the predicate selects, so a test can observe
    /// what the checkpoint does when a delete does not commit. See
    /// <c>TtlSpanSweeper.DeleteChunkFaultInjector</c> for why this cannot be provoked any other way.
    /// </summary>
    internal Task SetTtlDeleteFaultInjectorForTestsAsync(Func<IReadOnlyList<ObjectIdValue>, bool>? injector)
        => backgroundSchedulers is null
            ? Task.CompletedTask
            : backgroundSchedulers.SetTtlDeleteFaultInjectorAsync(injector);

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
    /// (with a tiny <see cref="CamusDBOptions.OrphanRetentionMs"/>) instead of waiting for the timer.
    /// </summary>
    /// <remarks>
    /// <para><b>Waits for the node to finish starting before sweeping.</b> The sweep is gated on this
    /// node leading the registry partition, and that check answers <c>false</c> <em>immediately</em>
    /// — without its usual wait for an election — while the Raft manager is still initializing. The
    /// sweep then returns 0, which a caller cannot tell apart from "nothing was due". Everything
    /// before this point is reachable without the node being started: the orphan record is written and
    /// read through ordinary KV paths.</para>
    ///
    /// <para>Nothing is needed for the election itself, which the leadership check already waits out.
    /// The background sweep needs neither, because it runs on an interval and tries again next tick.</para>
    /// </remarks>
    internal Task<int> RunOrphanReclaimForTestsAsync()
        => backgroundSchedulers is null ? Task.FromResult(0) : backgroundSchedulers.RunOrphanReclaimAsync();

    /// <summary>
    /// Clears this node's crash remnants: stale drop-intent markers, interrupted <c>DROP DATABASE</c>
    /// keyspace purges, and orphan branch metadata namespaces. Runs on startup ahead of the reclaimer;
    /// reachable here because tests invoke the production scrub path directly rather than
    /// reimplementing the same logic inline.
    /// </summary>
    internal Task ScrubOrphanBranchNamespacesAsync(EmbeddedKahuna node, DatabaseRegistry registry)
        => startupRecovery.ScrubOrphanBranchNamespacesAsync(node, registry);

    #region database

    public Task<DatabaseDescriptor> CreateDatabase(CreateDatabaseTicket ticket)
        => databaseLifecycle.CreateDatabase(ticket);

    public Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
        => databaseLifecycle.OpenDatabase(database, recoveryMode);


    public Task CloseDatabase(CloseDatabaseTicket ticket)
        => databaseLifecycle.CloseDatabase(ticket);

    public Task DropDatabase(DropDatabaseTicket ticket)
        => databaseLifecycle.DropDatabase(ticket);

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) root database by re-attaching a name to its preserved
    /// id and opening it against the retained keyspace.
    /// </summary>
    public Task<DatabaseDescriptor> RelinkDatabase(RelinkDatabaseTicket ticket)
        => databaseLifecycle.RelinkDatabase(ticket);

    public Task RenameDatabase(RenameDatabaseTicket ticket)
        => databaseLifecycle.RenameDatabase(ticket);

    #endregion

    #region DDL

    /// <summary>
    /// Executes a parsed DDL statement, routing it to the service that owns it, and times it for the
    /// slow query log.
    ///
    /// <para>Unlike a query, a DDL statement returns no cursor and is finished when this call
    /// returns, so the whole call is the duration. Schema DDL is often the slowest thing a node
    /// does — a backfill, a replicated schema change waiting on acks — which is precisely why it is
    /// recorded.</para>
    /// </summary>
    public async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(ExecuteSQLTicket ticket)
    {
        Diagnostics.SlowQueryRecording? recording = slowQueries?.Begin(ticket.Sql, ticket.DatabaseName, ticket.Principal?.UserName);

        if (recording is null)
            return await ddlDispatcher.ExecuteDDLSQL(this, ticket).ConfigureAwait(false);

        recording.Describe(SafeParseKind(ticket.Sql));

        try
        {
            ExecuteDDLSQLResult result = await ddlDispatcher
                .ExecuteDDLSQL(this, ticket.WithProbe(recording.Probe)).ConfigureAwait(false);

            recording.Finish(result.ModifiedRows, Diagnostics.SlowQueryOutcome.Completed);
            return result;
        }
        catch (Exception exception)
        {
            recording.FinishFailed(exception);
            throw;
        }
    }

    /// <summary>
    /// The statement's root node type for the slow query log, or <see cref="NodeType.Select"/>'s
    /// stand-in when it cannot be parsed.
    ///
    /// <para>It re-parses through the same cache the dispatcher below will use, so the cost is a
    /// dictionary lookup rather than a second parse. A parse failure here is swallowed: the
    /// dispatcher is about to raise the real error with the real message, and a diagnostic that
    /// changed which exception a caller sees would be worse than an entry labelled
    /// <c>unknown</c>.</para>
    /// </summary>
    private NodeType? SafeParseKind(string sql)
    {
        try
        {
            return ParseSql(sql).nodeType;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// Executes a SQL statement that returns no rows. Schema DDL is accepted here too and forwarded,
    /// so a client that routes every non-SELECT statement to this endpoint is never told a supported
    /// statement is unknown.
    /// </summary>
    public async Task<ExecuteNonSQLResult> ExecuteNonSQLQuery(ExecuteSQLTicket ticket)
    {
        Diagnostics.SlowQueryRecording? recording = slowQueries?.Begin(ticket.Sql, ticket.DatabaseName, ticket.Principal?.UserName);

        if (recording is null)
            return await nonQueryDispatcher.ExecuteNonSQLQuery(this, ticket).ConfigureAwait(false);

        recording.Describe(SafeParseKind(ticket.Sql));

        try
        {
            ExecuteNonSQLResult result = await nonQueryDispatcher
                .ExecuteNonSQLQuery(this, ticket.WithProbe(recording.Probe)).ConfigureAwait(false);

            // Rows affected stands in for rows returned: the column means "rows this statement was
            // about", and for a mutation that is what it changed.
            recording.Finish(result.ModifiedRows, Diagnostics.SlowQueryOutcome.Completed);
            return result;
        }
        catch (Exception exception)
        {
            recording.FinishFailed(exception);
            throw;
        }
    }

    /// <summary>
    /// How many times a statement re-attempts a table open that failed the schema catch-up fence
    /// (<see cref="CamusDBErrorCodes.SchemaCatchingUp"/>). The fence fires before any write or
    /// schema pin, so the in-flight transaction is unmodified and safe to reuse on each attempt.
    /// </summary>
    private const int MaxFenceRetries = 3;

    public Task<CreateTableResult> CreateTable(CreateTableTicket ticket) => schemaDdl.CreateTable(ticket);

    public Task<bool> AlterTable(AlterTableTicket ticket) => schemaDdl.AlterTable(ticket);

    /// <summary>
    /// Re-encodes every existing row so a newly added column is stored with its default value.
    /// Reachable here because <see cref="Controllers.DatabaseOpener"/> wires it into the
    /// leader-change resume coordinator.
    /// </summary>
    internal Task BackfillColumnDefaultsAsync(DatabaseDescriptor database, string tableName, ColumnInfo column)
        => schemaDdl.BackfillColumnDefaultsAsync(database, tableName, column);

    /// <summary>
    /// Test-only hook fired after each backfill batch checkpoint, so a test can force a leader change
    /// between batches without depending on timing. Writes through to the service that owns the
    /// backfill loop; both test projects assign it directly on the executor.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal Func<Task>? TestInterceptAfterBackfillCheckpoint
    {
        get => schemaDdl.TestInterceptAfterBackfillCheckpoint;
        set => schemaDdl.TestInterceptAfterBackfillCheckpoint = value;
    }

    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal CatalogsManager Catalogs => catalogs;

    /// <summary>
    /// Scans every existing row and writes an index entry for each, in bounded batches with a resume
    /// checkpoint. Reachable here because <see cref="Controllers.DatabaseOpener"/> wires it into the
    /// leader-change resume coordinator.
    /// </summary>
    internal Task BackfillIndexEntriesAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexInfo,
        string? startOffset,
        Func<string, Task>? onCheckpoint = null
    ) => schemaDdl.BackfillIndexEntriesAsync(database, tableName, indexInfo, startOffset, onCheckpoint);

    public Task<bool> AlterIndex(AlterIndexTicket ticket) => schemaDdl.AlterIndex(ticket);

    public Task<bool> DropTable(DropTableTicket ticket) => schemaDdl.DropTable(ticket);

    /// <summary>
    /// Empties a base table by replacing the key-space its rows live in. The relation keeps its
    /// identity, name and schema; only its contents generation moves.
    /// </summary>
    public Task<bool> TruncateTable(TruncateTableTicket ticket) => schemaDdl.TruncateTable(ticket);

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) table by reattaching it to the schema under a new name,
    /// reusing its preserved id and retained row/index data.
    /// </summary>
    public Task<bool> RelinkTable(RelinkTableTicket ticket) => schemaDdl.RelinkTable(ticket);

    /// <param name="dependentViews">
    /// Rewritten bodies for views that read this relation, applied in the rename's own delta. When
    /// null they are computed here; a caller that already resolved them (the materialized-view rename
    /// path) passes them so the work is not repeated.
    /// </param>
    public Task<bool> RenameTable(
        RenameTableTicket ticket,
        Dictionary<string, Catalogs.Models.ViewDefinition>? dependentViews = null)
        => schemaDdl.RenameTable(ticket, dependentViews);

    /// <summary>
    /// Opens a table by <b>database name</b>, resolving the database through the registry first.
    ///
    /// <para>For request-scoped callers that only have a name. Code that already holds a
    /// <see cref="DatabaseDescriptor"/> must call <see cref="OpenTableWithDescriptor"/> instead —
    /// passing <c>descriptor.Name</c> back into this method re-resolves a cached display name, which
    /// after a RENAME DATABASE was the pre-rename name and made every INSERT fail with
    /// "Database '&lt;old&gt;' does not exist". It is also two redundant lookups on a hot path.</para>
    /// </summary>
    public Task<TableDescriptor> OpenTable(OpenTableTicket ticket) => schemaDdl.OpenTable(ticket);

    /// <summary>
    /// Opens a table against an already-resolved database. The preferred entry point whenever the
    /// caller holds a descriptor: it skips the registry round-trip and cannot be affected by a rename
    /// that happened after the descriptor was cached.
    /// </summary>
    public Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
        => schemaDdl.OpenTableWithDescriptor(descriptor, ticket);



    /// <summary>
    /// Applies <c>ALTER TABLE t SET (key = value)</c> table storage parameters version-neutrally
    /// (rides the table blob, no <see cref="TableSchema.Version"/> bump). Public so a ticket caller
    /// can invoke it without the SQL path.
    /// </summary>
    public Task<ExecuteDDLSQLResult> AlterTableSettings(AlterTableSettingsTicket ticket)
        => tableSettings.AlterTableSettings(ticket);

    private Task<ExecuteDDLSQLResult> AlterTableSettings(DatabaseDescriptor database, AlterTableSettingsTicket ticket)
        => tableSettings.AlterTableSettings(database, ticket);

    /// <summary>
    /// Applies <c>ALTER TABLE t RESET (key, ...)</c>, removing table storage parameters so each falls
    /// back to its engine default. Public so a ticket caller can invoke it without the SQL path.
    /// </summary>
    public Task<ExecuteDDLSQLResult> AlterTableResetSettings(AlterTableResetSettingsTicket ticket)
        => tableSettings.AlterTableResetSettings(ticket);

    private Task<ExecuteDDLSQLResult> AlterTableResetSettings(DatabaseDescriptor database, AlterTableResetSettingsTicket ticket)
        => tableSettings.AlterTableResetSettings(database, ticket);

    /// <summary>
    /// Adds or drops a CHECK (or named NOT NULL) constraint on an existing table. For ADD CHECK,
    /// scans all existing rows and rejects if any row violates the expression.
    /// </summary>
    public Task<ExecuteDDLSQLResult> AlterConstraint(AlterConstraintTicket ticket)
        => schemaDdl.AlterConstraint(ticket);

    /// <summary>
    /// Attaches or removes a comment on a table, column, index, or database. Public so a ticket caller
    /// can invoke it without going through SQL.
    /// </summary>
    public Task<ExecuteDDLSQLResult> Comment(CommentTicket ticket) => schemaDdl.Comment(ticket);

    private Task<ExecuteDDLSQLResult> Comment(DatabaseDescriptor database, CommentTicket ticket)
        => schemaDdl.Comment(database, ticket);

    private Task CommentDatabase(CommentTicket ticket) => schemaDdl.CommentDatabase(ticket);


    /// <summary>
    /// The authorization gate: rejects an unauthenticated request and checks the statement against the
    /// caller's privileges before any lock or mutation. A no-op when authentication is off.
    /// </summary>
    private Task EnforceAsync(ExecuteSQLTicket ticket, NodeAst ast)
        => statementAuthorizer.EnforceAsync(ticket, ast);

    /// <summary>
    /// Publishes the principal and required privilege to the ambient authorization scope for the
    /// per-table check. Must stay a <b>synchronous</b> call from the entry method: the scope is an
    /// <see cref="AsyncLocal{T}"/>, and a write made inside an awaited method would not flow to the
    /// table-open callees.
    /// </summary>
    private void SetAuthorizationScope(ExecuteSQLTicket ticket, NodeAst ast)
        => statementAuthorizer.SetAuthorizationScope(ticket, ast);

    /// <summary>The principal whose grants filter a catalog listing, or null when no filtering applies.</summary>
    private Principal? VisibilityPrincipal(ExecuteSQLTicket ticket)
        => statementAuthorizer.VisibilityPrincipal(ticket);



    /// <summary>
    /// Requires that the caller may change who owns <paramref name="view"/>: a superuser, or its
    /// current owner.
    /// </summary>
    /// <remarks>
    /// Ownership decides whose privileges the view's body runs with, so transferring it is a transfer
    /// of authority — an <c>Alter</c> grant on the view is not enough, or anyone who could rename a
    /// view could also point its definer's rights at an account they control.
    /// </remarks>
    private Task RequireViewOwnershipAsync(
        DatabaseDescriptor database, string viewName, Catalogs.Models.ViewSchema view, ExecuteSQLTicket ticket)
        => statementAuthorizer.RequireViewOwnershipAsync(database, viewName, view, ticket);

    /// <summary>
    /// Test-only: runs the abandoned-refresh sweep for one database synchronously, so a test can drive
    /// it instead of waiting for the background reclaimer's interval.
    /// </summary>
    internal Task<int> ReclaimAbandonedRefreshesForTesting(string databaseName)
        => startupRecovery.ReclaimAbandonedRefreshesAsync(this, databaseName, CancellationToken.None);

    /// <summary>
    /// Test-only: the cross-database registry this engine uses, so a test can take the very fence a
    /// refresh takes and prove the gate is the cluster-visible one rather than process state.
    /// </summary>
    internal Task<DatabaseRegistry> GetDatabaseRegistryAsync() => registryTask;

    /// <summary>
    /// Test-only: this engine's catalog manager, so a test can inspect and write the durable records
    /// that own a refresh's staging storage.
    /// </summary>
    internal CatalogsManager GetCatalogsManagerForTesting() => catalogs;


    private AuthService RequireAuthService()
    {
        if (authService is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication service is unavailable (no shared node was configured)");
        return authService;
    }

    /// <summary>Verifies credentials and returns the bearer token plus its absolute expiry
    /// (see <see cref="AuthService.LoginAsync"/>). <paramref name="source"/> is the caller's origin
    /// (e.g. remote IP) for per-source rate limiting.</summary>
    public Task<LoginResult> LoginAsync(string user, string password, string source = "") => RequireAuthService().LoginAsync(user, password, source);

    /// <summary>Resolves a bearer token to a <see cref="Principal"/>, or throws AuthenticationFailed.</summary>
    public Task<Principal> ResolvePrincipalAsync(string? bearer) => RequireAuthService().ResolvePrincipalAsync(bearer);

    /// <summary>Revokes the presented token (logout).</summary>
    public Task LogoutAsync(string? bearer) => RequireAuthService().LogoutAsync(bearer);

    private BackupManager RequireBackupManager()
    {
        if (backupManager is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Backup service is unavailable (no shared node was configured)");
        return backupManager;
    }

    /// <summary>
    /// Superuser + backup-configured gate for every backup/PITR admin operation. The null-manager
    /// check lives here because an engine built without a shared node has no backup surface at all;
    /// the authorization and configured-directory checks belong to the manager that owns them.
    /// </summary>
    private void EnsureBackupAllowed(Principal? principal)
        => RequireBackupManager().EnsureAllowed(principal, options.AuthenticationEnabled);

    /// <summary>
    /// Takes a node-wide backup (full/incremental/coordinated). Superuser-gated; requires a configured
    /// backup directory. Covers every database at once.
    /// </summary>
    public Task<BackupInfo> TakeBackup(TakeBackupTicket ticket, CancellationToken cancellationToken = default)
    {
        validator.Validate(ticket);
        EnsureBackupAllowed(ticket.Principal);
        return RequireBackupManager().TakeBackup(ticket, cancellationToken);
    }

    /// <summary>Lists every backup in the node's catalog. Superuser-gated; requires a configured backup directory.</summary>
    public Task<IReadOnlyList<BackupInfo>> ListBackups(ListBackupsTicket ticket, CancellationToken cancellationToken = default)
    {
        EnsureBackupAllowed(ticket.Principal);
        return RequireBackupManager().ListBackups(cancellationToken);
    }

    /// <summary>
    /// Resolves and validates the backup chain ending at the given leaf. Superuser-gated; requires a
    /// configured backup directory. Throws <see cref="CamusDBErrorCodes.BackupChainInvalid"/> if the
    /// chain is broken.
    /// </summary>
    public Task<IReadOnlyList<BackupInfo>> GetBackupChain(GetBackupChainTicket ticket, CancellationToken cancellationToken = default)
    {
        validator.Validate(ticket);
        EnsureBackupAllowed(ticket.Principal);
        return RequireBackupManager().GetBackupChain(ticket, cancellationToken);
    }

    /// <summary>
    /// Offline restore into a fresh data root (optionally to a point in time). Superuser-gated; requires
    /// a configured backup directory and that restore is enabled (a configured restore root). Non-
    /// destructive to the live node — the operator restarts a fresh node with
    /// <c>data_dir = </c><see cref="RestoreResult.DataRoot"/> afterwards.
    /// </summary>
    public Task<RestoreResult> RestoreBackup(RestoreBackupTicket ticket, CancellationToken cancellationToken = default)
    {
        validator.Validate(ticket);
        EnsureBackupAllowed(ticket.Principal);
        return RequireBackupManager().Restore(ticket, cancellationToken);
    }

    /// <summary>
    /// Runs (or, when <paramref name="dryRun"/> is true, previews) backup garbage collection: retention
    /// enforcement plus orphan-artifact sweep. Superuser-gated; requires a configured backup directory.
    /// GC also runs automatically after each backup and on the periodic tick — this is the on-demand
    /// operator entry point.
    /// </summary>
    public Task<BackupGcResult> RunBackupGarbageCollection(bool dryRun, Principal? principal, CancellationToken cancellationToken = default)
    {
        EnsureBackupAllowed(principal);
        return RequireBackupManager().RunGarbageCollection(dryRun, cancellationToken);
    }

    /// <summary>
    /// If <see cref="CamusDBOptions.AuthenticationEnabled"/> is on, ensures the catalog has at least one
    /// user by seeding the configured bootstrap superuser when it is empty. Fails startup (fail-closed)
    /// when auth is enabled, the catalog is empty, and no bootstrap secret was supplied — never opens an
    /// unauthenticated administration window. A no-op when auth is disabled or a user already exists.
    ///
    /// <para>The password is a parameter rather than a read of <c>options</c> on purpose: the injected
    /// <see cref="CamusDBOptions"/> is registered with <see cref="CamusDBOptions.BootstrapSuperuserPassword"/>
    /// blanked, so no long-lived component retains the one-shot startup secret. The caller — which still
    /// holds the unscrubbed copy resolved from the environment — passes it here and drops it immediately
    /// afterwards. Reading it off <c>options</c> would always see the empty string and make seeding
    /// impossible.</para>
    /// </summary>
    /// <param name="bootstrapUser">Bootstrap superuser name, from the unscrubbed startup configuration.</param>
    /// <param name="bootstrapPassword">Cleartext bootstrap password; hashed here and never persisted or logged.</param>
    public Task EnsureBootstrapSuperuserAsync(string bootstrapUser, string bootstrapPassword)
        => userAdmin.EnsureBootstrapSuperuserAsync(bootstrapUser, bootstrapPassword);

    /// <summary>
    /// Deletes session records whose absolute expiry has passed, and returns how many went. A no-op
    /// returning zero when authentication is off or this engine has no shared node. Safe to call
    /// concurrently from every node and safe to repeat — see <c>AuthCatalog.ReapExpiredSessionsAsync</c>.
    /// </summary>
    public Task<int> ReapExpiredSessionsAsync() => userAdmin.ReapExpiredSessionsAsync();

    /// <summary>
    /// Creates a server-level user in the shared auth catalog. The cleartext password (if any) is hashed
    /// here and never persisted or logged; the ticket carries it no further. Server-level — returns no
    /// descriptor.
    /// </summary>
    public Task<ExecuteDDLSQLResult> CreateUser(CreateUserTicket ticket) => userAdmin.CreateUser(ticket);

    /// <summary>Rotates a user's password verifier and advances its credential epoch.</summary>
    public Task<ExecuteDDLSQLResult> AlterUser(AlterUserTicket ticket) => userAdmin.AlterUser(ticket);

    /// <summary>Drops a user and all its grants in one catalog transaction.</summary>
    public Task<ExecuteDDLSQLResult> DropUser(DropUserTicket ticket) => userAdmin.DropUser(ticket);

    /// <summary>
    /// Applies a <c>GRANT</c>/<c>REVOKE</c>. Resolves the grant object's name(s) to immutable ids first
    /// (a database via the registry; a table by opening the target database's catalog) so the grant is
    /// bound to the id, not the name, and never resurrects on a dropped-and-recreated object.
    /// </summary>
    public Task<ExecuteDDLSQLResult> Grant(GrantTicket ticket) => userAdmin.Grant(ticket);

    /// <summary>
    /// Turns a grant ticket's scope names into an id-bound <see cref="GrantScope"/>. The database must
    /// exist (resolved through the registry); a table scope additionally opens the target database and
    /// resolves the table's id. Global scope needs no resolution.
    /// </summary>

    /// <summary>
    /// Returns the grants for <paramref name="userName"/> as rows for <c>SHOW GRANTS</c>. Server-level:
    /// reads the auth catalog and needs no open database.
    /// </summary>
    internal Task<(IReadOnlyList<GrantRecord> Grants, bool UserExists)> ListGrantsForShowAsync(string userName)
        => userAdmin.ListGrantsForShowAsync(userName);

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



    #endregion

    #region DML

    /// <summary>Inserts a row from a typed ticket.</summary>
    public Task<InsertResult> Insert(InsertTicket ticket) => rowCommands.Insert(ticket);

    /// <summary>Updates rows specifying filters and sorts.</summary>
    public Task<UpdateResult> Update(UpdateTicket ticket) => rowCommands.Update(ticket);

    /// <summary>Deletes rows specifying a filter criteria.</summary>
    public Task<DeleteResult> Delete(DeleteTicket ticket) => rowCommands.Delete(ticket);

    /// <summary>Queries table data specifying filters and sorts.</summary>
    public Task<(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)> Query(QueryTicket ticket)
        => rowCommands.Query(ticket);

    /// <summary>Queries a table by the row's id.</summary>
    public Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
        => rowCommands.QueryById(ticket);

    /// <summary>Mints an HLC timestamp through the shared node's clock.</summary>
    internal HLCTimestamp ClusterNow() => ctasExecutor.ClusterNow();

    /// <summary>
    /// Creates a relation inside its own committed DDL transaction. Used by the materialized-view
    /// refresh to build its staging relation.
    /// </summary>
    internal Task<bool> CreateRelationInDdlTransactionAsync(
        DatabaseDescriptor database, CreateTableTicket ticket, string tableId, bool validate)
        => ctasExecutor.CreateRelationInDdlTransactionAsync(database, ticket, tableId, validate);

    /// <summary>Drops a refresh's staging relation, best-effort.</summary>
    internal Task DropStagingRelationAsync(DatabaseDescriptor database, string relationName)
        => ctasExecutor.DropStagingRelationAsync(database, relationName);

    /// <summary>Loads one chunk of a refresh's rows into its staging relation.</summary>
    internal Task<int> InsertRefreshChunkAsync(
        DatabaseDescriptor database,
        string relationName,
        IReadOnlyList<DerivedColumnSchema> sourceColumns,
        IReadOnlyList<string> targetColumns,
        IReadOnlyList<QueryResultRow> rows)
        => ctasExecutor.InsertRefreshChunkAsync(database, relationName, sourceColumns, targetColumns, rows);

    /// <summary>
    /// Executes a SQL statement that returns rows: <c>SELECT</c> in all its forms and the
    /// <c>SHOW</c> family.
    /// </summary>
    public Task<(DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)> ExecuteSQLQuery(
        ExecuteSQLTicket ticket, CacheMetadataHolder? metaOut = null, QuerySchemaHolder? schemaOut = null)
        => selectExecutor.ExecuteSQLQuery(ticket, metaOut, schemaOut);

    /// <summary>
    /// Executes a peer coordinator's span-scan fragment on this node. Read path only: a zero-identity
    /// snapshot transaction at the coordinator's timestamp, no locks, no session.
    /// </summary>
    public IAsyncEnumerable<QueryFragmentRow> ExecuteQueryFragment(
        QueryFragmentRequest request, CancellationToken cancellationToken = default)
        => selectExecutor.ExecuteQueryFragment(request, cancellationToken);

    /// <summary>
    /// Binds a view body and returns its output schema and projections without reading any rows, so a
    /// view's columns are derived through the same path a plain SELECT's metadata is.
    /// </summary>
    internal Task<SelectRowSource> BuildViewSourceAsync(
        DatabaseDescriptor database, NodeAst bodyAst, ExecuteSQLTicket ticket)
        => selectExecutor.BuildViewSourceAsync(database, bodyAst, ticket);

    /// <summary>
    /// Opens a materialized view's body as a row source pinned to a snapshot, with the revision floor
    /// held for as long as the source lives.
    /// </summary>
    internal Task<SelectRowSource> BuildMaterializedViewSourceAsync(
        DatabaseDescriptor database, NodeAst bodyAst, ExecuteSQLTicket ticket, HLCTimestamp snapshot)
        => selectExecutor.BuildMaterializedViewSourceAsync(database, bodyAst, ticket, snapshot);






















    #endregion













    public async ValueTask DisposeAsync()
    {
        // Unhook from the options holder first: a swap published mid-shutdown must not fan out into
        // components this method is about to dispose.
        optionsSubscription?.Dispose();

        // Stop the background loops first. Their host awaits its own deferred start, so a scheduler
        // created by an in-flight start is observed before disposal rather than left running.
        if (backgroundSchedulers is not null)
            await backgroundSchedulers.DisposeAsync().ConfigureAwait(false);

        // Before the closer: stopping the sweep first means it can never be mid-eviction while
        // shutdown is disposing the very descriptors it is inspecting.
        await databaseEvictor.DisposeAsync().ConfigureAwait(false);

        // Same ordering argument: the freshness sweep probes descriptors, so it must stop before
        // the closer disposes them.
        await schemaFreshnessSweeper.DisposeAsync().ConfigureAwait(false);

        await databaseCloser.DisposeAsync();
        await sqlParserCache.DisposeAsync().ConfigureAwait(false);

        // The Kommander/Kahuna meters are static, so a listener that outlived its engine would keep
        // observing for the life of the process — which the test suite, building many engines, would
        // accumulate.
        engineMetrics?.Dispose();

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
