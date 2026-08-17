
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna;
using CamusDB.Core.Cache;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The read path: every statement that returns rows. That is <c>SELECT</c> in all its forms — plain,
/// FROM-less, through a view, joined, time-travelled — plus the <c>SHOW</c> family, plus the
/// distributed query fragments a peer coordinator asks this node to execute.
///
/// <para><b>The dispatch is ordered, and the order is the design.</b> Statements that need no
/// database context (<c>SHOW DATABASES</c>, engine stats, variables, cluster settings, orphans,
/// branches, grants) are answered before anything is opened, so introspection never forces a
/// database resident. Only then is the database opened, views expanded, and the statement bound.</para>
///
/// <para><b>View expansion happens between opening and binding, and cannot move.</b> Resolving a name
/// to a view needs the schema, so it must follow the open; binding resolves relation names against
/// the table map, where a view does not appear, so it must precede the bind. Expansion is also the
/// only moment a read knows a view was named at all, which is why the view's own authorization check
/// hangs off it.</para>
///
/// <para><b>A source that pins a snapshot owns a lease for as long as its cursor lives.</b> Time
/// travel and materialized-view reads hold a Kahuna snapshot floor so the history they read cannot be
/// reclaimed mid-scan; the lease is tied to the returned <see cref="SelectRowSource"/>, so disposing
/// the source is what releases it. A caller that drains a cursor after disposing its source is
/// reading history nothing is holding open any more.</para>
/// </summary>
internal sealed class SelectStatementExecutor
{
    internal readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    internal CamusDBOptions options;

    internal readonly CatalogsManager catalogs;

    internal readonly QueryExecutor queryExecutor;

    internal readonly SchemaQuerier schemaQuerier;

    internal readonly QueryBinder queryBinder;

    internal readonly SubqueryRewriter subqueryRewriter;

    internal readonly ExistsSubqueryPreparer existsSubqueryPreparer;

    internal readonly ExplainExecutor explainExecutor;

    internal readonly TableAnalyzer tableAnalyzer;

    internal readonly SemiJoinAnalyzer semiJoinAnalyzer;

    internal readonly SelectQueryCreator selectQueryCreator;

    internal readonly SqlExecutor sqlExecutor;

    internal readonly SqlParserCache sqlParserCache;

    internal readonly Auth.StatementAuthorizer statementAuthorizer;

    internal readonly Auth.UserAdminService userAdmin;

    internal readonly Maintenance.BackgroundSchedulerHost? backgroundSchedulers;

    internal readonly ClusterSettingsService? clusterSettings;

    internal readonly EngineMetricsCollector? engineMetrics;

    /// <summary>
    /// How many times a statement re-attempts a table open that failed the schema catch-up fence.
    /// The fence fires before any write or schema pin, so the in-flight transaction is unmodified
    /// and safe to reuse on each attempt.
    /// </summary>
    internal const int MaxFenceRetries = 3;

    internal SelectStatementExecutor(
        ExecutorContext context,
        CamusDBOptions options,
        CatalogsManager catalogs,
        QueryExecutor queryExecutor,
        SchemaQuerier schemaQuerier,
        QueryBinder queryBinder,
        SubqueryRewriter subqueryRewriter,
        ExistsSubqueryPreparer existsSubqueryPreparer,
        ExplainExecutor explainExecutor,
        TableAnalyzer tableAnalyzer,
        SemiJoinAnalyzer semiJoinAnalyzer,
        SelectQueryCreator selectQueryCreator,
        SqlExecutor sqlExecutor,
        SqlParserCache sqlParserCache,
        Auth.StatementAuthorizer statementAuthorizer,
        Auth.UserAdminService userAdmin,
        Maintenance.BackgroundSchedulerHost? backgroundSchedulers,
        ClusterSettingsService? clusterSettings,
        EngineMetricsCollector? engineMetrics
    )
    {
        this.context = context;
        this.options = options;
        this.catalogs = catalogs;
        this.queryExecutor = queryExecutor;
        this.schemaQuerier = schemaQuerier;
        this.queryBinder = queryBinder;
        this.subqueryRewriter = subqueryRewriter;
        this.existsSubqueryPreparer = existsSubqueryPreparer;
        this.explainExecutor = explainExecutor;
        this.tableAnalyzer = tableAnalyzer;
        this.semiJoinAnalyzer = semiJoinAnalyzer;
        this.selectQueryCreator = selectQueryCreator;
        this.sqlExecutor = sqlExecutor;
        this.sqlParserCache = sqlParserCache;
        this.statementAuthorizer = statementAuthorizer;
        this.userAdmin = userAdmin;
        this.backgroundSchedulers = backgroundSchedulers;
        this.clusterSettings = clusterSettings;
        this.engineMetrics = engineMetrics;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Each statement pins the field once, so an
    /// in-flight statement keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Collects the engine-maintained (non-meter) counter rows <c>SHOW ENGINE STATS</c> merges
    /// with the meter snapshot: the TTL scheduler's totals and — only when distributed query
    /// execution is enabled, per the statement's all-or-nothing rule — the
    /// <c>distributed.*</c> counters.
    /// </summary>
    private IReadOnlyList<EngineMetricRow>? BuildEngineCounterRows()
    {
        IReadOnlyList<EngineMetricRow>? ttlRows = backgroundSchedulers?.TtlMetricRows();

        IReadOnlyList<EngineMetricRow>? distributedRows = options.DistributedQueryExecutionEnabled
            ? DistributedQueryMetricsReporter.Build(queryExecutor.DistributedMetrics)
            : null;

        if (ttlRows is null)
            return distributedRows;

        if (distributedRows is null)
            return ttlRows;

        return [.. ttlRows, .. distributedRows];
    }

    /// <summary>
    /// Execute a SQL statement that returns rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<(DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)> ExecuteSQLQuery(ExecuteSQLTicket ticket, CacheMetadataHolder? metaOut = null, QuerySchemaHolder? schemaOut = null)
    {
        context.Validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        statementAuthorizer.SetAuthorizationScope(ticket, ast);
        ticket = SessionScalarFunctions.AttachSessionValues(ticket, ast);
        await statementAuthorizer.EnforceAsync(ticket, ast).ConfigureAwait(false);

        // SHOW DATABASES does not require a database context — resolve the registry and return.
        if (ast.nodeType == NodeType.ShowDatabases)
        {
            DatabaseRegistry reg = await context.Registry.ConfigureAwait(false);
            string? dbPattern = UnquoteLikePattern(ast.leftAst?.yytext);
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowDatabasesSchema;
            return (null!, schemaQuerier.ShowDatabases(reg.List(), dbPattern, statementAuthorizer.VisibilityPrincipal(ticket)));
        }

        // SHOW ENGINE STATS reports this process's own embedded Kommander/Kahuna metrics. Node-local by
        // definition — it must not forward to the leader — and it opens no database and no transaction.
        if (ast.nodeType == NodeType.ShowEngineStats)
        {
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowEngineStatsSchema;

            return (null!, schemaQuerier.ShowEngineStats(
                engineMetrics,
                UnquoteLikePattern(ast.leftAst?.yytext),
                LocalNodeLabel(),
                BuildEngineCounterRows()));
        }

        // SHOW VARIABLES reports the configuration this engine was constructed with. Node-local for the
        // same reason as the metrics above, and it opens no database and no transaction.
        if (ast.nodeType == NodeType.ShowVariables)
        {
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowVariablesSchema;

            return (null!, schemaQuerier.ShowVariables(options, UnquoteLikePattern(ast.leftAst?.yytext)));
        }

        // SHOW CLUSTER SETTINGS lists the overlay entries the cluster currently carries — what SET
        // CLUSTER SETTING has changed and RESET has not yet dropped. It opens no database and no
        // transaction; the per-key effect on this node is what SHOW VARIABLES reports.
        if (ast.nodeType == NodeType.ShowClusterSettings)
        {
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowClusterSettingsSchema;

            if (clusterSettings is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Cluster settings are not available on this engine");

            return (null!, schemaQuerier.ShowClusterSettings(clusterSettings.List(), UnquoteLikePattern(ast.leftAst?.yytext)));
        }

        // SHOW ORPHAN DATABASES lists recoverable dropped databases from the registry — no db context.
        if (ast.nodeType == NodeType.ShowOrphanDatabases)
        {
            DatabaseRegistry reg = await context.Registry.ConfigureAwait(false);
            List<OrphanDatabaseRecord> orphans = await reg.LoadDatabaseOrphansAsync().ConfigureAwait(false);
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowOrphanDatabasesSchema;
            return (null!, schemaQuerier.ShowOrphanDatabases(orphans));
        }

        // SHOW BRANCHES and SHOW ANCESTORS operate on the registry directly.
        if (ast.nodeType is NodeType.ShowBranches or NodeType.ShowAncestors)
        {
            string targetName = ast.leftAst!.yytext!;
            DatabaseRegistry reg = await context.Registry.ConfigureAwait(false);
            DatabaseRegistryEntry? target = await reg.TryResolveEntryAsync(targetName).ConfigureAwait(false);
            Principal? branchPrincipal = statementAuthorizer.VisibilityPrincipal(ticket);

            // A database the caller has no grant on is reported as non-existent, deliberately using the
            // same error as a name that really is unregistered: an "insufficient privilege" here would
            // confirm the database exists, which is exactly what naming an arbitrary database in
            // SHOW BRANCHES / SHOW ANCESTORS would otherwise be used to probe for.
            if (target is null || (branchPrincipal is not null && !branchPrincipal.CanSeeDatabase(target.Id)))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{targetName}' does not exist");
            IReadOnlyList<DatabaseRegistryEntry> allEntries = await reg.ScanAllEntriesAsync().ConfigureAwait(false);
            if (ast.nodeType == NodeType.ShowBranches)
            {
                if (schemaOut is not null)
                    schemaOut.Schema = DerivedTableSchemaBuilder.ShowBranchesSchema;
                return (null!, schemaQuerier.ShowBranches(allEntries, target, branchPrincipal));
            }
            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowAncestorsSchema;
            return (null!, schemaQuerier.ShowAncestors(target, allEntries, branchPrincipal));
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

            (IReadOnlyList<GrantRecord> grants, bool userExists) = await userAdmin.ListGrantsForShowAsync(grantUser).ConfigureAwait(false);
            if (!userExists)
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{grantUser}' does not exist");

            if (schemaOut is not null)
                schemaOut.Schema = DerivedTableSchemaBuilder.ShowGrantsSchema;
            return (null!, schemaQuerier.ShowGrants(grantUser, grants));
        }

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName);

        ast = ExpandViews(database, ast);

        // Expansion can pull a session call (current_user() and friends) in from a view body that the
        // statement itself never names, so the snapshot is reconsidered against the expanded tree.
        ticket = SessionScalarFunctions.AttachSessionValues(ticket, ast);

        // Mark the transaction as having executed a statement for every statement type except the
        // SET TRANSACTION family — those must be the first statement per standard SQL semantics.
        if (!DML.SetTransactionStatement.IsSetTransactionStatement(ast.nodeType))
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

                    (BoundSelectQuery boundQuery, QueryTicket queryTicket) =
                        await BuildBoundQueryAsync(database, ast, ticket, metaOut: metaOut).ConfigureAwait(false);

                    SelectQuery selectQuery = boundQuery.Query;

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
                            // A query that reads through a view is multi-source only because expansion
                            // produced a derived table; reporting "Join" would send its author looking
                            // for a join they did not write.
                            metaOut.BypassReason =
                                selectQuery.Source is JoinSource || boundQuery.Sources.Count > 1
                                    ? QueryCacheBypassReason.Join
                                    : QueryCacheBypassReason.DerivedSource;
                        }
                        if (schemaOut is not null)
                            schemaOut.Schema = DerivedTableSchemaBuilder.Build(selectQuery, boundQuery);
                        return (database, ExecuteBoundQuery(database, boundQuery, queryTicket));
                    }

                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.Build(selectQuery, boundQuery);

                    return (database, ExecuteBoundQuery(database, boundQuery, queryTicket, metaOut));
                }

            case NodeType.ShowTables:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowTablesSchema;
                    string? tablePattern = UnquoteLikePattern(ast.leftAst?.yytext);
                    return (database, schemaQuerier.ShowTables(database, tablePattern, statementAuthorizer.VisibilityPrincipal(ticket)));
                }

            case NodeType.ShowViews:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowViewsSchema;
                    return (database, schemaQuerier.ShowViews(
                        database, UnquoteLikePattern(ast.leftAst?.yytext), statementAuthorizer.VisibilityPrincipal(ticket)));
                }

            case NodeType.ShowMaterializedViews:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowMaterializedViewsSchema;
                    return (database, schemaQuerier.ShowMaterializedViews(
                        database, UnquoteLikePattern(ast.leftAst?.yytext), statementAuthorizer.VisibilityPrincipal(ticket)));
                }

            case NodeType.ShowCreateView:
                {
                    string showViewName = ast.leftAst!.yytext!;

                    if (!database.Schema.Views.TryGetValue(showViewName, out ViewSchema? shownView))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ViewDoesntExist, $"View '{showViewName}' does not exist");

                    ViewAuthorization.Require(
                        database, showViewName, shownView, Privilege.Select);

                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowCreateViewSchema;

                    return (database, schemaQuerier.ShowCreateView(database.Schema, showViewName, shownView));
                }

            case NodeType.ShowCreateMaterializedView:
                {
                    TableSchema shownMatView = DDL.MaterializedViewRefresher
                        .RequireMaterializedView(database, ast.leftAst!.yytext!);

                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowCreateMaterializedViewSchema;

                    return (database, schemaQuerier.ShowCreateMaterializedView(database.Schema, shownMatView));
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

                    string describedName = ast.leftAst!.yytext!;

                    // Checked before the table open, which resolves physical relations only and would
                    // otherwise refuse a view. DESCRIBE is a read, so it must work on a view.
                    if (database.Schema.Views.TryGetValue(describedName, out ViewSchema? describedView))
                    {
                        ViewAuthorization.Require(
                            database, describedName, describedView, Privilege.Select);

                        return (database, schemaQuerier.ShowViewColumns(describedView));
                    }

                    TableDescriptor table = await context.TableOpener.Open(database, describedName).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowColumns(table));
                }

            case NodeType.ShowIndexes:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowIndexesSchema;
                    TableDescriptor table = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowIndexes(table));
                }

            case NodeType.ShowStatistics:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowStatisticsSchema;

                    string statisticsTarget = ast.leftAst!.yytext!;

                    // A non-materialized view stores no rows, so it has no statistics of its own —
                    // the estimates a plan over it uses belong to the tables its body reads. Caught
                    // here because the table-open path would otherwise reject it as "cannot be
                    // written to", which is both wrong (this is a read) and unhelpful.
                    //
                    // Privilege first, and through the view's own check: nothing opens a view, so the
                    // chokepoint that guards every other relation never runs for one. Explaining that
                    // a name is a view before checking the grant would answer "does this object
                    // exist, and what is it?" for a caller who may not read it at all.
                    if (database.Schema.Views.TryGetValue(statisticsTarget, out ViewSchema? viewDefinition))
                    {
                        ViewAuthorization.Require(
                            database, statisticsTarget, viewDefinition, Privilege.Select);

                        throw new CamusDBException(
                            CamusDBErrorCodes.ViewDoesntExist,
                            $"'{statisticsTarget}' is a view and has no statistics of its own; "
                            + "ask for the statistics of the tables its definition reads");
                    }

                    TableDescriptor table = await context.TableOpener.Open(database, statisticsTarget).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    // Read before returning the stream: a snapshot taken here fails as a statement
                    // error, while one taken lazily inside the projection would fail mid-result.
                    Statistics.Models.TableStatisticsView? statisticsView = await context.Statistics
                        .ReadForDisplayAsync(database, table, CancellationToken.None)
                        .ConfigureAwait(false);

                    return (database, schemaQuerier.ShowStatistics(table, statisticsView));
                }

            case NodeType.ShowCreateTable:
                {
                    if (schemaOut is not null)
                        schemaOut.Schema = DerivedTableSchemaBuilder.ShowCreateTableSchema;
                    TableDescriptor table = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
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
                    DatabaseRegistry showRegistry = await context.Registry.ConfigureAwait(false);
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
                    TableDescriptor table = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    QueryResultRow result = await tableAnalyzer.AnalyzeAsync(database, table, ticket.TxnState).ConfigureAwait(false);
                    return (database, QueryResultStream.FromRow(result));
                }

            case NodeType.SetTransaction:
            case NodeType.SetTransactionLocking:
            case NodeType.SetTransactionPriority:
                DML.SetTransactionStatement.Apply(ast, ticket);
                return (database, AsyncEnumerable.Empty<QueryResultRow>());

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Replaces every non-materialized view reference in <paramref name="ast"/> with a derived table
    /// over the view's stored body, so the rest of the pipeline never has to know views exist.
    ///
    /// <para>Must run <b>after</b> the database is opened (the schema is what resolves a name to a
    /// view) and <b>before</b> the statement is bound (binding resolves relation names against the
    /// table map, where a view is not). Returns the same AST instance when the statement references
    /// no view, which is both the common case and the only safe treatment of an AST shared from the
    /// parser cache.</para>
    ///
    /// <para>The view body is re-parsed through that same cache. Keying on the body's SQL text needs
    /// no explicit invalidation: <c>CREATE OR REPLACE VIEW</c> stores different normalized text, so
    /// the next expansion is simply a different cache key.</para>
    /// </summary>
    internal NodeAst ExpandViews(DatabaseDescriptor database, NodeAst ast)
        => ViewExpander.Expand(
            database.Schema,
            ast,
            options.MaxViewExpansionDepth,
            sql => SQLParserProcessor.Parse(sql, sqlParserCache),
            // Expansion is the only point at which a read knows a view was named, so the caller is
            // checked against the view object here or nowhere. Select regardless of the statement's own
            // requirement: reading through a view is a read of the view, whatever the outer statement
            // goes on to do with the rows.
            (viewName, view) => ViewAuthorization.Require(
                database, viewName, view, Privilege.Select));

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
    internal ExecuteSQLTicket ApplyAsOfSystemTime(NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (ast.extendedSeven is null)
            return ticket;

        KvTransaction current = ticket.TxnState;

        if (!current.IsReadOnly || current.TransactionId != HLCTimestamp.Zero)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidAsOfSystemTime,
                "AS OF SYSTEM TIME is only supported for an autocommit read-only SELECT, not inside an " +
                "explicit or promoted transaction.");

        if (context.SharedNode is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "AS OF SYSTEM TIME requires a storage node to resolve the snapshot timestamp.");

        HLCTimestamp now = context.SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());
        HLCTimestamp snapshotT = AsOfSystemTimeResolver.Resolve(ast.extendedSeven, ticket.Parameters, now);

        KvTransaction snapshotTx = KvTransaction.CreateSnapshotReadOnly(snapshotT);
        return new ExecuteSQLTicket(snapshotTx, ticket.DatabaseName, ticket.Sql, ticket.Parameters);
    }

    /// <summary>
    /// Runs a SELECT statement's AST through the full logical pipeline — semi-join extraction,
    /// subquery rewriting, binding, correlated-EXISTS preparation — and returns the bound query
    /// together with the executable <see cref="QueryTicket"/>, with every source's schema version
    /// pinned to the transaction.
    ///
    /// <para>This is shared by <c>SELECT</c> and by the statements that consume a query as a row
    /// source (<c>INSERT … SELECT</c>). Those must go through the identical pipeline: a second copy
    /// of this sequence would drift, and a source query that skipped, say, subquery rewriting would
    /// fail or silently read the wrong rows. The stages are order-dependent — semi-join extraction
    /// must precede subquery rewriting (which would otherwise materialise those subqueries), and
    /// EXISTS preparation needs the bound sources.</para>
    /// </summary>
    /// <param name="exclusivePredicateLocks">
    /// True when the caller's writes depend on the rows this scan reads, which makes the scan
    /// write-driving: predicate range locks become exclusive and the scan's reads fold into the
    /// commit-time read set.
    /// </param>
    /// <param name="suppressCacheHint">True to ignore any <c>{cache=name}</c> hint on the query.</param>
    internal async Task<(BoundSelectQuery Bound, QueryTicket QueryTicket)> BuildBoundQueryAsync(
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        bool exclusivePredicateLocks = false,
        bool suppressCacheHint = false,
        CacheMetadataHolder? metaOut = null)
    {
        SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(ast);

        // Detect an inner subquery that carries a {cache=name} hint when the outer SELECT has none.
        // SubqueryRewriter executes all inner subqueries live and discards inner cache hints — they
        // are inert. Surface the bypass in the outer response so the caller sees an explicit
        // "inner-hint" bypass rather than silence (which looks identical to an un-hinted query).
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
        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(
            boundQuery, ticket, existsRegistry, specs, exclusivePredicateLocks, suppressCacheHint);
        PinSchemaVersions(database, boundQuery.Sources, ticket.TxnState);

        return (boundQuery, queryTicket);
    }

    /// <summary>
    /// Opens the row cursor for an already-bound query, choosing the join executor or the
    /// single-table scan the same way the SELECT path does. Callers that consume a query as a row
    /// source must route through here so a multi-source source query is not silently executed as a
    /// single-table scan of its first table.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> ExecuteBoundQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket queryTicket,
        CacheMetadataHolder? metaOut = null)
        => bound.IsMultiSource
            ? queryExecutor.ExecuteJoinQuery(database, bound, queryTicket)
            : queryExecutor.Query(database, bound.PrimaryTable, queryTicket, metaOut);

    internal async Task<SelectRowSource> BuildSelectSourceAsync(
        DatabaseDescriptor database,
        NodeAst sourceAst,
        ExecuteSQLTicket ticket,
        string statementName,
        HLCTimestamp? explicitSnapshot = null)
    {
        // The source AST is a view/CTAS/INSERT…SELECT body that reached here already expanded, so it
        // may call a session function the outer statement never named.
        ticket = SessionScalarFunctions.AttachSessionValues(ticket, sourceAst);

        // A time-travel source reads through its OWN read-only snapshot transaction while the writes
        // keep using the caller's; see PrepareTimeTravelSourceAsync for why that is both possible and
        // safer than the live path. Null when the source carries no AS OF SYSTEM TIME clause.
        (ExecuteSQLTicket sourceTicket, SnapshotHoldLease? lease) =
            await PrepareTimeTravelSourceAsync(database, sourceAst, ticket, statementName, explicitSnapshot).ConfigureAwait(false);

        bool isTimeTravel = lease is not null;

        try
        {
            using AuthorizationContext.PrivilegeSwap _ = AuthorizationContext.WithRequiredPrivilege(Privilege.Select);

            // FROM-less source (SELECT <expressions>): one synthetic row, no scan to plan.
            if (sourceAst.rightAst is null)
            {
                QuerySchemaHolder fromlessSchema = new();
                IAsyncEnumerable<QueryResultRow> fromlessCursor = await ExecuteFromlessSelectAsync(
                    database, sourceAst, sourceTicket, fromlessSchema).ConfigureAwait(false);

                List<NodeAst> fromlessProjections = new();
                FlattenProjectionList(sourceAst.leftAst!, fromlessProjections);

                return new SelectRowSource(fromlessSchema.Schema, fromlessCursor, fromlessProjections, lease);
            }

            (BoundSelectQuery bound, QueryTicket queryTicket) = await BuildBoundQueryAsync(
                database,
                sourceAst,
                sourceTicket,
                // A live source scan is write-driving: the rows it reads decide what is written, so its
                // predicate range locks are exclusive and its reads fold into the commit-time read set.
                // A historical source needs neither — no transaction can alter what was committed at a
                // past timestamp, so there is no phantom to fence and nothing to detect at commit. That
                // also means a large as-of copy does not block concurrent writers on the source range.
                exclusivePredicateLocks: !isTimeTravel,
                suppressCacheHint: true).ConfigureAwait(false);

            IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.Build(bound.Query, bound);

            return new SelectRowSource(
                columns,
                ExecuteBoundQuery(database, bound, queryTicket),
                bound.Query.Projections.Select(projection => projection.Expression).ToList(),
                lease);
        }
        catch
        {
            // The lease is owned by the SelectRowSource once one exists; if we fail before returning it,
            // nothing else will ever release it, so do it here rather than leave the MVCC floor pinned
            // until the lease lapses.
            if (lease is not null)
                await lease.DisposeAsync().ConfigureAwait(false);

            throw;
        }
    }

    /// <summary>
    /// Binds a view body and returns its output schema and projections without reading any rows.
    ///
    /// <para>Exists so <c>CREATE VIEW</c> derives a view's columns through exactly the same path that
    /// produces a query's client-facing column metadata and a CTAS target's schema. A view column
    /// therefore always has the type a plain <c>SELECT</c> of that expression would report — a second
    /// derivation would be free to drift from the first, and the drift would only surface as a wrong
    /// type in somebody's client.</para>
    ///
    /// <para>The returned source's cursor is never opened by the caller; disposing it releases the
    /// binding without having scanned anything.</para>
    /// </summary>
    internal Task<SelectRowSource> BuildViewSourceAsync(
        DatabaseDescriptor database, NodeAst bodyAst, ExecuteSQLTicket ticket)
        => BuildSelectSourceAsync(database, ExpandViews(database, bodyAst), ticket, "CREATE VIEW");

    /// <summary>
    /// Opens a materialized view's body as a row source pinned to <paramref name="snapshot"/>, with
    /// the revision floor held for as long as the source lives.
    ///
    /// <para>A refresh writes its rows over many transactions, so the pin is what makes the result a
    /// relation that actually existed: reading each chunk at "now" would assemble a table from
    /// several different instants and so from a state the database was never in. Pinning also removes
    /// the need for range locks over the source — history at a past instant cannot gain a phantom —
    /// so a long rebuild does not block writers on the tables it reads.</para>
    /// </summary>
    internal Task<SelectRowSource> BuildMaterializedViewSourceAsync(
        DatabaseDescriptor database, NodeAst bodyAst, ExecuteSQLTicket ticket, HLCTimestamp snapshot)
        => BuildSelectSourceAsync(
            database, ExpandViews(database, bodyAst), ticket, "REFRESH MATERIALIZED VIEW", snapshot);

    /// <summary>
    /// Resolves an <c>AS OF SYSTEM TIME</c> source clause into a read-only snapshot transaction the
    /// source query alone will use, and pins Kahuna's revision floor for as long as the copy runs.
    /// Returns the caller's own ticket unchanged, and no hold, when the source is not time-travelling.
    ///
    /// <para>The statement ends up using <b>two</b> transactions: this snapshot one for the read, and
    /// the caller's live one for the writes. That is what makes "recover data as it was" expressible —
    /// and it is safer than the live path, not riskier. History at a past timestamp is immutable, so
    /// the source needs no range locks, contributes no phantoms, and cannot observe this statement's
    /// own writes (the resolver refuses future instants, so the snapshot always precedes them). The
    /// historical reads are deliberately not folded into the write transaction's read set: nothing a
    /// concurrent transaction does can change what was committed before the snapshot.</para>
    ///
    /// <para><b>Retention is the real hazard.</b> Revision GC is age/count based, so a copy reading at
    /// T while the sweeper reclaims past T would produce a silently partial table. The hold prevents
    /// that for the duration of the copy. It cannot resurrect revisions reclaimed <i>before</i> the
    /// hold was taken — Kahuna exposes no way to ask whether a timestamp is still readable — so a
    /// recovery from beyond the retention window can still come back empty. Callers report a
    /// zero-row time-travel copy for that reason.</para>
    /// </summary>
    /// <param name="explicitSnapshot">
    /// A snapshot chosen by the caller rather than written in the statement, used by materialized-view
    /// refresh. Supplying it pins the source exactly as an <c>AS OF SYSTEM TIME</c> clause would —
    /// including dropping the range locks a live source would take — which is what a multi-transaction
    /// rebuild needs and is the whole reason this parameter exists.
    /// </param>
    internal async Task<(ExecuteSQLTicket SourceTicket, SnapshotHoldLease? Lease)> PrepareTimeTravelSourceAsync(
        DatabaseDescriptor database,
        NodeAst sourceAst,
        ExecuteSQLTicket ticket,
        string statementName,
        HLCTimestamp? explicitSnapshot = null)
    {
        if (sourceAst.extendedSeven is null && explicitSnapshot is null)
            return (ticket, null);

        if (context.SharedNode is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "AS OF SYSTEM TIME requires a storage node to resolve the snapshot timestamp.");

        HLCTimestamp now = context.SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());
        HLCTimestamp snapshotT = explicitSnapshot
            ?? AsOfSystemTimeResolver.Resolve(sourceAst.extendedSeven!, ticket.Parameters, now);

        IKahuna kahuna = database.Kahuna.Kahuna;
        string holderId = $"{database.Id}-asof-{ObjectIdGenerator.Generate()}";

        // The hold is leased, and the lease is shorter than a large copy. Renewing it — and failing
        // closed when renewal stops being confirmed — is what makes "one pinned snapshot" true for the
        // whole read rather than only for its first few minutes.
        SnapshotHoldLease lease = await SnapshotHoldLease.AcquireAsync(
            kahuna, context.Logger, holderId, snapshotT, options.BranchSnapshotHoldLeaseMs, statementName)
            .ConfigureAwait(false);

        ExecuteSQLTicket snapshotTicket = new(
            KvTransaction.CreateSnapshotReadOnly(snapshotT),
            ticket.DatabaseName,
            ticket.Sql,
            ticket.Parameters);

        return (snapshotTicket, lease);
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
    internal async Task<IAsyncEnumerable<QueryResultRow>> ExecuteFromlessSelectAsync(
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
            return QueryResultStream.Empty();

        return QueryResultStream.FromRow(new QueryResultRow(new ObjectIdValue(), projected));
    }

    /// <summary>
    /// Executes a peer coordinator's span-scan fragment on this node: verifies the request
    /// resolves to exactly the objects the coordinator planned against (database and table by
    /// immutable id, schema by version — any mismatch fails closed so the coordinator falls
    /// back to a local scan), then runs the bounded snapshot scan with the residual filter
    /// evaluated here, yielding surviving rows' raw bytes. Read path only: a zero-identity
    /// snapshot transaction at the coordinator's timestamp, no locks, no session.
    /// </summary>
    public async IAsyncEnumerable<QueryFragmentRow> ExecuteQueryFragment(
        QueryFragmentRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (request.FormatVersion != QueryFragmentRequest.CurrentFormatVersion)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Unsupported query-fragment format version {request.FormatVersion} (this node speaks {QueryFragmentRequest.CurrentFormatVersion})");

        if (string.IsNullOrEmpty(request.FilterJson) && request.AggregatesJson is null && request.Join is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A row fragment must carry a residual filter; unfiltered spans are scanned by the coordinator");

        DatabaseDescriptor database = await context.DatabaseOpener.Open(request.DatabaseName).ConfigureAwait(false);

        if (!string.Equals(database.Id, request.DatabaseId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query fragment database id mismatch: resolved '{database.Id}', coordinator planned '{request.DatabaseId}'");

        TableDescriptor table = await database.TableDescriptors[request.TableName].ConfigureAwait(false);

        if (!string.Equals(table.Id, request.TableId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query fragment table id mismatch: resolved '{table.Id}', coordinator planned '{request.TableId}'");

        if (table.Schema.Version != request.SchemaVersion)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query fragment schema version mismatch: this node has {table.Schema.Version}, coordinator planned {request.SchemaVersion}");

        NodeAst? filter = string.IsNullOrEmpty(request.FilterJson)
            ? null
            : NodeAstWireCodec.Deserialize(request.FilterJson);

        ObjectIdValue? fromRowId = request.FromRowIdHex is null ? null : ObjectId.ToValue(request.FromRowIdHex);
        ObjectIdValue? untilRowId = request.UntilRowIdHex is null ? null : ObjectId.ToValue(request.UntilRowIdHex);

        KvTransaction snapshotTx = KvTransaction.CreateSnapshotReadOnly(
            new HLCTimestamp(request.ReadTsNode, request.ReadTsPhysical, request.ReadTsCounter));

        IReadOnlySet<string>? requiredColumns = request.RequiredColumns is { Length: > 0 }
            ? new HashSet<string>(request.RequiredColumns, StringComparer.Ordinal)
            : null;

        Interlocked.Increment(ref queryExecutor.DistributedMetrics.FragmentsServed);
        long shippedOut = 0;

        try
        {
            if (request.Join is not null)
            {
                await foreach (QueryFragmentRow probeRow in queryExecutor.JoinExecutor.ExecuteFragmentJoinProbe(
                    database, table, request.Join, fromRowId, untilRowId,
                    request.SchemaVersion, requiredColumns, snapshotTx,
                    wantStats: request.WantStats, cancellationToken).ConfigureAwait(false))
                {
                    if (probeRow.Stats is null)
                        shippedOut++;

                    yield return probeRow;
                }

                yield break;
            }

            if (request.AggregatesJson is not null)
            {
                NodeAst[] projections = request.AggregatesJson.Select(NodeAstWireCodec.Deserialize).ToArray();
                NodeAst[]? groupBy = request.GroupByJson?.Select(NodeAstWireCodec.Deserialize).ToArray();

                await foreach (QueryFragmentRow partial in queryExecutor.ExecuteFragmentAggregate(
                    database, table, projections, groupBy, filter, fromRowId, untilRowId,
                    request.SchemaVersion, requiredColumns, snapshotTx, cancellationToken).ConfigureAwait(false))
                {
                    shippedOut++;
                    yield return partial;
                }

                yield break;
            }

            await foreach (QueryFragmentRow row in queryExecutor.ExecuteFragmentScan(
                database, table, filter!, fromRowId, untilRowId, request.MaxRows, request.MaxSurvivors,
                request.SchemaVersion, requiredColumns, snapshotTx,
                wantStats: request.WantStats, cancellationToken).ConfigureAwait(false))
            {
                // Stats frames are protocol bookkeeping, not shipped data.
                if (row.Stats is null)
                    shippedOut++;

                yield return row;
            }
        }
        finally
        {
            if (shippedOut > 0)
                Interlocked.Add(ref queryExecutor.DistributedMetrics.RowsShippedOut, shippedOut);
        }
    }

    internal static void PinSchemaVersions(
        DatabaseDescriptor database,
        IEnumerable<BoundTableSource> sources,
        KvTransaction tx
    )
    {
        foreach (BoundTableSource source in sources)
            PinSchemaVersion(database, source.Table, tx);
    }

    internal static void PinSchemaVersion(DatabaseDescriptor database, TableDescriptor table, KvTransaction tx)
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
    internal static CacheHintOptions? FindInnerSubqueryCacheHint(NodeAst? node)
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

    /// <summary>
    /// Names this process for the <c>node</c> column of <c>SHOW ENGINE STATS</c>, so output pasted into
    /// an issue says which node produced it. This is the Raft endpoint, the identity peers use. An
    /// engine built without a shared node (some tests) has no endpoint and reports an empty label.
    /// </summary>
    internal string LocalNodeLabel()
    {
        try
        {
            return context.SharedNode?.Raft.GetLocalEndpoint() ?? "";
        }
        catch (Exception)
        {
            // A node still starting up (or already torn down) has no endpoint to give. Reporting the
            // metrics without a label beats failing an introspection statement.
            return "";
        }
    }

    internal static string? UnquoteLikePattern(string? raw)
        => raw is null ? null : SqlStringLiteral.Decode(raw);

    /// <summary>Flattens the left-recursive <see cref="NodeType.IdentifierList"/> projection chain into order.</summary>
    internal static void FlattenProjectionList(NodeAst ast, List<NodeAst> projections)
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
    internal static long EvalRowCount(NodeAst? node, Dictionary<string, ColumnValue>? parameters, string clause)
    {
        if (node is null)
            return 0;

        ColumnValue value = SQLExecutorBaseCreator.EvalExpr(node, new Dictionary<string, ColumnValue>(), parameters);
        if (value.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{clause} must be an integer");

        return value.LongValue;
    }

}