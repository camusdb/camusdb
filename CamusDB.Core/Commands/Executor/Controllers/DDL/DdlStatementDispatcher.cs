
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Auth;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Routes a parsed DDL statement to the service that executes it.
///
/// <para><b>The pre-open block is the part to understand.</b> Statements whose target is the server
/// or a database itself — CREATE/DROP/RENAME/RELINK DATABASE, COMMENT ON DATABASE, user and grant
/// administration, cluster settings — are dispatched <em>before</em> any database is opened. That is
/// not an optimization: opening a descriptor for the database you are about to destroy or rename
/// would load the very object the statement removes, and the server-level statements have no
/// database context to open at all.</para>
///
/// <para>Everything after that point runs against an open descriptor held for the statement's
/// duration.</para>
/// </summary>
internal sealed class DdlStatementDispatcher
{
    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    private readonly TriggerDdlService triggerDdl;

    private readonly ExtensionDdlService extensionDdl;

    private readonly FunctionDdlService functionDdl;

    private readonly SqlExecutor sqlExecutor;

    private readonly SqlParserCache sqlParserCache;

    private readonly Auth.StatementAuthorizer statementAuthorizer;

    private readonly Auth.UserAdminService userAdmin;

    private readonly ServerLevelStatementDispatcher serverLevelDispatcher;

    private readonly DatabaseLifecycleService databaseLifecycle;

    private readonly SchemaDdlService schemaDdl;

    private readonly TableSettingsService tableSettings;

    private readonly DdlForwardingCoordinator ddlForwarding;

    private readonly CreateTableAsSelectExecutor ctasExecutor;

    private readonly QueryExecutor queryExecutor;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableDropper tableDropper;

    private readonly RowDeleter rowDeleter;

    private readonly ViewCreator viewCreator;

    private readonly MaterializedViewCreator matViewCreator;

    private readonly MaterializedViewRefresher matViewRefresher;

    private readonly SequenceDdlService sequenceDdl;

    private readonly AuthService? authService;

    /// <summary>
    /// The runtime cluster-settings pipeline. Null on engines composed without one, where
    /// <c>SET/RESET CLUSTER SETTING</c> is rejected with a clear error rather than silently applying
    /// to nothing.
    /// </summary>
    private readonly ClusterSettingsService? clusterSettings;

    /// <summary>Commits and discards the change sets of explicit transactions; see <see cref="ChangeSetCommitter"/>.</summary>
    private readonly ChangeSetCommitter changeSetCommitter;

    internal DdlStatementDispatcher(
        ExecutorContext context,
        CatalogsManager catalogs,
        SqlExecutor sqlExecutor,
        SqlParserCache sqlParserCache,
        Auth.StatementAuthorizer statementAuthorizer,
        Auth.UserAdminService userAdmin,
        ServerLevelStatementDispatcher serverLevelDispatcher,
        DatabaseLifecycleService databaseLifecycle,
        SchemaDdlService schemaDdl,
        TableSettingsService tableSettings,
        DdlForwardingCoordinator ddlForwarding,
        CreateTableAsSelectExecutor ctasExecutor,
        QueryExecutor queryExecutor,
        TableCreator tableCreator,
        TableColumnAlterer tableColumnAlterer,
        TableIndexAlterer tableIndexAlterer,
        TableDropper tableDropper,
        RowDeleter rowDeleter,
        ViewCreator viewCreator,
        MaterializedViewCreator matViewCreator,
        MaterializedViewRefresher matViewRefresher,
        SequenceDdlService sequenceDdl,
        AuthService? authService,
        ClusterSettingsService? clusterSettings
    )
    {
        // Guarded because these are captured at construction, not read per call: a collaborator
        // built later in the composing constructor would be captured as null here and only fail
        // much later, deep inside a statement, where the cause is far from the mistake.
        ArgumentNullException.ThrowIfNull(statementAuthorizer);
        ArgumentNullException.ThrowIfNull(userAdmin);
        ArgumentNullException.ThrowIfNull(serverLevelDispatcher);
        ArgumentNullException.ThrowIfNull(databaseLifecycle);
        ArgumentNullException.ThrowIfNull(schemaDdl);
        ArgumentNullException.ThrowIfNull(tableSettings);
        ArgumentNullException.ThrowIfNull(ddlForwarding);
        ArgumentNullException.ThrowIfNull(ctasExecutor);

        this.context = context;
        this.catalogs = catalogs;
        // One count per engine of the statements that compile inline C#, shared by functions and
        // triggers, so a user cannot double the limit by alternating the two.
        InlineDefinitionRateLimiter inlineDefinitions = new();
        triggerDdl = new TriggerDdlService(context, catalogs, inlineDefinitions);
        extensionDdl = new ExtensionDdlService(context, catalogs, sqlParserCache);
        this.sqlExecutor = sqlExecutor;
        this.sqlParserCache = sqlParserCache;
        this.statementAuthorizer = statementAuthorizer;
        functionDdl = new FunctionDdlService(context, catalogs, sqlParserCache, statementAuthorizer, authService, inlineDefinitions);
        this.userAdmin = userAdmin;
        this.serverLevelDispatcher = serverLevelDispatcher;
        this.databaseLifecycle = databaseLifecycle;
        this.schemaDdl = schemaDdl;
        this.tableSettings = tableSettings;
        this.ddlForwarding = ddlForwarding;
        this.ctasExecutor = ctasExecutor;
        this.queryExecutor = queryExecutor;
        this.tableCreator = tableCreator;
        this.tableColumnAlterer = tableColumnAlterer;
        this.tableIndexAlterer = tableIndexAlterer;
        this.tableDropper = tableDropper;
        this.rowDeleter = rowDeleter;
        this.viewCreator = viewCreator;
        this.matViewCreator = matViewCreator;
        this.matViewRefresher = matViewRefresher;
        this.sequenceDdl = sequenceDdl;
        this.authService = authService;
        this.clusterSettings = clusterSettings;
        changeSetCommitter = new ChangeSetCommitter(catalogs, context.Logger);
    }

    /// <summary>
    /// The transactional-DDL rule for one schema statement on an open database. Returns the statement's
    /// transaction when the statement must run against that transaction's private schema, opening it at
    /// the first schema change; the caller then names the transaction for its flow
    /// (<see cref="TransactionSchemaScope.Enter"/>). Returns null when the statement runs as before: no
    /// explicit transaction, transactional DDL off and no schema change yet, or not a schema statement.
    /// </summary>
    /// <remarks>
    /// <para>With transactional DDL on, a statement of class <see cref="DdlStatementClass.Irrevocable"/>
    /// is refused with <see cref="CamusDBErrorCodes.StatementNotAllowedInTransaction"/>, so it never commits
    /// apart from a transaction whose client expects a ROLLBACK to undo it. TRUNCATE and REWRITE STORAGE
    /// keep their own refusal, which applies with the setting off too.</para>
    ///
    /// <para>A transaction that already holds schema changes keeps this rule after the setting is turned
    /// off, until it ends: half of its DDL in the change set and half committed apart would be neither
    /// rule.</para>
    /// </remarks>
    internal async Task<Transactions.KvTransaction?> OpenTransactionSchemaAsync(
        DatabaseDescriptor database, ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ticket.TxnState is not { IsSessionOwned: true } tx)
            return null;

        TransactionSchemaOverlay? existing = tx.SchemaChanges as TransactionSchemaOverlay;

        if (existing is null && !database.Options.TransactionalDdlEnabled)
            return null;

        DdlStatementClass? statementClass = DdlStatementClasses.ClassOf(ast.nodeType);
        if (statementClass is null)
            return null;

        switch (statementClass)
        {
            case DdlStatementClass.Irrevocable when ast.nodeType is NodeType.TruncateTable or NodeType.AlterTableRewriteStorage:
                return null;

            case DdlStatementClass.Irrevocable:
                throw new CamusDBException(
                    CamusDBErrorCodes.StatementNotAllowedInTransaction,
                    $"{ast.nodeType} cannot run inside an explicit transaction: it commits work that a later ROLLBACK " +
                    "cannot undo. Commit or roll back first, then run it.");
        }

        if (existing is not null)
        {
            if (!string.Equals(existing.DatabaseId, database.Id, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.StatementNotAllowedInTransaction,
                    "A transaction can change the schema of one database only; commit or roll back first");

            if (!existing.IsOpen)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionAlreadyCompleted,
                    $"Transaction {tx.UniqueId} already ended; its schema changes cannot take another statement");

            return tx;
        }

        await TransactionSchemaOverlay.OpenAsync(database, tx, changeSetCommitter, context.Logger).ConfigureAwait(false);
        return tx;
    }

    /// <summary>
    /// Takes the DDL lock of <paramref name="database"/> for one schema statement that runs outside a DDL
    /// transaction, when transactional DDL is on. The statement shares it with the other statements of this
    /// kind (<see cref="StatementDdlLock"/>). Null when the setting is off, when the statement is not a
    /// schema statement on the database, or when it belongs to an explicit transaction (which holds the
    /// lock itself, or is refused). Taken before the DDL gate, as the lock order requires.
    /// </summary>
    internal async Task<StatementDdlLock.Share?> AcquireStatementDdlLockAsync(
        DatabaseDescriptor database, ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!database.Options.TransactionalDdlEnabled)
            return null;

        if (DdlStatementClasses.ClassOf(ast.nodeType) is null)
            return null;

        if (ticket.TxnState is { IsSessionOwned: true })
            return null;

        return await database.StatementDdlLock.EnterAsync(
            database, TransactionalDdlLock.DefaultWaitTimeout, context.Logger).ConfigureAwait(false);
    }

    private AuthService RequireAuthService()
    {
        if (authService is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication service is unavailable (no shared node was configured)");
        return authService;
    }

    /// <summary>
    /// Applies <c>COMMENT ON SEQUENCE &lt;name&gt; IS &lt;value&gt;</c> to an open database.
    ///
    /// <para>Shared by the DDL entry point and the non-query entry point. A client routes every
    /// non-SELECT statement to whichever of the two it uses, so the statement must behave the same
    /// through either; one implementation keeps the two from drifting.</para>
    ///
    /// <para>A null value node is <c>IS NULL</c>, which removes the comment, while <c>IS ''</c>
    /// stores a present-but-empty one. The two must stay distinguishable end to end.</para>
    /// </summary>
    internal Task CommentOnSequenceAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string commentedSequence = ast.leftAst!.yytext!;

        string? sequenceComment = ast.rightAst is null
            ? null
            : DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst.yytext!);

        return sequenceDdl.SetCommentAsync(database, commentedSequence, sequenceComment);
    }

    /// <summary>
    /// Runs <c>COMMENT ON VIEW</c> for both entry points, for the same reason
    /// <see cref="CommentOnSequenceAsync"/> serves both: a client routes a non-SELECT statement to
    /// whichever endpoint it uses, and the statement must behave the same through either.
    /// </summary>
    /// <remarks>
    /// The view is authorized here because nothing else will: setting its comment opens no relation, so
    /// the per-table check that guards <c>COMMENT ON TABLE</c> never runs for a view.
    /// </remarks>
    internal Task CommentOnViewAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string viewName = ast.leftAst!.yytext!;

        if (!database.Schema.Views.TryGetValue(viewName, out ViewSchema? view))
        {
            // Named by the statement for the other kinds, as DROP VIEW does.
            if (database.Schema.Tables.TryGetValue(viewName, out TableSchema? relation))
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewDoesntExist,
                    relation.IsMaterializedView
                        ? $"'{viewName}' is a materialized view; use COMMENT ON MATERIALIZED VIEW"
                        : $"'{viewName}' is a table, not a view; use COMMENT ON TABLE");

            throw new CamusDBException(CamusDBErrorCodes.ViewDoesntExist, $"View '{viewName}' does not exist");
        }

        ViewAuthorization.Require(database, viewName, view, Privilege.Alter);

        string? comment = ast.rightAst is null
            ? null
            : DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst.yytext!);

        // This statement builds no CommentTicket, so it checks the bound every other comment has here.
        CommandsValidator.Validators.ValidatorBase.ValidateCommentLength(comment, $"View '{viewName}'");

        return catalogs.SetViewCommentAsync(database, viewName, comment);
    }

    /// <summary>
    /// Runs <c>COMMENT ON MATERIALIZED VIEW</c> for both entry points. The name must be a materialized
    /// view; the comment then goes through the relation comment path <c>COMMENT ON TABLE</c> uses,
    /// which forwards to the schema leader and checks the caller against the relation.
    /// </summary>
    internal async Task CommentOnMaterializedViewAsync(DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        string viewName = ast.leftAst!.yytext!;
        MaterializedViewRefresher.RequireMaterializedView(database, viewName);

        CommentTicket commentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);

        // Checked first so the error names a materialized view; the ticket's target is the relation.
        CommandsValidator.Validators.ValidatorBase.ValidateCommentLength(commentTicket.Comment, $"Materialized view '{viewName}'");
        context.Validator.Validate(commentTicket);
        await schemaDdl.Comment(database, commentTicket).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a schema statement, and guards the statement boundary as the DML dispatcher does,
    /// under PostgreSQL's rule: a statement on an aborted transaction is refused, and a statement that
    /// fails aborts the caller's transaction (a single-node schema change persists its metadata inside
    /// that transaction, so a half-applied change must never be committed). See
    /// <see cref="Transactions.KvTransaction.MarkRollbackOnly(string)"/>.
    /// </summary>
    /// <param name="activity">The statement's entry in the running-statement list, which the parse names; or null.</param>
    internal async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(
        CommandExecutor executor, ExecuteSQLTicket ticket, Diagnostics.QueryActivityEntry? activity = null)
    {
        // Null for a database-scoped statement (CREATE DATABASE, CREATE USER, ...), which writes no transaction.
        Transactions.KvTransaction? tx = ticket.TxnState;
        tx?.ThrowIfRollbackOnly();

        try
        {
            ExecuteDDLSQLResult result = await ExecuteDDLSQLCore(executor, ticket, activity).ConfigureAwait(false);

            // A schema statement that changed nothing (CREATE TABLE IF NOT EXISTS on a table that exists)
            // leaves no private schema behind; see DiscardIfEmptyAsync.
            if (tx is not null)
                await TransactionSchemaOverlay.DiscardIfEmptyAsync(tx).ConfigureAwait(false);

            return result;
        }
        catch (Exception ex)
        {
            tx?.MarkRollbackOnly(ex);
            throw;
        }
    }

    /// <summary>The statement dispatch behind <see cref="ExecuteDDLSQL"/>; see there for the guard around it.</summary>
    private async Task<ExecuteDDLSQLResult> ExecuteDDLSQLCore(
        CommandExecutor executor, ExecuteSQLTicket ticket, Diagnostics.QueryActivityEntry? activity)
    {
        context.Validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);
        activity?.Describe(ast.nodeType);

        // A transaction that the client declared read-only refuses schema DDL before anything runs:
        // the DDL commits in transactions of its own, so a rollback of the caller's could not undo it.
        ReadOnlyStatementRule.Enforce(ticket.TxnState, ast);

        // Schema DDL is not bounded by statement_timeout_ms: a backfill and a replicated schema change
        // run as jobs that a statement error cannot stop honestly. Taken off in this async frame only,
        // so the caller's deadline comes back when the statement returns. CREATE TABLE … AS keeps it:
        // it loads its rows in the statement and drops the table when the load fails.
        if (!StatementDeadline.BoundsSchemaStatement(ast.nodeType))
            StatementDeadline.Suspend();

        // CREATE TABLE … AS with a CTE that its query computes once keeps that CTE's rows until it ends.
        await using Queries.ComputedOnceScope? computedOnce = Queries.ComputedOnceRows.Begin(ticket.TxnState, ast);

        statementAuthorizer.SetAuthorizationScope(ticket, ast);
        ticket = StatementScopedValues.Attach(ticket, ast);
        await statementAuthorizer.EnforceAsync(ticket, ast).ConfigureAwait(false);

        using ServerDiagnostics.ExecuteScope executeScope = ServerDiagnostics.MeasureExecute(
            ServerDiagnostics.Tags.Operation.Ddl, ServerDiagnostics.Tags.Statement.Other);

        // Server-level statements are dispatched before any database is opened; see
        // ServerLevelStatementDispatcher for why opening first would be wrong rather than wasteful.
        ServerLevelOutcome serverLevel = await serverLevelDispatcher.TryExecuteAsync(ticket, ast).ConfigureAwait(false);
        if (serverLevel.Handled)
        {
            if (serverLevel.Database is not null)
                return new ExecuteDDLSQLResult(serverLevel.Database, true);

            return serverLevel.ReportsSuccess ? new ExecuteDDLSQLResult(null, true) : default;
        }

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Entered in this frame, so every step of the statement below reads the transaction's private
        // schema; see OpenTransactionSchemaAsync.
        Transactions.KvTransaction? ddlTransaction = await OpenTransactionSchemaAsync(database, ticket, ast).ConfigureAwait(false);
        if (ddlTransaction is not null)
            TransactionSchemaScope.Enter(ddlTransaction);

        // A schema statement outside a DDL transaction holds the same DDL lock for its length, so it waits
        // for a DDL transaction to end instead of moving the base that transaction will commit against.
        await using StatementDdlLock.Share? statementLock = ddlTransaction is null
            ? await AcquireStatementDdlLockAsync(database, ticket, ast).ConfigureAwait(false)
            : null;

        switch (ast.nodeType)
        {
            case NodeType.CommentOnTable:
            case NodeType.CommentOnColumn:
            case NodeType.CommentOnIndex:
                {
                    CommentTicket commentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
                    context.Validator.Validate(commentTicket);
                    return await schemaDdl.Comment(database, commentTicket).ConfigureAwait(false);
                }

            case NodeType.CommentOnView:
                await CommentOnViewAsync(database, ast).ConfigureAwait(false);
                return new ExecuteDDLSQLResult(database, true);

            case NodeType.CommentOnMaterializedView:
                await CommentOnMaterializedViewAsync(database, ast, ticket).ConfigureAwait(false);
                return new ExecuteDDLSQLResult(database, true);

            case NodeType.CreateView:
            case NodeType.CreateOrReplaceView:
                {
                    bool createdView = await viewCreator.CreateAsync(
                        executor, catalogs, context.Registry, database, ast, ticket,
                        replace: ast.nodeType == NodeType.CreateOrReplaceView).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, createdView);
                }

            case NodeType.DropView:
            case NodeType.DropViewIfExists:
                {
                    bool droppedView = await viewCreator.DropAsync(
                        executor, catalogs, context.Registry, database, ast,
                        ifExists: ast.nodeType == NodeType.DropViewIfExists,
                        context.Logger).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, droppedView);
                }

            case NodeType.AlterViewRenameTo:
                {
                    string renamedViewName = ast.leftAst!.yytext!;

                    if (!database.Schema.Views.TryGetValue(renamedViewName, out ViewSchema? renamedView))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ViewDoesntExist, $"View '{renamedViewName}' does not exist");

                    ViewAuthorization.Require(database, renamedViewName, renamedView, Privilege.Alter);

                    string newViewName = ast.rightAst!.yytext!;

                    // A view can be read by other views, so renaming one has the same single-delta
                    // requirement as renaming a table: any dependent still bound by name is
                    // converted to ids and rides the rename rather than following it.
                    Dictionary<string, ViewDefinition>? viewRenameRewrites =
                        ViewDependencyMaintainer.BuildRenameConversions(
                            database.Schema, renamedView.Id!,
                            sql => SQLParserProcessor.Parse(sql, sqlParserCache));

                    await catalogs.RenameViewAsync(
                        database, renamedViewName, newViewName, viewRenameRewrites).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.AlterViewOwnerTo:
                {
                    string ownedViewName = ast.leftAst!.yytext!;
                    string newOwnerName = ast.rightAst!.yytext!;

                    if (!database.Schema.Views.TryGetValue(ownedViewName, out ViewSchema? ownedView)
                        || ownedView.Definition is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.ViewDoesntExist, $"View '{ownedViewName}' does not exist");

                    await statementAuthorizer.RequireViewOwnershipAsync(database, ownedViewName, ownedView, ticket).ConfigureAwait(false);

                    // The new owner must exist now: a view whose owner cannot be resolved refuses every
                    // read, so accepting an unknown name here would break the view rather than move it.
                    UserRecord? newOwner = authService is null
                        ? null
                        : await RequireAuthService().TryGetUserAsync(newOwnerName).ConfigureAwait(false);

                    if (authService is not null && (newOwner is null || string.IsNullOrEmpty(newOwner.Id)))
                        throw new CamusDBException(
                            CamusDBErrorCodes.UserDoesNotExist,
                            $"User '{newOwnerName}' does not exist, or predates user ids and cannot own a view");

                    // A copy, never the live definition: editing that in place showed the new owner
                    // before the change committed, and kept showing it when the change failed.
                    ViewDefinition transferred = ownedView.Definition.Copy();
                    transferred.Owner = newOwner?.Name ?? newOwnerName;
                    transferred.OwnerId = newOwner?.Id;

                    // Checked against the generation the copy was taken from, so a concurrent replace
                    // is not overwritten with the body it replaced.
                    await catalogs.SetViewDefinitionAsync(
                        database, ownedViewName, transferred, expectedGeneration: ownedView.Generation).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateMaterializedView:
            case NodeType.CreateMaterializedViewIfNotExists:
                {
                    (bool createdMatView, int matViewRows) = await matViewCreator.CreateAsync(
                        executor, matViewRefresher, catalogs, context.Registry, database, ast, ticket, context.Logger).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, createdMatView, matViewRows);
                }

            case NodeType.DropMaterializedView:
            case NodeType.DropMaterializedViewIfExists:
                {
                    bool droppedMatView = await matViewCreator.DropAsync(
                        executor, catalogs, context.Registry, database, ast,
                        ifExists: ast.nodeType == NodeType.DropMaterializedViewIfExists,
                        context.Logger).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, droppedMatView);
                }

            // Normally a non-query statement (it writes rows), and accepted here too so a client that
            // classifies it as view DDL by its first words still works; see RefreshStatementAsync.
            case NodeType.RefreshMaterializedView:
                {
                    int refreshedRows = await matViewRefresher.RefreshStatementAsync(
                        executor, catalogs, context.Registry, database, ast, ticket, context.Logger).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, true, refreshedRows);
                }

            case NodeType.AlterMaterializedViewRenameTo:
                {
                    string oldMatViewName = ast.leftAst!.yytext!;
                    string newMatViewName = ast.rightAst!.yytext!;

                    // Resolved through the materialized-view check first so renaming a plain table with
                    // this statement is refused rather than quietly succeeding.
                    TableSchema renamedMatView = MaterializedViewRefresher
                        .RequireMaterializedView(database, oldMatViewName);

                    // Same single-delta rule as a table rename: a dependent still bound by name is
                    // converted to ids and rides the rename rather than following it.
                    Dictionary<string, ViewDefinition>? matViewRewrites =
                        ViewDependencyMaintainer.BuildRenameConversions(
                            database.Schema, renamedMatView.Id!,
                            sql => SQLParserProcessor.Parse(sql, sqlParserCache));

                    await schemaDdl.RenameTable(new RenameTableTicket(
                        ticket.DatabaseName, oldMatViewName, newMatViewName, allowMaterializedView: true), matViewRewrites).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = sqlExecutor.CreateCreateTableTicket(ticket, ast);
                    context.Validator.Validate(createTableTicket);
                    ForeignKeyPrivileges.RequireReferencePrivileges(database, createTableTicket);

                    bool? forwarded = await ddlForwarding.TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    // Allocate before the DDL transaction — only the proposer/leader allocates;
                    // the id is carried in the replicated payload so every follower applies the same id.
                    DatabaseRegistry sqlRegistry = await context.Registry.ConfigureAwait(false);
                    string sqlTableId = await sqlRegistry.AllocateTableIdAsync().ConfigureAwait(false);

                    // Shared with the ticket API's CreateTable so an identity column is desugared
                    // on both paths: the SQL path used to build its own transaction here, and a
                    // second copy of the create is how one of the two silently loses a step.
                    bool createdTable = await schemaDdl
                        .CreateTableWithSequencesAsync(database, createTableTicket, sqlTableId).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, createdTable);
                }

            case NodeType.CreateTableRelink:
                {
                    RelinkTableTicket relinkTicket = new(
                        ticket.DatabaseName,
                        ast.leftAst!.yytext!,
                        DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst!.yytext!));

                    // Read before the relink, which deletes the record: a restored table comes back
                    // without its foreign keys, and the statement says which ones the user must add again.
                    OrphanTableRecord? relinkRecord = await catalogs.TryGetTableOrphanAsync(database, relinkTicket.OrphanTableId).ConfigureAwait(false);

                    // Delegate to the executor method so fencing, forwarding, and orphan-load live in one place.
                    bool relinked = await schemaDdl.RelinkTable(relinkTicket).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, relinked, 0, relinked ? DroppedForeignKeysWarning(relinkRecord, relinkTicket.NewTableName) : null);
                }

            case NodeType.AlterTableAddColumn:
            case NodeType.AlterTableDropColumn:
                {
                    AlterTableTicket alterTableTicket = sqlExecutor.CreateAlterTableTicket(ticket, ast);
                    context.Validator.Validate(alterTableTicket);
                    SchemaDdlService.RequireNoViewDependsOnAlteredColumn(database, alterTableTicket);

                    bool? forwarded = await ddlForwarding.TryForwardAlterTableAsync(database, alterTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await context.TableOpener.Open(database, alterTableTicket.TableName).ConfigureAwait(false);

                    // Shared with the ticket API's AlterTable, in both modes: a dropped identity column
                    // takes its sequence with it, and an added column gets its sequence and its
                    // existing rows filled, on both paths.
                    bool alteredColumn = await schemaDdl
                        .AlterColumnWithSequencesAsync(database, table, alterTableTicket).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, alteredColumn);
                }

            case NodeType.AlterTableAddIndex:
            case NodeType.AlterTableAddIndexIfNotExists:
            case NodeType.AlterTableAddUniqueIndex:
            case NodeType.AlterTableAddUniqueIndexIfNotExists:
            case NodeType.AlterTableDropIndex:
            case NodeType.AlterTableDropIndexIfExists:
            case NodeType.AlterTableAddPrimaryKey:
            case NodeType.AlterTableDropPrimaryKey:
                {
                    AlterIndexTicket alterIndexTicket = sqlExecutor.CreateAlterIndexTicket(ticket, ast);
                    context.Validator.Validate(alterIndexTicket);

                    bool? forwarded = await ddlForwarding.TryForwardAlterIndexAsync(database, alterIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor? table = await schemaDdl.OpenIndexTableAsync(database, alterIndexTicket).ConfigureAwait(false);
                    if (table is null)
                        return new ExecuteDDLSQLResult(database, false);

                    bool sqlAddIndex = alterIndexTicket.Operation is
                        AlterIndexOperation.AddIndex or
                        AlterIndexOperation.AddUniqueIndex or
                        AlterIndexOperation.AddPrimaryKey or
                        AlterIndexOperation.AddInvertedIndex;

                    // Inside a transaction with a private schema an index add takes the two-phase shape,
                    // which runs its build in the transaction; the staged coordinator commits steps of its own.
                    if (context.IsClusterMode && sqlAddIndex && TransactionSchemaScope.OverlayFor(database) is null)
                    {
                        bool ok = await schemaDdl.ExecuteClusterAddIndexAsync(database, table, alterIndexTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    // Both cluster (non-add) and standalone paths require the two-phase DDL sequence
                    // so Phase 2 (ReplicateIndexChangeAsync) persists the schema change across
                    // close/reopen. ExecuteDdlInTransaction is single-phase and skips that step.
                    bool indexExistedBefore = table.AllIndexes.ContainsKey(alterIndexTicket.IndexName);
                    bool compensateOnAbort = sqlAddIndex && !indexExistedBefore;
                    {
                        bool ok = await schemaDdl.ExecuteClusteredIndexDdlAsync(
                            database, table, alterIndexTicket, compensateOnAbort,
                            tx => tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx)
                        ).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }
                }

            case NodeType.AlterTableRenameColumn:
                {
                    AlterTableTicket renameColumnTicket = sqlExecutor.CreateAlterTableTicket(ticket, ast);
                    context.Validator.Validate(renameColumnTicket);
                    SchemaDdlService.RequireNoViewDependsOnAlteredColumn(database, renameColumnTicket);

                    bool? forwarded = await ddlForwarding.TryForwardAlterTableAsync(database, renameColumnTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor tableForRenameCol = await context.TableOpener.Open(database, renameColumnTicket.TableName).ConfigureAwait(false);

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableColumnAlterer.Alter(queryExecutor, database, tableForRenameCol, renameColumnTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameCol.Id),
                    tableNames: [renameColumnTicket.TableName]
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableRenameIndex:
                {
                    AlterIndexTicket renameIndexTicket = sqlExecutor.CreateAlterIndexTicket(ticket, ast);
                    context.Validator.Validate(renameIndexTicket);

                    bool? forwarded = await ddlForwarding.TryForwardAlterIndexAsync(database, renameIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor tableForRenameIdx = await context.TableOpener.Open(database, renameIndexTicket.TableName).ConfigureAwait(false);

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableIndexAlterer.Alter(queryExecutor, database, tableForRenameIdx, renameIndexTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameIdx.Id),
                    tableNames: [renameIndexTicket.TableName]
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddConstraintCheck:
            case NodeType.AlterTableAddConstraintForeignKey:
            case NodeType.AlterTableDropConstraint:
            case NodeType.AlterTableSetNotNull:
            case NodeType.AlterTableDropNotNull:
            case NodeType.AlterTableSetColumnStorage:
                {
                    TableDescriptor tableForConstraint = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);

                    AlterConstraintTicket alterConstraintTicket = sqlExecutor.CreateAlterConstraintTicket(ticket, ast, tableForConstraint.Schema);
                    context.Validator.Validate(alterConstraintTicket);

                    // Before the forward: a forwarded statement runs on the leader with no user principal.
                    ForeignKeyPrivileges.RequireReferencePrivileges(database, alterConstraintTicket);

                    bool? constraintForwarded = await ddlForwarding.TryForwardAlterConstraintAsync(database, alterConstraintTicket).ConfigureAwait(false);
                    if (constraintForwarded is not null)
                        return new ExecuteDDLSQLResult(database, constraintForwarded.Value);

                    bool constraintOk = await schemaDdl.AlterConstraintLocalAsync(database, tableForConstraint, alterConstraintTicket).ConfigureAwait(false);
                    database.Cache?.InvalidateByTableId(database.Id, tableForConstraint.Id);
                    return new ExecuteDDLSQLResult(database, constraintOk);
                }

            case NodeType.AlterTableSetSetting:
                {
                    // Context-free validation (recognized key, well-formed value) happens at ticket
                    // creation; the schema-dependent half runs inside AlterTableSettings.
                    AlterTableSettingsTicket settingsTicket = sqlExecutor.CreateAlterTableSettingsTicket(ticket, ast);
                    return await tableSettings.AlterTableSettings(database, settingsTicket).ConfigureAwait(false);
                }

            case NodeType.AlterTableRewriteStorage:
                {
                    // Refused for the same reason as TRUNCATE, plus one of its own. The rewrite commits
                    // its batches in transactions of its own, so a later ROLLBACK of the caller's
                    // transaction cannot undo them. And a Serializable caller holds locks on every row it
                    // read or wrote: each batch over those rows waits out the lock deadline, fails, and is
                    // deferred, so the statement burns minutes on a large table, converts none of those
                    // rows, and still reports success.
                    if (ticket.TxnState is { IsSessionOwned: true })
                        throw new CamusDBException(
                            CamusDBErrorCodes.StatementNotAllowedInTransaction,
                            "ALTER TABLE ... REWRITE STORAGE cannot run inside an explicit transaction: it commits " +
                            "its batches in transactions of its own, which a later ROLLBACK cannot undo. " +
                            "Commit or roll back first, then run it.");

                    // Runs as ordinary bounded transactions on this node, not as a replicated schema
                    // change: it rewrites row data under the current rules and changes no metadata.
                    TableDescriptor tableToRewrite = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    MaterializedViewAccessGuard.RequireWritable(tableToRewrite);

                    StorageRewriter rewriter = new(context.Logger);
                    await rewriter.RewriteAsync(database, tableToRewrite, inline: ast.yytext == "inline").ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.AlterTableResetSetting:
                {
                    AlterTableResetSettingsTicket resetTicket = sqlExecutor.CreateAlterTableResetSettingsTicket(ticket, ast);
                    return await tableSettings.AlterTableResetSettings(database, resetTicket).ConfigureAwait(false);
                }

            case NodeType.AlterTableRenameTo:
                {
                    string oldTableName = ast.leftAst!.yytext!;
                    string newTableName = ast.rightAst!.yytext!;
                    RenameTableTicket renameTableTicket = new(ticket.DatabaseName, oldTableName, newTableName);
                    context.Validator.Validate(renameTableTicket);

                    bool? forwarded = await ddlForwarding.TryForwardRenameTableAsync(database, renameTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor renameTableDesc = await context.TableOpener.Open(database, oldTableName).ConfigureAwait(false);
                    SchemaDdlService.RequireRenamableAsTable(renameTableDesc, renameTableTicket);

                    // A body written before relation ids were stored names its sources in text, so a
                    // rename would leave it pointing at something that no longer exists. Such a body
                    // is rebound to ids here and carried IN the rename's own delta: converted
                    // afterwards, there would be an interval in which it still names the old
                    // relation — and since the rename frees that name, anything created under it in
                    // the meantime would be read through that body and return its rows.
                    Dictionary<string, ViewDefinition>? renameRewrites =
                        ViewDependencyMaintainer.BuildRenameConversions(
                            database.Schema, renameTableDesc.Id,
                            sql => SQLParserProcessor.Parse(sql, sqlParserCache));

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await catalogs.RenameTable(database, renameTableTicket, tx, renameRewrites).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTableDesc.Id),
                    tableNames: [renameTableTicket.TableName, renameTableTicket.NewName]
                    ).ConfigureAwait(false);
                }

            case NodeType.CreateTrigger:
                {
                    await triggerDdl.CreateAsync(database, ast, ticket).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.DropTrigger:
                {
                    await triggerDdl.DropAsync(database, ast).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateExtension:
                {
                    await extensionDdl.CreateAsync(database, ast).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.AlterExtensionUpdate:
                {
                    await extensionDdl.UpdateAsync(database, ast).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.DropExtension:
                {
                    await extensionDdl.DropAsync(database, ast).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateFunction:
            case NodeType.CreateOrReplaceFunction:
                {
                    await functionDdl.CreateAsync(database, ast, ticket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.DropFunction:
                {
                    await functionDdl.DropAsync(database, ast, ticket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.AlterFunctionOwnerTo:
                {
                    await functionDdl.AlterOwnerAsync(database, ast, ticket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }


            case NodeType.CommentOnSequence:
                {
                    await CommentOnSequenceAsync(database, ast).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateSequence:
            case NodeType.CreateSequenceIfNotExists:
                {
                    CreateSequenceTicket createSequenceTicket = sqlExecutor.CreateCreateSequenceTicket(ticket, ast);
                    context.Validator.Validate(createSequenceTicket);

                    bool createdSequence = await sequenceDdl.CreateAsync(
                        database, createSequenceTicket, ticket.CancellationToken).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, createdSequence);
                }

            case NodeType.DropSequence:
            case NodeType.DropSequenceIfExists:
                {
                    DropSequenceTicket dropSequenceTicket = SqlExecutor.CreateDropSequenceTicket(ticket, ast);
                    context.Validator.Validate(dropSequenceTicket);

                    bool droppedSequence = await sequenceDdl.DropAsync(
                        database, dropSequenceTicket, ticket.CancellationToken).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, droppedSequence);
                }

            case NodeType.AlterSequenceRenameTo:
                {
                    RenameSequenceTicket renameSequenceTicket = SqlExecutor.CreateRenameSequenceTicket(ticket, ast);
                    context.Validator.Validate(renameSequenceTicket);

                    await sequenceDdl.RenameAsync(database, renameSequenceTicket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.AlterSequence:
                {
                    AlterSequenceTicket alterSequenceTicket = sqlExecutor.CreateAlterSequenceTicket(ticket, ast);
                    context.Validator.Validate(alterSequenceTicket);

                    // Refused here rather than inside the executor because this is where the
                    // caller's own transaction is visible. Moving a live counter is irrevocable —
                    // the values it skips are never reissued — so a later ROLLBACK could not undo
                    // it, and accepting the statement would promise semantics the engine cannot
                    // honour. The same rule TRUNCATE follows below.
                    if (alterSequenceTicket.Restart && ticket.TxnState is { IsSessionOwned: true })
                        throw new CamusDBException(
                            CamusDBErrorCodes.StatementNotAllowedInTransaction,
                            "ALTER SEQUENCE ... RESTART cannot run inside an explicit transaction: moving a " +
                            "counter is irrevocable and a later ROLLBACK cannot undo it. Commit or roll back " +
                            "first, then run it.");

                    await sequenceDdl.AlterAsync(database, alterSequenceTicket, ticket.CancellationToken).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.TruncateTable:
                {
                    TruncateTableTicket truncateTicket = sqlExecutor.CreateTruncateTableTicket(ticket, ast);
                    context.Validator.Validate(truncateTicket);

                    // Refused here rather than inside the executor because this is where the caller's
                    // own transaction is visible. Once TRUNCATE's replicated schema entry commits it
                    // is irrevocable, so a later ROLLBACK of the caller's transaction could not undo
                    // it — accepting the statement would promise semantics the engine cannot honour.
                    if (ticket.TxnState is { IsSessionOwned: true })
                        throw new CamusDBException(
                            CamusDBErrorCodes.StatementNotAllowedInTransaction,
                            "TRUNCATE cannot run inside an explicit transaction: it commits a replicated schema " +
                            "change that a later ROLLBACK cannot undo. Commit or roll back first, then run it.");

                    bool truncated = await schemaDdl.TruncateTable(truncateTicket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, truncated);
                }

            case NodeType.DropTable:
            case NodeType.DropTableIfExists:
                {
                    DropTableTicket dropTableTicket = sqlExecutor.CreateDropTableTicket(ticket, ast);
                    context.Validator.Validate(dropTableTicket);

                    // The same path as the ticket API, on purpose. The rules that can refuse a drop (a
                    // materialized view, a dependent view, a referencing foreign key) live inside it,
                    // under the DDL semaphore, so a statement forwarded to the schema leader meets them
                    // exactly as a local one does. A check here would be skipped by the forward.
                    bool droppedTable = await schemaDdl.DropTable(dropTableTicket).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, droppedTable);
                }

            case NodeType.CreateTableAsSelect:
            case NodeType.CreateTableAsSelectIfNotExists:
                {
                    (bool ctasCreated, int ctasRows, string? ctasWarning) =
                        await ctasExecutor.ExecuteCreateTableAsSelectAsync(database, ast, ticket).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, ctasCreated, ctasRows, ctasWarning);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown DDL AST stmt: " + ast.nodeType);
        }
    }
    /// <summary>
    /// The warning a RELINK returns when the restored table had foreign keys. They are not restored:
    /// while the table was detached, no parent-side check could see its rows, so the parents they
    /// reference may be gone. ALTER TABLE … ADD CONSTRAINT adds each one again and validates the rows.
    /// </summary>
    private static string? DroppedForeignKeysWarning(OrphanTableRecord? record, string tableName)
    {
        if (record?.Schema.ForeignKeys is not { Count: > 0 } foreignKeys)
            return null;

        string names = string.Join(", ", foreignKeys.Select(fk => fk.Name));

        return $"Table '{tableName}' was restored without its foreign keys ({names}). " +
               "Add each one again with ALTER TABLE ... ADD CONSTRAINT, which validates the restored rows.";
    }
}
