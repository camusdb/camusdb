
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
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

    private readonly TableConstraintAlterer tableConstraintAlterer;

    private readonly ViewCreator viewCreator;

    private readonly MaterializedViewCreator matViewCreator;

    private readonly MaterializedViewRefresher matViewRefresher;

    private readonly AuthService? authService;

    /// <summary>
    /// The runtime cluster-settings pipeline. Null on engines composed without one, where
    /// <c>SET/RESET CLUSTER SETTING</c> is rejected with a clear error rather than silently applying
    /// to nothing.
    /// </summary>
    private readonly ClusterSettingsService? clusterSettings;

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
        TableConstraintAlterer tableConstraintAlterer,
        ViewCreator viewCreator,
        MaterializedViewCreator matViewCreator,
        MaterializedViewRefresher matViewRefresher,
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
        this.sqlExecutor = sqlExecutor;
        this.sqlParserCache = sqlParserCache;
        this.statementAuthorizer = statementAuthorizer;
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
        this.tableConstraintAlterer = tableConstraintAlterer;
        this.viewCreator = viewCreator;
        this.matViewCreator = matViewCreator;
        this.matViewRefresher = matViewRefresher;
        this.authService = authService;
        this.clusterSettings = clusterSettings;
    }

    private AuthService RequireAuthService()
    {
        if (authService is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication service is unavailable (no shared node was configured)");
        return authService;
    }

    internal async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(CommandExecutor executor, ExecuteSQLTicket ticket)
    {
        context.Validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        statementAuthorizer.SetAuthorizationScope(ticket, ast);
        ticket = SessionScalarFunctions.AttachSessionValues(ticket, ast);
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

            return serverLevel.ReportsSuccess ? new ExecuteDDLSQLResult(null!, true) : default;
        }

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

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
                        catalogs, database, ast,
                        ifExists: ast.nodeType == NodeType.DropViewIfExists).ConfigureAwait(false);

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

                    ViewDefinition transferred = ownedView.Definition;
                    transferred.Owner = newOwner?.Name ?? newOwnerName;
                    transferred.OwnerId = newOwner?.Id;

                    await catalogs.SetViewDefinitionAsync(database, ownedViewName, transferred).ConfigureAwait(false);
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
                        executor, catalogs, database, ast,
                        ifExists: ast.nodeType == NodeType.DropMaterializedViewIfExists,
                        context.Logger).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, droppedMatView);
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
                        ticket.DatabaseName, oldMatViewName, newMatViewName), matViewRewrites).ConfigureAwait(false);

                    return new ExecuteDDLSQLResult(database, true);
                }

            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = sqlExecutor.CreateCreateTableTicket(ticket, ast);
                    context.Validator.Validate(createTableTicket);

                    bool? forwarded = await ddlForwarding.TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    // Allocate before the DDL transaction — only the proposer/leader allocates;
                    // the id is carried in the replicated payload so every follower applies the same id.
                    DatabaseRegistry sqlRegistry = await context.Registry.ConfigureAwait(false);
                    string sqlTableId = await sqlRegistry.AllocateTableIdAsync().ConfigureAwait(false);

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableCreator.Create(queryExecutor, context.TableOpener, tableIndexAlterer, database, createTableTicket, tx, sqlTableId).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            case NodeType.CreateTableRelink:
                {
                    RelinkTableTicket relinkTicket = new(
                        ticket.DatabaseName,
                        ast.leftAst!.yytext!,
                        DML.SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst!.yytext!));

                    // Delegate to the executor method so fencing, forwarding, and orphan-load live in one place.
                    bool relinked = await schemaDdl.RelinkTable(relinkTicket).ConfigureAwait(false);
                    return new ExecuteDDLSQLResult(database, relinked);
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

                    if (context.IsClusterMode && alterTableTicket.Operation == AlterTableOperation.AddColumn)
                    {
                        bool ok = await schemaDdl.ExecuteClusterAddColumnAsync(database, table, alterTableTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
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
                    context.Validator.Validate(alterIndexTicket);

                    bool? forwarded = await ddlForwarding.TryForwardAlterIndexAsync(database, alterIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await context.TableOpener.Open(database, alterIndexTicket.TableName).ConfigureAwait(false);

                    bool sqlAddIndex = alterIndexTicket.Operation is
                        AlterIndexOperation.AddIndex or
                        AlterIndexOperation.AddUniqueIndex or
                        AlterIndexOperation.AddPrimaryKey;

                    if (context.IsClusterMode && sqlAddIndex)
                    {
                        bool ok = await schemaDdl.ExecuteClusterAddIndexAsync(database, table, alterIndexTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    // Both cluster (non-add) and standalone paths require the two-phase DDL sequence
                    // so Phase 2 (ReplicateIndexChangeAsync) persists the schema change across
                    // close/reopen. ExecuteDdlInTransaction is single-phase and skips that step.
                    bool indexExistedBefore = table.Indexes.ContainsKey(alterIndexTicket.IndexName);
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
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameCol.Id)
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
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, tableForRenameIdx.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddConstraintCheck:
            case NodeType.AlterTableDropConstraint:
            case NodeType.AlterTableSetNotNull:
            case NodeType.AlterTableDropNotNull:
                {
                    TableDescriptor tableForConstraint = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);

                    AlterConstraintTicket alterConstraintTicket = sqlExecutor.CreateAlterConstraintTicket(ticket, ast, tableForConstraint.Schema);
                    context.Validator.Validate(alterConstraintTicket);

                    bool? constraintForwarded = await ddlForwarding.TryForwardAlterConstraintAsync(database, alterConstraintTicket).ConfigureAwait(false);
                    if (constraintForwarded is not null)
                        return new ExecuteDDLSQLResult(database, constraintForwarded.Value);

                    bool constraintOk = await this.tableConstraintAlterer.Alter(
                        catalogs, database, tableForConstraint, alterConstraintTicket, context.IsClusterMode
                    ).ConfigureAwait(false);
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
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTableDesc.Id)
                    ).ConfigureAwait(false);
                }

            case NodeType.DropTable:
            case NodeType.DropTableIfExists:
                {
                    DropTableTicket dropTableTicket = sqlExecutor.CreateDropTableTicket(ticket, ast);
                    context.Validator.Validate(dropTableTicket);

                    bool? forwarded = await ddlForwarding.TryForwardDropTableAsync(database, dropTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    if (dropTableTicket.IfExists && !catalogs.TableExists(database, dropTableTicket.TableName))
                        return new(database, false);

                    TableDescriptor table = await context.TableOpener.Open(database, dropTableTicket.TableName).ConfigureAwait(false);

                    // A materialized view is stored as a relation, so DROP TABLE would happily remove
                    // one. Refusing keeps the statement that creates an object and the statement that
                    // removes it symmetric, and matches PostgreSQL.
                    if (table.Schema.IsMaterializedView)
                        throw new CamusDBException(
                            CamusDBErrorCodes.TableDoesntExist,
                            $"'{dropTableTicket.TableName}' is a materialized view; use DROP MATERIALIZED VIEW");

                    // Dropping a table a view reads would turn that view into a delayed error for
                    // whoever reads it next. Refuse instead, as PostgreSQL does. DROP TABLE has no
                    // CASCADE form yet, so there is deliberately no way to force it past this.
                    ViewDependencyMaintainer.RequireNoDependentViews(
                        database.Schema, dropTableTicket.TableName, table.Id, cascade: false);

                    return await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, dropTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    },
                    postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
                    ).ConfigureAwait(false);
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
}
