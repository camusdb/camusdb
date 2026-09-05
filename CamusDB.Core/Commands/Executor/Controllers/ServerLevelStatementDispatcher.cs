
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor.Controllers.Auth;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// What one server-level statement did, in the form both SQL entry points can map to their own
/// result type.
/// </summary>
/// <param name="Handled">
/// False when the statement is not server-level at all and the caller should carry on and open a
/// database for it.
/// </param>
/// <param name="Database">
/// The descriptor a statement produced, for the two that create one (<c>CREATE DATABASE</c> and
/// <c>CREATE DATABASE … RELINK</c>). Null for every other server-level statement, none of which
/// opens a database.
/// </param>
/// <param name="ReportsSuccess">
/// Whether the DDL surface reports this statement as an explicit success rather than a defaulted
/// result. Preserves a long-standing distinction: user and grant administration answer
/// <c>Success = true</c>, while drop/rename/comment/cluster-setting answer a defaulted result.
/// </param>
internal readonly record struct ServerLevelOutcome(
    bool Handled,
    DatabaseDescriptor? Database,
    bool ReportsSuccess)
{
    /// <summary>The statement is not server-level; the caller should open a database and continue.</summary>
    internal static readonly ServerLevelOutcome NotHandled = new(false, null, false);

    /// <summary>Handled, with no descriptor and a defaulted result on the DDL surface.</summary>
    internal static ServerLevelOutcome Done() => new(true, null, false);

    /// <summary>Handled, reported as an explicit success with no descriptor.</summary>
    internal static ServerLevelOutcome Succeeded() => new(true, null, true);

    /// <summary>Handled, producing a database descriptor.</summary>
    internal static ServerLevelOutcome Created(DatabaseDescriptor database) => new(true, database, true);
}

/// <summary>
/// The statements whose target is the server or a database itself, dispatched <b>before</b> any
/// database is opened: CREATE / DROP / RENAME / RELINK DATABASE, COMMENT ON DATABASE, user and grant
/// administration, and cluster settings.
///
/// <para><b>Opening first would be wrong, not merely wasteful.</b> Loading a descriptor for the
/// database a statement is about to destroy or rename resurrects the very object being removed, and
/// the server-level statements have no database context to open at all — a <c>CREATE DATABASE</c>
/// names a database that does not exist yet.</para>
///
/// <para><b>Shared by both SQL entry points on purpose.</b> A client routes any non-SELECT statement
/// to whichever endpoint it uses for those, so every statement here must be reachable through the
/// DDL entry point and the no-rows entry point alike. When these lists were maintained separately
/// they drifted: the no-rows path was missing <c>CREATE DATABASE</c> and <c>CREATE DATABASE …
/// RELINK</c>, so those statements fell through to the database open and failed with
/// "database does not exist" — indistinguishable, to that client, from an unsupported feature. One
/// list, consulted by both, is what keeps that from recurring.</para>
/// </summary>
internal sealed class ServerLevelStatementDispatcher
{
    private readonly ExecutorContext context;

    private readonly SqlExecutor sqlExecutor;

    private readonly DatabaseLifecycleService databaseLifecycle;

    private readonly SchemaDdlService schemaDdl;

    private readonly UserAdminService userAdmin;

    /// <summary>
    /// The runtime cluster-settings pipeline. Null on engines composed without one, where
    /// <c>SET/RESET CLUSTER SETTING</c> is rejected with a clear error rather than silently applying
    /// to nothing.
    /// </summary>
    private readonly ClusterSettingsService? clusterSettings;

    internal ServerLevelStatementDispatcher(
        ExecutorContext context,
        SqlExecutor sqlExecutor,
        DatabaseLifecycleService databaseLifecycle,
        SchemaDdlService schemaDdl,
        UserAdminService userAdmin,
        ClusterSettingsService? clusterSettings
    )
    {
        ArgumentNullException.ThrowIfNull(databaseLifecycle);
        ArgumentNullException.ThrowIfNull(schemaDdl);
        ArgumentNullException.ThrowIfNull(userAdmin);

        this.context = context;
        this.sqlExecutor = sqlExecutor;
        this.databaseLifecycle = databaseLifecycle;
        this.schemaDdl = schemaDdl;
        this.userAdmin = userAdmin;
        this.clusterSettings = clusterSettings;
    }

    /// <summary>
    /// Whether <paramref name="nodeType"/> is dispatched before any database is opened, for a caller
    /// that must reason about the routing without executing anything.
    ///
    /// <para>It forwards to <see cref="StatementScope.IsDatabaseScopedMutation"/> rather than
    /// restating the list. The two must agree exactly — the transports skip the transaction on the
    /// strength of that list, and the switch below is what decides whether a descriptor comes back —
    /// and a second copy is how they come to disagree. Keeping one list is the whole point.</para>
    /// </summary>
    internal static bool IsServerLevel(NodeType nodeType) => StatementScope.IsDatabaseScopedMutation(nodeType);

    /// <summary>
    /// Executes <paramref name="ast"/> when it is server-level, and reports
    /// <see cref="ServerLevelOutcome.NotHandled"/> otherwise so the caller can open a database and
    /// carry on.
    /// </summary>
    internal async Task<ServerLevelOutcome> TryExecuteAsync(ExecuteSQLTicket ticket, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.CreateDatabase:
            case NodeType.CreateDatabaseIfNotExists:
            case NodeType.CreateDatabaseBranch:
            case NodeType.CreateDatabaseBranchIfNotExists:
                {
                    bool isBranch = ast.nodeType is NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists;
                    bool ifNotExists = ast.nodeType is NodeType.CreateDatabaseIfNotExists or NodeType.CreateDatabaseBranchIfNotExists;
                    string dbName = ast.leftAst!.yytext!;
                    string? branchFrom = isBranch ? ast.rightAst!.yytext! : null;

                    DatabaseDescriptor created = await databaseLifecycle
                        .CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists, branchFrom))
                        .ConfigureAwait(false);

                    return ServerLevelOutcome.Created(created);
                }

            case NodeType.CreateDatabaseRelink:
                {
                    string dbName = ast.leftAst!.yytext!;
                    string orphanId = SQLExecutorBaseCreator.UnquoteStringLiteral(ast.rightAst!.yytext!);

                    DatabaseDescriptor relinked = await databaseLifecycle
                        .RelinkDatabase(new RelinkDatabaseTicket(dbName, orphanId))
                        .ConfigureAwait(false);

                    return ServerLevelOutcome.Created(relinked);
                }

            case NodeType.DropDatabase:
            case NodeType.DropDatabaseIfExists:
                {
                    string dbName = ast.leftAst!.yytext!;
                    bool ifExists = ast.nodeType == NodeType.DropDatabaseIfExists;
                    bool force = ast.yytext == "force";

                    await databaseLifecycle
                        .DropDatabase(new DropDatabaseTicket(dbName, ifExists, force))
                        .ConfigureAwait(false);

                    return ServerLevelOutcome.Done();
                }

            case NodeType.RenameDatabase:
                {
                    string oldName = ast.leftAst!.yytext!;
                    string newName = ast.rightAst!.yytext!;

                    await databaseLifecycle
                        .RenameDatabase(new RenameDatabaseTicket(oldName, newName))
                        .ConfigureAwait(false);

                    return ServerLevelOutcome.Done();
                }

            // A database comment lives on the cross-database registry entry, so it is handled here —
            // before any database is opened — alongside the other database-scoped DDL.
            case NodeType.CommentOnDatabase:
                {
                    CommentTicket databaseCommentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
                    context.Validator.Validate(databaseCommentTicket);
                    await schemaDdl.CommentDatabase(databaseCommentTicket).ConfigureAwait(false);

                    return ServerLevelOutcome.Done();
                }

            // User and grant administration lives in the shared _system/auth keyspace and opens no
            // database of its own (a table-scoped GRANT opens its target database itself, inside Grant).
            case NodeType.CreateUser:
            case NodeType.CreateUserIfNotExists:
                await userAdmin.CreateUser(sqlExecutor.CreateCreateUserTicket(ticket, ast)).ConfigureAwait(false);
                return ServerLevelOutcome.Succeeded();

            case NodeType.AlterUser:
                await userAdmin.AlterUser(sqlExecutor.CreateAlterUserTicket(ticket, ast)).ConfigureAwait(false);
                return ServerLevelOutcome.Succeeded();

            case NodeType.DropUser:
            case NodeType.DropUserIfExists:
                await userAdmin.DropUser(sqlExecutor.CreateDropUserTicket(ast)).ConfigureAwait(false);
                return ServerLevelOutcome.Succeeded();

            case NodeType.Grant:
            case NodeType.Revoke:
                await userAdmin.Grant(sqlExecutor.CreateGrantTicket(ast)).ConfigureAwait(false);
                return ServerLevelOutcome.Succeeded();

            // The change validates against the resulting configuration, replicates through the
            // settings log (or applies locally in standalone mode), and opens no database and no
            // transaction.
            case NodeType.SetClusterSetting:
            case NodeType.ResetClusterSetting:
                await ExecuteClusterSettingChangeAsync(ast).ConfigureAwait(false);
                return ServerLevelOutcome.Done();

            default:
                return ServerLevelOutcome.NotHandled;
        }
    }

    /// <summary>
    /// Executes <c>SET CLUSTER SETTING</c> / <c>RESET CLUSTER SETTING</c> through the settings
    /// pipeline: key and value validation (against the resulting configuration) happen there,
    /// before anything replicates. Engines composed without the service reject the statement
    /// rather than silently applying to nothing.
    /// </summary>
    internal async Task ExecuteClusterSettingChangeAsync(NodeAst ast)
    {
        if (clusterSettings is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cluster settings are not available on this engine");

        string key = ast.leftAst!.yytext!;

        if (ast.nodeType == NodeType.SetClusterSetting)
            await clusterSettings.SetAsync(key, ClusterSettingValueText(ast.rightAst!)).ConfigureAwait(false);
        else
            await clusterSettings.ResetAsync(key).ConfigureAwait(false);
    }

    /// <summary>
    /// Renders a parsed literal back to the text form a configuration file would carry — the one
    /// spelling the overlay parser accepts, so a value read from <c>SHOW VARIABLES</c> pastes back
    /// unchanged. Bare identifiers pass through for the enum-valued settings
    /// (<c>read_committed</c>, <c>adaptive</c>, …).
    /// </summary>
    private static string ClusterSettingValueText(NodeAst value) => value.nodeType switch
    {
        NodeType.String => SQLExecutorBaseCreator.UnquoteStringLiteral(value.yytext!),
        NodeType.Integer or NodeType.Float or NodeType.Bool or NodeType.Identifier => value.yytext!,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unsupported value literal for SET CLUSTER SETTING: {value.nodeType}"
        ),
    };
}
