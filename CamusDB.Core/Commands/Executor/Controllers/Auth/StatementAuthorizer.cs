
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Auth;

/// <summary>
/// The authorization gate every SQL entry point passes through before a statement touches any lock or
/// mutation. A no-op when <see cref="CamusDBOptions.AuthenticationEnabled"/> is off (the default).
///
/// <para><b>Enforcement happens at two levels, and this class is only the first.</b> Statements whose
/// target is the server or a whole database — user/grant administration, database lifecycle DDL,
/// engine metrics, configuration — are decided here, because there is no table to hang a check on.
/// Everything else is deliberately <em>not</em> decided here: it is enforced per table at the
/// resolution chokepoint in <c>TableOpener.Open</c>, which sees every referenced table including the
/// join and subquery sources that never reach a statement-level gate. This class publishes the
/// caller and the statement's required privilege to the ambient <see cref="AuthorizationContext"/>
/// so that check has what it needs.</para>
///
/// <para><b><see cref="SetAuthorizationScope"/> must be called synchronously from the entry method.</b>
/// It writes an <see cref="AsyncLocal{T}"/>, which flows from a synchronous write down into the
/// caller's callees — a write made inside an awaited method would not reach them, and the per-table
/// check would silently see no scope at all.</para>
///
/// <para>Catalog listings are <em>filtered</em> rather than refused: see
/// <see cref="VisibilityPrincipal"/>. The branch statements additionally report a database the caller
/// cannot see as non-existent, so the error itself cannot be used to probe for one.</para>
/// </summary>
internal sealed class StatementAuthorizer
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    internal StatementAuthorizer(ExecutorContext context, CamusDBOptions options)
    {
        this.context = context;
        this.options = options;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Each statement pins the field once, so an
    /// in-flight statement is authorized against the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Rejects an unauthenticated request and checks the parsed statement against the caller's
    /// privileges before any lock or mutation:
    /// <list type="bullet">
    ///   <item>user/grant administration and database lifecycle DDL require the superuser attribute;</item>
    ///   <item>server-level <c>SHOW</c> statements are allowed to any authenticated caller — the
    ///     catalog listings are not rejected but <em>filtered</em>, see <see cref="VisibilityPrincipal"/>;</item>
    ///   <item>an in-database statement requires its mapped privilege at the context database's scope
    ///     (a <c>db.*</c> or global grant satisfies it).</item>
    /// </list>
    ///
    /// <para><b>Scope note:</b> enforcement here is at the database scope. Table-scoped grants
    /// (<c>db.table</c>) and per-object checks for join / subquery / <c>INSERT … SELECT</c> sources are
    /// handled at the per-table chokepoint; this gate fails <em>closed</em> (a table-only grant is
    /// denied a database-wide check, never over-permitted).</para>
    /// </summary>
    internal async Task EnforceAsync(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!options.AuthenticationEnabled)
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

        // Engine metrics are deliberately held to a higher bar than the introspection statements below.
        // Raft partition ids, WAL batch sizes, and transaction-abort rates describe cluster topology and
        // workload volume for the whole node, which no per-database grant scopes down — unlike SHOW
        // DATABASES, whose output is filtered to what the caller can already reach.
        if (ast.nodeType is NodeType.ShowEngineStats)
        {
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Engine statistics require a superuser");
            return;
        }

        // The slow query log is held to a higher bar still, and for a sharper reason than the two
        // around it: its rows carry the literal SQL text of statements other users ran, so a
        // predicate value from a table this caller has no grant on can appear verbatim in the
        // output. No per-database grant scopes that down, which leaves superuser.
        if (ast.nodeType is NodeType.ShowSlowQueries)
        {
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "The slow query log requires a superuser");
            return;
        }

        // Configuration is held to the same bar, and for the same reason. Even with the three secret
        // settings masked, the output describes the node's entire security posture and limits — whether
        // authentication and TLS are on, the password hashing cost, the data directory, every rate-limit
        // ceiling — which is reconnaissance material that no per-database grant scopes down.
        if (ast.nodeType is NodeType.ShowVariables)
        {
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Configuration variables require a superuser");
            return;
        }

        // Changing configuration fleet-wide is gated harder still in effect: several of these knobs
        // bound memory, concurrency and background work, so a non-superuser who could SET one has a
        // denial-of-service lever on every node. Reading the overlay follows SHOW VARIABLES' bar so
        // the whole configuration surface is consistent.
        if (ast.nodeType is NodeType.SetClusterSetting or NodeType.ResetClusterSetting or NodeType.ShowClusterSettings)
        {
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Cluster settings require a superuser");
            return;
        }

        // Server-level introspection: any authenticated caller may run these.
        if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors
            or NodeType.ShowOrphanDatabases or NodeType.ShowGrants)
            return;

        // CREATE TABLE is checked at DATABASE scope here — the table does not exist yet, so it cannot
        // be a per-table grant target, and the check must happen before the table is created (not at
        // the post-create re-open). A db.* / global CreateTable grant (or superuser) passes.
        if (ast.nodeType is NodeType.CreateTable or NodeType.CreateTableIfNotExists or NodeType.CreateTableRelink
            or NodeType.CreateTableAsSelect or NodeType.CreateTableAsSelectIfNotExists
            // A view and a materialized view are relations too, and are equally unable to be a
            // per-table grant target before they exist.
            or NodeType.CreateView or NodeType.CreateOrReplaceView
            or NodeType.CreateMaterializedView or NodeType.CreateMaterializedViewIfNotExists)
        {
            DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
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
    internal void SetAuthorizationScope(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (options.AuthenticationEnabled)
        {
            AuthorizationContext.Current = new AuthorizationScope(ticket.Principal, MapRequiredPrivilege(ast.nodeType));
            return;
        }

        // Auth disabled: the scope must still be cleared when a pooled execution context carries a
        // stale value — but writing an AsyncLocal (even `default`) forces the runtime to clone the
        // ExecutionContext and re-copy it across every await in the statement. Reading is free, so
        // only pay for the write when there is actually something to clear.
        if (AuthorizationContext.Current != default)
            AuthorizationContext.Current = default;
    }

    /// <summary>
    /// The principal whose grants should filter a catalog listing (<c>SHOW TABLES</c>,
    /// <c>SHOW DATABASES</c>, <c>SHOW BRANCHES</c>, <c>SHOW ANCESTORS</c>), or null when no filtering
    /// applies.
    ///
    /// <para>Null when authentication is disabled, which keeps the unauthenticated deployment listing
    /// every object as before. With authentication on the ticket always carries a principal — a null
    /// one was already rejected by <see cref="EnforceAsync"/> before this is reached — so the
    /// null-propagation here is only a safety net, never a way to opt out of filtering.</para>
    /// </summary>
    internal Principal? VisibilityPrincipal(ExecuteSQLTicket ticket)
        => options.AuthenticationEnabled ? ticket.Principal : null;

    /// <summary>
    /// Requires that the caller may change who owns <paramref name="view"/>: a superuser, or its
    /// current owner.
    /// </summary>
    /// <remarks>
    /// Ownership decides whose privileges the view's body runs with, so transferring it is a transfer
    /// of authority — an <c>Alter</c> grant on the view is not enough, or anyone who could rename a
    /// view could also point its definer's rights at an account they control.
    /// </remarks>
    internal async Task RequireViewOwnershipAsync(
        DatabaseDescriptor database, string viewName, Catalogs.Models.ViewSchema view, ExecuteSQLTicket ticket)
    {
        await Task.CompletedTask;

        if (!options.AuthenticationEnabled)
            return;

        Principal? caller = ticket.Principal;

        if (caller is null)
            throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Authentication is required");

        if (caller.IsSuperuser)
            return;

        if (view.Definition?.OwnerId is { Length: > 0 } ownerId
            && string.Equals(caller.UserId, ownerId, StringComparison.Ordinal))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InsufficientPrivilege,
            $"Only a superuser or the current owner may change the owner of view '{database.Name}.{viewName}'");
    }

    /// <summary>Maps an in-database statement to the privilege it requires, or null when it needs none.</summary>
    internal static Privilege? MapRequiredPrivilege(NodeType nodeType) => nodeType switch
    {
        NodeType.Select => Privilege.Select,
        // INSERT … SELECT maps to the privilege for its TARGET. Its source tables are resolved under
        // a narrowed Select requirement while the source query is built, so this never demands Insert
        // on a table the statement only reads.
        NodeType.Insert or NodeType.InsertSelect => Privilege.Insert,
        NodeType.Update => Privilege.Update,
        NodeType.Delete => Privilege.Delete,
        NodeType.CreateTable or NodeType.CreateTableIfNotExists or NodeType.CreateTableRelink
            or NodeType.CreateTableAsSelect or NodeType.CreateTableAsSelectIfNotExists => Privilege.CreateTable,
        NodeType.DropTable or NodeType.DropTableIfExists => Privilege.Drop,
        // TRUNCATE needs DELETE *and* DROP. Only one privilege can ride the ambient scope the
        // per-table chokepoint reads, so this names DELETE and the truncate path checks DROP
        // itself — separately, because two privileges granted in two statements live in two
        // grant records and a combined mask would match neither.
        NodeType.TruncateTable => Privilege.Delete,
        NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists
            or NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists
            or NodeType.AlterTableDropIndex => Privilege.Index,
        NodeType.AlterTableAddColumn or NodeType.AlterTableDropColumn or NodeType.AlterTableRenameTo
            or NodeType.AlterTableRenameColumn or NodeType.AlterTableRenameIndex
            or NodeType.AlterTableAddConstraintCheck or NodeType.AlterTableDropConstraint
            or NodeType.AlterTableSetNotNull or NodeType.AlterTableDropNotNull
            or NodeType.AlterTableAddPrimaryKey or NodeType.AlterTableDropPrimaryKey
            or NodeType.AlterTableSetSetting or NodeType.AlterTableResetSetting or NodeType.AnalyzeTable
            or NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex => Privilege.Alter,
        NodeType.ShowTables or NodeType.ShowColumns or NodeType.ShowIndexes or NodeType.ShowCreateTable
            or NodeType.ShowDatabase or NodeType.ShowOrphanTables => Privilege.Select,
        // Statistics report bounds drawn from real column values, so reading them is a read of the
        // table's data — the same bar as selecting from it, and deliberately not a superuser gate.
        NodeType.ShowStatistics => Privilege.Select,
        // Range bounds are decoded from real column values too — a split point IS a value stored in
        // the table — so whoever may read the rows may see where they divide, and nobody else. Same
        // bar as SHOW STATISTICS, and deliberately not a superuser gate: this is the statement an
        // operator reaches for to explain a slow query over a table they already read.
        NodeType.ShowRanges => Privilege.Select,
        // Creating a view or materialized view creates a relation, so it needs the same privilege
        // creating a table does — and is checked at database scope for the same reason: the object
        // does not exist yet, so it cannot be a per-table grant target.
        NodeType.CreateView or NodeType.CreateOrReplaceView
            or NodeType.CreateMaterializedView or NodeType.CreateMaterializedViewIfNotExists => Privilege.CreateTable,
        NodeType.DropView or NodeType.DropViewIfExists
            or NodeType.DropMaterializedView or NodeType.DropMaterializedViewIfExists => Privilege.Drop,
        NodeType.AlterViewRenameTo or NodeType.AlterViewOwnerTo
            or NodeType.AlterMaterializedViewRenameTo => Privilege.Alter,
        // REFRESH replaces the relation's contents, so it is a write, not a read.
        NodeType.RefreshMaterializedView => Privilege.Insert,
        NodeType.ShowViews or NodeType.ShowMaterializedViews
            or NodeType.ShowCreateView or NodeType.ShowCreateMaterializedView => Privilege.Select,
        _ => null,
    };
}
