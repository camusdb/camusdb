/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Auth;

/// <summary>
/// Server-level user and grant administration against the shared <c>_system/auth</c> catalog:
/// <c>CREATE/ALTER/DROP USER</c>, <c>GRANT</c>/<c>REVOKE</c>, <c>SHOW GRANTS</c>, and bootstrap
/// seeding. These statements open no database of their own — a table-scoped grant opens its target
/// database itself, inside <see cref="Grant"/> — and so return no descriptor.
///
/// <para><b>Grants bind to immutable ids, never to names.</b> Every scope is resolved to a database
/// id (and, for a table scope, a table id) before it is stored, so a grant cannot resurrect against
/// a different object that later takes the same name after a drop and recreate.</para>
///
/// <para><b>Cleartext passwords are hashed here and go no further.</b> They are never persisted,
/// logged, or carried past this class.</para>
/// </summary>
internal sealed class UserAdminService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// The server-level user/grant catalog, still opening. Null when this engine was built without a
    /// shared node, in which case every statement here reports the surface as unavailable rather than
    /// failing with a raw engine error.
    /// </summary>
    private readonly Task<AuthCatalog>? authCatalogTask;

    /// <summary>
    /// The per-node authentication service, whose resolved-principal cache the flush statements drop.
    /// Null on the same engines <see cref="authCatalogTask"/> is null on — an engine with no shared
    /// node has neither — so every use of it is null-conditional rather than guarded separately.
    /// </summary>
    private readonly AuthService? authService;

    internal UserAdminService(
        ExecutorContext context,
        CamusDBOptions options,
        Task<AuthCatalog>? authCatalogTask,
        AuthService? authService = null)
    {
        this.context = context;
        this.options = options;
        this.authCatalogTask = authCatalogTask;
        this.authService = authService;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Each statement pins the field once, so an
    /// in-flight statement keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Records one change to the authentication catalog: who made it, what it did, and to whom.
    ///
    /// <para>Every mutation of users and grants passes through this class, which is why the record
    /// belongs here rather than in each transport. Without it a password rotation, a grant, or a
    /// dropped account leaves no trace at all — so an account takeover through a stolen token would be
    /// invisible after the fact, and there would be nothing to answer "who granted this" with.
    /// Cluster leave and backup already keep records of the same shape.</para>
    ///
    /// <para>Names are structured log values, never concatenated into the message. An account name is
    /// attacker-chosen text, and a name carrying newlines pasted into a log line could otherwise forge
    /// a second entry.</para>
    ///
    /// <para>The acting principal comes from the ambient authorization scope, which the statement gate
    /// publishes. It is absent when authentication is off, and is recorded as <c>anonymous</c> — an
    /// honest answer, because on such a node there genuinely is no principal to name.</para>
    /// </summary>
    /// <param name="operation">What changed, e.g. <c>create-user</c>. A fixed vocabulary, not free text.</param>
    /// <param name="target">The account or grantee the change applied to.</param>
    private void Audit(string operation, string target)
    {
        if (!context.Logger.IsEnabled(LogLevel.Information))
            return;

        string actor = AuthorizationContext.Current.Principal?.UserName ?? "anonymous";

        context.Logger.LogInformation(
            "Auth catalog change: {Operation} on {Target} by {Actor}", operation, target, actor);
    }

    internal async Task<AuthCatalog> GetAuthCatalogAsync()
    {
        if (authCatalogTask is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Authentication catalog is unavailable (no shared node was configured)");

        return await authCatalogTask.ConfigureAwait(false);
    }

    /// <summary>
    /// If <see cref="CamusDBOptions.AuthenticationEnabled"/> is on, ensures the catalog has at least one
    /// user by seeding the configured bootstrap superuser when it is empty. Fails startup (fail-closed)
    /// when auth is enabled, the catalog is empty, and no bootstrap secret was supplied — never opens an
    /// unauthenticated administration window. A no-op when auth is disabled or a user already exists.
    ///
    /// <para>It also gates the deployment's two authentication secrets through
    /// <see cref="AuthSecretPolicy"/>. That check runs before the catalog is read, and so applies on
    /// every start with authentication on — not only the first one that seeds a user.</para>
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
    internal async Task EnsureBootstrapSuperuserAsync(string bootstrapUser, string bootstrapPassword)
    {
        CamusDBOptions currentOptions = options;

        if (!currentOptions.AuthenticationEnabled || authCatalogTask is null)
            return;

        // Both secrets are checked here, before the catalog is touched, because this is the one
        // startup path that runs with authentication on. Checking them where they are used instead
        // would surface a misconfiguration on a user's first login rather than at boot, and a weak
        // token key is a property of the deployment, not of the request that trips over it.
        if (string.IsNullOrEmpty(currentOptions.AccessTokenServerKey))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                "Authentication is enabled but no access token server key is configured; " +
                "set CAMUSDB_AUTH_TOKEN_KEY before starting with authentication on.");

        AuthSecretPolicy.EnsureStrongEnough(currentOptions.AccessTokenServerKey, "CAMUSDB_AUTH_TOKEN_KEY");

        // Unlike the token key, an empty node secret is a valid single-node configuration: it leaves
        // the peer routes refused rather than weakly guarded. Only a configured value is measured.
        AuthSecretPolicy.EnsureStrongEnough(currentOptions.NodeSecret, "CAMUSDB_NODE_SECRET");

        AuthCatalog catalog = await GetAuthCatalogAsync().ConfigureAwait(false);
        if (await catalog.UserCountAsync().ConfigureAwait(false) > 0)
            return;

        if (string.IsNullOrEmpty(bootstrapUser) || string.IsNullOrEmpty(bootstrapPassword))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                "Authentication is enabled with an empty user catalog but no bootstrap superuser is configured; " +
                "refusing to start without an administrator (set the bootstrap superuser secret).");

        bool created = await catalog.TryBootstrapSuperuserAsync(
            bootstrapUser,
            PasswordHasher.Hash(bootstrapPassword, currentOptions.PasswordHashIterations)).ConfigureAwait(false);

        if (created && context.Logger.IsEnabled(LogLevel.Information))
            context.Logger.LogInformation("Bootstrap superuser '{User}' created", bootstrapUser);
    }

    /// <summary>
    /// Deletes expired session records, returning how many went. Returns zero without touching the
    /// catalog when authentication is off or this engine was built without a shared node, so the
    /// background sweep that drives it needs no knowledge of either condition.
    /// </summary>
    internal async Task<int> ReapExpiredSessionsAsync()
    {
        CamusDBOptions currentOptions = options;

        if (!currentOptions.AuthenticationEnabled || authCatalogTask is null)
            return 0;

        AuthCatalog catalog = await GetAuthCatalogAsync().ConfigureAwait(false);
        return await catalog.ReapExpiredSessionsAsync(DateTime.UtcNow).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a server-level user in the shared auth catalog. The cleartext password (if any) is hashed
    /// here and never persisted or logged; the ticket carries it no further. Server-level — returns no
    /// descriptor.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> CreateUser(CreateUserTicket ticket)
    {
        context.Validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        Credential? credential = ticket.Password is null ? null : PasswordHasher.Hash(ticket.Password, options.PasswordHashIterations);
        await auth.CreateUserAsync(ticket.UserName, credential, ticket.IfNotExists).ConfigureAwait(false);

        Audit("create-user", ticket.UserName);
        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Rotates a user's password verifier and advances its credential epoch.
    ///
    /// <para>When the statement carried a <c>REPLACE</c> clause, the password it names is verified
    /// against the stored credential before anything is written. Whether that clause was <em>required</em>
    /// is not decided here — that needs the calling principal, which this ticket does not carry, and
    /// <c>StatementAuthorizer</c> has already refused a self-change that omitted it. This method's
    /// contract is narrower and worth stating plainly: it verifies whatever it is given, and a supplied
    /// current password that does not match always fails the statement.</para>
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> AlterUser(AlterUserTicket ticket)
    {
        context.Validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);

        if (ticket.CurrentPassword is not null)
            await VerifyCurrentPasswordAsync(auth, ticket.UserName, ticket.CurrentPassword).ConfigureAwait(false);

        await auth.SetPasswordAsync(ticket.UserName, PasswordHasher.Hash(ticket.Password, options.PasswordHashIterations)).ConfigureAwait(false);

        // A password change is the step that turns a stolen token into a lasting takeover, so it is the
        // single most useful line in this log even though the rotation itself succeeded legitimately.
        Audit("alter-user-password", ticket.UserName);
        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Checks a presented current password against the account's stored verifier, throwing
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/> when it does not match.
    ///
    /// <para>An account with no password at all also fails, rather than being treated as matching
    /// anything. Such an account cannot log in, so no legitimate caller can be holding a session for it
    /// and asking to rotate its own secret.</para>
    ///
    /// <para>The failure carries the same shape and message as a failed login on purpose, so this does
    /// not become a second oracle that distinguishes "wrong password" from "no password set".</para>
    /// </summary>
    private async Task VerifyCurrentPasswordAsync(AuthCatalog auth, string userName, string currentPassword)
    {
        UserRecord? record = await auth.TryGetUserAsync(userName).ConfigureAwait(false);

        if (record?.Credential is null || !PasswordHasher.Verify(currentPassword, record.Credential))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");
    }

    /// <summary>Drops a user and all its grants in one catalog transaction.</summary>
    internal async Task<ExecuteDDLSQLResult> DropUser(DropUserTicket ticket)
    {
        context.Validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        await auth.DropUserAsync(ticket.UserName, ticket.IfExists).ConfigureAwait(false);

        Audit("drop-user", ticket.UserName);
        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Applies a <c>GRANT</c>/<c>REVOKE</c>. Resolves the grant object's name(s) to immutable ids first
    /// (a database via the registry; a table by opening the target database's catalog) so the grant is
    /// bound to the id, not the name, and never resurrects on a dropped-and-recreated object.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> Grant(GrantTicket ticket)
    {
        context.Validator.Validate(ticket);

        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        GrantScope scope = await ResolveGrantScopeAsync(ticket).ConfigureAwait(false);
        await auth.GrantAsync(ticket.UserName, scope, ticket.Privileges, ticket.Revoke).ConfigureAwait(false);

        Audit(ticket.Revoke ? "revoke" : "grant", ticket.UserName);
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
                    DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
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
                    DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
                    DatabaseRegistryEntry? entry = await registry.TryResolveEntryAsync(ticket.DatabaseName).ConfigureAwait(false);
                    if (entry is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.DatabaseDoesntExist,
                            $"Database '{ticket.DatabaseName}' does not exist");

                    // Open the TARGET database (not the empty context database) to resolve the table id.
                    DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
                    using DatabaseUseHandle _ = database.Use();

                    // A view is a grantable object, and it cannot be resolved by opening a relation —
                    // opening one refuses views outright. Resolved from the view map instead, which is
                    // what makes a grant on a view expressible at all; without this the per-view
                    // authorization checks would be unsatisfiable, and every view permanently
                    // unreachable to anyone but a superuser.
                    if (database.Schema.Views.TryGetValue(ticket.TableName, out Catalogs.Models.ViewSchema? grantedView))
                        return new GrantScope
                        {
                            Kind = GrantScopeKind.Table,
                            DatabaseId = entry.Id,
                            DatabaseName = entry.Name,
                            TableId = grantedView.Id ?? "",
                            TableName = grantedView.Name ?? "",
                        };

                    TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

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
    /// Returns the whole catalog — every account with its grants — for <c>SHOW USERS</c> and
    /// <c>SHOW GRANTS FOR *</c>. Server-level: it reads the auth catalog and needs no open database.
    ///
    /// <para>It takes a fresh picture from storage rather than reading the in-memory caches, because
    /// these two statements are what an operator inventories accounts with, and a listing that is
    /// quietly short is worse than no listing at all. The snapshot is complete or it raises; see
    /// <see cref="AuthCatalog.SnapshotAsync"/>.</para>
    /// </summary>
    internal async Task<AuthCatalogSnapshot> SnapshotForShowAsync()
    {
        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        return await auth.SnapshotAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Makes an authorization change take effect now: advances the catalog's coherence generation,
    /// reloads this node's user and grant caches from storage, and drops this node's resolved-principal
    /// cache.
    ///
    /// <para>The generation is durable and replicated, so the other nodes discard what they derived
    /// from the older one as soon as they observe it — which is on the first read each one makes after
    /// its own cached authorization snapshots expire. No node-to-node call is involved.</para>
    ///
    /// <para>It revokes nothing. A client whose grants did not change keeps working across it without
    /// logging in again; <see cref="FlushSessionsAsync"/> is the statement that ends sessions.</para>
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> FlushPrivilegesAsync()
    {
        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        await auth.FlushPrivilegesAsync().ConfigureAwait(false);

        // The catalog owns the durable half; this drops what this process derived from it. A flush
        // moves no account's epochs, so the per-account revalidation on a cache hit would otherwise
        // keep every entry — which is right for a grant and wrong for an operator forcing a reload.
        authService?.DropPrincipalCache();

        Audit("flush-privileges", "*");
        return new ExecuteDDLSQLResult(null!, true);
    }

    /// <summary>
    /// Deletes every stored login session, so every client must authenticate again, and drops this
    /// node's resolved-principal cache so none of them is served from it in the meantime.
    ///
    /// <para>This is the blunt instrument, for staleness that <see cref="FlushPrivilegesAsync"/> cannot
    /// reach because something outside this server holds it. On another node a principal cached before
    /// the flush stays usable until it expires, which is the same bound every revocation already
    /// carries (<see cref="CamusDBOptions.AuthenticationCacheTtl"/>).</para>
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> FlushSessionsAsync()
    {
        AuthCatalog auth = await GetAuthCatalogAsync().ConfigureAwait(false);
        int revoked;

        try
        {
            revoked = await auth.RevokeAllSessionsAsync().ConfigureAwait(false);
        }
        finally
        {
            // In the finally, because a statement that deleted most of the sessions and then failed
            // must not leave this node still serving cached decisions for the ones it did delete.
            authService?.DropPrincipalCache();
        }

        // Logging out a whole fleet is the kind of change an incident review asks about afterwards,
        // so how many sessions went is recorded and not only the fact that the statement ran. The
        // count goes in the target field because the operation field is a fixed vocabulary.
        Audit("flush-sessions", $"{revoked} session(s)");
        return new ExecuteDDLSQLResult(null!, true);
    }
}
