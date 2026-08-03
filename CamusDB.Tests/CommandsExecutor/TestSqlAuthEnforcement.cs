/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end Phase 2 authentication + privilege enforcement, driven through the real
/// <see cref="CommandExecutor"/> entry points. Enforcement lives in the engine (keyed off the ticket's
/// <see cref="Principal"/>); authentication (login → token → principal) is exercised via the executor's
/// auth service, so no HTTP transport is needed. The auth config flag is toggled per test and reset in
/// teardown.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestSqlAuthEnforcement : BaseTest
{

    /// <summary>
    /// Auth on, with a known signing key and bootstrap superuser — the baseline every test here starts
    /// from. A test needing different auth settings derives its own options and builds its own engine.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-server-key",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
    };



    private static async Task<Principal> LoginAsync(CommandExecutor executor, string user, string password)
    {
        string token = (await executor.LoginAsync(user, password)).Token;
        return await executor.ResolvePrincipalAsync(token);
    }

    private static Task RunDdl(CommandExecutor executor, string dbname, string sql, Principal? principal)
        => executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null, principal: principal));

    private static async Task RunTxnDdl(CommandExecutor executor, string dbname, string sql, Principal? principal)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null, principal));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task RunQuery(CommandExecutor executor, string dbname, string sql, Principal? principal)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null, principal));
        await foreach (QueryResultRow _ in cursor) { }
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task RunNonQuery(CommandExecutor executor, string dbname, string sql, Principal? principal)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null, principal));
        await db.Transactions.CommitAsync(tx);
    }

    // Builds a database with a superuser (bootstrapped) and a table, all before/with auth as noted.
    private async Task<(string dbname, CommandExecutor executor, Principal root)> SetupWithSuperuser(
        CamusDBOptions? options = null)
    {
        // CreateDatabase is a direct API call (not a SQL statement), so it bypasses the SQL gate — fine
        // for test setup. Use a letter-prefixed name so it is a valid bare identifier in SQL.
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        await executor.EnsureBootstrapSuperuserAsync();
        Principal root = await LoginAsync(executor, "root", "root-password");

        await RunTxnDdl(executor, dbname, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, name string NOT NULL)", root);
        return (dbname, executor, root);
    }

    [Test]
    public async Task Disabled_NoPrincipalRequired()
    {
        // Auth off: statements run with no principal. This fixture's engines are authenticated by
        // default, so the auth-off case builds its own.
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(
            Options with { AuthenticationEnabled = false });
        await RunTxnDdl(executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL)", principal: null);
        await RunQuery(executor, dbname, "SELECT id FROM t", principal: null);
        Assert.Pass();
    }

    [Test]
    public async Task Enabled_NullPrincipalRejected()
    {
        (string dbname, CommandExecutor executor, _) = await SetupWithSuperuser();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunQuery(executor, dbname, "SELECT id FROM items", principal: null))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    [Test]
    public async Task WrongPassword_AuthenticationFailed()
    {
        (_, CommandExecutor executor, _) = await SetupWithSuperuser();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.LoginAsync("root", "wrong-password"))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    [Test]
    public async Task Superuser_CanAdministerAndAccess()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        // Superuser bypasses every check: create a user, grant, select, insert.
        await RunDdl(executor, "", "CREATE USER app IDENTIFIED BY 'app-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO app", root);
        await RunQuery(executor, dbname, "SELECT id FROM items", root);
    }

    [Test]
    public async Task GrantedUser_SelectAllowed_InsertDenied()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER reader IDENTIFIED BY 'reader-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO reader", root);

        Principal reader = await LoginAsync(executor, "reader", "reader-pw");

        // SELECT is granted.
        await RunQuery(executor, dbname, "SELECT id FROM items", reader);

        // INSERT is not.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (1, 'x')", reader))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    /// <summary>
    /// SHOW ENGINE STATS sits above the other server-level introspection statements: SHOW DATABASES is
    /// filtered down to what the caller can already reach, whereas engine metrics expose Raft topology
    /// and whole-node workload volume that no per-database grant scopes. A user with grants on the
    /// database must therefore still be refused.
    /// </summary>
    [Test]
    public async Task ShowEngineStats_RequiresSuperuser()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER metrics_reader IDENTIFIED BY 'metrics-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO metrics_reader", root);
        Principal granted = await LoginAsync(executor, "metrics_reader", "metrics-pw");

        // A grant that is enough for SHOW DATABASES is not enough here.
        await RunServerQuery(executor, "SHOW DATABASES", granted);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunServerQuery(executor, "SHOW ENGINE STATS", granted))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);

        // The superuser gets rows.
        await RunServerQuery(executor, "SHOW ENGINE STATS", root);
    }

    /// <summary>Runs a server-level statement: no database context and no transaction.</summary>
    private static async Task RunServerQuery(CommandExecutor executor, string sql, Principal? principal)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: principal));
        await foreach (QueryResultRow _ in cursor) { }
    }

    [Test]
    public async Task NonSuperuser_CannotAdministerUsers()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER plain IDENTIFIED BY 'plain-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT, INSERT ON {dbname}.* TO plain", root);
        Principal plain = await LoginAsync(executor, "plain", "plain-pw");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "CREATE USER sneaky IDENTIFIED BY 'x'", plain))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    [Test]
    public async Task RevokedPrivilege_StopsWorking()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER svc IDENTIFIED BY 'svc-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO svc", root);

        // A token obtained now must reflect a later revoke within the cache TTL; use a fresh login
        // after the revoke to get the current authorization snapshot.
        await RunDdl(executor, "", $"REVOKE SELECT ON {dbname}.* FROM svc", root);
        Principal svc = await LoginAsync(executor, "svc", "svc-pw");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunQuery(executor, dbname, "SELECT id FROM items", svc))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    [Test]
    public async Task PasswordRotation_InvalidatesToken()
    {
        // Force authoritative validation on every resolve so the old token is checked against the
        // catalog immediately (the default 1s cache would otherwise serve it until the TTL elapses —
        // that documented staleness bound is covered by the token-cache tests, not here).
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser(
            Options with { AuthenticationCacheTtl = TimeSpan.Zero });

        await RunDdl(executor, "", "CREATE USER rot IDENTIFIED BY 'old-pw'", root);
        string token = (await executor.LoginAsync("rot", "old-pw")).Token;
        // Token resolves fine now.
        await executor.ResolvePrincipalAsync(token);

        // Superuser rotates rot's password → credential epoch advances → old token invalid.
        await RunDdl(executor, "", "ALTER USER rot IDENTIFIED BY 'new-pw'", root);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ResolvePrincipalAsync(token))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    [Test]
    public async Task Logout_InvalidatesToken()
    {
        (_, CommandExecutor executor, _) = await SetupWithSuperuser();

        string token = (await executor.LoginAsync("root", "root-password")).Token;
        await executor.ResolvePrincipalAsync(token);

        await executor.LogoutAsync(token);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ResolvePrincipalAsync(token))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    [Test]
    public async Task ShowGrants_NoFor_ReturnsOwnGrants()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();
        await RunDdl(executor, "", "CREATE USER viewer IDENTIFIED BY 'v-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO viewer", root);
        Principal viewer = await LoginAsync(executor, "viewer", "v-pw");

        // SHOW GRANTS with no FOR defaults to the caller.
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "SHOW GRANTS", parameters: null, principal: viewer));
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor) rows.Add(row);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("viewer", rows[0].Row["user"].StrValue);
    }

    [Test]
    public async Task SelfPasswordChange_Allowed_OthersDenied()
    {
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser();
        await RunDdl(executor, "", "CREATE USER selfie IDENTIFIED BY 'old-pw'", root);
        Principal selfie = await LoginAsync(executor, "selfie", "old-pw");

        // A non-superuser may change their OWN password.
        await RunDdl(executor, "", "ALTER USER selfie IDENTIFIED BY 'new-pw'", selfie);

        // But not someone else's.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "ALTER USER root IDENTIFIED BY 'hijack'", selfie))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    [Test]
    public async Task BootstrapFailsClosed_WhenNoSecret()
    {
        // Auth on but no bootstrap secret configured: seeding must refuse rather than start a server
        // whose superuser has no password. This fixture's default supplies a secret, so it is removed
        // here — the missing secret is the condition under test.
        CommandExecutor executor = CreateCommandExecutor(Options with { BootstrapSuperuserPassword = "" });
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.EnsureBootstrapSuperuserAsync())!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }
}
