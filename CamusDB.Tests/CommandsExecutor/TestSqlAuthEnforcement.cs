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
using CamusDB.Core.Auth;
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
        AccessTokenServerKey = "test-server-key-padded-to-meet-the-32-byte-secret-floor",
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

        await executor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
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

    /// <summary>
    /// The slow query log is held to the same bar as engine metrics, for a sharper reason: its rows
    /// carry the literal SQL text of statements other users ran, so a predicate value from a table
    /// this caller has no grant on can appear verbatim in the output.
    /// </summary>
    [Test]
    public async Task ShowSlowQueries_RequiresSuperuser()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER log_reader IDENTIFIED BY 'log-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO log_reader", root);
        Principal granted = await LoginAsync(executor, "log_reader", "log-pw");

        // A grant that is enough for SHOW DATABASES is not enough here.
        await RunServerQuery(executor, "SHOW DATABASES", granted);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunServerQuery(executor, "SHOW SLOW QUERIES", granted))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);

        // The superuser is admitted. The engine under test has the log off, so it returns no rows
        // rather than an error — which is itself the contract.
        await RunServerQuery(executor, "SHOW SLOW QUERIES", root);
    }

    /// <summary>
    /// SHOW VARIABLES is held to the same bar as engine metrics and for the same reason: even with the
    /// secret settings masked, the output describes the node's whole security posture and limits, which
    /// no per-database grant scopes down.
    /// </summary>
    [Test]
    public async Task ShowVariables_RequiresSuperuser()
    {
        (string dbname, CommandExecutor executor, Principal root) = await SetupWithSuperuser();

        await RunDdl(executor, "", "CREATE USER config_reader IDENTIFIED BY 'config-pw'", root);
        await RunDdl(executor, "", $"GRANT SELECT ON {dbname}.* TO config_reader", root);
        Principal granted = await LoginAsync(executor, "config_reader", "config-pw");

        // A grant that is enough for SHOW DATABASES is not enough here.
        await RunServerQuery(executor, "SHOW DATABASES", granted);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunServerQuery(executor, "SHOW VARIABLES", granted))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);

        // The superuser gets rows.
        await RunServerQuery(executor, "SHOW VARIABLES", root);
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

        // Changing your own password requires presenting the current one. Without that rule a stolen
        // token converts into a lasting takeover: the thief rotates the password and logs back in, and
        // the credential-epoch bump that kills the stolen token does not help, because they set the
        // replacement. This is the assertion that keeps the REPLACE clause mandatory.
        CamusDBException noCurrent = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "ALTER USER selfie IDENTIFIED BY 'new-pw'", selfie))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, noCurrent.Code);

        // A wrong current password fails too, and fails as an authentication failure rather than a
        // privilege one — the caller is allowed to make this change, they just did not prove it.
        CamusDBException wrongCurrent = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "ALTER USER selfie IDENTIFIED BY 'new-pw' REPLACE 'not-the-password'", selfie))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, wrongCurrent.Code);

        // With the real current password it goes through, and the new one is what works afterwards.
        await RunDdl(executor, "", "ALTER USER selfie IDENTIFIED BY 'new-pw' REPLACE 'old-pw'", selfie);
        await LoginAsync(executor, "selfie", "new-pw");

        // Someone else's password is still refused, clause or no clause.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "ALTER USER root IDENTIFIED BY 'hijack'", selfie))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);

        CamusDBException withClause = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", "ALTER USER root IDENTIFIED BY 'hijack' REPLACE 'new-pw'", selfie))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, withClause.Code);
    }

    [Test]
    public async Task Superuser_ResetsAnotherPasswordWithoutTheOldOne_ButNeedsItForTheirOwn()
    {
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser();
        await RunDdl(executor, "", "CREATE USER forgetful IDENTIFIED BY 'lost-pw'", root);

        // Account recovery: the operator is resetting this password precisely because nobody knows it,
        // so demanding the old one would make recovery impossible. This path must stay open.
        await RunDdl(executor, "", "ALTER USER forgetful IDENTIFIED BY 'reset-pw'", root);
        await LoginAsync(executor, "forgetful", "reset-pw");

        // The superuser's own password is a self-change like any other. Exempting it would leave the
        // single most valuable account on the node as the one account a stolen token can take over.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, "", $"ALTER USER {Options.BootstrapSuperuser} IDENTIFIED BY 'rotated'", root))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    /// <summary>
    /// One source naming many accounts must not be able to refuse logins for everybody else.
    ///
    /// <para>The limiter used to key only on (account, source) and fail closed for every caller once
    /// that map filled. An account name is chosen by the request, so a caller varying it never reached
    /// the per-account ceiling and instead inserted one entry per attempt — turning a modest request
    /// rate into a node-wide login outage. The per-source ceiling is what stops it now, and the
    /// assertion that matters is the last one: a different source still gets in.</para>
    /// </summary>
    [Test]
    public async Task LoginFlood_WithVaryingAccountNames_DoesNotLockOutOtherSources()
    {
        // A tiny map, so the flood below reaches saturation in a handful of attempts rather than the
        // hundred thousand a production node allows. Two engines are not needed: one configuration is
        // under test, and the limiter lives on the engine this builds.
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser(
            Options with { LoginRateLimitMaxEntries = 4, LoginMaxAttemptsPerSourcePerMinute = 200 });

        await RunDdl(executor, "", "CREATE USER victim IDENTIFIED BY 'victim-pw-1234'", root);

        // Every attempt names a distinct account from one source: the shape that used to fill the map.
        for (int i = 0; i < 40; i++)
        {
            try
            {
                await executor.LoginAsync($"ghost{i}", "wrong", source: "10.0.0.9");
            }
            catch (CamusDBException)
            {
                // Expected: the account does not exist. The flood is the point, not the outcome.
            }
        }

        // The victim logs in from somewhere else and must be unaffected. Before the fix this threw
        // TooManyAuthAttempts with "Authentication is saturated".
        LoginResult ok = await executor.LoginAsync("victim", "victim-pw-1234", source: "10.0.0.1");
        Assert.IsNotEmpty(ok.Token);
    }

    /// <summary>
    /// The per-source ceiling has to actually stop a flood, not merely avoid blaming bystanders for
    /// it. A limiter that never refuses the attacker would trade one failure for another.
    /// </summary>
    [Test]
    public async Task LoginFlood_FromOneSource_IsRefusedAtThePerSourceCeiling()
    {
        (_, CommandExecutor executor, _) = await SetupWithSuperuser(
            Options with { LoginMaxAttemptsPerSourcePerMinute = 5 });

        CamusDBException? refused = null;

        for (int i = 0; i < 30 && refused is null; i++)
        {
            try
            {
                await executor.LoginAsync($"ghost{i}", "wrong", source: "10.0.0.9");
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.TooManyAuthAttempts)
            {
                refused = e;
            }
            catch (CamusDBException)
            {
                // An ordinary authentication failure; keep going until the ceiling answers.
            }
        }

        Assert.IsNotNull(refused, "varying the account name walked past the per-source ceiling");
    }

    /// <summary>
    /// Brute force against one account from one source must still be refused. The per-source ceiling
    /// is set far above the per-account one so that a shared proxy address does not lock out its
    /// users, which means the per-account ceiling is the only thing catching this.
    /// </summary>
    [Test]
    public async Task RepeatedWrongPasswordsForOneAccount_HitThePerAccountCeiling()
    {
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser(
            Options with { LoginMaxAttemptsPerMinute = 3, LoginMaxAttemptsPerSourcePerMinute = 10_000 });

        await RunDdl(executor, "", "CREATE USER target IDENTIFIED BY 'target-pw-1234'", root);

        CamusDBException? refused = null;

        for (int i = 0; i < 20 && refused is null; i++)
        {
            try
            {
                await executor.LoginAsync("target", "wrong", source: "10.0.0.7");
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.TooManyAuthAttempts)
            {
                refused = e;
            }
            catch (CamusDBException)
            {
                // Wrong password, as configured. Keep going.
            }
        }

        Assert.IsNotNull(refused, "the per-account ceiling no longer refuses a brute-force attempt");
    }

    /// <summary>
    /// Session records carry no storage TTL and are deleted only by an explicit logout or by dropping
    /// the owning user. With a short token lifetime and re-login as the only refresh, a client that
    /// reconnects on a timer leaves one dead key per reconnection, forever — and the drop-user path
    /// scans that whole family, so an unrelated statement pays for the growth.
    /// </summary>
    [Test]
    public async Task ExpiredSessions_AreReaped_AndLiveOnesAreNot()
    {
        // A short token lifetime, so the test can watch a session cross its expiry without waiting out
        // a realistic one. Built into the engine rather than set afterwards: an executor captures its
        // options at construction, so assigning the setting later would be a no-op that still passed.
        // Two seconds rather than milliseconds because the setup below must log in before it lapses.
        (_, CommandExecutor executor, Principal root) = await SetupWithSuperuser(
            Options with { AccessTokenTtl = TimeSpan.FromSeconds(2) });

        await RunDdl(executor, "", "CREATE USER sleeper IDENTIFIED BY 'sleeper-pw-12'", root);
        await RunDdl(executor, "", "CREATE USER awake IDENTIFIED BY 'awake-pw-1234'", root);

        LoginResult dying = await executor.LoginAsync("sleeper", "sleeper-pw-12");

        await Task.Delay(2500);

        // Past its expiry the token no longer authenticates, which is what makes deleting its record
        // safe. Asserted rather than assumed, because the sweep's whole justification rests on it.
        Assert.ThrowsAsync<CamusDBException>(async () => await executor.ResolvePrincipalAsync(dying.Token));

        // Logged in after the wait, so this session is live while the sweep runs.
        LoginResult live = await executor.LoginAsync("awake", "awake-pw-1234");

        int reaped = await executor.ReapExpiredSessionsAsync();
        Assert.GreaterOrEqual(reaped, 1, "the expired session was not removed");

        // The live session survives. Without this the test would pass equally well against a sweep
        // that deleted every session it found.
        Principal stillValid = await executor.ResolvePrincipalAsync(live.Token);
        Assert.AreEqual("awake", stillValid.UserName);

        // Idempotent, and it stops when there is nothing left: every node runs this concurrently and
        // none of them coordinates, so a second pass over the same records must be a quiet no-op.
        Assert.AreEqual(0, await executor.ReapExpiredSessionsAsync());
    }

    [Test]
    public async Task BootstrapFailsClosed_WhenNoSecret()
    {
        // Auth on but no bootstrap secret supplied: seeding must refuse rather than start a server
        // whose superuser has no password. This fixture's default supplies a secret, so an empty one is
        // passed here — the missing secret is the condition under test.
        CommandExecutor executor = CreateCommandExecutor(Options);
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, ""))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    public async Task BootstrapSucceeds_WhenInjectedOptionsCarryNoPassword()
    {
        // Reproduces the production wiring: Program.cs registers CamusDBOptions in DI with
        // BootstrapSuperuserPassword blanked, so the executor's injected options never carry the secret —
        // it arrives only as the argument. Seeding read the password off `options` once, which meant a
        // real server refused to start no matter what CAMUSDB_BOOTSTRAP_PASSWORD was set to, while every
        // test here passed because its executor was built with the secret still in the options.
        CommandExecutor executor = CreateCommandExecutor(Options with { BootstrapSuperuserPassword = "" });
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        await executor.EnsureBootstrapSuperuserAsync("root", "root-password");

        // Logging in proves the hash was seeded from the argument, and the superuser attribute proves it
        // went through the bootstrap path — the only one that grants it.
        Principal root = await LoginAsync(executor, "root", "root-password");
        Assert.IsTrue(root.IsSuperuser);
    }
}
