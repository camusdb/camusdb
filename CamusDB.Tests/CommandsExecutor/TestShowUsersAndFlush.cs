/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW USERS</c>, <c>SHOW GRANTS FOR *</c>, <c>FLUSH PRIVILEGES</c> and <c>FLUSH SESSIONS</c>,
/// driven through the real parser and <see cref="CommandExecutor"/>.
///
/// <para>The flush cases build their engines with a deliberately long authorization cache lifetime, so
/// what they prove is the flush and not an expiry that would have happened anyway. A setting is fixed
/// when the engine is constructed, which is why each arm builds its own.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowUsersAndFlush : BaseTest
{
    /// <summary>
    /// Authentication on, with a known token key and bootstrap superuser, and an authorization cache
    /// that will not expire during a test. The long lifetime is the point: a flush test whose engine
    /// forgot its cached principal after a second would pass without the flush working at all.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-server-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
        AuthenticationCacheTtl = TimeSpan.FromMinutes(30),
    };

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static Task ServerDdlAsync(CommandExecutor executor, string sql, Principal? principal = null)
        => executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: sql, parameters: null, principal: principal));

    private static async Task<List<QueryResultRow>> ServerQueryAsync(
        CommandExecutor executor, string sql, Principal? principal = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: sql, parameters: null, principal: principal));

        return await cursor.ToListAsync();
    }

    /// <summary>
    /// Resolves the principal from the token on every call, which is what a transport does per request.
    /// A test that resolved once and reused the result would be carrying its own stale snapshot and
    /// could not observe a privilege change at all.
    /// </summary>
    private static Task<Principal> AsAsync(CommandExecutor executor, string token)
        => executor.ResolvePrincipalAsync(token);

    private async Task<(CommandExecutor executor, Principal root)> AuthenticatedEngineAsync(CamusDBOptions? options = null)
    {
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        await executor.EnsureBootstrapSuperuserAsync("root", "root-password");
        Principal root = await AsAsync(executor, (await executor.LoginAsync("root", "root-password")).Token);
        return (executor, root);
    }

    private async Task<string> CreateAuthDatabaseAsync(CommandExecutor executor)
    {
        // A letter-prefixed name, because the name is written into GRANT text and a raw GUID starting
        // with a digit lexes as a number.
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);
        return dbname;
    }

    private static string CellOf(QueryResultRow row, string column) => row.Row[column].StrValue!;

    // ─── Grammar ──────────────────────────────────────────────────────────────

    [Test]
    public void TheFourStatementsParseToTheirOwnNodeTypes()
    {
        Assert.AreEqual(NodeType.ShowUsers, SQLParserProcessor.Parse("SHOW USERS").nodeType);
        Assert.AreEqual(NodeType.ShowUsers, SQLParserProcessor.Parse("show users like 'app_%'").nodeType);
        Assert.AreEqual(NodeType.ShowAllGrants, SQLParserProcessor.Parse("SHOW GRANTS FOR *").nodeType);
        Assert.AreEqual(NodeType.FlushPrivileges, SQLParserProcessor.Parse("FLUSH PRIVILEGES").nodeType);
        Assert.AreEqual(NodeType.FlushSessions, SQLParserProcessor.Parse("flush sessions").nodeType);

        // The single-account forms keep their own node type.
        Assert.AreEqual(NodeType.ShowGrants, SQLParserProcessor.Parse("SHOW GRANTS").nodeType);
        Assert.AreEqual(NodeType.ShowGrants, SQLParserProcessor.Parse("SHOW GRANTS FOR app").nodeType);
    }

    /// <summary>
    /// These statements reserve no new word, so <c>users</c>, <c>flush</c> and <c>sessions</c> each
    /// still name a table, a column and an alias. Reserving <c>users</c> in particular would break most
    /// schemas that exist, which is why it is matched as a plain identifier and checked in the parse
    /// action instead.
    /// </summary>
    [Test]
    public void TheNewWordsAreStillUsableAsIdentifiers()
    {
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT id FROM users"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT users FROM t"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT t.users AS users FROM t AS users"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT flush, sessions FROM flush"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("CREATE TABLE users (id int64 PRIMARY KEY NOT NULL)"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("INSERT INTO sessions (flush) VALUES (1)"));
    }

    /// <summary>
    /// <c>PRIVILEGES</c> is the one word in <c>FLUSH PRIVILEGES</c> that is not a plain identifier, and
    /// it was a keyword long before this statement existed — <c>GRANT ALL PRIVILEGES</c> needs it, and
    /// its lexer rule is untouched by these statements. Pinned here so the boundary is recorded rather
    /// than rediscovered: writing <c>FLUSH PRIVILEGES</c> reserved nothing new, and a schema that wants
    /// a <c>privileges</c> column is no worse off than it already was.
    /// </summary>
    [Test]
    public void PrivilegesWasAlreadyReservedBeforeTheseStatements()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT privileges FROM t"));
    }

    [Test]
    public void AMisspelledFlushNamesBothAcceptedStatements()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("FLUSH GARBAGE"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("FLUSH PRIVILEGES", ex.Message);
        StringAssert.Contains("FLUSH SESSIONS", ex.Message);
    }

    [Test]
    public void AMisspelledOneWordShowNamesBothAcceptedStatements()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW GARBAGE"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("SHOW VARIABLES", ex.Message);
        StringAssert.Contains("SHOW USERS", ex.Message);
    }

    // ─── SHOW USERS ───────────────────────────────────────────────────────────

    /// <summary>
    /// A server holding only its bootstrap superuser reports exactly that, with the column schema and
    /// the values a dump tool reads.
    /// </summary>
    [Test]
    public async Task ShowUsersOnAFreshServerReportsTheBootstrapSuperuserAlone()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        List<QueryResultRow> rows = await ServerQueryAsync(executor, "SHOW USERS", root);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("root", CellOf(rows[0], "user"));
        Assert.IsTrue(rows[0].Row["superuser"].BoolValue, "the bootstrap account is a superuser");
        Assert.IsTrue(rows[0].Row["has_password"].BoolValue, "the bootstrap account can log in");
        Assert.AreEqual(0, rows[0].Row["grants"].LongValue, "a superuser needs no grants");
        Assert.IsNotEmpty(CellOf(rows[0], "created_at"));
    }

    /// <summary>
    /// The emitted schema is the contract clients decode against, so it is asserted by name and in
    /// order. Asserting the exact set also means a later column carrying credential material cannot be
    /// added without this failing, which is the reason the assertion is written this way.
    /// </summary>
    [Test]
    public async Task ShowUsersDeclaresItsColumnsAndCarriesNoCredentialMaterial()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        QuerySchemaHolder schema = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "SHOW USERS", parameters: null, principal: root),
            schemaOut: schema);

        await cursor.ToListAsync();

        Assert.AreEqual(
            new[] { "user", "id", "superuser", "has_password", "grants", "created_at" },
            schema.Schema.Select(c => c.Name).ToArray());

        foreach (string forbidden in new[] { "hash", "salt", "credential", "password", "iterations", "algorithm", "epoch" })
        {
            Assert.IsFalse(
                schema.Schema.Any(c => c.Name.Contains(forbidden, StringComparison.OrdinalIgnoreCase)
                                        && !string.Equals(c.Name, "has_password", StringComparison.Ordinal)),
                $"no SHOW USERS column may carry '{forbidden}'");
        }
    }

    /// <summary>An account with no password cannot authenticate, and the listing says so.</summary>
    [Test]
    public async Task AnAccountWithNoPasswordReportsSoAndHasNoGrants()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        await ServerDdlAsync(executor, "CREATE USER nopass", root);

        QueryResultRow row = (await ServerQueryAsync(executor, "SHOW USERS", root))
            .Single(r => CellOf(r, "user") == "nopass");

        Assert.IsFalse(row.Row["has_password"].BoolValue, "an account with no verifier cannot log in");
        Assert.IsFalse(row.Row["superuser"].BoolValue);
        Assert.AreEqual(0, row.Row["grants"].LongValue);
    }

    [Test]
    public async Task ShowUsersIsOrderedByNameAndFiltersOnLike()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        foreach (string account in new[] { "app_zulu", "app_alpha", "other" })
            await ServerDdlAsync(executor, $"CREATE USER {account} IDENTIFIED BY 'Pw123456789012'", root);

        List<string> all = (await ServerQueryAsync(executor, "SHOW USERS", root))
            .Select(r => CellOf(r, "user")).ToList();

        Assert.AreEqual(all.OrderBy(n => n, StringComparer.Ordinal).ToList(), all, "rows come out name-ordered");

        List<string> filtered = (await ServerQueryAsync(executor, "SHOW USERS LIKE 'app_%'", root))
            .Select(r => CellOf(r, "user")).ToList();

        Assert.AreEqual(new[] { "app_alpha", "app_zulu" }, filtered.ToArray());
    }

    [Test]
    public async Task ShowUsersCountsAnAccountsStoredGrants()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();
        string first = await CreateAuthDatabaseAsync(executor);
        string second = await CreateAuthDatabaseAsync(executor);

        await ServerDdlAsync(executor, "CREATE USER counted IDENTIFIED BY 'Pw123456789012'", root);
        await ServerDdlAsync(executor, $"GRANT SELECT ON {first}.* TO counted", root);
        await ServerDdlAsync(executor, $"GRANT SELECT, INSERT ON {second}.* TO counted", root);

        QueryResultRow row = (await ServerQueryAsync(executor, "SHOW USERS", root))
            .Single(r => CellOf(r, "user") == "counted");

        Assert.AreEqual(2, row.Row["grants"].LongValue, "one record per granted scope, not per privilege");
    }

    // ─── SHOW GRANTS FOR * ────────────────────────────────────────────────────

    /// <summary>
    /// The all-account form is exactly the union of the per-account form, in account order, with the
    /// same three columns — so a client that reads one reads the other unchanged.
    /// </summary>
    [Test]
    public async Task ShowAllGrantsIsTheUnionOfThePerAccountForm()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();
        string dbname = await CreateAuthDatabaseAsync(executor);

        foreach (string account in new[] { "zulu", "alpha" })
        {
            await ServerDdlAsync(executor, $"CREATE USER {account} IDENTIFIED BY 'Pw123456789012'", root);
            await ServerDdlAsync(executor, $"GRANT SELECT ON {dbname}.* TO {account}", root);
        }

        // An account with no grants contributes nothing, which SHOW USERS is where to see instead.
        await ServerDdlAsync(executor, "CREATE USER ungranted IDENTIFIED BY 'Pw123456789012'", root);

        List<QueryResultRow> all = await ServerQueryAsync(executor, "SHOW GRANTS FOR *", root);

        Assert.AreEqual(new[] { "alpha", "zulu" }, all.Select(r => CellOf(r, "user")).ToArray());
        Assert.IsFalse(all.Any(r => CellOf(r, "user") == "ungranted"), "an account with no grants yields no row");

        foreach (string account in new[] { "alpha", "zulu" })
        {
            List<QueryResultRow> one = await ServerQueryAsync(executor, $"SHOW GRANTS FOR {account}", root);

            Assert.AreEqual(
                one.Select(r => (CellOf(r, "object"), CellOf(r, "privileges"))).ToArray(),
                all.Where(r => CellOf(r, "user") == account)
                   .Select(r => (CellOf(r, "object"), CellOf(r, "privileges"))).ToArray());
        }
    }

    /// <summary>The single-account statement is untouched, error included.</summary>
    [Test]
    public async Task ShowGrantsForAnUnknownAccountStillReportsUserDoesNotExist()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ServerQueryAsync(executor, "SHOW GRANTS FOR nosuchaccount", root))!;

        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ex.Code);
    }

    // ─── Authorization ────────────────────────────────────────────────────────

    /// <summary>
    /// All four are superuser-only, and the refusal must not vary with the catalog's contents — a gate
    /// that answered differently once an account existed would enumerate the catalog as surely as the
    /// listing it refuses.
    /// </summary>
    [Test]
    public async Task AllFourAreRefusedToANonSuperuserWithNoExistenceOracle()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        await ServerDdlAsync(executor, "CREATE USER plain IDENTIFIED BY 'Pw123456789012'", root);
        await ServerDdlAsync(executor, "CREATE USER known IDENTIFIED BY 'Pw123456789012'", root);

        string token = (await executor.LoginAsync("plain", "Pw123456789012")).Token;

        foreach (string sql in new[] { "SHOW USERS", "SHOW USERS LIKE '%'", "SHOW GRANTS FOR *" })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await ServerQueryAsync(executor, sql, await AsAsync(executor, token)))!;

            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code, sql);
        }

        foreach (string sql in new[] { "FLUSH PRIVILEGES", "FLUSH SESSIONS" })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await ServerDdlAsync(executor, sql, await AsAsync(executor, token)))!;

            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code, sql);
        }

        // Naming a real account and naming an invented one must fail identically.
        CamusDBException real = Assert.ThrowsAsync<CamusDBException>(
            async () => await ServerQueryAsync(executor, "SHOW GRANTS FOR known", await AsAsync(executor, token)))!;

        CamusDBException invented = Assert.ThrowsAsync<CamusDBException>(
            async () => await ServerQueryAsync(executor, "SHOW GRANTS FOR nosuchaccount", await AsAsync(executor, token)))!;

        Assert.AreEqual(real.Code, invented.Code);
        Assert.AreEqual(real.Message, invented.Message);
    }

    // ─── A privilege change taking effect ─────────────────────────────────────

    /// <summary>
    /// The reported complaint, at its root: a <c>GRANT</c> made on this node must reach a client that
    /// is already holding a token, on that client's very next request, with no wait and no re-login.
    ///
    /// <para>The authorization cache lifetime is thirty minutes here, so nothing expires during the
    /// test. What makes the change visible is the catalog's generation moving and the account's own
    /// authorization epoch moving with it.</para>
    /// </summary>
    [Test]
    public async Task AGrantOnThisNodeAppliesToTheVeryNextRequest()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();
        string dbname = await CreateAuthDatabaseAsync(executor);

        await ServerDdlAsync(executor, "CREATE USER app IDENTIFIED BY 'Pw123456789012'", root);
        string token = (await executor.LoginAsync("app", "Pw123456789012")).Token;

        Assert.IsFalse((await AsAsync(executor, token)).CanSeeDatabase(
            (await executor.OpenDatabase(dbname)).Id), "no grant yet");

        await ServerDdlAsync(executor, $"GRANT SELECT ON {dbname}.* TO app", root);

        Assert.IsTrue((await AsAsync(executor, token)).CanSeeDatabase(
            (await executor.OpenDatabase(dbname)).Id), "the grant must apply without waiting for a cache to expire");
    }

    /// <summary>
    /// The flush does what no ordinary path can: it makes this node re-read the catalog even when
    /// nothing it tracks locally says anything changed.
    ///
    /// <para>Two engines over one store stand in for that state. The second engine writes the grant, so
    /// the first engine's caches never hear about it — the standalone path trusts them without
    /// revalidating. The first engine therefore keeps refusing until the flush, which is precisely the
    /// situation an operator reaches for this statement in.</para>
    /// </summary>
    [Test]
    public async Task FlushPrivilegesAdmitsAClientWhoseGrantThisNodeHadNotSeen()
    {
        (CommandExecutor stale, Principal root) = await AuthenticatedEngineAsync();
        string dbname = await CreateAuthDatabaseAsync(stale);

        await ServerDdlAsync(stale, "CREATE USER app IDENTIFIED BY 'Pw123456789012'", root);
        string token = (await stale.LoginAsync("app", "Pw123456789012")).Token;
        string databaseId = (await stale.OpenDatabase(dbname)).Id;

        Assert.IsFalse((await AsAsync(stale, token)).CanSeeDatabase(databaseId), "no grant yet");

        (CommandExecutor other, Principal otherRoot) = await AuthenticatedEngineAsync();
        await ServerDdlAsync(other, $"GRANT SELECT ON {dbname}.* TO app", otherRoot);

        Assert.IsFalse((await AsAsync(stale, token)).CanSeeDatabase(databaseId),
            "this engine has not observed the change, which is what the flush is for");

        await ServerDdlAsync(stale, "FLUSH PRIVILEGES", root);

        Assert.IsTrue((await AsAsync(stale, token)).CanSeeDatabase(databaseId),
            "after the flush the grant applies on the next request");
    }

    /// <summary>The same, in the direction that matters more: a revoke must land.</summary>
    [Test]
    public async Task FlushPrivilegesRefusesAClientWhoseGrantWasRevokedElsewhere()
    {
        (CommandExecutor stale, Principal root) = await AuthenticatedEngineAsync();
        string dbname = await CreateAuthDatabaseAsync(stale);

        await ServerDdlAsync(stale, "CREATE USER app IDENTIFIED BY 'Pw123456789012'", root);
        await ServerDdlAsync(stale, $"GRANT SELECT ON {dbname}.* TO app", root);

        string token = (await stale.LoginAsync("app", "Pw123456789012")).Token;
        string databaseId = (await stale.OpenDatabase(dbname)).Id;

        Assert.IsTrue((await AsAsync(stale, token)).CanSeeDatabase(databaseId));

        (CommandExecutor other, Principal otherRoot) = await AuthenticatedEngineAsync();
        await ServerDdlAsync(other, $"REVOKE SELECT ON {dbname}.* FROM app", otherRoot);

        Assert.IsTrue((await AsAsync(stale, token)).CanSeeDatabase(databaseId),
            "still stale, as the previous test establishes");

        await ServerDdlAsync(stale, "FLUSH PRIVILEGES", root);

        Assert.IsFalse((await AsAsync(stale, token)).CanSeeDatabase(databaseId),
            "after the flush the revoke applies on the next request");
    }

    /// <summary>
    /// The flush must not log anyone out. An operator running it wants one change believed, not a
    /// fleet of clients sent back to the login endpoint.
    /// </summary>
    [Test]
    public async Task FlushPrivilegesLeavesSessionsAlone()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();
        string dbname = await CreateAuthDatabaseAsync(executor);

        await ServerDdlAsync(executor, "CREATE USER steady IDENTIFIED BY 'Pw123456789012'", root);
        await ServerDdlAsync(executor, $"GRANT SELECT ON {dbname}.* TO steady", root);

        string token = (await executor.LoginAsync("steady", "Pw123456789012")).Token;
        string databaseId = (await executor.OpenDatabase(dbname)).Id;

        Assert.IsTrue((await AsAsync(executor, token)).CanSeeDatabase(databaseId));

        await ServerDdlAsync(executor, "FLUSH PRIVILEGES", root);

        Principal after = await AsAsync(executor, token);
        Assert.AreEqual("steady", after.UserName, "the same token still authenticates the same account");
        Assert.IsTrue(after.CanSeeDatabase(databaseId), "an unchanged grant keeps working across a flush");
    }

    /// <summary>
    /// <c>FLUSH SESSIONS</c> ends every session, and only that: a fresh login works at once, so the
    /// statement recovers from staleness without locking the fleet out.
    /// </summary>
    [Test]
    public async Task FlushSessionsEndsEveryTokenAndLoginWorksImmediatelyAfter()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        await ServerDdlAsync(executor, "CREATE USER one IDENTIFIED BY 'Pw123456789012'", root);
        await ServerDdlAsync(executor, "CREATE USER two IDENTIFIED BY 'Pw123456789012'", root);

        string first = (await executor.LoginAsync("one", "Pw123456789012")).Token;
        string second = (await executor.LoginAsync("two", "Pw123456789012")).Token;

        Assert.AreEqual("one", (await AsAsync(executor, first)).UserName);
        Assert.AreEqual("two", (await AsAsync(executor, second)).UserName);

        await ServerDdlAsync(executor, "FLUSH SESSIONS", root);

        foreach (string dead in new[] { first, second })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await AsAsync(executor, dead))!;

            Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
        }

        string renewed = (await executor.LoginAsync("one", "Pw123456789012")).Token;
        Assert.AreEqual("one", (await AsAsync(executor, renewed)).UserName, "a fresh login works at once");
    }

    /// <summary>
    /// A password rotation invalidates the account's tokens, and the cached authorization decision must
    /// not keep one alive. The account's credential epoch is what the cache compares, and this is the
    /// test that it is compared at all.
    /// </summary>
    [Test]
    public async Task ARotatedPasswordInvalidatesACachedTokenAtOnce()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        await ServerDdlAsync(executor, "CREATE USER rotating IDENTIFIED BY 'Pw123456789012'", root);
        string token = (await executor.LoginAsync("rotating", "Pw123456789012")).Token;

        Assert.AreEqual("rotating", (await AsAsync(executor, token)).UserName);

        await ServerDdlAsync(executor, "ALTER USER rotating IDENTIFIED BY 'Pw210987654321'", root);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await AsAsync(executor, token))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    // ─── Routing ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The two reads open no database, and the two flushes are routed as server-level mutations. The
    /// transports skip the transaction on the strength of exactly these two lists, so a statement
    /// missing from one of them fails only at runtime.
    /// </summary>
    [Test]
    public async Task TheFourStatementsAreRoutedWithoutADatabase()
    {
        (CommandExecutor executor, Principal root) = await AuthenticatedEngineAsync();

        foreach (NodeType read in new[] { NodeType.ShowUsers, NodeType.ShowAllGrants })
        {
            Assert.IsTrue(StatementScope.IsServerLevelQuery(read), $"{read} must be routed as server-level");
            Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(read), $"{read} must not need a context database");
        }

        foreach (NodeType flush in new[] { NodeType.FlushPrivileges, NodeType.FlushSessions })
        {
            Assert.IsTrue(StatementScope.IsDatabaseScopedMutation(flush), $"{flush} must be routed as server-level");
            Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(flush), $"{flush} must not need a context database");
        }

        foreach (string sql in new[] { "SHOW USERS", "SHOW GRANTS FOR *" })
        {
            (DatabaseDescriptor? descriptor, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: null!, database: "", sql: sql, parameters: null, principal: root));

            Assert.IsNull(descriptor, $"{sql} opened a database the transports assume it does not");
            await foreach (QueryResultRow _ in cursor) { }
        }
    }
}
