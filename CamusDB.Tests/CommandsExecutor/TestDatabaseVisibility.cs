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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A database the caller holds no grant on is reported as non-existent, by every statement and every
/// entry point that names it, with exactly the error a name that is not registered gets. Any
/// difference between the two answers makes the statement an existence oracle.
///
/// <para>Also covers the statements that read database-level data without opening a table, which the
/// per-table check never sees: <c>SHOW DATABASE</c>, <c>SHOW ORPHAN TABLES</c>,
/// <c>SHOW CREATE MATERIALIZED VIEW</c> and <c>SHOW ORPHAN DATABASES</c>.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestDatabaseVisibility : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "visibility-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task TxnDdl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs a row-returning statement in a real transaction and returns its rows.</summary>
    private static async Task<List<QueryResultRow>> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await d.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>Runs a server-level statement, with no database context and no transaction.</summary>
    private static async Task<List<QueryResultRow>> ServerQuery(CommandExecutor ex, string sql, Principal? p)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: null!, database: "", sql, null, p));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return rows;
    }

    /// <summary>
    /// The statement's refusal, through the entry point that statement uses. No transaction is begun:
    /// a missing database cannot begin one, and the hidden one must be refused before it is needed.
    /// </summary>
    private static CamusDBException Refusal(CommandExecutor ex, string entry, string db, string sql, Principal p)
    {
        ExecuteSQLTicket ticket = new(txnState: null!, database: db, sql: sql, parameters: null, principal: p);

        return entry switch
        {
            "query" => Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(ticket);
                await foreach (QueryResultRow _ in cursor) { }
            })!,
            "ddl" => Assert.ThrowsAsync<CamusDBException>(async () => await ex.ExecuteDDLSQL(ticket))!,
            "nonquery" => Assert.ThrowsAsync<CamusDBException>(async () => await ex.ExecuteNonSQLQuery(ticket))!,
            _ => throw new ArgumentOutOfRangeException(nameof(entry)),
        };
    }

    /// <summary>
    /// A row-returning statement's refusal inside a real transaction, for a database the caller can
    /// see: the statement passes the visibility gate and needs a transaction to reach its own check.
    /// </summary>
    private static async Task<CamusDBException> RefusalInTransaction(CommandExecutor ex, string db, string sql, Principal p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            return Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) =
                    await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
                await foreach (QueryResultRow _ in cursor) { }
            })!;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Asserts the refusal is the exact answer a name that is not registered gets.</summary>
    private static void AssertReportedMissing(CamusDBException ex, string requestedName, string what)
    {
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex.Code, $"{what}: {ex.Message}");
        Assert.AreEqual($"Database '{requestedName}' does not exist", ex.Message, what);
    }

    /// <summary>
    /// A database with two tables and a comment, the superuser, and one grant-less user
    /// <c>u</c>. Callers grant <c>u</c> what each test needs, then log in.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t1 (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE t2 (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await ServerDdl(ex, $"COMMENT ON DATABASE {db} IS 'acquisition target list'", root);
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);

        return (db, ex, root);
    }

    private static string MissingName() => "nosuchdb" + Guid.NewGuid().ToString("n");

    // ── SHOW DATABASE ─────────────────────────────────────────────────────────

    [Test]
    public async Task ShowDatabase_NoGrant_IsReportedExactlyAsAMissingDatabase()
    {
        (string db, CommandExecutor ex, _) = await Setup();
        Principal u = await Login(ex, "u", "pw");
        string missing = MissingName();

        AssertReportedMissing(Refusal(ex, "query", db, "SHOW DATABASE", u), db, "existing database");
        AssertReportedMissing(Refusal(ex, "query", missing, "SHOW DATABASE", u), missing, "missing database");
    }

    [Test]
    public async Task ShowDatabase_DatabaseSelectGrant_ReturnsTheComment()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        List<QueryResultRow> rows = await Query(ex, db, "SHOW DATABASE", u);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(db, rows[0].Row["database"].StrValue);
        Assert.AreEqual("acquisition target list", rows[0].Row["comment"].StrValue);
    }

    [Test]
    public async Task ShowDatabase_GlobalSelectGrant_ReturnsTheComment()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "GRANT SELECT ON *.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        List<QueryResultRow> rows = await Query(ex, db, "SHOW DATABASE", u);

        Assert.AreEqual("acquisition target list", rows.Single().Row["comment"].StrValue);
    }

    [Test]
    public async Task ShowDatabase_Superuser_ReturnsTheComment()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        List<QueryResultRow> rows = await Query(ex, db, "SHOW DATABASE", root);

        Assert.AreEqual("acquisition target list", rows.Single().Row["comment"].StrValue);
    }

    /// <summary>
    /// A grant that is not SELECT makes the database visible but does not let the caller read its
    /// metadata. CADB0517 here is not an oracle: the caller already knows the database exists.
    /// </summary>
    [Test]
    public async Task ShowDatabase_CreateTableGrantOnly_IsInsufficientPrivilege()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        CamusDBException refused = await RefusalInTransaction(ex, db, "SHOW DATABASE", u);

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
    }

    /// <summary>A table-scoped grant does not satisfy a database-scope check: it fails closed.</summary>
    [Test]
    public async Task ShowDatabase_TableOnlySelectGrant_IsInsufficientPrivilege()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        CamusDBException refused = await RefusalInTransaction(ex, db, "SHOW DATABASE", u);

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
    }

    // ── Every statement that names a hidden database ─────────────────────────

    /// <summary>
    /// The same rule for every statement, not only for SHOW DATABASE: each of these once answered
    /// differently for a registered name (rows, an empty listing, or CADB0517) than for a missing one
    /// (CADB0010), so each was an existence oracle on its own.
    /// </summary>
    [TestCase("query", "SHOW TABLES")]
    [TestCase("query", "SHOW VIEWS")]
    [TestCase("query", "SHOW SEQUENCES")]
    [TestCase("query", "SHOW ORPHAN TABLES")]
    [TestCase("query", "SELECT 1")]
    [TestCase("query", "SELECT nextval('s1')")]
    [TestCase("query", "SELECT * FROM t1")]
    [TestCase("query", "SHOW CREATE TABLE t1")]
    [TestCase("query", "EXPLAIN SELECT * FROM t1")]
    [TestCase("ddl", "CREATE TABLE t9 (id int64 PRIMARY KEY NOT NULL)")]
    [TestCase("ddl", "CREATE SEQUENCE s9")]
    [TestCase("ddl", "DROP TABLE t1")]
    [TestCase("nonquery", "INSERT INTO t1 (id, v) VALUES (1, 1)")]
    [TestCase("nonquery", "DELETE FROM t1 WHERE id = 1")]
    public async Task EveryStatement_NoGrant_IsReportedExactlyAsAMissingDatabase(string entry, string sql)
    {
        (string db, CommandExecutor ex, _) = await Setup();
        Principal u = await Login(ex, "u", "pw");
        string missing = MissingName();

        AssertReportedMissing(Refusal(ex, entry, db, sql, u), db, "existing database");
        AssertReportedMissing(Refusal(ex, entry, missing, sql, u), missing, "missing database");
    }

    /// <summary>
    /// Database names resolve case-insensitively. The refusal must echo the name as the caller sent
    /// it: a message carrying the registered spelling would differ from the one a missing name gets.
    /// </summary>
    [Test]
    public async Task HiddenDatabase_RefusalEchoesTheNameAsSent()
    {
        (string db, CommandExecutor ex, _) = await Setup();
        Principal u = await Login(ex, "u", "pw");
        string shouted = db.ToUpperInvariant();

        AssertReportedMissing(Refusal(ex, "query", shouted, "SHOW DATABASE", u), shouted, "existing database, other case");
    }

    /// <summary>
    /// A grant on one table makes the database visible, so a statement that needs more than that grant
    /// gives is refused with CADB0517, as before.
    /// </summary>
    [Test]
    public async Task VisibleDatabase_MissingTablePrivilege_IsStillInsufficientPrivilege()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t2 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        CamusDBException refused = await RefusalInTransaction(ex, db, "SELECT * FROM t1", u);

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
        Assert.AreEqual(0, (await Query(ex, db, "SELECT * FROM t2", u)).Count);
    }

    /// <summary>
    /// Server-level statements ignore the context database, so a client whose default database is
    /// hidden must still be able to run them.
    /// </summary>
    [Test]
    public async Task ServerLevelStatement_HiddenContextDatabase_IsNotRefused()
    {
        (string db, CommandExecutor ex, _) = await Setup();
        Principal u = await Login(ex, "u", "pw");

        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: null!, database: db, "SHOW DATABASES", null, u));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        Assert.IsEmpty(rows);
    }

    // ── Entry points that do not go through SQL ──────────────────────────────

    /// <summary>
    /// The row and DDL ticket APIs publish the caller through the ambient scope and open the
    /// database themselves, with no SQL gate in front. The opener applies the same rule.
    /// </summary>
    [Test]
    public async Task TicketApi_NoGrant_IsReportedExactlyAsAMissingDatabase()
    {
        (string db, CommandExecutor ex, _) = await Setup();
        Principal u = await Login(ex, "u", "pw");
        string missing = MissingName();

        AuthorizationContext.Current = new AuthorizationScope(u, Privilege.Select);
        try
        {
            CamusDBException hidden = Assert.ThrowsAsync<CamusDBException>(
                async () => await ex.OpenTable(new OpenTableTicket(db, "t1")))!;
            CamusDBException absent = Assert.ThrowsAsync<CamusDBException>(
                async () => await ex.OpenTable(new OpenTableTicket(missing, "t1")))!;

            AssertReportedMissing(hidden, db, "existing database");
            AssertReportedMissing(absent, missing, "missing database");
        }
        finally
        {
            AuthorizationContext.Current = default;
        }
    }

    [Test]
    public async Task TicketApi_VisibleDatabase_KeepsThePerTableAnswer()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t2 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        AuthorizationContext.Current = new AuthorizationScope(u, Privilege.Select);
        try
        {
            Assert.IsNotNull(await ex.OpenTable(new OpenTableTicket(db, "t2")));

            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
                async () => await ex.OpenTable(new OpenTableTicket(db, "t1")))!;
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
        }
        finally
        {
            AuthorizationContext.Current = default;
        }
    }

    // ── Other statements that open no table ──────────────────────────────────

    [Test]
    public async Task ShowOrphanTables_RequiresDatabaseSelect()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        await ServerDdl(ex, "CREATE USER w IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO w", root);
        await TxnDdl(ex, db, "DROP TABLE t2", root);
        Principal u = await Login(ex, "u", "pw");
        Principal w = await Login(ex, "w", "pw");

        CamusDBException refused = await RefusalInTransaction(ex, db, "SHOW ORPHAN TABLES", u);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);

        List<QueryResultRow> rows = await Query(ex, db, "SHOW ORPHAN TABLES", w);
        Assert.IsTrue(rows.Any(r => r.Row["former_name"].StrValue == "t2"));
    }

    [Test]
    public async Task ShowCreateMaterializedView_RequiresSelectOnTheView()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t2 TO u", root);
        await ServerDdl(ex, "CREATE USER w IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.mv TO w", root);
        Principal u = await Login(ex, "u", "pw");
        Principal w = await Login(ex, "w", "pw");

        CamusDBException refused = await RefusalInTransaction(ex, db, "SHOW CREATE MATERIALIZED VIEW mv", u);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);

        Assert.AreEqual(1, (await Query(ex, db, "SHOW CREATE MATERIALIZED VIEW mv", w)).Count);
    }

    [Test]
    public async Task ShowOrphanDatabases_ListsOnlyDatabasesTheCallerCanSee()
    {
        (string granted, CommandExecutor ex, Principal root) = await Setup();
        string hidden = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: hidden, ifNotExists: false));
        TrackDatabase(hidden, ex);

        string grantedId = (await ex.OpenDatabase(granted)).Id;
        string hiddenId = (await ex.OpenDatabase(hidden)).Id;

        await ServerDdl(ex, $"GRANT SELECT ON {granted}.* TO u", root);
        await ex.DropDatabase(new DropDatabaseTicket(granted));
        await ex.DropDatabase(new DropDatabaseTicket(hidden));
        Principal u = await Login(ex, "u", "pw");

        List<string> seenByUser = (await ServerQuery(ex, "SHOW ORPHAN DATABASES", u))
            .Select(r => r.Row["id"].StrValue!).ToList();
        List<string> seenByRoot = (await ServerQuery(ex, "SHOW ORPHAN DATABASES", root))
            .Select(r => r.Row["id"].StrValue!).ToList();

        CollectionAssert.Contains(seenByUser, grantedId);
        CollectionAssert.DoesNotContain(seenByUser, hiddenId);
        CollectionAssert.IsSubsetOf(new[] { grantedId, hiddenId }, seenByRoot);
    }
}
