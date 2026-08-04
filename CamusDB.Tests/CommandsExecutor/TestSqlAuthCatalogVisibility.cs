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
/// Catalog-listing visibility: <c>SHOW TABLES</c> and <c>SHOW DATABASES</c> must list only the objects
/// the calling principal holds a grant on, so a name is never disclosed to a caller who could not use
/// it. Filtering (empty row set) is the expected outcome rather than an InsufficientPrivilege error —
/// erroring would itself confirm the object exists.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestSqlAuthCatalogVisibility : BaseTest
{

    /// <summary>
    /// Auth on, with a known signing key and bootstrap superuser — the baseline every test here starts
    /// from. A test needing different auth settings derives its own options and builds its own engine.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };



    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task TxnDdl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs a listing statement and returns the single string column named <paramref name="column"/>.</summary>
    private static async Task<List<string>> ListAsync(
        CommandExecutor ex, string db, string sql, string column, Principal? p, bool inTransaction)
    {
        KvTransaction? tx = null;
        DatabaseDescriptor? d = null;

        if (inTransaction)
        {
            d = await ex.OpenDatabase(db);
            tx = await d.Transactions.BeginAsync();
        }

        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx!, db, sql, null, p));

        List<string> values = [];
        await foreach (QueryResultRow row in cursor)
            values.Add(row.Row[column].StrValue!);

        if (tx is not null)
            await d!.Transactions.CommitAsync(tx);

        return values;
    }

    private static Task<List<string>> ShowTables(CommandExecutor ex, string db, Principal? p, string sql = "SHOW TABLES")
        => ListAsync(ex, db, sql, "tables", p, inTransaction: true);

    // SHOW DATABASES needs no database context or transaction.
    private static Task<List<string>> ShowDatabases(CommandExecutor ex, string contextDb, Principal? p)
        => ListAsync(ex, contextDb, "SHOW DATABASES", "Database", p, inTransaction: false);

    // Enables auth, creates a database with tables t1/t2/t3 as the superuser.
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
        await TxnDdl(ex, db, "CREATE TABLE t3 (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        return (db, ex, root);
    }

    [Test]
    public async Task ShowTables_ListsOnlyGrantedTables()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.t3 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        List<string> tables = await ShowTables(ex, db, u);

        // t3 is visible on INSERT alone: visibility asks for *any* privilege, not SELECT.
        CollectionAssert.AreEquivalent(new[] { "t1", "t3" }, tables);
    }

    [Test]
    public async Task ShowTables_NoGrants_ListsNothing()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        Principal u = await Login(ex, "u", "pw");

        // A grant-less user must not learn that t1/t2/t3 exist — and must not get an error either.
        Assert.IsEmpty(await ShowTables(ex, db, u));
    }

    [Test]
    public async Task ShowTables_DatabaseGrant_ListsEveryTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        CollectionAssert.AreEquivalent(new[] { "t1", "t2", "t3" }, await ShowTables(ex, db, u));
    }

    [Test]
    public async Task ShowTables_Superuser_ListsEveryTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        CollectionAssert.AreEquivalent(new[] { "t1", "t2", "t3" }, await ShowTables(ex, db, root));
    }

    [Test]
    public async Task ShowTables_LikePatternAndVisibilityBothApply()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        // The pattern matches t1/t2/t3; only the granted one survives the visibility filter.
        CollectionAssert.AreEquivalent(new[] { "t1" }, await ShowTables(ex, db, u, "SHOW TABLES LIKE 't%'"));
        Assert.IsEmpty(await ShowTables(ex, db, u, "SHOW TABLES LIKE 't2'"));
    }

    [Test]
    public async Task ShowTables_RevokedTableDisappears()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");
        CollectionAssert.AreEquivalent(new[] { "t1" }, await ShowTables(ex, db, u));

        await ServerDdl(ex, $"REVOKE SELECT ON {db}.t1 FROM u", root);

        // The principal is an immutable per-request snapshot, so a re-login is what picks the
        // revocation up — the same thing a new session would do.
        Principal after = await Login(ex, "u", "pw");
        Assert.IsEmpty(await ShowTables(ex, db, after));
    }

    [Test]
    public async Task ShowDatabases_ListsOnlyDatabasesReachableByAGrant()
    {
        (string dbA, CommandExecutor ex, Principal root) = await Setup();

        string dbB = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: dbB, ifNotExists: false));
        TrackDatabase(dbB, ex);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {dbA}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        List<string> visible = await ShowDatabases(ex, dbA, u);

        // A single table grant inside dbA makes dbA discoverable; dbB stays hidden.
        Assert.IsTrue(visible.Contains(dbA), "the database holding the granted table must be listed");
        Assert.IsFalse(visible.Contains(dbB), "a database with no grant must not be listed");
    }

    [Test]
    public async Task ShowDatabases_NoGrants_ListsNothing()
    {
        (string dbA, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        Principal u = await Login(ex, "u", "pw");

        Assert.IsEmpty(await ShowDatabases(ex, dbA, u));
    }

    [Test]
    public async Task ShowDatabases_Superuser_SeesAll()
    {
        (string dbA, CommandExecutor ex, Principal root) = await Setup();

        string dbB = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: dbB, ifNotExists: false));
        TrackDatabase(dbB, ex);

        List<string> visible = await ShowDatabases(ex, dbA, root);
        Assert.IsTrue(visible.Contains(dbA));
        Assert.IsTrue(visible.Contains(dbB));
    }

    [Test]
    public async Task ShowDatabases_GlobalGrant_SeesAll()
    {
        (string dbA, CommandExecutor ex, Principal root) = await Setup();

        string dbB = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: dbB, ifNotExists: false));
        TrackDatabase(dbB, ex);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, "GRANT SELECT ON *.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        List<string> visible = await ShowDatabases(ex, dbA, u);
        Assert.IsTrue(visible.Contains(dbA));
        Assert.IsTrue(visible.Contains(dbB));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SHOW BRANCHES / SHOW ANCESTORS
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Branches <paramref name="root"/> into a root → child → grandchild chain and returns all three
    /// names. Branches are created through the ticket API as the test harness (not through SQL), so the
    /// chain exists regardless of which grants the test then hands out.
    /// </summary>
    private async Task<(string root, string child, string grandchild)> NewBranchChain(
        CommandExecutor ex, string root)
    {
        string child = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(child, ifNotExists: false, branchFrom: root));
        TrackDatabase(child, ex);

        string grandchild = "visdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(grandchild, ifNotExists: false, branchFrom: child));
        TrackDatabase(grandchild, ex);

        return (root, child, grandchild);
    }

    private static void AssertReportedMissing(Func<Task> act, string db)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, e.Code,
            $"a database the caller cannot see must be reported as non-existent, not as a privilege error ({db})");
    }

    [Test]
    public async Task ShowBranches_InvisibleTarget_ReportedAsNonExistent()
    {
        (string root, CommandExecutor ex, Principal rootUser) = await Setup();
        (_, string child, _) = await NewBranchChain(ex, root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {child}.* TO u", rootUser);
        Principal u = await Login(ex, "u", "pw");

        // The caller can see `child` but not `root`: naming root must not confirm it exists.
        AssertReportedMissing(() => ListAsync(ex, "", $"SHOW BRANCHES FROM {root}", "database", u, false), root);
        AssertReportedMissing(() => ListAsync(ex, "", $"SHOW ANCESTORS FROM {root}", "database", u, false), root);
    }

    [Test]
    public async Task ShowBranches_ListsOnlyVisibleDescendants()
    {
        (string root, CommandExecutor ex, Principal rootUser) = await Setup();
        (_, string child, string grandchild) = await NewBranchChain(ex, root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {root}.* TO u", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {grandchild}.* TO u", rootUser);
        Principal u = await Login(ex, "u", "pw");

        List<string> branches = await ListAsync(ex, "", $"SHOW BRANCHES FROM {root}", "database", u, false);

        // The superuser sees both levels; u sees only the grandchild it was granted.
        CollectionAssert.AreEquivalent(
            new[] { child, grandchild },
            await ListAsync(ex, "", $"SHOW BRANCHES FROM {root}", "database", rootUser, false));
        CollectionAssert.AreEquivalent(new[] { grandchild }, branches);
    }

    [Test]
    public async Task ShowBranches_BlanksParentColumnForInvisibleParent()
    {
        (string root, CommandExecutor ex, Principal rootUser) = await Setup();
        (_, string child, string grandchild) = await NewBranchChain(ex, root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {root}.* TO u", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {grandchild}.* TO u", rootUser);
        Principal u = await Login(ex, "u", "pw");

        // The grandchild's parent is `child`, which u cannot see — its name must not leak in `parent`.
        List<string> parents = await ListAsync(ex, "", $"SHOW BRANCHES FROM {root}", "parent", u, false);
        CollectionAssert.AreEquivalent(new[] { "" }, parents);

        // The superuser still gets the real parent name, so the blanking is caller-specific.
        List<string> rootParents = await ListAsync(ex, "", $"SHOW BRANCHES FROM {root}", "parent", rootUser, false);
        Assert.IsTrue(rootParents.Contains(child), "the superuser must still see the real parent name");
    }

    [Test]
    public async Task ShowAncestors_ListsOnlyVisibleAncestors()
    {
        (string root, CommandExecutor ex, Principal rootUser) = await Setup();
        (_, string child, string grandchild) = await NewBranchChain(ex, root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {grandchild}.* TO u", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {root}.* TO u", rootUser);
        Principal u = await Login(ex, "u", "pw");

        // Real chain is grandchild → child (depth 1) → root (depth 2). u cannot see `child`, so that
        // row is dropped whole (its id is a RELINK handle) and only the depth-2 root row survives.
        List<QueryResultRow> rows = await RowsAsync(ex, $"SHOW ANCESTORS FROM {grandchild}", u);

        Assert.AreEqual(1, rows.Count, "only the visible ancestor may be listed");
        Assert.AreEqual(root, rows[0].Row["database"].StrValue);
        Assert.AreEqual(2L, rows[0].Row["depth"].LongValue, "depth stays the true position in the chain");

        // The superuser sees the full chain, proving the filter — not the fixture — dropped the row.
        Assert.AreEqual(2, (await RowsAsync(ex, $"SHOW ANCESTORS FROM {grandchild}", rootUser)).Count);
    }

    [Test]
    public async Task ShowAncestors_NoGrantOnAnyAncestor_ListsNothing()
    {
        (string root, CommandExecutor ex, Principal rootUser) = await Setup();
        (_, _, string grandchild) = await NewBranchChain(ex, root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", rootUser);
        await ServerDdl(ex, $"GRANT SELECT ON {grandchild}.* TO u", rootUser);
        Principal u = await Login(ex, "u", "pw");

        // u may use the grandchild but holds nothing on either database above it.
        Assert.IsEmpty(await RowsAsync(ex, $"SHOW ANCESTORS FROM {grandchild}", u));
    }

    [Test]
    public async Task BranchTree_AuthenticationDisabled_ListsEverything()
    {
        (string root, CommandExecutor ex, _) = await Setup();
        (_, string child, string grandchild) = await NewBranchChain(ex, root);

        // An engine fixes its configuration when it is constructed, so the unauthenticated listing is
        // exercised through a second executor built with authentication off.
        CommandExecutor unauthenticated = CreateCommandExecutor(Options with { AuthenticationEnabled = false });

        CollectionAssert.AreEquivalent(
            new[] { child, grandchild },
            await ListAsync(unauthenticated, "", $"SHOW BRANCHES FROM {root}", "database", null, false));
        Assert.AreEqual(2, (await RowsAsync(unauthenticated, $"SHOW ANCESTORS FROM {grandchild}", null)).Count);
    }

    /// <summary>Runs a server-level statement and returns the raw rows (for multi-column assertions).</summary>
    private static async Task<List<QueryResultRow>> RowsAsync(CommandExecutor ex, string sql, Principal? p)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return rows;
    }

    [Test]
    public async Task AuthenticationDisabled_ListsEverything()
    {
        (string db, CommandExecutor ex, _) = await Setup();

        // With the flag off there is no principal to filter by; both listings must behave exactly as
        // they did before visibility filtering existed. An engine fixes its configuration when it is
        // constructed, so the unauthenticated behaviour is exercised through a second executor built
        // with authentication off, not by flipping a flag under the one that created the tables.
        CommandExecutor unauthenticated = CreateCommandExecutor(Options with { AuthenticationEnabled = false });

        CollectionAssert.AreEquivalent(new[] { "t1", "t2", "t3" }, await ShowTables(unauthenticated, db, null));
        Assert.IsTrue((await ShowDatabases(unauthenticated, db, null)).Contains(db));
    }
}
