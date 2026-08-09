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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Materialized-view statements with authentication on.
///
/// <para>The interesting case is not "does the grant work" but the relation a refresh builds into.
/// It is created and destroyed mid-statement with a generated id no grant could ever have named, so
/// a per-table check applied to it would refuse <em>every</em> non-superuser refresh — while the
/// same statements pass on an unauthenticated engine, which is where the whole matview suite
/// otherwise runs. These tests exist to keep that path honest.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test, like the other auth fixtures.
[NonParallelizable]
internal sealed class TestMaterializedViewAuth : BaseTest
{
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

    private static async Task<int> NonQuery(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<int> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await d.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>Creates a database with a seeded `orders` table, owned by the superuser.</summary>
    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "mvauthdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE orders (id int64 PRIMARY KEY NOT NULL, total int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO orders (id, total) VALUES (1, 10), (2, 20), (3, 30)", root);
        return (db, ex, root);
    }

    private static void AssertDenied(Func<Task> act)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code);
    }

    [Test]
    public async Task CreateAndRefreshWorkForANonSuperuserWithTheRightGrants()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO u", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO u", root);
        Principal u = await Login(ex, "u", "pw");

        // The staging relation this creates gets a brand-new id that no grant names. Checking it
        // per-table would refuse here, on a statement the caller is plainly authorized to run.
        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders", u);

        // Read it back as the superuser: creating a relation grants nothing on it, here as for a
        // table, so the creator cannot yet select from it.
        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM order_totals", root),
            "the rows must actually have been materialized, not merely the relation created");

        await ServerDdl(ex, $"GRANT SELECT ON {db}.order_totals TO u", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.order_totals TO u", root);
        Principal u2 = await Login(ex, "u", "pw");

        Assert.AreEqual(3, await NonQuery(ex, db, "REFRESH MATERIALIZED VIEW order_totals", u2),
            "a refresh must not be refused because of the relation it internally builds into");

        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM order_totals", u2));
    }

    [Test]
    public async Task ReadingAMaterializedViewNeedsAGrantOnIt()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders", root);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        // A grant on the base table says nothing about the materialized view: it is a separate
        // relation holding a separate copy of the rows, and it is checked as one.
        AssertDenied(() => Query(ex, db, "SELECT id FROM order_totals", reader));

        await ServerDdl(ex, $"GRANT SELECT ON {db}.order_totals TO reader", root);
        Principal reader2 = await Login(ex, "reader", "pw");

        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM order_totals", reader2));
    }

    /// <summary>
    /// A view is checked as an object in its own right. It used to be checked as nothing at all:
    /// expansion removes the view's name before anything opens a relation, and drop/rename/describe
    /// read the view map directly, so the mapped privileges were never consumed by any opener. A user
    /// holding only base-table access could read, describe, list and <b>drop</b> another user's view.
    /// </summary>
    [Test]
    public async Task AViewIsAuthorizedAsAnObjectAndNotViaItsBaseTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await TxnDdl(ex, db, "CREATE VIEW v AS SELECT id, total FROM orders", root);

        // Table-scoped grants only. A database-wide grant would legitimately make every object in the
        // database visible, which would mask exactly what this test is about.
        await ServerDdl(ex, "CREATE USER baseonly IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO baseonly", root);
        await ServerDdl(ex, $"GRANT DROP ON {db}.orders TO baseonly", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.orders TO baseonly", root);
        Principal baseOnly = await Login(ex, "baseonly", "pw");

        AssertDenied(() => Query(ex, db, "SELECT id FROM v", baseOnly));
        AssertDenied(() => Query(ex, db, "SELECT id FROM v WHERE total > 1", baseOnly));
        AssertDenied(() => Query(ex, db, "SHOW CREATE VIEW v", baseOnly));
        AssertDenied(() => Query(ex, db, "SHOW COLUMNS FROM v", baseOnly));
        AssertDenied(() => TxnDdl(ex, db, "ALTER VIEW v RENAME TO v2", baseOnly));
        AssertDenied(() => TxnDdl(ex, db, "DROP VIEW v", baseOnly));

        // Not listed either: the name alone is a disclosure, so it is omitted rather than refused.
        Assert.AreEqual(0, await Query(ex, db, "SHOW VIEWS", baseOnly));

        // A user who may create views in this database still may not overwrite somebody else's.
        await ServerDdl(ex, "CREATE USER creator IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO creator", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO creator", root);
        Principal creator = await Login(ex, "creator", "pw");

        AssertDenied(() => TxnDdl(ex, db, "CREATE OR REPLACE VIEW v AS SELECT id, total FROM orders", creator));

        // Every refusal above must have left the view intact.
        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM v", root));

        // With a grant on the view itself, the same statements work.
        await ServerDdl(ex, $"GRANT SELECT ON {db}.v TO baseonly", root);
        Principal withView = await Login(ex, "baseonly", "pw");

        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM v", withView));
        Assert.AreEqual(1, await Query(ex, db, "SHOW VIEWS", withView));
    }

    /// <summary>
    /// A refresh never opens the materialized view — it builds a separate relation and swaps its
    /// storage in — so the per-table chokepoint that guards every ordinary write is never reached and
    /// the mapped privilege enforced nothing on its own.
    ///
    /// <para>The grants are arranged so the refusal can only come from the target check: the caller
    /// can read the source, so source binding would succeed. An earlier version of this fixture
    /// appeared to prove the denial while actually failing on the source.</para>
    /// </summary>
    [Test]
    public async Task RefreshChecksTheTargetEvenWhenTheSourceIsReadable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, total FROM orders", root);

        await ServerDdl(ex, "CREATE USER srconly IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO srconly", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.mv TO srconly", root);
        Principal srcOnly = await Login(ex, "srconly", "pw");

        AssertDenied(() => NonQuery(ex, db, "REFRESH MATERIALIZED VIEW mv", srcOnly));

        // A denied refresh must not have created staging state on the way to being refused.
        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        CollectionAssert.IsEmpty(
            descriptor.Schema.Tables.Keys.Where(CamusDB.Core.Catalogs.Models.MaterializedViewNaming.IsStagingRelation).ToList(),
            "a refusal must leave no staging relation behind");

        await ServerDdl(ex, $"GRANT INSERT ON {db}.mv TO srconly", root);
        Principal withInsert = await Login(ex, "srconly", "pw");
        Assert.AreEqual(3, await NonQuery(ex, db, "REFRESH MATERIALIZED VIEW mv", withInsert));
    }

    [Test]
    public async Task AViewGrantsAccessToRowsTheCallerCannotReadDirectly()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        // Owned by a user who CAN read the base table.
        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");

        await TxnDdl(ex, db, "CREATE VIEW cheap AS SELECT id, total FROM orders WHERE total < 25", owner1);

        // The reader gets the view and nothing else.
        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cheap TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM cheap", reader),
            "the view's body must run with its owner's privileges");

        // ...and the base table is still closed to them.
        AssertDenied(() => Query(ex, db, "SELECT id FROM orders", reader));
    }

    /// <summary>
    /// The owner swap is scoped to the view's own subtree. A statement that names the same table both
    /// through a view and directly must not have the direct reference inherit the owner's rights —
    /// that would turn any view over a table into a general grant on it.
    /// </summary>
    [Test]
    public async Task TheOwnerSwapDoesNotLeakToASiblingDirectReference()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");
        await TxnDdl(ex, db, "CREATE VIEW cheap AS SELECT id, total FROM orders WHERE total < 25", owner1);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cheap TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        AssertDenied(() => Query(ex, db, "SELECT c.id FROM cheap c JOIN orders o ON c.id = o.id", reader));
    }

    /// <summary>
    /// Ownership decides whose privileges a view's body runs with, so it must not follow a name that
    /// somebody else can later claim, and a replace must not seize it.
    /// </summary>
    [Test]
    public async Task OwnershipSurvivesReplaceAndDoesNotFollowARecreatedName()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");
        await TxnDdl(ex, db, "CREATE VIEW cheap AS SELECT id, total FROM orders WHERE total < 25", owner1);

        // A replace by the owner keeps the owner (and must not need re-granting).
        await ServerDdl(ex, $"GRANT ALTER ON {db}.cheap TO owner1", root);
        Principal owner1b = await Login(ex, "owner1", "pw");
        await TxnDdl(ex, db, "CREATE OR REPLACE VIEW cheap AS SELECT id, total FROM orders WHERE total < 45", owner1b);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cheap TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        // The fixture seeds three orders (10, 20, 30), so widening to < 45 admits all of them.
        Assert.AreEqual(3, await Query(ex, db, "SELECT id FROM cheap", reader));

        // Drop the owner and recreate the same NAME with no access to the base table. The view must
        // fail closed rather than run as the impostor — or as the caller.
        await ServerDdl(ex, "DROP USER owner1", root);
        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw2'", root);

        Principal reader2 = await Login(ex, "reader", "pw");
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(ex, db, "SELECT id FROM cheap", reader2))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code);
    }

    [Test]
    public async Task OwnershipTransferRequiresOwnershipAndRestoresTheView()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");
        await TxnDdl(ex, db, "CREATE VIEW cheap AS SELECT id, total FROM orders WHERE total < 25", owner1);

        await ServerDdl(ex, "CREATE USER other IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.cheap TO other", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO other", root);
        Principal other = await Login(ex, "other", "pw");

        // An Alter grant on the view is not authority over who it runs as.
        AssertDenied(() => TxnDdl(ex, db, "ALTER VIEW cheap OWNER TO other", other));

        // A superuser may transfer it, and the view then runs as the new owner. Sent through the
        // NON-QUERY path on purpose: ownership transfer is administrative, so it is exactly the kind of
        // statement a client sends down its generic non-SELECT route, and this one was the last view
        // statement still answering "unknown statement" there.
        await NonQuery(ex, db, "ALTER VIEW cheap OWNER TO other", root);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cheap TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");
        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM cheap", reader));

        // Transferring to a user who does not exist is refused rather than breaking the view.
        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(async () =>
            await TxnDdl(ex, db, "ALTER VIEW cheap OWNER TO nobody", root))!;
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, missing.Code);
    }

    /// <summary>Each view in a chain runs as its own owner, so a nested view cannot widen the outer one.</summary>
    [Test]
    public async Task NestedViewsEachRunAsTheirOwnOwner()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await ServerDdl(ex, "CREATE USER inner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO inner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO inner1", root);
        Principal innerOwner = await Login(ex, "inner1", "pw");
        await TxnDdl(ex, db, "CREATE VIEW base_v AS SELECT id, total FROM orders WHERE total < 25", innerOwner);

        // The outer owner can read base_v but NOT orders.
        await ServerDdl(ex, "CREATE USER outer1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO outer1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.base_v TO outer1", root);
        Principal outerOwner = await Login(ex, "outer1", "pw");
        await TxnDdl(ex, db, "CREATE VIEW outer_v AS SELECT id FROM base_v", outerOwner);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.outer_v TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_v", reader),
            "the inner view must run as its own owner, not as the outer view's owner or the caller");
    }

    [Test]
    public async Task RefreshNeedsMoreThanReadAccess()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders", root);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.order_totals TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        // REFRESH replaces the contents, so being allowed to read them is not enough.
        AssertDenied(() => NonQuery(ex, db, "REFRESH MATERIALIZED VIEW order_totals", reader));
    }
}
