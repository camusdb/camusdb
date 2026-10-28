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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Whose privileges a subquery inside a nested SELECT runs with. A view body binds as the view's
/// owner, and the subquery stages of that body now run inside the same scope, so an IN or EXISTS in
/// a view body reads its table as the owner: a caller with SELECT on the view alone can read it. A
/// hand-written derived table, and an operand of a set operation the caller writes, have no owner
/// and run as the caller, so a subquery there is refused when the caller cannot read its table.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestDerivedTableSubqueriesAuth : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
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

    private static async Task NonQuery(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
    }

    private static async Task<int> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            int count = 0;
            await foreach (QueryResultRow _ in cursor) count++;
            await d.Transactions.CommitAsync(tx);
            return count;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The one cell of the one row of an aggregate query.</summary>
    private static async Task<long> Scalar(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            long? value = null;
            await foreach (QueryResultRow row in cursor)
            {
                Assert.IsNull(value, sql + " returned more than one row");
                Assert.AreEqual(1, row.Row.Count, sql);
                foreach (ColumnValue cell in row.Row.Values)
                    value = cell.LongValue;
            }
            await d.Transactions.CommitAsync(tx);
            Assert.IsNotNull(value, sql + " returned no row");
            return value!.Value;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertDenied(Func<Task> act, string what)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act(), what)!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code, what + ": " + e.Message);
    }

    /// <summary>
    /// Tables <c>a</c> and <c>b</c> seeded by the superuser; a view owner who can read both and owns
    /// three views whose bodies hold a subquery over <c>b</c>; and a reader who can read <c>a</c> and
    /// the three views, but not <c>b</c>.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal reader)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "dtsubqauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE a (id int64 PRIMARY KEY, x int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE b (id int64 PRIMARY KEY, y int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO a (id, x) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 3), (6, NULL)", root);
        await NonQuery(ex, db, "INSERT INTO b (id, y) VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 4), (6, NULL)", root);

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.a TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.b TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");

        await TxnDdl(ex, db, "CREATE VIEW p_in AS SELECT id FROM a WHERE x IN (SELECT y FROM b)", owner1);
        await TxnDdl(ex, db, "CREATE VIEW p_exists AS SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)", owner1);
        await TxnDdl(ex, db,
            "CREATE VIEW p_union AS SELECT id FROM a WHERE x IN (SELECT y FROM b) UNION SELECT id FROM a WHERE x = 3", owner1);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.a TO reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.p_in TO reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.p_exists TO reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.p_union TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        return (db, ex, reader);
    }

    [Test]
    public async Task ASubqueryInAViewBody_ReadsItsTableAsTheOwner()
    {
        (string db, CommandExecutor ex, Principal reader) = await Setup();

        // The reader cannot read b directly, so these rows come through the owner's rights.
        AssertDenied(() => Query(ex, db, "SELECT id FROM b", reader), "direct read of b");

        Assert.AreEqual(4, await Query(ex, db, "SELECT id FROM p_in", reader));
        Assert.AreEqual(4, await Scalar(ex, db, "SELECT COUNT(*) FROM p_in", reader));
        Assert.AreEqual(4, await Query(ex, db, "SELECT id FROM p_exists", reader));
        Assert.AreEqual(4, await Scalar(ex, db, "SELECT COUNT(*) FROM p_exists", reader));
        Assert.AreEqual(5, await Query(ex, db, "SELECT id FROM p_union", reader));

        // The same views inside a derived table the reader writes: the derived table runs as the
        // reader, the view inside it still runs as its owner.
        Assert.AreEqual(3, await Query(ex, db, "SELECT s.id FROM (SELECT id FROM p_in WHERE id > 1) AS s", reader));
        Assert.AreEqual(4, await Scalar(ex, db, "SELECT COUNT(*) FROM (SELECT id FROM p_exists) AS sub", reader));
    }

    [Test]
    public async Task ASubqueryInAHandWrittenDerivedTable_RunsAsTheCaller()
    {
        (string db, CommandExecutor ex, Principal reader) = await Setup();

        // The reader may read a, so only the subquery's table is what refuses the statement.
        Assert.AreEqual(6, await Query(ex, db, "SELECT s.id FROM (SELECT id FROM a) AS s", reader));

        AssertDenied(() => Query(ex, db,
            "SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS s", reader), "IN in a derived table");
        AssertDenied(() => Query(ex, db,
            "SELECT COUNT(*) FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS sub", reader),
            "EXISTS in a derived table");
        AssertDenied(() => Query(ex, db,
            "SELECT s.id FROM (SELECT id FROM a WHERE x = (SELECT min(y) FROM b)) AS s", reader), "scalar in a derived table");
    }

    [Test]
    public async Task ASubqueryInASetOperationOperand_RunsAsTheCaller()
    {
        (string db, CommandExecutor ex, Principal reader) = await Setup();

        AssertDenied(() => Query(ex, db,
            "SELECT id FROM a WHERE x IN (SELECT y FROM b) UNION SELECT id FROM a WHERE x = 3", reader),
            "IN in a set-operation operand");
        AssertDenied(() => Query(ex, db,
            "SELECT id FROM a WHERE x = 3 UNION ALL SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)", reader),
            "EXISTS in a set-operation operand");

        // The view that holds the same set operation reads b as its owner, in the operand too.
        Assert.AreEqual(5, await Query(ex, db, "SELECT id FROM p_union", reader));
        Assert.AreEqual(6, await Query(ex, db, "SELECT id FROM p_union UNION SELECT id FROM a WHERE x IS NULL", reader));
    }
}
