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
/// Whose privileges a <c>LATERAL</c> derived table runs with. In a view body it reads its tables as the
/// view's owner, also when its body binds again for each left row (a nested subquery that reads a left
/// value). A hand-written <c>LATERAL</c> derived table runs as the caller.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestLateralJoinsAuth : BaseTest
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

    /// <summary>The sum of the one-column rows of <paramref name="sql"/>.</summary>
    private static async Task<long> Sum(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            long sum = 0;
            await foreach (QueryResultRow row in cursor)
                foreach (ColumnValue cell in row.Row.Values)
                    sum += cell.LongValue;
            await d.Transactions.CommitAsync(tx);
            return sum;
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
    /// Tables <c>a</c> and <c>b</c>; an owner who can read both and owns two views whose <c>LATERAL</c>
    /// body reads <c>b</c>; a reader who can read <c>a</c> and the views, but not <c>b</c>.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal reader)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "lateralauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE a (id int64 PRIMARY KEY, x int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE b (id int64 PRIMARY KEY, y int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO a (id, x) VALUES (1, 1), (2, 2), (3, 3)", root);
        await NonQuery(ex, db, "INSERT INTO b (id, y) VALUES (1, 1), (2, 1), (3, 2)", root);

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.a TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.b TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");

        // The first body binds once; the second reads a left value in a nested subquery, so it binds
        // again for each left row.
        await TxnDdl(ex, db,
            "CREATE VIEW v_once AS SELECT s.n FROM a, LATERAL (SELECT COUNT(*) AS n FROM b WHERE b.y = a.x) s", owner1);
        await TxnDdl(ex, db,
            "CREATE VIEW v_each AS SELECT s.n FROM a, LATERAL (SELECT (SELECT COUNT(*) FROM b WHERE b.y = a.x) AS n) s", owner1);

        await ServerDdl(ex, "CREATE USER reader1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.a TO reader1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.v_once TO reader1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.v_each TO reader1", root);
        Principal reader1 = await Login(ex, "reader1", "pw");

        return (db, ex, reader1);
    }

    [Test]
    public async Task AViewLateralReadsItsTablesAsTheOwner()
    {
        (string db, CommandExecutor ex, Principal reader) = await Setup();

        // x = 1 matches two rows of b, x = 2 one row, x = 3 none.
        Assert.AreEqual(3, await Sum(ex, db, "SELECT n FROM v_once", reader));
        Assert.AreEqual(3, await Sum(ex, db, "SELECT n FROM v_each", reader));
    }

    [Test]
    public async Task AHandWrittenLateralRunsAsTheCaller()
    {
        (string db, CommandExecutor ex, Principal reader) = await Setup();

        AssertDenied(() => Sum(ex, db, "SELECT s.n FROM a, LATERAL (SELECT COUNT(*) AS n FROM b WHERE b.y = a.x) s", reader),
            "a LATERAL body that binds once");
        AssertDenied(() => Sum(ex, db, "SELECT s.n FROM a, LATERAL (SELECT (SELECT COUNT(*) FROM b WHERE b.y = a.x) AS n) s", reader),
            "a LATERAL body that binds for each row");
    }
}
