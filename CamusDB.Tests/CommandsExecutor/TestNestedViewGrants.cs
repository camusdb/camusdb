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
/// A view whose body reads another view needs SELECT on that inner view — held by the outer view's
/// owner, the principal the body runs as — on every read, as in PostgreSQL. It used to be checked only
/// when the outer view was created, so a grant revoked afterwards went unnoticed until the outer view
/// was replaced.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNestedViewGrants : BaseTest
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

    private static CamusDBException Denied(Func<Task> act, string what)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act(), what)!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code, what + ": " + e.Message);
        return e;
    }

    /// <summary>
    /// Table <c>t</c>; <c>inner_owner</c> reads it and owns <c>inner_v</c>; <c>outer_owner</c> cannot
    /// read <c>t</c> but holds SELECT on <c>inner_v</c>, and owns three views over it — a plain read, a
    /// read inside an IN subquery, and a set operation; <c>reader</c> holds SELECT on the outer views
    /// only.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal root, Principal reader)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "nestedgrants" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY, x int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE u (id int64 PRIMARY KEY)", root);
        await NonQuery(ex, db, "INSERT INTO t (id, x) VALUES (1, 10), (2, 20), (3, 30)", root);
        await NonQuery(ex, db, "INSERT INTO u (id) VALUES (1), (2), (9)", root);

        await ServerDdl(ex, "CREATE USER inner_owner IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO inner_owner", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO inner_owner", root);
        Principal innerOwner = await Login(ex, "inner_owner", "pw");
        await TxnDdl(ex, db, "CREATE VIEW inner_v AS SELECT id, x FROM t WHERE x >= 20", innerOwner);

        await ServerDdl(ex, "CREATE USER outer_owner IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO outer_owner", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.inner_v TO outer_owner", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.u TO outer_owner", root);
        Principal outerOwner = await Login(ex, "outer_owner", "pw");
        await TxnDdl(ex, db, "CREATE VIEW outer_v AS SELECT id FROM inner_v", outerOwner);
        await TxnDdl(ex, db, "CREATE VIEW outer_in AS SELECT id FROM u WHERE id IN (SELECT id FROM inner_v)", outerOwner);
        await TxnDdl(ex, db, "CREATE VIEW outer_union AS SELECT id FROM inner_v UNION SELECT id FROM inner_v", outerOwner);

        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.outer_v TO reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.outer_in TO reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.outer_union TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        return (db, ex, root, reader);
    }

    [Test]
    public async Task ARevokedInnerGrantStopsReadsOfTheOuterView()
    {
        (string db, CommandExecutor ex, Principal root, Principal reader) = await Setup();

        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_v", reader));
        Assert.AreEqual(1, await Query(ex, db, "SELECT id FROM outer_in", reader));
        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_union", reader));

        await ServerDdl(ex, $"REVOKE SELECT ON {db}.inner_v FROM outer_owner", root);

        CamusDBException plain = Denied(() => Query(ex, db, "SELECT id FROM outer_v", reader), "plain nested read");
        StringAssert.Contains("inner_v", plain.Message);
        StringAssert.Contains("outer_owner", plain.Message,
            "the denial must say whose grant is missing: nested, it is not the caller's");

        Denied(() => Query(ex, db, "SELECT id FROM outer_in", reader), "nested read inside a subquery");
        Denied(() => Query(ex, db, "SELECT id FROM outer_union", reader), "nested read inside a set operation");

        // Granting it back restores the outer views without replacing them.
        await ServerDdl(ex, $"GRANT SELECT ON {db}.inner_v TO outer_owner", root);

        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_v", reader));
        Assert.AreEqual(1, await Query(ex, db, "SELECT id FROM outer_in", reader));
        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_union", reader));
    }

    /// <summary>
    /// The nested check is against the outer view's owner, not the reader: the reader holds no grant on
    /// the inner view and does not need one. Requiring it would make every nested view a grant the
    /// reader must hold, which is the encapsulation a view exists to provide.
    /// </summary>
    [Test]
    public async Task TheReaderNeedsNoGrantOnTheInnerView()
    {
        (string db, CommandExecutor ex, Principal _, Principal reader) = await Setup();

        Denied(() => Query(ex, db, "SELECT id FROM inner_v", reader), "the reader has no grant on inner_v");
        Assert.AreEqual(2, await Query(ex, db, "SELECT id FROM outer_v", reader));
    }

    /// <summary>
    /// A view named inside a subquery runs as its owner like one named in FROM. The correlation analysis
    /// of the subquery used to walk into the view's body and bind its tables as the caller, outside the
    /// owner's scope, so a caller allowed to read the view but not its tables was refused.
    /// </summary>
    [Test]
    public async Task AViewInASubqueryOfTheCallersOwnStatementRunsAsItsOwner()
    {
        (string db, CommandExecutor ex, Principal root, Principal _) = await Setup();

        await ServerDdl(ex, "CREATE USER sub_reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.u TO sub_reader", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.inner_v TO sub_reader", root);
        Principal subReader = await Login(ex, "sub_reader", "pw");

        Assert.AreEqual(1, await Query(ex, db, "SELECT id FROM u WHERE id IN (SELECT id FROM inner_v)", subReader));
        Assert.AreEqual(1, await Query(ex, db,
            "SELECT id FROM u WHERE EXISTS (SELECT 1 FROM inner_v WHERE inner_v.id = u.id)", subReader));

        Denied(() => Query(ex, db, "SELECT id FROM u WHERE id IN (SELECT id FROM t)", subReader),
            "the base table itself stays unreadable");
    }

    /// <summary>
    /// The inner view's own body still runs as its own owner. Revoking the outer owner's access to the
    /// base table changes nothing — the outer owner never had it — while revoking the inner owner's
    /// access stops the chain.
    /// </summary>
    [Test]
    public async Task EachViewInTheChainRunsAsItsOwnOwner()
    {
        (string db, CommandExecutor ex, Principal root, Principal reader) = await Setup();

        await ServerDdl(ex, $"REVOKE SELECT ON {db}.t FROM inner_owner", root);

        Denied(() => Query(ex, db, "SELECT id FROM outer_v", reader), "the inner owner lost the base table");
    }
}
