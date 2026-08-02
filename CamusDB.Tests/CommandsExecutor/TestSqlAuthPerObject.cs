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
/// Per-object (per-table) privilege enforcement: a <c>db.table</c> grant authorizes exactly that
/// table, and every table a statement touches — including JOIN and subquery sources — is checked at
/// the <c>TableOpener.Open</c> chokepoint. Driven through the real <see cref="CommandExecutor"/> with
/// the auth flag toggled per test.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSqlAuthPerObject : BaseTest
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

    private static async Task Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
        await foreach (QueryResultRow _ in cursor) { }
        await d.Transactions.CommitAsync(tx);
    }

    // Enables auth, creates a db with two tables (t1, t2) as the superuser, returns (db, executor, root).
    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync();
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t1 (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE t2 (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        return (db, ex, root);
    }

    private static void AssertDenied(Func<Task> act)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code);
    }

    [Test]
    public async Task TableGrant_AuthorizesOnlyThatTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        await Query(ex, db, "SELECT id FROM t1", u);                 // granted table — allowed
        AssertDenied(() => Query(ex, db, "SELECT id FROM t2", u));   // sibling table — denied
    }

    [Test]
    public async Task DbWildcardGrant_AuthorizesAllTables()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO u", root);
        Principal u = await Login(ex, "u", "pw");

        await Query(ex, db, "SELECT id FROM t1", u);
        await Query(ex, db, "SELECT id FROM t2", u);
    }

    [Test]
    public async Task Join_RequiresPrivilegeOnBothTables()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        // Join touches t2 as well — denied until t2 is also granted.
        AssertDenied(() => Query(ex, db, "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id", u));

        await ServerDdl(ex, $"GRANT SELECT ON {db}.t2 TO u", root);
        Principal u2 = await Login(ex, "u", "pw"); // fresh snapshot with both grants
        await Query(ex, db, "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id", u2);
    }

    [Test]
    public async Task Subquery_RequiresPrivilegeOnSubqueryTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        // The WHERE subquery reads t2 — must be denied even though the outer table t1 is granted.
        AssertDenied(() => Query(ex, db, "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)", u));
    }

    [Test]
    public async Task InsertOnGrantedTable_Works_OtherDenied()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.t1 TO u", root);
        Principal u = await Login(ex, "u", "pw");

        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t1 (id, v) VALUES (1, 1)", null, u));
        await d.Transactions.CommitAsync(tx);

        KvTransaction tx2 = await d.Transactions.BeginAsync();
        AssertDenied(async () =>
        {
            await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx2, db, "INSERT INTO t2 (id, v) VALUES (1, 1)", null, u));
        });
        await d.Transactions.RollbackIfNotCompletedAsync(tx2);
    }

    [Test]
    public async Task Superuser_TouchesEveryTable()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await Query(ex, db, "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id", root);
    }
}
