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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>INSERT … RETURNING</c> reads what it inserted — stored values, defaults and sequence draws — so
/// it needs SELECT on the target in addition to INSERT, as in PostgreSQL. These tests drive the real
/// executor with authentication on and check both entry points and the count-only flag.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestInsertReturningAuthorization : BaseTest
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

    private static async Task<ExecuteNonSQLResult> NonQuery(
        CommandExecutor ex, string db, string sql, Principal? p, bool discardReturningRows = false)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await ex.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(tx, db, sql, null, p, discardReturningRows: discardReturningRows));
            await d.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> QueryCount(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;
            await d.Transactions.CommitAsync(tx);
            return count;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertDenied(Func<Task> act)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code, e.Message);
    }

    /// <summary>A database with table <c>t</c>, and a user <c>u</c> holding INSERT on it only.</summary>
    private async Task<(string db, CommandExecutor ex, Principal root, Principal user)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        return (db, ex, root, user);
    }

    [Test]
    public async Task InsertOnlyMayInsertButNotReturn()
    {
        (string db, CommandExecutor ex, Principal root, Principal user) = await Setup();

        // The plain insert is what the grant is for.
        ExecuteNonSQLResult plain = await NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 10)", user);
        Assert.AreEqual(1, plain.ModifiedRows);

        // RETURNING reads the inserted row, which the grant does not cover — on both entry points,
        // and also when the caller asks for the count only, so the flag cannot be used to probe.
        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (2, 20) RETURNING v", user));
        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (3, 30) RETURNING v", user, discardReturningRows: true));
        AssertDenied(() => QueryCount(ex, db, "INSERT INTO t (id, v) VALUES (4, 40) RETURNING v", user));

        // None of the refused statements wrote a row.
        Assert.AreEqual(1, await QueryCount(ex, db, "SELECT id FROM t", root));
    }

    [Test]
    public async Task InsertAndSelectMayReturn()
    {
        (string db, CommandExecutor ex, Principal root, Principal _) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        ExecuteNonSQLResult result = await NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (5, 50) RETURNING v", user);
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(50, result.ReturningRows[0].Row[result.ReturningColumns![0].RowKey].LongValue);

        Assert.AreEqual(1, await QueryCount(ex, db, "INSERT INTO t (id, v) VALUES (6, 60) RETURNING v", user));
    }

    /// <summary>
    /// SELECT alone does not let a caller insert: the RETURNING bind narrows the requirement only for
    /// its own phase, and the statement still needs INSERT on the target.
    /// </summary>
    [Test]
    public async Task SelectOnlyMayNotInsertWithReturning()
    {
        (string db, CommandExecutor ex, Principal root, Principal _) = await Setup();
        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (7, 70) RETURNING v", reader));
        Assert.AreEqual(0, await QueryCount(ex, db, "SELECT id FROM t", root));
    }
}
