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
/// Each operand of a set operation needs SELECT on its own tables. The per-table check runs when the
/// binder opens a table, and the binder binds each operand as a complete query, so a grant on one
/// operand's table gives nothing on the other's. A walker that did not go into the set operation
/// would be a privilege bypass; these tests are the guard against it.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSetOperationsAuth : BaseTest
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

    [Test]
    public async Task AGrantOnOneOperandsTable_DoesNotCoverTheOther()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "setopauthdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE a (id int64 PRIMARY KEY NOT NULL, x int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE b (id int64 PRIMARY KEY NOT NULL, x int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO a (id, x) VALUES (1, 10), (2, 20)", root);
        await NonQuery(ex, db, "INSERT INTO b (id, x) VALUES (1, 20), (2, 30)", root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.a TO u", root);
        Principal u = await Login(ex, "u", "pw");

        Assert.AreEqual(2, await Query(ex, db, "SELECT x FROM a UNION SELECT x FROM a", u));

        foreach (string sql in new[]
        {
            "SELECT x FROM a UNION SELECT x FROM b",
            "SELECT x FROM b UNION ALL SELECT x FROM a",
            "SELECT x FROM a INTERSECT SELECT x FROM b",
            "SELECT x FROM a EXCEPT SELECT x FROM b",
            "SELECT 1 UNION SELECT x FROM b",
            "SELECT s.x FROM (SELECT x FROM a UNION SELECT x FROM b) AS s",
            "SELECT id FROM a WHERE x IN (SELECT x FROM a UNION SELECT x FROM b)",
            "SELECT x FROM a UNION SELECT x FROM a UNION SELECT x FROM b",
        })
        {
            CamusDBException denied = Assert.ThrowsAsync<CamusDBException>(async () => await Query(ex, db, sql, u), sql)!;
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, denied.Code, sql);
        }

        await ServerDdl(ex, $"GRANT SELECT ON {db}.b TO u", root);
        Principal granted = await Login(ex, "u", "pw");

        Assert.AreEqual(3, await Query(ex, db, "SELECT x FROM a UNION SELECT x FROM b", granted));
    }
}
