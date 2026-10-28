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
/// <c>INSERT … ON CONFLICT DO NOTHING</c> needs INSERT only. <c>DO UPDATE</c> and <c>UPSERT</c> can
/// change a stored row, so they need UPDATE as well, as in PostgreSQL. The tests drive the real
/// executor with authentication on, and check that a refused statement wrote nothing.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestInsertOnConflictAuthorization : BaseTest
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

    private static async Task<ExecuteNonSQLResult> NonQuery(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> Value(CommandExecutor ex, string db, Principal p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SELECT v FROM t WHERE id = 1", null, p));
            long v = -1;
            await foreach (QueryResultRow row in cursor)
                v = row.Row["v"].LongValue;
            await d.Transactions.CommitAsync(tx);
            return v;
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

    /// <summary>A database with table <c>t</c> holding row 1, and a user <c>u</c> holding INSERT on it only.</summary>
    private async Task<(string db, CommandExecutor ex, Principal root, Principal user)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);
        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);

        Principal root = await Login(ex, "root", "root-pw");
        await TxnDdl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 10)", root);
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.t TO u", root);

        Principal user = await Login(ex, "u", "pw");
        return (db, ex, root, user);
    }

    [Test]
    public async Task InsertOnlyAllowsDoNothingAndRefusesDoUpdateAndUpsert()
    {
        (string db, CommandExecutor ex, Principal root, Principal user) = await Setup();

        ExecuteNonSQLResult nothing = await NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 99), (2, 20) ON CONFLICT (id) DO NOTHING", user);
        Assert.AreEqual(1, nothing.ModifiedRows);

        // Refused even when no proposed row conflicts: the privilege does not depend on the data.
        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (3, 30) ON CONFLICT (id) DO UPDATE SET v = excluded.v", user));
        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET v = excluded.v", user));
        AssertDenied(() => NonQuery(ex, db, "UPSERT INTO t (id, v) VALUES (1, 40)", user));

        Assert.AreEqual(10, await Value(ex, db, root));
    }

    [Test]
    public async Task InsertAndUpdateAllowDoUpdateAndUpsert()
    {
        (string db, CommandExecutor ex, Principal root, Principal _) = await Setup();
        await ServerDdl(ex, $"GRANT UPDATE ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        await NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET v = excluded.v", user);
        Assert.AreEqual(30, await Value(ex, db, root));

        await NonQuery(ex, db, "UPSERT INTO t (id, v) VALUES (1, 40)", user);
        Assert.AreEqual(40, await Value(ex, db, root));
    }

    [Test]
    public async Task UpdateWithoutInsertIsRefused()
    {
        (string db, CommandExecutor ex, Principal root, Principal _) = await Setup();
        await ServerDdl(ex, "CREATE USER w IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT UPDATE ON {db}.t TO w", root);
        Principal updater = await Login(ex, "w", "pw");

        AssertDenied(() => NonQuery(ex, db, "INSERT INTO t (id, v) VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET v = excluded.v", updater));
        AssertDenied(() => NonQuery(ex, db, "UPSERT INTO t (id, v) VALUES (1, 40)", updater));

        Assert.AreEqual(10, await Value(ex, db, root));
    }
}
