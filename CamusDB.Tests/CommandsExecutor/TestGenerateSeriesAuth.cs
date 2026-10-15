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
/// <c>generate_series</c> in <c>FROM</c> with authentication on. The function reads no table, so it
/// needs no privilege of its own; a table joined to it still needs <c>SELECT</c> on that table.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGenerateSeriesAuth : BaseTest
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
    public async Task TheFunctionNeedsNoGrant_AJoinedTableStillDoes()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "gsauthdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE orders (id int64 PRIMARY KEY NOT NULL, total int64 NULL)", root);
        await NonQuery(ex, db, "INSERT INTO orders (id, total) VALUES (1, 10), (2, 20), (3, 30)", root);

        await TxnDdl(ex, db, "CREATE TABLE other (id int64 PRIMARY KEY NOT NULL)", root);

        // A grant on an unrelated table only makes the database visible to u; it says nothing about
        // orders, and the function needs nothing.
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.other TO u", root);
        Principal u = await Login(ex, "u", "pw");

        Assert.AreEqual(3, await Query(ex, db, "SELECT i FROM generate_series(1, 3) i", u),
            "a function reads no table, so it needs no grant");

        CamusDBException denied = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(ex, db, "SELECT o.id, i FROM orders o, generate_series(1, 2) i", u))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, denied.Code);

        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO u", root);
        Principal granted = await Login(ex, "u", "pw");

        Assert.AreEqual(6, await Query(ex, db, "SELECT o.id, i FROM orders o, generate_series(1, 2) i", granted));
    }
}
