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
/// Whose privileges a write through a view uses. The caller needs the write privilege on the view; the
/// view's owner needs it on the table the view reads — the owner's-rights model reads through a view
/// already follow. A subquery the caller writes still reads as the caller.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestUpdatableViewsAuth : BaseTest
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

    private static async Task Ddl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Write(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            int rows = (await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p))).ModifiedRows;
            await d.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Count(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
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

    private static CamusDBException Denied(Func<Task> act)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code, e.Message);
        return e;
    }

    /// <summary>
    /// Table <c>orders</c>; <c>owner1</c> may read and write it and owns <c>acme</c> over it;
    /// <c>writer</c> holds INSERT, UPDATE and DELETE on <c>acme</c> only.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal root, Principal writer)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "viewwrites" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(32) NULL, total int64 NULL)", root);
        await Write(ex, db, "INSERT INTO orders (id, customer, total) VALUES (1, 'acme', 10), (2, 'globex', 20)", root);

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        foreach (string privilege in new[] { "SELECT", "INSERT", "UPDATE", "DELETE" })
            await ServerDdl(ex, $"GRANT {privilege} ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");
        await Ddl(ex, db, "CREATE VIEW acme AS SELECT id, customer, total FROM orders WHERE customer = 'acme'", owner1);

        await ServerDdl(ex, "CREATE USER writer IDENTIFIED BY 'pw'", root);
        foreach (string privilege in new[] { "SELECT", "INSERT", "UPDATE", "DELETE" })
            await ServerDdl(ex, $"GRANT {privilege} ON {db}.acme TO writer", root);
        Principal writer = await Login(ex, "writer", "pw");

        return (db, ex, root, writer);
    }

    [Test]
    public async Task AWriterWithGrantsOnTheViewOnlyWritesThroughItAsTheOwner()
    {
        (string db, CommandExecutor ex, Principal _, Principal writer) = await Setup();

        Assert.AreEqual(1, await Write(ex, db, "INSERT INTO acme (id, customer, total) VALUES (3, 'acme', 30)", writer));
        Assert.AreEqual(2, await Write(ex, db, "UPDATE acme SET total = total + 1 WHERE TRUE", writer));
        Assert.AreEqual(1, await Write(ex, db, "DELETE FROM acme WHERE id = 3", writer));

        // The table itself stays out of reach.
        Denied(() => Write(ex, db, "DELETE FROM orders WHERE TRUE", writer));
        Denied(() => Count(ex, db, "SELECT id FROM orders", writer));
    }

    [Test]
    public async Task TheCallerNeedsTheWritePrivilegeOnTheView()
    {
        (string db, CommandExecutor ex, Principal root, Principal writer) = await Setup();

        await ServerDdl(ex, $"REVOKE DELETE ON {db}.acme FROM writer", root);
        writer = await Login(ex, "writer", "pw");

        Denied(() => Write(ex, db, "DELETE FROM acme WHERE TRUE", writer));
        Assert.AreEqual(2, await Count(ex, db, "SELECT id FROM orders", root), "a refused write changes nothing");
    }

    [Test]
    public async Task TheOwnerNeedsTheWritePrivilegeOnTheTable()
    {
        (string db, CommandExecutor ex, Principal root, Principal writer) = await Setup();

        await ServerDdl(ex, $"REVOKE UPDATE ON {db}.orders FROM owner1", root);

        CamusDBException denied = Denied(() => Write(ex, db, "UPDATE acme SET total = 0 WHERE TRUE", writer));
        StringAssert.Contains("owner1", denied.Message);
        Assert.AreEqual(1, await Count(ex, db, "SELECT id FROM orders WHERE total = 10", root), "a refused write changes nothing");
    }

    /// <summary>A subquery the caller writes in the statement reads its tables as the caller, not as the owner.</summary>
    [Test]
    public async Task ASubqueryInTheCallersWriteStillReadsAsTheCaller()
    {
        (string db, CommandExecutor ex, Principal _, Principal writer) = await Setup();

        Denied(() => Write(ex, db, "UPDATE acme SET total = (SELECT total FROM orders WHERE id = 2) WHERE id = 1", writer));
        Denied(() => Write(ex, db, "DELETE FROM acme WHERE id IN (SELECT id FROM orders WHERE customer = 'globex')", writer));
    }
}
