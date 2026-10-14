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
/// The server-level MONITOR privilege: <c>GRANT MONITOR ON *.* TO u</c> lets an account read node
/// stats without being a superuser, and nothing else. It is refused on any narrower scope, it is not
/// part of <c>ALL PRIVILEGES</c>, and it grants no data access and no catalog visibility — a
/// monitoring account must not be able to list every database because it may read a disk gauge.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestMonitorPrivilege : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "monitor-privilege-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? principal)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: principal));

    private static async Task TxnDdl(CommandExecutor ex, string db, string sql, Principal? principal)
    {
        DatabaseDescriptor database = await ex.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, principal));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<string>> Column(CommandExecutor ex, string db, string sql, string column, Principal? principal, bool inTransaction)
    {
        KvTransaction? tx = null;
        DatabaseDescriptor? database = null;

        if (inTransaction)
        {
            database = await ex.OpenDatabase(db);
            tx = await database.Transactions.BeginAsync();
        }

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx!, db, sql, null, principal));

            List<string> values = [];
            await foreach (QueryResultRow row in cursor)
                values.Add(row.Row[column].StrValue!);

            if (tx is not null)
                await database!.Transactions.CommitAsync(tx);

            return values;
        }
        catch
        {
            if (tx is not null)
                await database!.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    /// <summary>A database with one table, a superuser principal, and a plain account <c>mon</c> with no grants.</summary>
    private async Task<(string db, CommandExecutor ex, Principal root)> SetupAsync()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "mondb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, name string NULL)", root);
        await ServerDdl(ex, "CREATE USER mon IDENTIFIED BY 'mon-pw'", root);
        return (db, ex, root);
    }

    [Test]
    public async Task GrantMonitorOnGlobalScope_LetsTheAccountMonitorAndShowsInShowGrants()
    {
        (_, CommandExecutor ex, Principal root) = await SetupAsync();

        Assert.IsFalse((await Login(ex, "mon", "mon-pw")).CanMonitorNode, "no grant yet");

        await ServerDdl(ex, "GRANT MONITOR ON *.* TO mon", root);

        Principal mon = await Login(ex, "mon", "mon-pw");
        Assert.IsTrue(mon.CanMonitorNode);
        Assert.IsFalse(mon.IsSuperuser, "MONITOR must not need, or confer, the superuser attribute");

        List<string> privileges = await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false);
        CollectionAssert.AreEqual(new[] { "MONITOR" }, privileges);
    }

    [Test]
    public async Task MonitorKeyword_IsCaseInsensitive_AndCombinesWithObjectPrivileges()
    {
        (_, CommandExecutor ex, Principal root) = await SetupAsync();

        await ServerDdl(ex, "GRANT select, monitor ON *.* TO mon", root);

        Assert.IsTrue((await Login(ex, "mon", "mon-pw")).CanMonitorNode);
        List<string> privileges = await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false);
        CollectionAssert.AreEqual(new[] { "SELECT, MONITOR" }, privileges);
    }

    [Test]
    public async Task Monitor_IsRefusedOnADatabaseOrTableScope_ForGrantAndRevoke()
    {
        (string db, CommandExecutor ex, Principal root) = await SetupAsync();

        foreach (string sql in new[]
        {
            $"GRANT MONITOR ON {db}.* TO mon",
            $"GRANT MONITOR ON {db}.items TO mon",
            $"GRANT SELECT, MONITOR ON {db}.* TO mon",
            $"REVOKE MONITOR ON {db}.* FROM mon",
        })
        {
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => ServerDdl(ex, sql, root), sql)!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidPrivilege, refused.Code, sql);
        }

        // Nothing was written by the refused statements.
        Assert.IsFalse((await Login(ex, "mon", "mon-pw")).CanMonitorNode);
        CollectionAssert.IsEmpty(await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false));
    }

    [Test]
    public async Task AllPrivileges_DoesNotIncludeMonitor_AndBothRenderTogether()
    {
        (_, CommandExecutor ex, Principal root) = await SetupAsync();

        await ServerDdl(ex, "GRANT ALL PRIVILEGES ON *.* TO mon", root);
        Assert.IsFalse((await Login(ex, "mon", "mon-pw")).CanMonitorNode, "ALL is the object privileges only");
        CollectionAssert.AreEqual(new[] { "ALL PRIVILEGES" },
            await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false));

        await ServerDdl(ex, "GRANT MONITOR ON *.* TO mon", root);
        Assert.IsTrue((await Login(ex, "mon", "mon-pw")).CanMonitorNode);
        CollectionAssert.AreEqual(new[] { "ALL PRIVILEGES, MONITOR" },
            await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false));

        // REVOKE ALL removes the object privileges and leaves MONITOR, which is revoked by name.
        await ServerDdl(ex, "REVOKE ALL PRIVILEGES ON *.* FROM mon", root);
        Assert.IsTrue((await Login(ex, "mon", "mon-pw")).CanMonitorNode);

        await ServerDdl(ex, "REVOKE MONITOR ON *.* FROM mon", root);
        Assert.IsFalse((await Login(ex, "mon", "mon-pw")).CanMonitorNode);
        CollectionAssert.IsEmpty(await Column(ex, "", "SHOW GRANTS FOR mon", "privileges", root, inTransaction: false));
    }

    [Test]
    public async Task MonitorOnly_GrantsNoDataAccessAndNoCatalogVisibility()
    {
        (string db, CommandExecutor ex, Principal root) = await SetupAsync();

        await ServerDdl(ex, "CREATE USER nobody IDENTIFIED BY 'nobody-pw'", root);
        await ServerDdl(ex, "GRANT MONITOR ON *.* TO mon", root);

        Principal mon = await Login(ex, "mon", "mon-pw");
        Principal nobody = await Login(ex, "nobody", "nobody-pw");

        // A global grant would normally make every database listable; MONITOR alone must not.
        CollectionAssert.IsEmpty(await Column(ex, db, "SHOW DATABASES", "Database", mon, inTransaction: false));

        // Reading a table is refused exactly as it is for an account with no grant at all, so MONITOR
        // neither reads the row nor reveals that the database exists.
        CamusDBException monRefused = Assert.ThrowsAsync<CamusDBException>(() =>
            Column(ex, db, "SELECT name FROM items", "name", mon, inTransaction: true))!;
        CamusDBException nobodyRefused = Assert.ThrowsAsync<CamusDBException>(() =>
            Column(ex, db, "SELECT name FROM items", "name", nobody, inTransaction: true))!;

        Assert.AreEqual(nobodyRefused.Code, monRefused.Code);
        Assert.AreEqual(nobodyRefused.Message, monRefused.Message);
    }

    [Test]
    public void CanMonitorNode_CountsOnlyAGlobalGrant()
    {
        GrantRecord Grant(GrantScopeKind kind, Privilege privileges) => new()
        {
            User = "u",
            Scope = new GrantScope { Kind = kind, DatabaseId = kind == GrantScopeKind.Global ? "" : "db1", TableId = kind == GrantScopeKind.Table ? "t1" : "" },
            Privileges = privileges,
        };

        Assert.IsTrue(new Principal("root", isSuperuser: true, []).CanMonitorNode);
        Assert.IsFalse(new Principal("u", isSuperuser: false, []).CanMonitorNode);
        Assert.IsTrue(new Principal("u", false, [Grant(GrantScopeKind.Global, Privilege.Monitor)]).CanMonitorNode);
        Assert.IsFalse(new Principal("u", false, [Grant(GrantScopeKind.Global, Privilege.All)]).CanMonitorNode);

        // A narrower grant carrying the bit cannot be written by GRANT; if one exists anyway it is ignored.
        Assert.IsFalse(new Principal("u", false, [Grant(GrantScopeKind.Database, Privilege.Monitor)]).CanMonitorNode);
        Assert.IsFalse(new Principal("u", false, [Grant(GrantScopeKind.Table, Privilege.Monitor)]).CanMonitorNode);

        // And MONITOR alone makes nothing visible.
        Principal monitorOnly = new("u", false, [Grant(GrantScopeKind.Global, Privilege.Monitor)]);
        Assert.IsFalse(monitorOnly.CanSeeDatabase("db1"));
        Assert.IsFalse(monitorOnly.HasAnyPrivilege("db1", "t1"));
        Assert.IsFalse(monitorOnly.HasPrivilege(Privilege.Select, "db1", "t1"));
    }
}
