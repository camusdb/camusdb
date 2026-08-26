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
/// <c>TRUNCATE</c> requires <c>DELETE</c> <b>and</b> <c>DROP</c> on the target.
///
/// <para>The pair is checked one privilege at a time, and the test that proves it is the one where
/// the two arrive in separate <c>GRANT</c> statements: a privilege test asks whether a single grant
/// record carries the whole requested mask, so a combined <c>Delete | Drop</c> mask would refuse a
/// principal who legitimately holds both.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test, like the other auth fixtures.
[NonParallelizable]
internal sealed class TestTruncateAuthorization : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? principal)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: principal));

    private static async Task Ddl(CommandExecutor ex, string db, string sql, Principal? principal)
    {
        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, principal));
        await descriptor.Transactions.CommitAsync(tx);
    }

    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE robots (id int64 PRIMARY KEY NOT NULL, name STRING NULL)", root);
        return (db, ex, root);
    }

    /// <summary>Runs the statement through the DDL entry point, which is what publishes the scope.</summary>
    private static Task Truncate(CommandExecutor ex, string db, Principal? principal)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: "TRUNCATE TABLE robots", parameters: null, principal: principal));

    private static void AssertDenied(Func<Task> act)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, exception.Code);
    }

    [Test]
    public async Task Superuser_MayTruncate()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        await Truncate(ex, db, root);

        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        Assert.AreEqual(1, descriptor.Schema.Tables["robots"].ContentsGeneration);
    }

    [Test]
    public async Task SelectAndInsertOnly_IsRefused()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT, INSERT ON {db}.robots TO reader", root);

        Principal reader = await Login(ex, "reader", "pw");
        AssertDenied(() => Truncate(ex, db, reader));
    }

    [Test]
    public async Task DeleteWithoutDrop_IsRefused()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER deleter IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT DELETE ON {db}.robots TO deleter", root);

        Principal deleter = await Login(ex, "deleter", "pw");
        AssertDenied(() => Truncate(ex, db, deleter));
    }

    [Test]
    public async Task DropWithoutDelete_IsRefused()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER dropper IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT DROP ON {db}.robots TO dropper", root);

        Principal dropper = await Login(ex, "dropper", "pw");
        AssertDenied(() => Truncate(ex, db, dropper));
    }

    [Test]
    public async Task DeleteAndDropInOneGrant_IsAllowed()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER combined IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT DELETE, DROP ON {db}.robots TO combined", root);

        Principal combined = await Login(ex, "combined", "pw");
        await Truncate(ex, db, combined);

        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        Assert.AreEqual(1, descriptor.Schema.Tables["robots"].ContentsGeneration);
    }

    [Test]
    public async Task DeleteAndDropInSeparateGrants_IsAllowed()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER split IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT DELETE ON {db}.robots TO split", root);
        await ServerDdl(ex, $"GRANT DROP ON {db}.robots TO split", root);

        Principal split = await Login(ex, "split", "pw");
        await Truncate(ex, db, split);

        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        Assert.AreEqual(1, descriptor.Schema.Tables["robots"].ContentsGeneration);
    }
}
