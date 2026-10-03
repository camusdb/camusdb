/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A foreign key needs SELECT on the table it references. Each child insert answers whether a parent
/// key exists, so a constraint on a table the caller cannot read would be a way to read it.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestForeignKeyPrivileges : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private const string ChildSql = "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))";

    [Test]
    public async Task ReferencingATableWithoutSelectIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await TxnDdl(ex, db, ChildSql, user))!;

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, exception.Code);
        Assert.That(exception.Message, Does.Contain("cities"));
        Assert.IsFalse((await ex.OpenDatabase(db)).Schema.Tables.ContainsKey("weather"));
    }

    [Test]
    public async Task ReferencingATableWithSelectIsAllowed()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup();
        Principal root = await Login(ex, "root", "root-pw");
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cities TO u", root);
        user = await Login(ex, "u", "pw");

        await TxnDdl(ex, db, ChildSql, user);

        Assert.IsNotNull((await ex.OpenDatabase(db)).Schema.Tables["weather"].ForeignKeys);
    }

    [Test]
    public async Task SelfReferenceNeedsNoExtraGrant()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup();

        await TxnDdl(ex, db, "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees)", user);

        Assert.IsNotNull((await ex.OpenDatabase(db)).Schema.Tables["employees"].ForeignKeys);
    }

    /// <summary>
    /// ALTER TABLE ... ADD CONSTRAINT needs the same SELECT on the parent as CREATE TABLE. The ALTER
    /// privilege on the child is not enough.
    /// </summary>
    [Test]
    public async Task AddingAConstraintWithoutSelectOnTheParentIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup();
        Principal root = await Login(ex, "root", "root-pw");
        await TxnDdl(ex, db, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)", user);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.* TO u", root);
        user = await Login(ex, "u", "pw");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await TxnDdl(ex, db, AlterSql, user))!;

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, exception.Code);
        Assert.That(exception.Message, Does.Contain("cities"));
        Assert.IsNull((await ex.OpenDatabase(db)).Schema.Tables["weather"].ForeignKeys);
    }

    [Test]
    public async Task AddingAConstraintWithSelectOnTheParentIsAllowed()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup();
        Principal root = await Login(ex, "root", "root-pw");
        await TxnDdl(ex, db, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)", user);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.* TO u", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.cities TO u", root);
        user = await Login(ex, "u", "pw");

        await TxnDdl(ex, db, AlterSql, user);

        Assert.AreEqual("weather_city_fk", (await ex.OpenDatabase(db)).Schema.Tables["weather"].ForeignKeys!.Single().Name);
    }

    private const string AlterSql = "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)";

    /// <summary>Creates the database and cities as root, and a user u who may only create tables.</summary>
    private async Task<(string db, CommandExecutor ex, Principal user)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "fkauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))", root);
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO u", root);

        return (db, ex, await Login(ex, "u", "pw"));
    }

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
}
