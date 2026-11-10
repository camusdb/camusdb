/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Who may create, replace, drop and give away an inline function, with authentication on. A user who
/// is not a superuser creates one with the <c>CREATE</c> privilege on the database; after that the
/// function has an owner, and only the owner or a superuser changes it.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestInlineFunctionPrivileges : BaseTest
{
    private const string Body = "RETURNS INT64 LANGUAGE csharp AS $$ return n + 1; $$";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        InlineFunctionsEnabled = true,
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
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            List<QueryResultRow> rows = [];

            await foreach (QueryResultRow row in cursor)
                rows.Add(row);

            await d.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<string> Code(Func<Task> action) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await action())!.Code;

    private async Task<(CommandExecutor Executor, string Database, Principal Root)> Start()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "fnauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL)", root);
        return (ex, db, root);
    }

    [Test]
    public async Task CreateFunction_NeedsTheCreatePrivilege_AndNotASuperuser()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER dev IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO dev", root);
        Principal reader = await Login(ex, "dev", "pw");

        CamusDBException denied = Assert.ThrowsAsync<CamusDBException>(() => Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", reader))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, denied.Code);
        StringAssert.Contains("Create", denied.Message);

        await ServerDdl(ex, $"GRANT CREATE ON {db}.* TO dev", root);
        Principal dev = await Login(ex, "dev", "pw");
        Assert.IsFalse(dev.IsSuperuser);

        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", dev);

        InlineFunctionDefinition record = (await ex.OpenDatabase(db)).Schema.Functions["bump"];
        Assert.AreEqual("dev", record.Owner);
        Assert.AreEqual(dev.UserId, record.OwnerId);

        Assert.AreEqual(8, (await Query(ex, db, "SELECT bump(7) AS v", dev))[0].Row["v"].LongValue);

        // ALL PRIVILEGES on the database holds CREATE.
        await ServerDdl(ex, "CREATE USER lead IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALL PRIVILEGES ON {db}.* TO lead", root);
        await Ddl(ex, db, $"CREATE FUNCTION lead_fn(n INT64) {Body}", await Login(ex, "lead", "pw"));

        // The body is still checked: the privilege gives no way around the list.
        Assert.AreEqual(
            CamusDBErrorCodes.InlineFunctionRefused,
            await Code(() => Ddl(ex, db, "CREATE FUNCTION leak(p STRING) RETURNS STRING LANGUAGE csharp AS $$ return System.IO.File.ReadAllText(p); $$", dev)));
    }

    [Test]
    public async Task OnlyTheOwnerOrASuperuser_ReplacesDropsOrGivesAwayAFunction()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER owner IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, "CREATE USER other IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE, SELECT ON {db}.* TO owner", root);
        await ServerDdl(ex, $"GRANT ALL PRIVILEGES ON {db}.* TO other", root);

        Principal owner = await Login(ex, "owner", "pw");
        Principal other = await Login(ex, "other", "pw");

        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", owner);

        // The other user holds every privilege on the database, and is still not the owner.
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, $"CREATE OR REPLACE FUNCTION bump(n INT64) {Body}", other)));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP FUNCTION bump", other)));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP FUNCTION IF EXISTS bump", other)));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "ALTER FUNCTION bump OWNER TO other", other)));

        // Any user who can run the statement can call the function.
        Assert.AreEqual(8, (await Query(ex, db, "SELECT bump(7) AS v", other))[0].Row["v"].LongValue);

        // The owner replaces it, and stays the owner.
        await Ddl(ex, db, "CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 10; $$", owner);
        Assert.AreEqual(17, (await Query(ex, db, "SELECT bump(7) AS v", owner))[0].Row["v"].LongValue);
        Assert.AreEqual(owner.UserId, (await ex.OpenDatabase(db)).Schema.Functions["bump"].OwnerId);

        // A superuser replaces it too, and the owner does not change.
        await Ddl(ex, db, "CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 20; $$", root);
        Assert.AreEqual(owner.UserId, (await ex.OpenDatabase(db)).Schema.Functions["bump"].OwnerId);

        // The owner gives it away; after that the old owner is like anyone else.
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, await Code(() => Ddl(ex, db, "ALTER FUNCTION bump OWNER TO nobody", owner)));
        await Ddl(ex, db, "ALTER FUNCTION bump OWNER TO other", owner);

        InlineFunctionDefinition given = (await ex.OpenDatabase(db)).Schema.Functions["bump"];
        Assert.AreEqual("other", given.Owner);
        Assert.AreEqual(other.UserId, given.OwnerId);
        Assert.AreEqual(2, given.Generation, "an owner change is not a new body");

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP FUNCTION bump", owner)));
        await Ddl(ex, db, "DROP FUNCTION bump", other);
    }

    [Test]
    public async Task AfterTheOwnerIsDropped_OnlyASuperuserChangesTheFunction()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER owner IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE ON {db}.* TO owner", root);
        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", await Login(ex, "owner", "pw"));

        await ServerDdl(ex, "DROP USER owner", root);

        // A new account with the old name is another account: it has another id.
        await ServerDdl(ex, "CREATE USER owner IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALL PRIVILEGES ON {db}.* TO owner", root);
        Principal impostor = await Login(ex, "owner", "pw");

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP FUNCTION bump", impostor)));
        Assert.AreEqual(8, (await Query(ex, db, "SELECT bump(7) AS v", impostor))[0].Row["v"].LongValue, "the function still runs");

        await Ddl(ex, db, "DROP FUNCTION bump", root);
    }

    [Test]
    public async Task ShowFunctions_NeedsSelectOrCreateOnTheDatabase()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();
        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", root);

        await ServerDdl(ex, "CREATE USER tableonly IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO tableonly", root);
        Principal tableOnly = await Login(ex, "tableonly", "pw");

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Query(ex, db, "SHOW FUNCTIONS", tableOnly)));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Query(ex, db, "SHOW CREATE FUNCTION bump", tableOnly)));

        await ServerDdl(ex, "CREATE USER maker IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE ON {db}.* TO maker", root);
        Principal maker = await Login(ex, "maker", "pw");

        Assert.AreEqual(1, (await Query(ex, db, "SHOW FUNCTIONS", maker)).Count);
        Assert.AreEqual("root", (await Query(ex, db, "SHOW FUNCTIONS", maker))[0].Row["owner"].StrValue);
        StringAssert.StartsWith("CREATE FUNCTION bump(n INT64)", (await Query(ex, db, "SHOW CREATE FUNCTION bump", maker))[0].Row["create function"].StrValue);
    }
}
