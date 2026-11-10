/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
/// Whose rights an inline trigger's SQL runs with, with authentication on: its owner's, never the
/// rights of the user whose statement fired it. A writer who may not touch the audit table still
/// writes to it through a trigger whose owner may; an owner who may not cannot borrow a writer's or a
/// superuser's rights; an owner who is gone fails the fire closed with
/// <see cref="CamusDBErrorCodes.TriggerOwnerMissing"/>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestInlineTriggerPrivileges : BaseTest
{
    private const string AuditBody =
        "AFTER INSERT LANGUAGE csharp AS $$ await Sql.Set(\"id\", NEW!.id).ExecuteAsync(\"INSERT INTO audit_log (post_id) VALUES (@id)\"); $$";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        InlineFunctionsEnabled = true,
        InlineTriggersEnabled = true,
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

    private static async Task Dml(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();

        try
        {
            await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> Count(CommandExecutor ex, string db, string sql, Principal p)
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
            return rows[0].Row.Values.Single().LongValue;
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
        string db = "tgauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE posts (id INT64 PRIMARY KEY NOT NULL, title STRING)", root);
        await Ddl(ex, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), post_id INT64)", root);

        // The writer may insert posts and read them, and has no right on the audit table.
        await ServerDdl(ex, "CREATE USER writer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT INSERT, SELECT ON {db}.posts TO writer", root);
        return (ex, db, root);
    }

    [Test]
    public async Task CreateTrigger_NeedsAlterOnTheTable_AndRecordsTheOwner()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        Principal writer = await Login(ex, "writer", "pw");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, writer)));

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        Principal author = await Login(ex, "author", "pw");

        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, author);

        TriggerDefinition record = (await ex.OpenDatabase(db)).Schema.Tables["posts"].Triggers!.Single();
        Assert.AreEqual("author", record.Owner);
        Assert.AreEqual(author.UserId, record.OwnerId);
    }

    [Test]
    public async Task TheBodysSqlRunsAsTheOwner_SoAWriterWithoutRightsOnTheAuditTableStillWritesToIt()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.audit_log TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        Principal writer = await Login(ex, "writer", "pw");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Dml(ex, db, "INSERT INTO audit_log (post_id) VALUES (0)", writer)),
            "the writer may not write the audit table directly");

        await Dml(ex, db, "INSERT INTO posts (id, title) VALUES (1, 'a')", writer);

        Assert.AreEqual(1, await Count(ex, db, "SELECT COUNT(*) FROM audit_log WHERE post_id = 1", root));
    }

    [Test]
    public async Task AnOwnerWithoutTheRight_CannotBorrowItFromTheWriterOrASuperuser()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        // The author may alter posts, and may not write the audit table.
        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        // The writer may write both tables; the trigger still runs as its author.
        await ServerDdl(ex, $"GRANT INSERT ON {db}.audit_log TO writer", root);
        Principal writer = await Login(ex, "writer", "pw");

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Dml(ex, db, "INSERT INTO posts (id, title) VALUES (1, 'a')", writer)));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Dml(ex, db, "INSERT INTO posts (id, title) VALUES (2, 'b')", root)),
            "a superuser's statement lends the trigger nothing either");

        Assert.AreEqual(0, await Count(ex, db, "SELECT COUNT(*) FROM posts", root), "the failed fire rolls the row back");
    }

    [Test]
    public async Task AnOwnerWhoIsGone_FailsTheFireClosed()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.audit_log TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        await ServerDdl(ex, "DROP USER author", root);

        // A new account under the old name is another account, with another id.
        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALL PRIVILEGES ON {db}.* TO author", root);

        Principal writer = await Login(ex, "writer", "pw");

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() => Dml(ex, db, "INSERT INTO posts (id, title) VALUES (1, 'a')", writer))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerOwnerMissing, missing.Code);
        StringAssert.Contains("'author'", missing.Message);

        Assert.AreEqual(CamusDBErrorCodes.TriggerOwnerMissing, await Code(() => Dml(ex, db, "INSERT INTO posts (id, title) VALUES (2, 'b')", root)),
            "a superuser's statement does not fall back to its own rights");

        // A superuser drops the trigger, and the table takes writes again.
        await Ddl(ex, db, "DROP TRIGGER audit ON posts", root);
        await Dml(ex, db, "INSERT INTO posts (id, title) VALUES (3, 'c')", writer);
    }

    private static async Task<List<TriggerDefinition>?> Triggers(CommandExecutor ex, string db) =>
        (await ex.OpenDatabase(db)).Schema.Tables["posts"].Triggers;

    [Test]
    public async Task TheOwner_DropsItsTrigger_WithoutAlter()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        // SELECT keeps the database visible to the author after ALTER is revoked.
        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER, SELECT ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        await ServerDdl(ex, $"REVOKE ALTER ON {db}.posts FROM author", root);
        Principal author = await Login(ex, "author", "pw");

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege,
            await Code(() => Ddl(ex, db, "ALTER TABLE posts ADD COLUMN extra STRING", author)), "the author holds no ALTER now");

        await Ddl(ex, db, "DROP TRIGGER audit ON posts", author);

        Assert.IsNull(await Triggers(ex, db));
    }

    [Test]
    public async Task AUserWithoutAlter_WhoIsNotTheOwner_CannotDropTheTrigger()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        Principal writer = await Login(ex, "writer", "pw");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP TRIGGER audit ON posts", writer)));

        // A trigger name the table does not have gives the writer the same answer, so the refusal
        // tells nothing about which triggers exist.
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP TRIGGER missing ON posts", writer)));

        Assert.AreEqual(1, (await Triggers(ex, db))!.Count);
    }

    [Test]
    public async Task AUserWithAlter_DropsATriggerOfAnotherOwner()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        await ServerDdl(ex, "CREATE USER admin IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO admin", root);

        await Ddl(ex, db, "DROP TRIGGER audit ON posts", await Login(ex, "admin", "pw"));

        Assert.IsNull(await Triggers(ex, db));
    }

    [Test]
    public async Task TheOwnerOfAnOldTrigger_CannotDropANewTriggerOfTheSameName()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER, SELECT ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        await ServerDdl(ex, $"REVOKE ALTER ON {db}.posts FROM author", root);
        Principal author = await Login(ex, "author", "pw");

        // A superuser replaces the trigger with one of its own under the same name.
        await Ddl(ex, db, "DROP TRIGGER audit ON posts", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, root);

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, await Code(() => Ddl(ex, db, "DROP TRIGGER audit ON posts", author)));
        Assert.AreEqual("root", (await Triggers(ex, db))!.Single().Owner);
    }

    [Test]
    public async Task ANewAccountWithTheOldOwnersName_CannotDropTheTrigger()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER audit ON posts " + AuditBody, await Login(ex, "author", "pw"));

        await ServerDdl(ex, "DROP USER author", root);
        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.posts TO author", root);

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege,
            await Code(async () => await Ddl(ex, db, "DROP TRIGGER audit ON posts", await Login(ex, "author", "pw"))));
        Assert.AreEqual(1, (await Triggers(ex, db))!.Count);
    }

    [Test]
    public async Task ABeforeTriggersRowChange_NeedsNoRightOfTheOwner()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        // The author may alter posts and holds no INSERT or UPDATE on it.
        await ServerDdl(ex, "CREATE USER author IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO author", root);
        await Ddl(ex, db, "CREATE TRIGGER upper ON posts BEFORE INSERT LANGUAGE csharp AS $$ NEW!.title = NEW.title?.ToUpperInvariant(); $$",
            await Login(ex, "author", "pw"));

        Principal writer = await Login(ex, "writer", "pw");
        await Dml(ex, db, "INSERT INTO posts (id, title) VALUES (1, 'abc')", writer);

        Assert.AreEqual(1, await Count(ex, db, "SELECT COUNT(*) FROM posts WHERE title = 'ABC'", root));
    }
}
