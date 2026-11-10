/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Inline C# triggers on a single node, through the real DML path: <c>CREATE TRIGGER ... LANGUAGE
/// csharp</c>, BEFORE and AFTER on INSERT, UPDATE and DELETE, the typed <c>NEW</c> and <c>OLD</c>,
/// the function stubs, the body's SQL, veto and rollback, a new unit after a schema or function change,
/// <c>DROP TRIGGER</c>, the order against a DLL trigger, the setting, a reload, and the <c>SHOW</c>
/// statements.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInlineTriggers : InlineFunctionTestBase
{
    private const string Posts = "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING, slug STRING, price INT64)";

    private const string AuditLog = "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), seq INT64, op STRING, detail STRING)";

    private const string SlugTrigger =
        "CREATE TRIGGER slug ON posts BEFORE INSERT, UPDATE LANGUAGE csharp AS $$\n" +
        "    if (NEW!.title is { } title)\n" +
        "        NEW.slug = slugify(title);\n" +
        "    if (NEW.price is < 0)\n" +
        "        throw new ArgumentException(\"price must not be negative\");\n" +
        "$$";

    private const string AuditTrigger =
        "CREATE TRIGGER audit ON posts AFTER INSERT, UPDATE, DELETE LANGUAGE csharp AS $$\n" +
        "    var row = (NEW ?? OLD)!;\n" +
        "    await Sql.Set(\"op\", Operation.ToString())\n" +
        "             .Set(\"detail\", $\"{row.id}:{OLD?.title}>{NEW?.slug}\")\n" +
        "             .ExecuteAsync(\"INSERT INTO audit_log (op, detail) VALUES (@op, @detail)\");\n" +
        "$$";

    private async Task<(string Db, CommandExecutor Executor)> Start()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, AuditLog);
        await ExecDdl(executor, db, Slugify);
        return (db, executor);
    }

    private static async Task<List<string>> Audit(CommandExecutor executor, string db) =>
        (await ExecQuery(executor, db, "SELECT op, detail FROM audit_log"))
            .Select(row => row["op"].StrValue + " " + row["detail"].StrValue)
            .OrderBy(text => text, System.StringComparer.Ordinal)
            .ToList();

    [Test]
    public async Task Before_ChangesNew_AndTheChangedRowIsWritten()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);

        TriggerDefinition record = (await executor.OpenDatabase(db)).Schema.Tables["posts"].Triggers!.Single();
        Assert.IsTrue(record.IsInline);
        Assert.AreEqual("csharp", record.Language);
        Assert.AreEqual("", record.TypeName);
        Assert.AreEqual(64, record.SourceSha256.Length);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title, price) VALUES (1, ' Hello World ', 5), (2, 'Second Post', 7)");
        Assert.AreEqual("hello-world", await Text(executor, db, "SELECT slug FROM posts WHERE id = 1"));
        Assert.AreEqual("second-post", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"));

        await ExecNonQuery(executor, db, "UPDATE posts SET title = 'New Title' WHERE id = 1");
        Assert.AreEqual("new-title", await Text(executor, db, "SELECT slug FROM posts WHERE id = 1"));
        Assert.AreEqual("second-post", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"), "the other row is not touched");
    }

    [Test]
    public async Task ThrowVetoes_TheStatementFails_AndNoRowIsWritten()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);
        await ExecDdl(executor, db, AuditTrigger);

        CamusDBException vetoed = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO posts (id, title, price) VALUES (1, 'A', 1), (2, 'B', -1)"))!;

        Assert.AreEqual(CamusDBErrorCodes.TriggerFailed, vetoed.Code);
        StringAssert.Contains("price must not be negative", vetoed.Message);
        StringAssert.Contains("'slug'", vetoed.Message);

        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM posts"), "the first row rolls back with the statement");
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM audit_log"));
    }

    [Test]
    public async Task After_SeesTheFinalRow_AndOld_OnUpdateAndDelete()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);
        await ExecDdl(executor, db, AuditTrigger);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title, price) VALUES (1, 'A B', 1)");
        await ExecNonQuery(executor, db, "UPDATE posts SET title = 'C D' WHERE id = 1");
        await ExecNonQuery(executor, db, "DELETE FROM posts WHERE id = 1");

        CollectionAssert.AreEqual(
            new[] { "Delete 1:C D>", "Insert 1:>a-b", "Update 1:A B>c-d" },
            await Audit(executor, db),
            "AFTER sees the slug the BEFORE trigger set; OLD is null on INSERT and NEW on DELETE");
    }

    [Test]
    public async Task After_ASetterThrows_AndTheStatementRollsBack()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, "CREATE TRIGGER late ON posts AFTER INSERT LANGUAGE csharp AS $$ NEW!.slug = \"x\"; $$");

        CamusDBException failed = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A')"))!;

        Assert.AreEqual(CamusDBErrorCodes.TriggerFailed, failed.Code);
        StringAssert.Contains("only NEW in a BEFORE trigger can be changed", failed.Message);
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM posts"));
    }

    [Test]
    public async Task AColumnThatIsNoPropertyName_IsReachedThroughContext_AndAKeywordColumnWithAnAt()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE ev (id INT64 PRIMARY KEY, Timing STRING, class STRING, row INT64, note STRING)");

        await ExecDdl(executor, db,
            "CREATE TRIGGER fill ON ev BEFORE INSERT LANGUAGE csharp AS $$\n" +
            "    Context.New!.Set(\"Timing\", Timing.ToString());\n" +
            "    NEW!.@class = \"c\" + NEW.row;\n" +
            "    NEW.note = Context.New.GetString(\"Timing\");\n" +
            "$$");

        await ExecNonQuery(executor, db, "INSERT INTO ev (id, row) VALUES (1, 7)");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT Timing, class, note FROM ev");
        Assert.AreEqual("Before", rows[0]["Timing"].StrValue);
        Assert.AreEqual("c7", rows[0]["class"].StrValue);
        Assert.AreEqual("Before", rows[0]["note"].StrValue);
    }

    [Test]
    public async Task AddColumn_TheNextFireCompilesANewUnit_WithTheNewColumn()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, "CREATE TRIGGER fill ON posts BEFORE INSERT LANGUAGE csharp AS $$ NEW!.slug = \"set\"; $$");

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'a')");
        long compiled = executor.InlineFunctions.Compilations;

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'b')");
        Assert.AreEqual(compiled, executor.InlineFunctions.Compilations, "a second statement reuses the binding");

        await ExecDdl(executor, db, "ALTER TABLE posts ADD COLUMN extra STRING");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (3, 'c')");

        Assert.AreEqual(compiled + 1, executor.InlineFunctions.Compilations, "the wider table has a new row class");
        Assert.AreEqual("set", await Text(executor, db, "SELECT slug FROM posts WHERE id = 3"));

        // A body that names the new column compiles against the new row class.
        await ExecDdl(executor, db, "CREATE TRIGGER extra ON posts BEFORE INSERT LANGUAGE csharp AS $$ NEW!.extra = NEW.title + \"!\"; $$");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (4, 'd')");
        Assert.AreEqual("d!", await Text(executor, db, "SELECT extra FROM posts WHERE id = 4"));
    }

    [Test]
    public async Task ReplaceFunction_TheStubCallsTheNewBodyAtTheNextFire()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slug FROM posts WHERE id = 1"));

        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return \"v2:\" + s; $$");

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'C D')");
        Assert.AreEqual("v2:C D", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"));
    }

    [Test]
    public async Task ABodyThatCatchesItsSqlError_StillFailsTheStatement()
    {
        (string db, CommandExecutor executor) = await Start();

        await ExecDdl(executor, db,
            "CREATE TRIGGER swallow ON posts AFTER INSERT LANGUAGE csharp AS $$\n" +
            "    try { await Sql.ExecuteAsync(\"INSERT INTO no_such_table (x) VALUES (1)\"); }\n" +
            "    catch (Exception) { }\n" +
            "$$");

        CamusDBException failed = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'a')"))!;

        Assert.AreEqual(CamusDBErrorCodes.ExtensionCaughtSqlError, failed.Code);
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM posts"));
    }

    [Test]
    public async Task ABodyThatLoopsForEver_IsStoppedByTheBudget()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, "CREATE TRIGGER spin ON posts BEFORE INSERT LANGUAGE csharp AS $$ long n = 0; while (true) n++; $$");

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'a')"))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped.Code);
        StringAssert.Contains("Trigger 'spin'", stopped.Message);
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM posts"));
    }

    [Test]
    public async Task ARefusedBody_NeverReachesTheSchema()
    {
        (string db, CommandExecutor executor) = await Start();

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, await Code(() => ExecDdl(executor, db,
            "CREATE TRIGGER t1 ON posts BEFORE INSERT LANGUAGE csharp AS $$ System.IO.File.Delete(\"x\"); $$")));
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, await Code(() => ExecDdl(executor, db,
            "CREATE TRIGGER t2 ON posts AFTER INSERT LANGUAGE csharp AS $$ await Task.Delay(1); $$")));
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, await Code(() => ExecDdl(executor, db,
            "CREATE TRIGGER t3 ON posts BEFORE INSERT LANGUAGE csharp AS $$ NEW!.no_such_column = 1; $$")));

        Assert.IsNull((await executor.OpenDatabase(db)).Schema.Tables["posts"].Triggers);
    }

    [Test]
    public async Task DropTrigger_StopsTheFire()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");

        await ExecDdl(executor, db, "DROP TRIGGER slug ON posts");
        Assert.IsNull((await executor.OpenDatabase(db)).Schema.Tables["posts"].Triggers);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title, price) VALUES (2, 'C D', -1)");
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT slug FROM posts WHERE id = 2")).Type);
    }

    [Test]
    public async Task DropTrigger_GivesTheCompiledUnitBack()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");

        await ExecDdl(executor, db, "DROP TRIGGER slug ON posts");
        await ExecDdl(executor, db, SlugTrigger);
        long afterCreate = executor.InlineFunctions.Compilations;

        // Had the dropped record kept its reference, the unit of the same body would still be in the
        // runtime and the fire would reuse it.
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'C D')");
        Assert.AreEqual(afterCreate + 1, executor.InlineFunctions.Compilations, "the drop released the unit, so the fire compiles again");
        Assert.AreEqual("c-d", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"));

        // DROP TABLE gives the units of its triggers back too.
        await ExecDdl(executor, db, "DROP TABLE posts");
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, SlugTrigger);
        afterCreate = executor.InlineFunctions.Compilations;

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (3, 'E F')");
        Assert.AreEqual(afterCreate + 1, executor.InlineFunctions.Compilations);
    }

    [Test]
    public async Task AnInlineAndADllTrigger_FireInCreationOrder()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGood(
            WithExtensions("Fixture.Good") with { InlineFunctionsEnabled = true, InlineTriggersEnabled = true });

        await ExecDdl(executor, db, "CREATE TABLE t (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, wide STRING(200000))");

        // The DLL trigger upper-cases the name; the inline one, created after it, sees the upper case.
        await ExecDdl(executor, db, "CREATE TRIGGER upper ON t BEFORE INSERT EXTERNAL NAME 'CamusDB.Fixtures.Good.UppercaseTrigger'");
        await ExecDdl(executor, db, "CREATE TRIGGER suffix ON t BEFORE INSERT LANGUAGE csharp AS $$ NEW!.name = NEW.name + \"-x\"; $$");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('abc')");
        Assert.AreEqual("ABC-x", await Text(executor, db, "SELECT name FROM t"));

        await ExecDdl(executor, db, "DROP TRIGGER upper ON t");
        await ExecDdl(executor, db, "CREATE TRIGGER upper ON t BEFORE INSERT EXTERNAL NAME 'CamusDB.Fixtures.Good.UppercaseTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('def')");
        Assert.AreEqual("DEF-X", await Text(executor, db, "SELECT name FROM t WHERE name <> 'ABC-x'"), "now the DLL trigger fires second");
    }

    [Test]
    public async Task TheSettingOff_RefusesCreate_AndAnExistingTriggerStillFiresAfterAReload()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, SlugTrigger);

        CommandExecutor off = CreateCommandExecutor(Options with { InlineTriggersEnabled = false });

        await ExecNonQuery(off, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");
        Assert.AreEqual("a-b", await Text(off, db, "SELECT slug FROM posts WHERE id = 1"), "the reload kept the trigger, and it fires with the setting off");

        CamusDBException disabled = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(off, db,
            "CREATE TRIGGER more ON posts AFTER INSERT LANGUAGE csharp AS $$ return; $$"))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionsDisabled, disabled.Code);
        StringAssert.Contains("inline_triggers_enabled", disabled.Message);

        // A drop needs no compiler.
        await ExecDdl(off, db, "DROP TRIGGER slug ON posts");
    }

    [Test]
    public async Task TheBodysSqlRunsInTheStatementsTransaction()
    {
        (string db, CommandExecutor executor) = await Start();
        await ExecDdl(executor, db, AuditTrigger);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO posts (id, title) VALUES (1, 'a')", null));
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM audit_log"), "the audit row rolls back with the statement's transaction");
    }

    [Test]
    public async Task ShowTriggers_AndShowCreateTrigger_RoundTripBothForms()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGood(
            WithExtensions("Fixture.Good") with { InlineFunctionsEnabled = true, InlineTriggersEnabled = true });

        await ExecDdl(executor, db, "CREATE TABLE t (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, wide STRING(200000))");
        await ExecDdl(executor, db, "CREATE TABLE other (id INT64 PRIMARY KEY, name STRING)");
        await ExecDdl(executor, db, "CREATE TRIGGER upper ON t BEFORE INSERT, UPDATE EXTERNAL NAME 'CamusDB.Fixtures.Good.UppercaseTrigger'");

        // A body that holds $$ needs a tagged quote to come back out.
        const string body = " NEW!.name = NEW.name + \"$$\"; ";
        await ExecDdl(executor, db, "CREATE TRIGGER dollars ON t BEFORE INSERT LANGUAGE csharp AS $fn$" + body + "$fn$");
        await ExecDdl(executor, db, "CREATE TRIGGER audit ON other AFTER DELETE, INSERT LANGUAGE csharp AS $$ return; $$");

        List<Dictionary<string, ColumnValue>> all = await ExecQuery(executor, db, "SHOW TRIGGERS");
        CollectionAssert.AreEqual(new[] { "other.audit", "t.upper", "t.dollars" }, all.Select(r => r["table"].StrValue + "." + r["name"].StrValue).ToArray(),
            "ordered by table, then in creation order");

        Dictionary<string, ColumnValue> dll = all[1];
        Assert.AreEqual("extension", dll["form"].StrValue);
        Assert.AreEqual("CamusDB.Fixtures.Good.UppercaseTrigger", dll["type_name"].StrValue);
        Assert.AreEqual("good", dll["extension"].StrValue);
        Assert.AreEqual(ColumnType.Null, dll["source_sha256"].Type);
        Assert.AreEqual("BEFORE", dll["timing"].StrValue);
        Assert.AreEqual("INSERT, UPDATE", dll["events"].StrValue);

        Dictionary<string, ColumnValue> inline = all[0];
        Assert.AreEqual("csharp", inline["form"].StrValue);
        Assert.AreEqual(ColumnType.Null, inline["type_name"].Type);
        Assert.AreEqual("INSERT, DELETE", inline["events"].StrValue, "events in a fixed order");
        Assert.AreEqual(64, inline["source_sha256"].StrValue!.Length);

        Assert.AreEqual(2, (await ExecQuery(executor, db, "SHOW TRIGGERS ON t")).Count);
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, await Code(() => ExecQuery(executor, db, "SHOW TRIGGERS ON nope")));
        Assert.AreEqual(CamusDBErrorCodes.TriggerNotFound, await Code(() => ExecQuery(executor, db, "SHOW CREATE TRIGGER nope ON t")));

        string createDll = (await ExecQuery(executor, db, "SHOW CREATE TRIGGER upper ON t"))[0]["create trigger"].StrValue!;
        Assert.AreEqual("CREATE TRIGGER upper ON t BEFORE INSERT, UPDATE EXTERNAL NAME 'CamusDB.Fixtures.Good.UppercaseTrigger'", createDll);

        string createInline = (await ExecQuery(executor, db, "SHOW CREATE TRIGGER dollars ON t"))[0]["create trigger"].StrValue!;
        Assert.AreEqual("CREATE TRIGGER dollars ON t BEFORE INSERT LANGUAGE csharp AS $fn$" + body + "$fn$", createInline);

        string sha = (await executor.OpenDatabase(db)).Schema.Tables["t"].Triggers!.Single(x => x.Name == "dollars").SourceSha256;

        // Both statements create the same records again, in the same order.
        await ExecDdl(executor, db, "DROP TRIGGER upper ON t");
        await ExecDdl(executor, db, "DROP TRIGGER dollars ON t");
        await ExecDdl(executor, db, createDll);
        await ExecDdl(executor, db, createInline);

        List<TriggerDefinition> again = (await executor.OpenDatabase(db)).Schema.Tables["t"].Triggers!;
        Assert.AreEqual("CamusDB.Fixtures.Good.UppercaseTrigger", again[0].TypeName);
        Assert.AreEqual(body, again[1].Source);
        Assert.AreEqual(sha, again[1].SourceSha256);

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('ab')");
        Assert.AreEqual("AB$$", await Text(executor, db, "SELECT name FROM t"));
    }
}
