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

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Inline SQL functions on a single node, through SQL: <c>CREATE [OR REPLACE] FUNCTION</c>,
/// <c>DROP FUNCTION</c>, the calls from queries, views, CHECK constraints and defaults, the one name
/// space with built-in and extension functions, the dependency rules, the records after a reload, and
/// the <c>SHOW</c> statements.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInlineFunctions : InlineFunctionTestBase
{
    [Test]
    public async Task CreateCallAndDrop()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db, Slugify);
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify(' A B ')"));
        Assert.AreEqual("x-y", await Text(executor, db, "SELECT SLUGIFY('X Y')"), "the name is case-insensitive");
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT slugify(NULL)")).Type, "the default NULL rule: NULL in, NULL out");

        await ExecDdl(executor, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'C D'), (2, 'E F')");

        Assert.AreEqual("c-d", await Text(executor, db, "SELECT slugify(title) FROM posts WHERE id = 1"));
        Assert.AreEqual(2, await Number(executor, db, "SELECT id FROM posts WHERE slugify(title) = 'e-f'"));
        Assert.AreEqual("c-d,e-f", string.Join(",", (await ExecQuery(executor, db, "SELECT slugify(title) AS slug FROM posts ORDER BY slug")).Select(r => r["slug"].StrValue)));

        Assert.IsTrue(database.Schema.Functions.ContainsKey("SLUGIFY"));

        await ExecDdl(executor, db, "DROP FUNCTION slugify");

        CamusDBException gone = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT slugify('x')"))!;
        StringAssert.Contains("Function not found", gone.Message);

        Assert.AreEqual(CamusDBErrorCodes.FunctionNotFound, await Code(() => ExecDdl(executor, db, "DROP FUNCTION slugify")));
        await ExecDdl(executor, db, "DROP FUNCTION IF EXISTS slugify");
    }

    [Test]
    public async Task AFunctionBelongsToOneDatabase()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        (string other, _, _) = await CreateDatabaseWith(executor);

        await ExecDdl(executor, db, Slugify);

        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
        StringAssert.Contains("Function not found", Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, other, "SELECT slugify('A B')"))!.Message);

        // The same name with another body in the other database.
        await ExecDdl(executor, other, "CREATE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.ToUpperInvariant(); $$");

        Assert.AreEqual("A B", await Text(executor, other, "SELECT slugify('A B')"));
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"), "the scope follows the statement's database");
    }

    [Test]
    public async Task CreateOrReplace_ChangesTheBody_AndKeepsTheFunction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db, "CREATE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 1; $$");
        Assert.AreEqual(8, await Number(executor, db, "SELECT bump(7)"));

        Assert.AreEqual(
            CamusDBErrorCodes.FunctionAlreadyExists,
            await Code(() => ExecDdl(executor, db, "CREATE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 2; $$")));

        InlineFunctionDefinition before = database.Schema.Functions["bump"];

        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 100; $$");
        Assert.AreEqual(107, await Number(executor, db, "SELECT bump(7)"));

        InlineFunctionDefinition after = database.Schema.Functions["bump"];
        Assert.AreNotSame(before, after, "a replace publishes a new record");
        Assert.AreEqual(before.CreatedAt, after.CreatedAt);
        Assert.AreEqual(1, after.Generation);

        // OR REPLACE creates a function that does not exist.
        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION fresh() RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$");
        Assert.AreEqual(1, await Number(executor, db, "SELECT fresh()"));
        Assert.AreEqual(0, database.Schema.Functions["fresh"].Generation);
    }

    [Test]
    public async Task NullRules_AndTypes()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db,
            "CREATE FUNCTION label(s STRING, n INT64) RETURNS STRING CALLED ON NULL INPUT LANGUAGE csharp AS $$ return (s ?? \"none\") + \":\" + (n ?? -1); $$");
        Assert.AreEqual("none:-1", await Text(executor, db, "SELECT label(NULL, NULL)"));
        Assert.AreEqual("a:5", await Text(executor, db, "SELECT label('a', 5)"));

        await ExecDdl(executor, db,
            "CREATE FUNCTION strict(s STRING) RETURNS STRING RETURNS NULL ON NULL INPUT LANGUAGE csharp AS $$ return \"ran\"; $$");
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT strict(NULL)")).Type);

        await ExecDdl(executor, db,
            "CREATE FUNCTION total(xs ARRAY(INT64)) RETURNS INT64 LANGUAGE csharp AS $$ return xs.Sum(x => x ?? 0); $$");
        Assert.AreEqual(6, await Number(executor, db, "SELECT total(ARRAY[1, 2, 3])"));

        await ExecDdl(executor, db,
            "CREATE FUNCTION words(s STRING) RETURNS ARRAY(STRING) LANGUAGE csharp AS $$ return s.Split(' '); $$");
        Assert.AreEqual(3, (await Scalar(executor, db, "SELECT words('a b c')")).ArrayValues!.Count);

        await ExecDdl(executor, db,
            "CREATE FUNCTION mix(b BOOL, f FLOAT64, d NUMERIC, u UUID) RETURNS STRING LANGUAGE csharp AS $$ return $\"{b}|{f}|{d:0.##}|{u}\"; $$");
        Assert.AreEqual(
            "True|1.5|2.25|6f9619ff-8b86-d011-b42d-00c04fc964ff",
            await Text(executor, db, "SELECT mix(true, 1.5, CAST(2.25 AS NUMERIC), CAST('6f9619ff-8b86-d011-b42d-00c04fc964ff' AS UUID))"));

        // The wrong number of arguments, and a wrong type, fail the call and not the server.
        Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT total()"));
        Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT total('x')"));

        // Types outside the accepted set are refused with the list.
        foreach (string type in new[] { "INT", "SMALLINT", "DATE", "STRING(10)", "ARRAY(BYTES)", "OID" })
        {
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
                () => ExecDdl(executor, db, $"CREATE FUNCTION bad(x {type}) RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$"))!;
            Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, refused.Code, type);
        }

        Assert.AreEqual(
            CamusDBErrorCodes.InvalidInput,
            await Code(() => ExecDdl(executor, db, "CREATE FUNCTION py(x INT64) RETURNS INT64 LANGUAGE python AS $$ return 1 $$")));
    }

    [Test]
    public async Task ARefusedBody_ReachesNeitherTheSchemaNorTheLog()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long version = database.Schema.SchemaVersion;

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db,
            "CREATE FUNCTION leak(path STRING) RETURNS STRING LANGUAGE csharp AS $$ return System.IO.File.ReadAllText(path); $$"))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, refused.Code);
        StringAssert.Contains("File", refused.Message);

        CamusDBException broken = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db,
            "CREATE FUNCTION broken(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return missing; $$"))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, broken.Code);
        StringAssert.Contains("CS0103", broken.Message);

        Assert.IsEmpty(database.Schema.Functions);
        Assert.AreEqual(version, database.Schema.SchemaVersion, "a refused body proposes nothing");
    }

    [Test]
    public async Task OneNameSpace_WithBuiltInsAndExtensions()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good") with { InlineFunctionsEnabled = true });

        foreach (string taken in new[] { "upper", "count", "sum", "generate_series", "nextval", "fx_slugify" })
        {
            CamusDBException collision = Assert.ThrowsAsync<CamusDBException>(
                () => ExecDdl(executor, db, $"CREATE FUNCTION {taken}(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s; $$"))!;

            Assert.AreEqual(CamusDBErrorCodes.FunctionAlreadyExists, collision.Code, taken);
        }

        Assert.AreEqual(
            CamusDBErrorCodes.InvalidInput,
            await Code(() => ExecDdl(executor, db, "CREATE FUNCTION a.b(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s; $$")),
            "a function name is a plain identifier");
    }

    [Test]
    public async Task AnExtension_CannotBringANameAnInlineFunctionHas()
    {
        // An engine that loads no extension creates the function; an engine on the same storage that
        // loads the extension then tries to install it. The rule is checked against the replicated
        // records, so it holds between two nodes with different files.
        (string db, _, CommandExecutor plain) = await CreateDatabase();
        await ExecDdl(plain, db, "CREATE FUNCTION fx_slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s; $$");

        CommandExecutor withGood = CreateCommandExecutor(WithExtensions("Fixture.Good") with { InlineFunctionsEnabled = true });

        CamusDBException conflict = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(withGood, db, "CREATE EXTENSION good"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionMemberConflict, conflict.Code);
        StringAssert.Contains("fx_slugify", conflict.Message);

        await ExecDdl(withGood, db, "DROP FUNCTION fx_slugify");
        await ExecDdl(withGood, db, "CREATE EXTENSION good");
        Assert.AreEqual("a-b", await Text(withGood, db, "SELECT fx_slugify('A B')"));
    }

    [Test]
    public async Task ViewsAndCheckConstraints_CallTheFunction_AndBlockItsDrop()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db, Slugify);
        await ExecDdl(executor, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING, CONSTRAINT no_admin CHECK (slugify(title) <> 'admin'))");
        await ExecDdl(executor, db, "CREATE VIEW slugs AS SELECT id, slugify(title) AS slug FROM posts");

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'Hello World')");
        Assert.AreEqual("hello-world", await Text(executor, db, "SELECT slug FROM slugs WHERE id = 1"));

        Assert.AreEqual(
            CamusDBErrorCodes.CheckConstraintViolation,
            await Code(() => ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, ' ADMIN ')")));

        CamusDBException blocked = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "DROP FUNCTION slugify"))!;
        Assert.AreEqual(CamusDBErrorCodes.FunctionHasDependents, blocked.Code);
        StringAssert.Contains("view slugs", blocked.Message);
        StringAssert.Contains("CHECK constraint no_admin on table posts", blocked.Message);

        Assert.AreEqual(CamusDBErrorCodes.FunctionHasDependents, await Code(() => ExecDdl(executor, db, "DROP FUNCTION IF EXISTS slugify")),
            "IF EXISTS forgives only a function that does not exist");

        // A replace that changes the signature is refused for the same reason.
        Assert.AreEqual(
            CamusDBErrorCodes.FunctionHasDependents,
            await Code(() => ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS INT64 LANGUAGE csharp AS $$ return s.Length; $$")));

        Assert.AreEqual(
            CamusDBErrorCodes.FunctionHasDependents,
            await Code(() => ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION slugify(s STRING, sep STRING) RETURNS STRING LANGUAGE csharp AS $$ return s; $$")));

        // A replace that keeps the signature but changes the body is refused while a CHECK calls the
        // function: the new body would change what the constraint accepts, with no check of the rows
        // already stored. The owner of a function needs no right on the table.
        const string NewBody = "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.Trim().ToUpperInvariant().Replace(' ', '_'); $$";

        CamusDBException bodyBlocked = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, NewBody))!;
        Assert.AreEqual(CamusDBErrorCodes.FunctionHasDependents, bodyBlocked.Code);
        StringAssert.Contains("CHECK constraint no_admin on table posts", bodyBlocked.Message);
        Assert.AreEqual("hello-world", await Text(executor, db, "SELECT slug FROM slugs WHERE id = 1"), "the refused replace changed nothing");

        // The same body again is not a change, so it passes.
        await ExecDdl(executor, db, "CREATE OR REPLACE " + Slugify["CREATE ".Length..]);

        // Without the CHECK, the replace passes and the view sees the new body. Adding the CHECK again
        // checks the stored rows against the new body.
        await ExecDdl(executor, db, "ALTER TABLE posts DROP CONSTRAINT no_admin");
        await ExecDdl(executor, db, NewBody);

        Assert.AreEqual("HELLO_WORLD", await Text(executor, db, "SELECT slug FROM slugs WHERE id = 1"));
        await ExecDdl(executor, db, "ALTER TABLE posts ADD CONSTRAINT no_admin CHECK (slugify(title) <> 'ADMIN')");
        Assert.AreEqual(
            CamusDBErrorCodes.CheckConstraintViolation,
            await Code(() => ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, ' ADMIN ')")));

        await ExecDdl(executor, db, "DROP VIEW slugs");
        await ExecDdl(executor, db, "ALTER TABLE posts DROP CONSTRAINT no_admin");
        await ExecDdl(executor, db, "DROP FUNCTION slugify");
    }

    [Test]
    public async Task ADefault_IsComputedOnceFromTheFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db, Slugify);
        await ExecDdl(executor, db, "CREATE TABLE d (id INT64 PRIMARY KEY, slug STRING DEFAULT (slugify('New Post')))");
        await ExecNonQuery(executor, db, "INSERT INTO d (id) VALUES (1)");

        Assert.AreEqual("new-post", await Text(executor, db, "SELECT slug FROM d WHERE id = 1"));

        // An inline function is deterministic, so its default is a constant that the table keeps:
        // the function can go, and the table still has its default.
        await ExecDdl(executor, db, "DROP FUNCTION slugify");
        await ExecNonQuery(executor, db, "INSERT INTO d (id) VALUES (2)");
        Assert.AreEqual("new-post", await Text(executor, db, "SELECT slug FROM d WHERE id = 2"));
    }

    [Test]
    public async Task Functions_SurviveAReload_AndCompileAtTheFirstCall()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, db, Slugify);

        CommandExecutor reloaded = CreateCommandExecutor();
        Assert.AreEqual("not compiled", (await ShownFunction(reloaded, db, "slugify"))!["state"].StrValue, "the load compiles nothing");
        Assert.AreEqual("a-b", await Text(reloaded, db, "SELECT slugify('A B')"), "the create was persisted");
        Assert.AreEqual("compiled", (await ShownFunction(reloaded, db, "slugify"))!["state"].StrValue);

        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return \"v2\"; $$");
        reloaded = CreateCommandExecutor();
        Assert.AreEqual("v2", await Text(reloaded, db, "SELECT slugify('A B')"), "the replace was persisted");
        Assert.AreEqual(1, (await ShownFunction(reloaded, db, "slugify"))!["generation"].LongValue);

        await ExecDdl(executor, db, "DROP FUNCTION slugify");
        reloaded = CreateCommandExecutor();
        StringAssert.Contains("Function not found", Assert.ThrowsAsync<CamusDBException>(() => Scalar(reloaded, db, "SELECT slugify('x')"))!.Message);
    }

    [Test]
    public async Task ANodeWithTheSettingOff_StillRunsAFunctionThatExists()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);

        CommandExecutor off = CreateCommandExecutor(Options with { InlineFunctionsEnabled = false });

        Assert.AreEqual("a-b", await Text(off, db, "SELECT slugify('A B')"), "a restart with the setting off must not break a view that calls a function");

        CamusDBException disabled = Assert.ThrowsAsync<CamusDBException>(
            () => ExecDdl(off, db, "CREATE FUNCTION more(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n; $$"))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionsDisabled, disabled.Code);
        StringAssert.Contains("inline_functions_enabled", disabled.Message);

        // A drop needs no compiler, so it works with the setting off.
        await ExecDdl(off, db, "DROP FUNCTION slugify");
    }

    [Test]
    public async Task ShowFunctions_AndShowCreateFunction_RoundTrip()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        (string copy, _, _) = await CreateDatabaseWith(executor);

        // A body that holds the plain dollar quote, a semicolon, quotes and a line break.
        const string body = " string quote = \"$$\";\n    return s + quote + ';' + '\\'' + n; ";

        await ExecDdl(executor, db, "CREATE FUNCTION odd(s STRING, n INT64) RETURNS STRING CALLED ON NULL INPUT LANGUAGE csharp AS $body$" + body + "$body$");
        Assert.AreEqual("a$$;'5", await Text(executor, db, "SELECT odd('a', 5)"));

        Dictionary<string, ColumnValue> row = (await ShownFunction(executor, db, "odd"))!;
        Assert.AreEqual("s STRING, n INT64", row["arguments"].StrValue);
        Assert.AreEqual("STRING", row["returns"].StrValue);
        Assert.AreEqual("called", row["null_input"].StrValue);
        Assert.AreEqual("csharp", row["language"].StrValue);
        Assert.AreEqual(0, row["generation"].LongValue);
        Assert.AreEqual(64, row["source_sha256"].StrValue!.Length);
        Assert.AreEqual("compiled", row["state"].StrValue);

        List<Dictionary<string, ColumnValue>> shown = await ExecQuery(executor, db, "SHOW CREATE FUNCTION odd");
        string create = shown.Single()["create function"].StrValue!;

        StringAssert.DoesNotContain("AS $$", create, "a body with $$ needs a tagged quote");
        StringAssert.Contains(body, create, "the body is shown exactly as written");

        // The statement SHOW CREATE gives creates the same function elsewhere.
        await ExecDdl(executor, copy, create);
        Assert.AreEqual("a$$;'5", await Text(executor, copy, "SELECT odd('a', 5)"));
        Assert.AreEqual(create, (await ExecQuery(executor, copy, "SHOW CREATE FUNCTION odd")).Single()["create function"].StrValue);

        Assert.AreEqual(CamusDBErrorCodes.FunctionNotFound, await Code(() => ExecQuery(executor, db, "SHOW CREATE FUNCTION nope")));
        Assert.IsEmpty(await ExecQuery(executor, await NewDatabase(executor), "SHOW FUNCTIONS"));
    }

    private async Task<string> NewDatabase(CommandExecutor executor) => (await CreateDatabaseWith(executor)).dbname;

    [Test]
    public async Task BoundAndPlannedStatements_SeeTheReplacedFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (1)");
        await ExecDdl(executor, db, "CREATE FUNCTION version() RETURNS STRING LANGUAGE csharp AS $$ return \"one\"; $$");

        // Repeated text reuses the parse, the binding and the plan.
        const string sql = "SELECT version() AS v FROM t WHERE id = 1";

        for (int i = 0; i < 3; i++)
            Assert.AreEqual("one", await Text(executor, db, sql));

        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION version() RETURNS STRING LANGUAGE csharp AS $$ return \"two\"; $$");
        Assert.AreEqual("two", await Text(executor, db, sql));

        await ExecDdl(executor, db, "DROP FUNCTION version");
        StringAssert.Contains("Function not found", Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, sql))!.Message);

        await ExecDdl(executor, db, "CREATE FUNCTION version() RETURNS STRING LANGUAGE csharp AS $$ return \"three\"; $$");
        Assert.AreEqual("three", await Text(executor, db, sql), "a new function of the same name is not the old one");
    }

    [Test]
    public async Task ACallOverItsBudget_FailsTheStatement_AndTheNextStatementRuns()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { InlineFunctionCallTimeoutMs = 100 });

        await ExecDdl(executor, db, "CREATE FUNCTION spin(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ while (true) { } $$");
        await ExecDdl(executor, db, "CREATE FUNCTION deep(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ long D(long x) => D(x + 1) + 1; return D(n); $$");
        await ExecDdl(executor, db, "CREATE FUNCTION fine(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n * 2; $$");

        CamusDBException spun = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT spin(1)"))!;
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, spun.Code);
        StringAssert.Contains("spin", spun.Message);

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, await Code(() => Scalar(executor, db, "SELECT deep(1)")));
        Assert.AreEqual(4, await Number(executor, db, "SELECT fine(2)"));
    }

    [Test]
    public async Task TheNumberOfFunctionsInADatabase_IsLimited()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { MaxInlineFunctionsPerDatabase = 2 });

        await ExecDdl(executor, db, "CREATE FUNCTION one() RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$");
        await ExecDdl(executor, db, "CREATE FUNCTION two() RETURNS INT64 LANGUAGE csharp AS $$ return 2; $$");

        CamusDBException full = Assert.ThrowsAsync<CamusDBException>(
            () => ExecDdl(executor, db, "CREATE FUNCTION three() RETURNS INT64 LANGUAGE csharp AS $$ return 3; $$"))!;

        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, full.Code);
        StringAssert.Contains("max_inline_functions_per_database", full.Message);

        // A replace is not one more function.
        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION two() RETURNS INT64 LANGUAGE csharp AS $$ return 22; $$");
        Assert.AreEqual(22, await Number(executor, db, "SELECT two()"));
    }

    [Test]
    public async Task ADrop_RacingACreateThatCallsTheFunction_NeverLeavesADanglingCall()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, title STRING)");
        await ExecDdl(executor, db, Slugify);

        // Even rounds race a CHECK constraint, odd rounds a view body: two of the paths that resolve
        // a function before they take the schema lock.
        for (int i = 0; i < 16; i++)
        {
            bool view = i % 2 == 1;
            string name = (view ? "v" : "c") + i;

            Task drop = ExecDdl(executor, db, "DROP FUNCTION slugify");
            Task create = ExecDdl(executor, db, view
                ? $"CREATE VIEW {name} AS SELECT slugify(title) AS slug FROM src"
                : $"CREATE TABLE {name} (id INT64 PRIMARY KEY, title STRING, CONSTRAINT k{i} CHECK (slugify(title) <> 'x'))");

            try { await drop; } catch (CamusDBException) { }
            try { await create; } catch (CamusDBException) { }

            bool dropped = !database.Schema.Functions.ContainsKey("slugify");
            bool created = view ? database.Schema.Views.ContainsKey(name) : database.Schema.Tables.ContainsKey(name);

            Assert.IsFalse(dropped && created, $"round {i}: the function was dropped while {name} calls it");

            if (created)
                await ExecDdl(executor, db, view ? $"DROP VIEW {name}" : $"DROP TABLE {name}");

            if (dropped)
                await ExecDdl(executor, db, Slugify);
        }
    }

    [Test]
    public async Task ADeltaDeliveredTwice_ChangesNothing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);

        InlineFunctionDefinition record = database.Schema.Functions["slugify"];
        long stamp = database.Schema.FunctionCatalog.Stamp;

        // The same record again is the same delta again: no error, no new record.
        CamusDB.Core.Catalogs.Apply.InlineFunctionDeltaApplier.ApplyCreateFunction(
            database.Schema, new SchemaFunctionPayload { Function = record }, database.Schema.SchemaVersion + 1);

        Assert.AreSame(record, database.Schema.Functions["slugify"]);
        Assert.AreEqual(stamp, database.Schema.FunctionCatalog.Stamp);

        // A drop for a function with another creation time is a drop of another function.
        CamusDB.Core.Catalogs.Apply.InlineFunctionDeltaApplier.ApplyDropFunction(
            database.Schema,
            new SchemaDropFunctionPayload { FunctionName = "slugify", CreatedAt = new HLCTimestamp(1, record.CreatedAt.L + 999, 7) },
            database.Schema.SchemaVersion + 1);

        Assert.IsTrue(database.Schema.Functions.ContainsKey("slugify"));

        // A replace proposed against a generation that is no longer current is a no-op.
        InlineFunctionDefinition stale = new()
        {
            Name = "slugify", Source = "return s;", SourceSha256 = "x", ReturnType = ColumnType.String,
            Parameters = record.Parameters, CreatedAt = record.CreatedAt, Generation = 6,
        };

        CamusDB.Core.Catalogs.Apply.InlineFunctionDeltaApplier.ApplyReplaceFunction(
            database.Schema, new SchemaFunctionPayload { Function = stale, FromGeneration = 5 }, database.Schema.SchemaVersion + 1);

        Assert.AreSame(record, database.Schema.Functions["slugify"]);
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
    }

    [Test]
    public async Task DroppingTheDatabase_RemovesItsFunctionRecords()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);

        string prefix = CamusDB.Core.Catalogs.Meta.MetaKeys.FunctionKeyPrefix(database.Id);
        Assert.AreEqual(1, await CountKeys(database, prefix), "the record is in the meta bucket of the database");

        // A drop without FORCE keeps the keyspace of a database for recovery; FORCE purges it.
        await executor.DropDatabase(new DropDatabaseTicket(db, ifExists: false, force: true));

        Assert.AreEqual(0, await CountKeys(database, prefix), "a forced database drop purges the function records with the rest of its meta bucket");
    }

    private async Task<int> CountKeys(DatabaseDescriptor database, string prefix)
    {
        int count = 0;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in TestNode!.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, CamusDB.Core.Catalogs.Meta.MetaKeys.MetaBucketPrefix(database.Id), null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, default))
        {
            if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }

        return count;
    }

    [Test]
    public void FunctionDdl_IsSchemaDdlForTheNonQueryEndpoint()
    {
        string[] ddl =
        [
            "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s; $$",
            "CREATE OR REPLACE FUNCTION f() RETURNS INT64 CALLED ON NULL INPUT LANGUAGE csharp AS $$ return 1; $$",
            "DROP FUNCTION f",
            "DROP FUNCTION IF EXISTS f",
            "ALTER FUNCTION f OWNER TO someone",
        ];

        foreach (string sql in ddl)
            Assert.IsTrue(StatementScope.IsSchemaDdl(SQLParserProcessor.Parse(sql).nodeType), sql);

        foreach (string sql in new[] { "SHOW FUNCTIONS", "SHOW CREATE FUNCTION f" })
            Assert.IsTrue(StatementScope.ReturnsRows(SQLParserProcessor.Parse(sql).nodeType), sql);
    }
}

/// <summary>
/// The result cache and the function stamp: a cached result that one body of a function computed is
/// not served after the function is replaced, although an inline function has no volatility flag and
/// its results are cached like those of a built-in function.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInlineFunctionResultCache : InlineFunctionTestBase
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        QueryResultCache cache = new(options, sweepIntervalMs: -1);
        return new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false, cache: cache);
    }

    private static async Task<(string Value, CacheMetadataHolder Meta)> Cached(CommandExecutor executor, string db, string sql)
    {
        CacheMetadataHolder meta = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db, sql, null), meta);

        List<QueryResultRow> rows = await cursor.ToListAsync();
        return (rows.Single().Row["v"].StrValue!, meta);
    }

    [Test]
    public async Task ACachedResult_IsServedForAFunction_AndNotAfterItsReplace()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (1)");
        await ExecDdl(executor, db, "CREATE FUNCTION version() RETURNS STRING LANGUAGE csharp AS $$ return \"one\"; $$");

        const string sql = "SELECT version() AS v FROM t {cache=fn}";

        _ = await Cached(executor, db, sql);
        (string warm, CacheMetadataHolder hit) = await Cached(executor, db, sql);
        Assert.AreEqual(QueryCacheStatus.Hit, hit.Status, $"a query that calls an inline function is cached (was {hit.Status}/{hit.BypassReason})");
        Assert.AreEqual("one", warm);

        await ExecDdl(executor, db, "CREATE OR REPLACE FUNCTION version() RETURNS STRING LANGUAGE csharp AS $$ return \"two\"; $$");

        (string after, CacheMetadataHolder meta) = await Cached(executor, db, sql);
        Assert.AreNotEqual(QueryCacheStatus.Hit, meta.Status);
        Assert.AreEqual("two", after);
    }
}
