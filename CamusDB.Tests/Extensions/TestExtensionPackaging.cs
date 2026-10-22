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
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// The extension packaging layer on a single node: a loaded extension is only available until a
/// database installs it with <c>CREATE EXTENSION</c>; <c>ALTER EXTENSION ... UPDATE</c> moves every
/// member of the extension to another loaded version in one step, triggers included; <c>DROP
/// EXTENSION</c> refuses while anything depends on the extension; the records survive a reload; and a
/// node that does not load the installed build refuses only the statements that use it.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestExtensionPackaging : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Good.";

    private static async Task<string> Code(Func<Task> action) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await action())!.Code;

    private static async Task<string> Text(CommandExecutor executor, string db, string sql) =>
        (await Scalar(executor, db, sql)).StrValue!;

    // ── installed, not loaded ─────────────────────────────────────────────────

    [Test]
    public async Task AnExtensionIsCallableOnlyWhereItIsInstalled()
    {
        (string installedDb, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good"));
        (string otherDb, _, _) = await CreateDatabase(WithExtensions("Fixture.Good"));

        CamusDBException before = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, installedDb, "SELECT fx_slugify('A B')"))!;
        StringAssert.Contains("Function not found", before.Message, "loaded but not installed: not callable");

        await ExecDdl(executor, installedDb, "CREATE EXTENSION good");

        Assert.AreEqual("a-b", await Text(executor, installedDb, "SELECT fx_slugify('A B')"));

        // The same engine, the next statement, another database: the extension is not installed there.
        CamusDBException other = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, otherDb, "SELECT fx_slugify('A B')"))!;
        StringAssert.Contains("Function not found", other.Message);

        // And back again: the scope follows the statement's database, not the last one used.
        Assert.AreEqual("a-b", await Text(executor, installedDb, "SELECT fx_slugify('A B')"));

        // Procedures follow the same rule.
        Assert.AreEqual(CamusDBErrorCodes.ProcedureNotFound, await Code(() => ExecNonQuery(executor, otherDb, "CALL fx_noop()")));
        await ExecNonQuery(executor, installedDb, "CALL fx_noop()");
    }

    [Test]
    public async Task QualifiedCall_NamesTheExtension()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        Assert.AreEqual("a-b", await Text(executor, db, "SELECT good.fx_slugify('A B')"));
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT GOOD.FX_SLUGIFY('A B')"), "both parts are case-insensitive");
        Assert.AreEqual("[x]", await Text(executor, db, "SELECT good.fx_tag_alias('x')"), "an alias qualifies too");

        await ExecDdl(executor, db, "CREATE TABLE q (id INT64 PRIMARY KEY, title STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO q (id, title) VALUES (1, 'C D')");
        Assert.AreEqual("c-d", await Text(executor, db, "SELECT good.fx_slugify(title) FROM q WHERE good.fx_add(id, 0) = 1"));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT other.fx_slugify('x')"))!;
        StringAssert.Contains("Function not found", ex.Message, "the qualifier must name the installed extension");
    }

    [Test]
    public async Task ExtensionWords_StayUsableAsNames()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options);

        await ExecDdl(executor, db, "CREATE TABLE extension (id INT64 PRIMARY KEY, version STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO extension (id, version) VALUES (1, 'v')");
        Assert.AreEqual("v", await Text(executor, db, "SELECT version FROM extension"));
    }

    // ── CREATE EXTENSION ──────────────────────────────────────────────────────

    [Test]
    public async Task CreateExtension_Refusals()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(
            WithExtensions("Fixture.Good", "Fixture.Duplicate", "Fixture.Requires"));

        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotAvailable, await Code(() => ExecDdl(executor, db, "CREATE EXTENSION nope")));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotAvailable, await Code(() => ExecDdl(executor, db, "CREATE EXTENSION good VERSION '9.9.9'")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Code(() => ExecDdl(executor, db, "CREATE EXTENSION good RELEASE '1.0.0'")));

        // A required extension must be installed first.
        Assert.AreEqual(CamusDBErrorCodes.ExtensionRequirementMissing, await Code(() => ExecDdl(executor, db, "CREATE EXTENSION needs_good")));

        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        await ExecDdl(executor, db, "CREATE EXTENSION needs_good");
        Assert.AreEqual("needs:x", await Text(executor, db, "SELECT fx_needs_good('x')"));

        Assert.AreEqual(CamusDBErrorCodes.ExtensionAlreadyInstalled, await Code(() => ExecDdl(executor, db, "CREATE EXTENSION good")));

        // "duplicate" defines fx_slugify too: two installed extensions cannot share a function name.
        CamusDBException conflict = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "CREATE EXTENSION duplicate"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionMemberConflict, conflict.Code);
        StringAssert.Contains("fx_slugify", conflict.Message);

        // In another database "duplicate" installs, and its fx_slugify is the one that runs there.
        (string other, _, _) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.Duplicate", "Fixture.Requires"));
        await ExecDdl(executor, other, "CREATE EXTENSION duplicate");
        Assert.AreEqual("A B", await Text(executor, other, "SELECT fx_slugify('A B')"));
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT fx_slugify('A B')"));
    }

    [Test]
    public async Task CreateExtension_WithoutVersion_InstallsTheHighestLoadedVersion()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));

        await ExecDdl(executor, db, "CREATE EXTENSION good");

        Assert.AreEqual("2.0.0", await Text(executor, db, "SELECT fx_good_version()"));
        ExtensionDefinition record = database.Schema.Extensions["good"];
        Assert.AreEqual("2.0.0", record.Version);
        Assert.AreEqual(executor.Extensions.Find("good", "2.0.0")!.Sha256, record.Sha256);
        Assert.IsTrue(record.HasMember(ExtensionMemberKind.Function, "fx_slugify"));
        Assert.IsTrue(record.HasMember(ExtensionMemberKind.Trigger, Fixture + "AuditTrigger"));
        Assert.IsTrue(record.HasMember(ExtensionMemberKind.Procedure, "fx_transfer"));
    }

    // ── ALTER EXTENSION ... UPDATE ────────────────────────────────────────────

    [Test]
    public async Task Update_MovesFunctionsAndTriggersToTheNewBuild_WithoutRecreatingTriggers()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));

        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, build STRING)");
        await ExecDdl(executor, db, $"CREATE TRIGGER stamp ON t BEFORE INSERT EXTERNAL NAME '{Fixture}BuildStampTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (1)");
        Assert.AreEqual("1.0.0", await Text(executor, db, "SELECT fx_good_version()"));
        Assert.AreEqual("1.0.0", await Text(executor, db, "SELECT build FROM t WHERE id = 1"));

        TriggerDefinition trigger = database.Schema.Tables["t"].Triggers!.Single();

        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE");

        Assert.AreEqual("2.0.0", database.Schema.Extensions["good"].Version);
        Assert.AreEqual("2.0.0", await Text(executor, db, "SELECT fx_good_version()"));

        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (2)");
        Assert.AreEqual("2.0.0", await Text(executor, db, "SELECT build FROM t WHERE id = 2"), "the same trigger fires the new build");
        Assert.AreSame(trigger, database.Schema.Tables["t"].Triggers!.Single(), "the trigger record was not touched");

        // And back down, to a named version.
        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '1.0.0'");
        Assert.AreEqual("1.0.0", await Text(executor, db, "SELECT fx_good_version()"));

        // Updating to the installed version changes nothing and is not an error.
        long version = database.Schema.SchemaVersion;
        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '1.0.0'");
        Assert.AreEqual(version, database.Schema.SchemaVersion, "no delta is proposed");

        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotAvailable, await Code(() => ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '3.0.0'")));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotInstalled, await Code(() => ExecDdl(executor, db, "ALTER EXTENSION nope UPDATE")));
    }

    [Test]
    public async Task Update_IsRefusedWhenTheTargetLacksAMemberInUse()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));

        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, n INT64)");
        await ExecDdl(executor, db, "CREATE VIEW v AS SELECT fx_v1_only(n) AS n FROM t");
        await ExecDdl(executor, db, $"CREATE TRIGGER old ON t AFTER INSERT EXTERNAL NAME '{Fixture}V1OnlyTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '2.0.0'"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionHasDependents, ex.Code);
        StringAssert.Contains("view v", ex.Message);
        StringAssert.Contains("trigger old on table t", ex.Message);

        await ExecDdl(executor, db, "DROP VIEW v");
        await ExecDdl(executor, db, "DROP TRIGGER old ON t");
        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");
        Assert.AreEqual("2.0.0", await Text(executor, db, "SELECT fx_good_version()"));
    }

    // ── DROP EXTENSION ────────────────────────────────────────────────────────

    [Test]
    public async Task Drop_IsRefusedWhileObjectsDependOnTheExtension()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.Requires"));

        await ExecDdl(executor, db, "CREATE EXTENSION good");
        await ExecDdl(executor, db, "CREATE EXTENSION needs_good");
        await ExecDdl(executor, db, "CREATE TABLE d (id INT64 PRIMARY KEY, n INT64 DEFAULT (fx_counter()))");
        await ExecDdl(executor, db, "CREATE TABLE c (id INT64 PRIMARY KEY, s STRING, CHECK (fx_slugify(s) = s))");
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, title STRING)");
        await ExecDdl(executor, db, "CREATE VIEW v AS SELECT good.fx_slugify(title) AS slug FROM src");
        await ExecDdl(executor, db, "CREATE MATERIALIZED VIEW mv AS SELECT fx_slugify(title) AS slug FROM src");
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON src AFTER INSERT EXTERNAL NAME '{Fixture}CountingTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "DROP EXTENSION good"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionHasDependents, ex.Code);

        foreach (string dependent in new[]
                 {
                     "extension needs_good", "the default of column d.n", "on table c", "view v", "materialized view mv",
                     "trigger audit on table src",
                 })
            StringAssert.Contains(dependent, ex.Message);

        await ExecDdl(executor, db, "DROP EXTENSION needs_good");
        await ExecDdl(executor, db, "DROP TABLE d");
        await ExecDdl(executor, db, "DROP TABLE c");
        await ExecDdl(executor, db, "DROP VIEW v");
        await ExecDdl(executor, db, "DROP MATERIALIZED VIEW mv");

        // Still one left: the error names only what remains.
        ex = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "DROP EXTENSION good"))!;
        StringAssert.Contains("trigger audit on table src", ex.Message);
        StringAssert.DoesNotContain("view v", ex.Message);

        await ExecDdl(executor, db, "DROP TRIGGER audit ON src");
        await ExecDdl(executor, db, "DROP EXTENSION good");

        CamusDBException gone = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT fx_slugify('x')"))!;
        StringAssert.Contains("Function not found", gone.Message);
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotInstalled, await Code(() => ExecDdl(executor, db, "DROP EXTENSION good")));

        // A dropped extension installs again.
        await ExecDdl(executor, db, "CREATE EXTENSION good");
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT fx_slugify('A B')"));
    }

    [Test]
    public async Task Drop_RacingACreateThatUsesTheExtension_NeverLeavesADanglingDependent()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, title STRING)");

        // Even rounds race a column default, odd rounds a view body: two of the paths that resolve a
        // function before they take the schema lock.
        for (int i = 0; i < 16; i++)
        {
            bool view = i % 2 == 1;
            string name = (view ? "v" : "d") + i;

            Task drop = ExecDdl(executor, db, "DROP EXTENSION good");
            Task create = ExecDdl(executor, db, view
                ? $"CREATE VIEW {name} AS SELECT fx_slugify(title) AS slug FROM src"
                : $"CREATE TABLE {name} (id INT64 PRIMARY KEY, n INT64 DEFAULT (fx_counter()))");

            try { await drop; } catch (CamusDBException) { }
            try { await create; } catch (CamusDBException) { }

            bool dropped = !database.Schema.Extensions.ContainsKey("good");
            bool created = view ? database.Schema.Views.ContainsKey(name) : database.Schema.Tables.ContainsKey(name);

            Assert.IsFalse(dropped && created, $"round {i}: the extension was dropped while {name} uses it");

            if (created)
                await ExecDdl(executor, db, view ? $"DROP VIEW {name}" : $"DROP TABLE {name}");

            if (dropped)
                await ExecDdl(executor, db, "CREATE EXTENSION good");
        }
    }

    [Test]
    public async Task CreateTrigger_AfterTheExtensionIsDropped_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecDdl(executor, db, "DROP EXTENSION good");

        // The apply refuses a trigger whose extension is not installed, in log order, so a CREATE TRIGGER
        // that resolved its type just before a concurrent DROP EXTENSION cannot leave a dangling record.
        TriggerDefinition trigger = new()
        {
            Name = "late", Timing = TriggerFireTiming.After, Events = TriggerEventSet.Insert,
            TypeName = Fixture + "AuditTrigger", ExtensionName = "good",
        };

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => new CatalogsManager(logger).ReplicateCreateTriggerAsync(database, "t", database.Schema.Tables["t"].Id!, trigger))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotInstalled, ex.Code);
    }

    // ── persistence and other nodes ───────────────────────────────────────────

    [Test]
    public async Task InstallUpdateAndDrop_SurviveAReload()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));

        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        CommandExecutor reloaded = CreateCommandExecutor(WithExtensions("Fixture.Good", "Fixture.GoodV2"));
        Assert.AreEqual("1.0.0", await Text(reloaded, db, "SELECT fx_good_version()"), "the install was persisted");

        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");
        reloaded = CreateCommandExecutor(WithExtensions("Fixture.Good", "Fixture.GoodV2"));
        Assert.AreEqual("2.0.0", await Text(reloaded, db, "SELECT fx_good_version()"), "the update was persisted");

        await ExecDdl(executor, db, "DROP EXTENSION good");
        reloaded = CreateCommandExecutor(WithExtensions("Fixture.Good", "Fixture.GoodV2"));
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(reloaded, db, "SELECT fx_good_version()"))!;
        StringAssert.Contains("Function not found", ex.Message, "the drop was persisted");
    }

    [Test]
    public async Task ANodeWithoutTheInstalledBuild_RefusesOnlyTheStatementsThatUseIt()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");

        // Engines on the same storage, like nodes that deploy no file, or another version.
        foreach (CommandExecutor node in new[] { CreateCommandExecutor(Options), CreateCommandExecutor(WithExtensions("Fixture.GoodV2")) })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(node, db, "SELECT fx_slugify('x')"))!;
            Assert.AreEqual(CamusDBErrorCodes.ExtensionNotLoadedOnNode, ex.Code);
            StringAssert.Contains("good", ex.Message);

            Assert.AreEqual(CamusDBErrorCodes.ExtensionNotLoadedOnNode, await Code(() => Scalar(node, db, "SELECT good.fx_slugify('x')")));
            Assert.AreEqual(CamusDBErrorCodes.ExtensionNotLoadedOnNode, await Code(() => ExecNonQuery(node, db, "CALL fx_noop()")));

            // Everything else still runs.
            await ExecNonQuery(node, db, "INSERT INTO t (id) VALUES (" + (node.Extensions.IsEmpty ? 1 : 2) + ")");

            List<Dictionary<string, ColumnValue>> rows = await ExecQuery(node, db, "SHOW EXTENSIONS");
            Dictionary<string, ColumnValue> missing = rows.Single(r => r["status"].StrValue == "missing");
            Assert.AreEqual("good", missing["name"].StrValue);
            Assert.AreEqual("1.0.0", missing["version"].StrValue);
            Assert.AreEqual(ColumnType.Null, missing["path"].Type);
        }
    }

    [Test]
    public async Task ShowExtensions_MarksTheInstalledBuild()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2", "Fixture.Requires"));
        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '2.0.0'");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SHOW EXTENSIONS");
        Assert.AreEqual(3, rows.Count);

        Assert.AreEqual("installed", rows.Single(r => r["name"].StrValue == "good" && r["version"].StrValue == "2.0.0")["status"].StrValue);
        Assert.AreEqual("available", rows.Single(r => r["name"].StrValue == "good" && r["version"].StrValue == "1.0.0")["status"].StrValue);

        Dictionary<string, ColumnValue> needs = rows.Single(r => r["name"].StrValue == "needs_good");
        Assert.AreEqual("available", needs["status"].StrValue);
        Assert.AreEqual("good", needs["requires"].StrValue);
    }

    // ── caches ────────────────────────────────────────────────────────────────

    [Test]
    public async Task BoundAndPlannedStatements_SeeTheUpdatedExtension()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));
        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (1)");

        // Repeated text reuses the parse, the binding and the plan.
        const string sql = "SELECT fx_good_version() AS v FROM t WHERE id = 1";
        for (int i = 0; i < 3; i++)
            Assert.AreEqual("1.0.0", await Text(executor, db, sql));

        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");
        Assert.AreEqual("2.0.0", await Text(executor, db, sql));

        await ExecDdl(executor, db, "DROP EXTENSION good");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, sql))!;
        StringAssert.Contains("Function not found", ex.Message);
    }

    // ── privileges and the statement surface ─────────────────────────────────

    [Test]
    public void ExtensionDdl_IsSchemaDdlForTheNonQueryEndpoint()
    {
        foreach (string sql in new[] { "CREATE EXTENSION geo", "CREATE EXTENSION geo VERSION '1.0.0'", "ALTER EXTENSION geo UPDATE", "ALTER EXTENSION geo UPDATE TO '2.0.0'", "DROP EXTENSION geo" })
            Assert.IsTrue(CamusDB.Core.SQLParser.StatementScope.IsSchemaDdl(CamusDB.Core.SQLParser.SQLParserProcessor.Parse(sql).nodeType), sql);
    }
}

/// <summary>
/// The result cache and the extension stamp: a cached result computed with one installed build of an
/// extension is not served after the database moves to another build.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestExtensionPackagingResultCache : ExtensionTestBase
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
    public async Task ACachedResult_IsNotServedAfterTheExtensionUpdate()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Good", "Fixture.GoodV2"));
        await ExecDdl(executor, db, "CREATE EXTENSION good VERSION '1.0.0'");
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id) VALUES (1)");

        const string sql = "SELECT fx_good_version() AS v FROM t {cache=ext}";

        _ = await Cached(executor, db, sql);
        (string warm, CacheMetadataHolder hit) = await Cached(executor, db, sql);
        Assert.AreEqual(QueryCacheStatus.Hit, hit.Status, $"the cache must be serving first (was {hit.Status}/{hit.BypassReason})");
        Assert.AreEqual("1.0.0", warm);

        await ExecDdl(executor, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");

        (string after, CacheMetadataHolder meta) = await Cached(executor, db, sql);
        Assert.AreNotEqual(QueryCacheStatus.Hit, meta.Status);
        Assert.AreEqual("2.0.0", after);
    }
}
