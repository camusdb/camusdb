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

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Row triggers end to end, through SQL: CREATE and DROP TRIGGER and their refusals, BEFORE triggers
/// that change rows which must still validate, AFTER triggers that see final rows once per row after
/// the statement, rollback of trigger writes, full OLD rows with a large carried column, the paths
/// that must not fire, recursion, and binding on an engine that lacks or differs from the extension.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionTriggers : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Good.";

    private static async Task CreateTables(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE t (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, wide STRING(200000))");
        await ExecDdl(executor, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), op STRING, timing STRING, tbl STRING, detail STRING)");
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Audit(CommandExecutor executor, string db) =>
        await ExecQuery(executor, db, "SELECT op, timing, tbl, detail FROM audit_log");

    private static async Task<TableSchema> Schema(CommandExecutor executor, string db, string table) =>
        (await executor.OpenDatabase(db)).Schema.Tables[table];

    // ── CREATE / DROP TRIGGER ─────────────────────────────────────────────────

    [Test]
    public async Task CreateTrigger_StoresTheResolvedRecord_DropRemovesIt()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);

        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT, DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");

        TriggerDefinition trigger = (await Schema(executor, db, "t")).Triggers!.Single();
        Assert.AreEqual("audit", trigger.Name);
        Assert.AreEqual(TriggerFireTiming.After, trigger.Timing);
        Assert.AreEqual(TriggerEventSet.Insert | TriggerEventSet.Delete, trigger.Events);
        Assert.AreEqual(Fixture + "AuditTrigger", trigger.TypeName);
        Assert.AreEqual("Fixture.Good", trigger.AssemblyName);
        Assert.AreEqual(executor.Extensions.Extensions[0].Sha256, trigger.AssemblySha256);

        await ExecDdl(executor, db, "DROP TRIGGER audit ON t");
        Assert.IsNull((await Schema(executor, db, "t")).Triggers);
    }

    [Test]
    public async Task CreateTrigger_Refusals()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, "CREATE VIEW v AS SELECT id, name FROM t");
        await ExecDdl(executor, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t");

        async Task<string> Code(string sql) => Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, sql))!.Code;

        Assert.AreEqual(CamusDBErrorCodes.TriggerTypeNotFound,
            await Code($"CREATE TRIGGER x ON t AFTER INSERT EXTERNAL NAME '{Fixture}NoSuchType'"));
        Assert.AreEqual(CamusDBErrorCodes.TriggerTypeInvalid,
            await Code($"CREATE TRIGGER x ON t AFTER INSERT EXTERNAL NAME '{Fixture}NotATrigger'"));
        Assert.AreEqual(CamusDBErrorCodes.TriggerTargetInvalid,
            await Code($"CREATE TRIGGER x ON v AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"));
        Assert.AreEqual(CamusDBErrorCodes.TriggerTargetInvalid,
            await Code($"CREATE TRIGGER x ON mv AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist,
            await Code($"CREATE TRIGGER x ON nope AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput,
            await Code($"CREATE TRIGGER x ON t DURING INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput,
            await Code($"CREATE TRIGGER x ON t AFTER INSERT, INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"));

        await ExecDdl(executor, db, $"CREATE TRIGGER x ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");
        Assert.AreEqual(CamusDBErrorCodes.TriggerAlreadyExists,
            await Code($"CREATE TRIGGER X ON t BEFORE UPDATE EXTERNAL NAME '{Fixture}GuardTrigger'"));

        Assert.AreEqual(CamusDBErrorCodes.TriggerNotFound, await Code("DROP TRIGGER nope ON t"));
    }

    [Test]
    public async Task CreateTrigger_WithoutTheExtension_IsRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateTables(executor, db);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecDdl(executor, db, $"CREATE TRIGGER x ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerTypeNotFound, ex.Code);
    }

    [Test]
    public async Task TriggerWords_StayUsableAsNames()
    {
        // No word was reserved for triggers or CALL.
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE trigger (id INT64 PRIMARY KEY, before STRING, after STRING, external STRING, name STRING, call STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO trigger (id, before, after, external, name, call) VALUES (1, 'b', 'a', 'e', 'n', 'c')");
        Assert.AreEqual("c", (await Scalar(executor, db, "SELECT call FROM trigger")).StrValue);
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    [Test]
    public async Task AfterInsert_FiresOncePerRow_AndSeesTheFinalRows()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('a'), ('b'), ('c')");

        List<Dictionary<string, ColumnValue>> audit = await Audit(executor, db);
        Assert.AreEqual(3, audit.Count);
        Assert.IsTrue(audit.All(r => r["op"].StrValue == "Insert" && r["timing"].StrValue == "After" && r["tbl"].StrValue == "t"));
        CollectionAssert.AreEquivalent(new[] { "a", "b", "c" }, audit.Select(r => r["detail"].StrValue));
    }

    [Test]
    public async Task AfterInsert_FiresAfterTheWholeStatement_AcrossChunks()
    {
        // Chunks of two rows: an AFTER trigger that fired per chunk would count 2 or 4.
        (string db, _, CommandExecutor executor) = await CreateDatabase(
            WithExtensions("Fixture.Good") with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER counter ON t AFTER INSERT EXTERNAL NAME '{Fixture}CountRowsTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('1'), ('2'), ('3'), ('4'), ('5')");

        List<Dictionary<string, ColumnValue>> audit = await Audit(executor, db);
        Assert.AreEqual(5, audit.Count);
        Assert.IsTrue(audit.All(r => r["detail"].StrValue == "5"), "every firing must see all five rows: " +
            string.Join(",", audit.Select(r => r["detail"].StrValue)));
    }

    [Test]
    public async Task InsertSelect_FiresAfterTheWholeStatement()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(
            WithExtensions("Fixture.Good") with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await CreateTables(executor, db);
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, name STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO src (id, name) VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        await ExecDdl(executor, db, $"CREATE TRIGGER counter ON t AFTER INSERT EXTERNAL NAME '{Fixture}CountRowsTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) SELECT name FROM src");

        List<Dictionary<string, ColumnValue>> audit = await Audit(executor, db);
        Assert.AreEqual(3, audit.Count);
        Assert.IsTrue(audit.All(r => r["detail"].StrValue == "3"));
    }

    [Test]
    public async Task BeforeInsert_ChangesTheStoredRow()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER upper ON t BEFORE INSERT EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('hello')");

        Assert.AreEqual("HELLO", (await Scalar(executor, db, "SELECT name FROM t")).StrValue);
    }

    [Test]
    public async Task BeforeInsert_OutputMustStillValidate()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER nuller ON t BEFORE INSERT EXTERNAL NAME '{Fixture}NullNameTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('x')"))!;
        Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, ex.Code, ex.Message);
        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM t"));
    }

    [Test]
    public async Task TypedInsert_FiresTriggers()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER upper ON t BEFORE INSERT EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        Dictionary<string, ColumnValue> row = new()
        {
            ["id"] = new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3"),
            ["name"] = new ColumnValue(ColumnType.String, "typed"),
        };

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, db, "t", [row]));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual("typed", row["name"].StrValue, "the caller's row must not change; a retry replays it");
        Assert.AreEqual("TYPED", (await Scalar(executor, db, "SELECT name FROM t")).StrValue);
        Assert.AreEqual("TYPED", (await Audit(executor, db)).Single()["detail"].StrValue);
    }

    // ── errors and rollback ───────────────────────────────────────────────────

    [Test]
    public async Task ThrowingTrigger_EndsTheStatement_AndRollsBackTriggerWrites()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        // Created in this order, so the audit row is written before the guard throws.
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER guard ON t AFTER INSERT EXTERNAL NAME '{Fixture}GuardTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('bad apple')"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerFailed, ex.Code);
        StringAssert.Contains("guard rejected bad apple", ex.Message);

        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM t"), "the row must not persist");
        Assert.IsEmpty(await Audit(executor, db), "the trigger's own write must roll back with the statement");

        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('good apple')");
        Assert.AreEqual(1, (await Audit(executor, db)).Count);
    }

    [Test]
    public async Task RecursiveTrigger_StopsAtTheNestingLimit()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER loop ON t AFTER INSERT EXTERNAL NAME '{Fixture}RecursiveTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('start')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNestingTooDeep, ex.Code);
        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM t"));
    }

    // ── UPDATE and DELETE ─────────────────────────────────────────────────────

    [Test]
    public async Task Update_TriggersSeeTheWholeRow_AndBeforeKeepsUnassignedLargeValues()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);

        string wide = new('w', 100_000);
        await ExecNonQuery(executor, db, "INSERT INTO t (name, wide) VALUES ('a', @wide)",
            new Dictionary<string, ColumnValue> { ["@wide"] = new ColumnValue(ColumnType.String, wide) });

        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER upper ON t BEFORE UPDATE EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        // Assigns only name: wide is a large value the update would normally carry without a decode.
        await ExecNonQuery(executor, db, "UPDATE t SET name = 'b' WHERE name = 'a'");

        List<Dictionary<string, ColumnValue>> audit = await Audit(executor, db);
        Assert.AreEqual("B:100000", audit.Single()["detail"].StrValue, "the AFTER trigger saw NEW in full, uppercased by the BEFORE trigger");

        Dictionary<string, ColumnValue> stored = (await ExecQuery(executor, db, "SELECT name, wide FROM t")).Single();
        Assert.AreEqual("B", stored["name"].StrValue);
        Assert.AreEqual(wide, stored["wide"].StrValue, "the unassigned large value must survive a BEFORE trigger");
    }

    [Test]
    public async Task Delete_TriggersSeeTheWholeOldRow_AndBeforeCanVeto()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);

        string wide = new('w', 50_000);
        await ExecNonQuery(executor, db, "INSERT INTO t (name, wide) VALUES ('keep', @wide), ('bad row', 'x')",
            new Dictionary<string, ColumnValue> { ["@wide"] = new ColumnValue(ColumnType.String, wide) });

        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER guard ON t BEFORE DELETE EXTERNAL NAME '{Fixture}GuardTrigger'");

        // The WHERE reads only name; without the trigger the delete would decode index and predicate
        // columns only, and OLD would miss the wide column.
        await ExecNonQuery(executor, db, "DELETE FROM t WHERE name = 'keep'");
        Assert.AreEqual("keep:50000", (await Audit(executor, db)).Single()["detail"].StrValue);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "DELETE FROM t WHERE name = 'bad row'"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerFailed, ex.Code);
        Assert.AreEqual(1, (await ExecQuery(executor, db, "SELECT id FROM t")).Count, "the vetoed delete must remove nothing");
    }

    // ── paths that must not fire ──────────────────────────────────────────────

    [Test]
    public async Task EngineInternalWrites_FireNothing()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, "CREATE TABLE victim (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO victim (name) VALUES ('a'), ('b')");
        await ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('a'), ('b')");

        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON victim AFTER INSERT, UPDATE, DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT, UPDATE, DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");

        // ADD COLUMN rewrites every row; TRUNCATE swaps storage; DROP TABLE removes the rows.
        await ExecDdl(executor, db, "ALTER TABLE t ADD COLUMN extra INT64 DEFAULT (7)");
        await ExecDdl(executor, db, "TRUNCATE TABLE t");
        await ExecDdl(executor, db, "DROP TABLE victim");

        Assert.IsEmpty(await Audit(executor, db));
    }

    [Test]
    public async Task TtlExpiry_FiresNothing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, "CREATE TABLE sessions (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING, expires INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO sessions (name, expires) VALUES ('old', 1000)");
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON sessions AFTER DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");

        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SELECT id FROM sessions", null));
        List<CamusDB.Core.Util.ObjectIds.ObjectIdValue> rowIds = (await cursor.ToListAsync()).Select(r => r.RowId).ToList();
        await database.Transactions.CommitAsync(tx);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "sessions"));
        (int deleted, _) = await executor.DeleteExpiredRowsForTestsAsync(
            await executor.OpenDatabase(db), table, rowIds, "expires", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        Assert.AreEqual(1, deleted, "the sweep deleted the expired row");
        Assert.IsEmpty(await Audit(executor, db), "an engine-internal expiry fires no user trigger");
    }

    // ── binding across engines ────────────────────────────────────────────────

    [Test]
    public async Task EngineWithoutTheExtension_RefusesDml_OnTablesWithTriggers()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");

        // A second engine on the same storage, like a node that does not deploy the extension.
        CommandExecutor bare = CreateCommandExecutor(Options);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(bare, db, "INSERT INTO t (name) VALUES ('x')"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerBindingMismatch, ex.Code);

        // DML on a table without triggers is not affected, and reads never bind.
        await ExecNonQuery(bare, db, "INSERT INTO audit_log (op) VALUES ('manual')");
        Assert.IsEmpty(await ExecQuery(bare, db, "SELECT id FROM t"));
    }

    [Test]
    public async Task EngineWithADifferentBuild_RefusesDml()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");

        // The record as another build would have stored it: same type, another hash.
        TableSchema schema = await Schema(executor, db, "t");
        TriggerDefinition original = schema.Triggers!.Single();
        schema.Triggers = [new TriggerDefinition
        {
            Name = original.Name, Timing = original.Timing, Events = original.Events, TypeName = original.TypeName,
            AssemblyName = original.AssemblyName, AssemblySha256 = new string('0', 64), CreatedAt = original.CreatedAt,
        }];

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO t (name) VALUES ('x')"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerBindingMismatch, ex.Code);
        StringAssert.Contains("sha256", ex.Message);
    }

    [Test]
    public async Task SecondEngineWithTheExtension_LoadsAndFiresThePersistedTrigger()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER audit ON t AFTER INSERT EXTERNAL NAME '{Fixture}AuditTrigger'");

        // A fresh engine reads the schema back from storage: the record must have been persisted on the
        // table's own meta key, and the same file binds with the same hash.
        CommandExecutor reloaded = CreateCommandExecutor(WithExtensions("Fixture.Good"));

        await ExecNonQuery(reloaded, db, "INSERT INTO t (name) VALUES ('after-reload')");
        Assert.AreEqual("after-reload", (await Audit(reloaded, db)).Single()["detail"].StrValue);
        Assert.AreEqual("audit", (await Schema(reloaded, db, "t")).Triggers!.Single().Name);
    }
}
