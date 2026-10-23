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
using CamusDB.Core.Extensions;
using CamusDB.Extensions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Typed trigger rows (<c>ICamusTrigger&lt;TRow&gt;</c>): the mapping of every value type and of
/// <c>[CamusColumn]</c>, NULL into nullable and non-nullable properties, the BEFORE write-back of
/// changed properties only (validated like any other value), the AFTER rule that changes are ignored,
/// the CREATE TRIGGER refusals, and the refusal to drop or rename a mapped column.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionTypedTriggers : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Rich.";

    private static async Task CreateOrders(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db,
            "CREATE TABLE orders (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, total_cents INT64, " +
            "tags array(string), note STRING, extra STRING, CHECK (total_cents >= 0))");
        await ExecDdl(executor, db, "CREATE TABLE order_log (id OID PRIMARY KEY DEFAULT (gen_id()), detail STRING)");
    }

    private static async Task<string> Code(CommandExecutor executor, string db, string sql, bool ddl = true) =>
        Assert.ThrowsAsync<CamusDBException>(() => ddl ? ExecDdl(executor, db, sql) : ExecNonQuery(executor, db, sql))!.Code;

    [Test]
    public async Task Before_ChangedPropertiesAreWrittenBack()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER trim ON orders BEFORE INSERT, UPDATE EXTERNAL NAME '{Fixture}TrimOrderName'");

        TriggerDefinition trigger = (await executor.OpenDatabase(db)).Schema.Tables["orders"].Triggers!.Single();
        CollectionAssert.AreEquivalent(new[] { "id", "name", "total_cents", "tags", "note" }, trigger.RowColumns,
            "the record keeps the columns the row class maps; [CamusColumn] maps Total to total_cents, and extra is not mapped");

        await ExecNonQuery(executor, db, "INSERT INTO orders (name, total_cents, extra) VALUES ('  widget  ', 99, 'kept')");

        Dictionary<string, ColumnValue> row = (await ExecQuery(executor, db, "SELECT name, total_cents, extra FROM orders")).Single();
        Assert.AreEqual("widget", row["name"].StrValue, "the trimmed name is written back");
        Assert.AreEqual(100, row["total_cents"].LongValue, "a [CamusColumn] property is written back to its column");
        Assert.AreEqual("kept", row["extra"].StrValue, "an unmapped column is untouched");

        await ExecNonQuery(executor, db, "UPDATE orders SET name = '  gadget ' WHERE extra = 'kept'");
        Assert.AreEqual("gadget", (await Scalar(executor, db, "SELECT name FROM orders")).StrValue);
    }

    [Test]
    public async Task BeforeUpdate_SeesOldAsATypedRow()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER note ON orders BEFORE UPDATE EXTERNAL NAME '{Fixture}NoteOldName'");

        await ExecNonQuery(executor, db, "INSERT INTO orders (name, total_cents) VALUES ('first', 1)");
        await ExecNonQuery(executor, db, "UPDATE orders SET name = 'second' WHERE total_cents = 1");

        Dictionary<string, ColumnValue> row = (await ExecQuery(executor, db, "SELECT name, note FROM orders")).Single();
        Assert.AreEqual("second", row["name"].StrValue);
        Assert.AreEqual("was first", row["note"].StrValue);
    }

    [Test]
    public async Task Before_WrittenBackValue_IsValidated()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER neg ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}NegativeTotal'");

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation,
            await Code(executor, db, "INSERT INTO orders (name, total_cents) VALUES ('x', 5)", ddl: false),
            "a property the trigger changed goes through the CHECK constraint like a statement value");

        Assert.AreEqual(0, (await ExecQuery(executor, db, "SELECT name FROM orders")).Count);
    }

    [Test]
    public async Task After_ChangesToTheRowObjectAreIgnored()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER log ON orders AFTER INSERT, DELETE EXTERNAL NAME '{Fixture}AfterOrderLog'");

        await ExecNonQuery(executor, db, "INSERT INTO orders (name, total_cents, tags) VALUES ('a', 3, ARRAY['x', 'y'])");
        await ExecNonQuery(executor, db, "INSERT INTO orders (name) VALUES ('b')");

        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT name FROM orders WHERE name = 'a' OR name = 'b'")).Count,
            "an AFTER trigger cannot change the stored row");

        await ExecNonQuery(executor, db, "DELETE FROM orders WHERE name = 'a'");

        List<string> log = (await ExecQuery(executor, db, "SELECT detail FROM order_log")).Select(r => r["detail"].StrValue!).ToList();
        CollectionAssert.AreEquivalent(new[] { "Insert:a:3:x+y", "Insert:b:<null>:<null>", "Delete:a:3:x+y" }, log);
    }

    [Test]
    public async Task Mapping_EveryValueType()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        await ExecDdl(executor, db,
            "CREATE TABLE typed_all (id OID PRIMARY KEY DEFAULT (gen_id()), i64 INT64, f64 FLOAT64, f32 FLOAT32, flag BOOL, txt STRING, " +
            "uid UUID, data BYTES, stamp DATE, amount NUMERIC, ids array(int64), summary STRING)");
        await ExecDdl(executor, db, $"CREATE TRIGGER sum ON typed_all BEFORE INSERT EXTERNAL NAME '{Fixture}SummarizeAllTypes'");

        await ExecNonQuery(executor, db,
            "INSERT INTO typed_all (i64, f64, f32, flag, txt, uid, data, stamp, amount, ids) VALUES " +
            "(7, 1.5, CAST(2.5 AS FLOAT32), true, 'hi', CAST('00000000-0000-0000-0000-00000000000a' AS UUID), " +
            "CAST('0x0102' AS BYTES), CAST('2026-03-04' AS DATE), CAST('12.5' AS NUMERIC), ARRAY[1, 2])");

        Dictionary<string, ColumnValue> row = (await ExecQuery(executor, db, "SELECT summary, data FROM typed_all")).Single();
        Assert.AreEqual("7|1.5|2.5|True|hi|00000000-0000-0000-0000-00000000000a|01-02|Utc:2026-03-04|12.500000000|1+2", row["summary"].StrValue);
        CollectionAssert.AreEqual(new byte[] { 0xFF, 0x02 }, row["data"].BytesValue,
            "a byte array changed in place counts as a change and is written back");

        // NULL reaches every nullable property as null.
        await ExecNonQuery(executor, db, "DELETE FROM typed_all WHERE i64 = 7");
        await ExecNonQuery(executor, db, "INSERT INTO typed_all (txt) VALUES (NULL)");
        Assert.AreEqual("<null>|<null>|<null>|<null>|<null>|<null>|<null>|<null>|<null>|<null>",
            (await Scalar(executor, db, "SELECT summary FROM typed_all")).StrValue);
    }

    [Test]
    public async Task NullInANonNullableProperty_NamesTheColumn()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER strict ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}StrictTotalTrigger'");

        await ExecNonQuery(executor, db, "INSERT INTO orders (name, total_cents) VALUES ('ok', 1)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "INSERT INTO orders (name) VALUES ('no total')"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerFailed, ex.Code);
        StringAssert.Contains("total_cents", ex.Message);
        StringAssert.Contains("StrictTotalRow.Total", ex.Message);
    }

    [Test]
    public async Task CreateTrigger_RefusesAMappingThatDoesNotFit()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, db, $"CREATE TRIGGER m ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}MissingColumnTrigger'"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerRowMappingInvalid, missing.Code);
        StringAssert.Contains("'Nope'", missing.Message);

        CamusDBException wrongType = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, db, $"CREATE TRIGGER w ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}WrongTypeTrigger'"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerRowMappingInvalid, wrongType.Code);
        StringAssert.Contains("total_cents", wrongType.Message);
        StringAssert.Contains("Int64", wrongType.Message, "the message says which property type fits");

        Assert.IsNull((await executor.OpenDatabase(db)).Schema.Tables["orders"].Triggers);
    }

    [Test]
    public async Task MappedColumn_CannotBeDroppedOrRenamed_UnmappedCan()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER trim ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}TrimOrderName'");

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, await Code(executor, db, "ALTER TABLE orders DROP COLUMN total_cents"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, await Code(executor, db, "ALTER TABLE orders RENAME COLUMN note TO remark"));

        // A column the row class does not map is free to change.
        await ExecDdl(executor, db, "ALTER TABLE orders RENAME COLUMN extra TO spare");
        await ExecDdl(executor, db, "ALTER TABLE orders DROP COLUMN spare");
        await ExecDdl(executor, db, "ALTER TABLE orders ADD COLUMN later STRING");

        // The trigger still fires after the unrelated schema changes.
        await ExecNonQuery(executor, db, "INSERT INTO orders (name) VALUES ('  spaced ')");
        Assert.AreEqual("spaced", (await Scalar(executor, db, "SELECT name FROM orders")).StrValue);

        // Once the trigger is gone, the column can go too.
        await ExecDdl(executor, db, "DROP TRIGGER trim ON orders");
        await ExecDdl(executor, db, "ALTER TABLE orders DROP COLUMN note");
    }

    [Test]
    public async Task Restart_KeepsTheMappedColumns()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await CreateOrders(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER trim ON orders BEFORE INSERT EXTERNAL NAME '{Fixture}TrimOrderName'");

        CommandExecutor reopened = CreateCommandExecutor(WithExtensions("Fixture.Rich"));

        TriggerDefinition trigger = (await reopened.OpenDatabase(db)).Schema.Tables["orders"].Triggers!.Single();
        CollectionAssert.Contains(trigger.RowColumns, "total_cents", "the mapped columns survive the schema load");

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, await Code(reopened, db, "ALTER TABLE orders DROP COLUMN total_cents"));

        await ExecNonQuery(reopened, db, "INSERT INTO orders (name) VALUES (' again ')");
        Assert.AreEqual("again", (await Scalar(reopened, db, "SELECT name FROM orders")).StrValue);
    }

    // ── The adapter, without SQL ─────────────────────────────────────────────

    public sealed class PlainRow
    {
        public string? Name { get; set; }

        public long? Total { get; set; }

        public byte[]? Data { get; set; }
    }

    public sealed class LeavePlain : ICamusTrigger<PlainRow>
    {
        public Task FireAsync(ITriggerContext<PlainRow> context) => Task.CompletedTask;
    }

    public sealed class RewriteSameValues : ICamusTrigger<PlainRow>
    {
        public Task FireAsync(ITriggerContext<PlainRow> context)
        {
            PlainRow row = context.New!;

            // New objects with equal values are not changes.
            row.Name = new string(row.Name!.ToCharArray());
            row.Data = (byte[])row.Data!.Clone();
            return Task.CompletedTask;
        }
    }

    public sealed class ThrowInTrigger : ICamusTrigger<PlainRow>
    {
        public Task FireAsync(ITriggerContext<PlainRow> context) => throw new InvalidOperationException("typed boom");
    }

    private static (Dictionary<string, ColumnValue> Values, TriggerRow Row) WritableRow()
    {
        Dictionary<string, ColumnValue> values = new(StringComparer.OrdinalIgnoreCase)
        {
            ["name"] = new ColumnValue(ColumnType.String, "n"),
            ["total"] = new ColumnValue(ColumnType.Integer64, 5),
            ["data"] = new ColumnValue(new byte[] { 1, 2 }),
        };

        List<TableColumnSchema> columns =
        [
            new("c1", "name", ColumnType.String, notNull: false, defaultValue: null),
            new("c2", "total", ColumnType.Integer64, notNull: false, defaultValue: null),
            new("c3", "data", ColumnType.Bytes, notNull: false, defaultValue: null),
        ];

        return (values, new TriggerRow(values, columns));
    }

    [TestCase(typeof(LeavePlain))]
    [TestCase(typeof(RewriteSameValues))]
    public async Task UnchangedProperties_AreNotWritten(Type triggerType)
    {
        TypedTriggerAdapter adapter = TypedTriggerAdapter.Create(typeof(PlainRow));
        (Dictionary<string, ColumnValue> values, TriggerRow row) = WritableRow();

        object trigger = Activator.CreateInstance(triggerType)!;
        await adapter.FireAsync(trigger, new StubContext(), null, values, row, "t");

        Assert.IsNull(row.ChangedColumns, "no property differs from its column, so nothing is written and the update keeps its stored cells");
    }

    [Test]
    public void TypedRowPath_HasNoReflectionInvoke()
    {
        TypedTriggerAdapter adapter = TypedTriggerAdapter.Create(typeof(PlainRow));
        (Dictionary<string, ColumnValue> values, TriggerRow row) = WritableRow();

        InvalidOperationException ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.FireAsync(new ThrowInTrigger(), new StubContext(), null, values, row, "t"))!;

        string trace = ex.StackTrace ?? "";
        StringAssert.Contains(nameof(ThrowInTrigger), trace);
        StringAssert.DoesNotContain("MethodBase.Invoke", trace);
        StringAssert.DoesNotContain("RuntimeMethodInfo.Invoke", trace);
        StringAssert.DoesNotContain("MethodBaseInvoker", trace);
    }

    private sealed class StubContext : ITriggerContext
    {
        public string TriggerName => "t";

        public string TableName => "tbl";

        public TriggerTiming Timing => TriggerTiming.Before;

        public TriggerOperation Operation => TriggerOperation.Update;

        public IRow? Old => null;

        public IWritableRow? New => null;

        public ISqlSession Sql => throw new NotSupportedException();

        public System.Threading.CancellationToken CancellationToken => default;
    }
}
