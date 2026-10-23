/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
/// Arrays in the extension value mapping: <c>IReadOnlyList&lt;T&gt;</c> parameters and results for each
/// element type, NULL arrays and NULL elements, the copy rule both ways, the element type a CTAS and a
/// derived table get from an array function, and the signatures the binder refuses.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionArrays : ExtensionTestBase
{
    private static IReadOnlyList<ColumnValue> Elements(ColumnValue value)
    {
        Assert.AreEqual(ColumnType.Array, value.Type, value.ToString());
        return value.ArrayValues!;
    }

    [Test]
    public async Task ArrayParameter_EachElementType()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        Assert.AreEqual(6, (await Scalar(executor, db, "SELECT rx_sum_list(ARRAY[1, 2, 3])")).LongValue);
        Assert.AreEqual(0, (await Scalar(executor, db, "SELECT rx_sum_list(ARRAY[])")).LongValue, "an empty array is an empty list");

        // An Integer64 element reaches a double list as a double, the scalar rule.
        Assert.AreEqual("1,2", (await Scalar(executor, db, "SELECT rx_describe_doubles(ARRAY[1, 2])")).StrValue);

        IReadOnlyList<ColumnValue> doubles = Elements(await Scalar(executor, db, "SELECT rx_echo_doubles(ARRAY[1.5, 2.5])"));
        CollectionAssert.AreEqual(new[] { 3.0, 5.0 }, doubles.Select(v => v.FloatValue));
        Assert.IsTrue(doubles.All(v => v.Type == ColumnType.Float64));

        IReadOnlyList<ColumnValue> bools = Elements(await Scalar(executor, db, "SELECT rx_echo_bools(ARRAY[true, false])"));
        CollectionAssert.AreEqual(new[] { false, true }, bools.Select(v => v.BoolValue));

        IReadOnlyList<ColumnValue> strings = Elements(await Scalar(executor, db, "SELECT rx_echo_strings(ARRAY['a', 'b'])"));
        CollectionAssert.AreEqual(new[] { "A", "B" }, strings.Select(v => v.StrValue));

        await ExecDdl(executor, db,
            "CREATE TABLE typed (id int64 PRIMARY KEY NOT NULL, f array(float32), u array(uuid), d array(date))");
        await ExecNonQuery(executor, db,
            "INSERT INTO typed (id, f, u, d) VALUES (1, ARRAY[CAST(1.5 AS FLOAT32)], " +
            "ARRAY[CAST('00000000-0000-0000-0000-000000000001' AS UUID), CAST('00000000-0000-0000-0000-000000000002' AS UUID)], " +
            "ARRAY[CAST('2026-01-31' AS DATE)])");

        Dictionary<string, ColumnValue> row = (await ExecQuery(executor, db,
            "SELECT rx_echo_floats(f) AS f, rx_echo_uuids(u) AS u, rx_echo_dates(d) AS d, " +
            "rx_echo_decimals(ARRAY[CAST('1.25' AS NUMERIC)]) AS n FROM typed")).Single();

        Assert.AreEqual(ColumnType.Float32, Elements(row["f"])[0].Type);
        Assert.AreEqual(1.5, Elements(row["f"])[0].FloatValue, 1e-6);
        CollectionAssert.AreEqual(
            new[] { Guid.Parse("00000000-0000-0000-0000-000000000002"), Guid.Parse("00000000-0000-0000-0000-000000000001") },
            Elements(row["u"]).Select(v => v.ToGuid()));
        Assert.AreEqual(ColumnType.DateTime, Elements(row["d"])[0].Type, "a Date element reaches the extension as DateTime and comes back as DateTime");
        Assert.AreEqual(new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc).Ticks, Elements(row["d"])[0].LongValue);
        Assert.AreEqual(ColumnType.Numeric, Elements(row["n"])[0].Type);
        Assert.AreEqual(ExtensionValueMapping.DecimalToUnscaled(12.5m), Elements(row["n"])[0].NumericUnscaled);
    }

    [Test]
    public async Task NullArraysAndNullElements()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        // A non-nullable list parameter propagates a NULL array; a nullable one receives it.
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT rx_sum_list(NULL)")).Type);
        Assert.AreEqual(-1, (await Scalar(executor, db, "SELECT rx_list_length(NULL)")).LongValue);
        Assert.AreEqual(2, (await Scalar(executor, db, "SELECT rx_list_length(ARRAY['a', 'b'])")).LongValue);

        // A nullable element type receives NULL elements; a reference element always does.
        Assert.AreEqual(2, (await Scalar(executor, db, "SELECT rx_count_null_elements(ARRAY[1, NULL, 3, NULL])")).LongValue);
        Assert.AreEqual("a,<null>,c", (await Scalar(executor, db, "SELECT rx_join(ARRAY['a', NULL, 'c'], ',')")).StrValue);

        // A NULL element in a non-nullable value element type is an error that names the function and the argument.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT rx_sum_list(ARRAY[1, NULL])"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("rx_sum_list", ex.Message);
        StringAssert.Contains("argument 1", ex.Message);
        StringAssert.Contains("position 2", ex.Message);

        // A wrong argument type names the function too.
        ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT rx_sum_list(5)"))!;
        StringAssert.Contains("rx_sum_list", ex.Message);

        // Results: a null list is SQL NULL, a null element is a NULL element.
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT rx_null_list(1)")).Type);
        IReadOnlyList<ColumnValue> withNull = Elements(await Scalar(executor, db, "SELECT rx_with_null_element(7)"));
        Assert.AreEqual(7, withNull[0].LongValue);
        Assert.AreEqual(ColumnType.Null, withNull[1].Type);
    }

    [Test]
    public async Task ReturnedList_IsCopied_LaterChangesDoNotReachTheStoredValue()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        await ExecDdl(executor, db, "CREATE TABLE kept (id int64 PRIMARY KEY NOT NULL, ids array(int64))");
        await ExecNonQuery(executor, db, "INSERT INTO kept (id, ids) VALUES (1, rx_shared_list())");

        // The extension changes the list it returned, after the INSERT stored it.
        Assert.AreEqual(4, (await Scalar(executor, db, "SELECT rx_mutate_shared(42)")).LongValue);

        IReadOnlyList<ColumnValue> stored = Elements(await Scalar(executor, db, "SELECT ids FROM kept WHERE id = 1"));
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, stored.Select(v => v.LongValue));

        // The list an extension receives is read-only: it cannot reach engine storage.
        Assert.AreEqual("read-only", (await Scalar(executor, db, "SELECT rx_try_mutate(ARRAY[1, 2])")).StrValue);
    }

    [Test]
    public async Task CreateTableAsSelect_AndDerivedTable_GetTheElementType()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        await ExecDdl(executor, db, "CREATE TABLE src (id int64 PRIMARY KEY NOT NULL, n int64)");
        await ExecNonQuery(executor, db, "INSERT INTO src (id, n) VALUES (1, 2), (2, 3)");

        await ExecDdl(executor, db, "CREATE TABLE copied AS SELECT id, rx_range_list(n) AS ids, rx_echo_strings(ARRAY['x']) AS names FROM src");

        TableSchema copied = (await executor.OpenDatabase(db)).Schema.Tables["copied"];
        TableColumnSchema ids = copied.Columns!.Single(c => c.Name == "ids");
        Assert.AreEqual(ColumnType.Array, ids.Type);
        Assert.AreEqual(ColumnType.Integer64, ids.ArrayElementType, "the function's declared element type types the column");
        Assert.AreEqual(ColumnType.String, copied.Columns!.Single(c => c.Name == "names").ArrayElementType);

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT id, ids FROM copied ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Elements(rows[1]["ids"]).Select(v => v.LongValue));

        // A derived table over the function carries the element type to its outer query.
        await ExecDdl(executor, db, "CREATE TABLE copied2 AS SELECT d.ids FROM (SELECT rx_range_list(n) AS ids FROM src) AS d");
        Assert.AreEqual(ColumnType.Integer64,
            (await executor.OpenDatabase(db)).Schema.Tables["copied2"].Columns!.Single(c => c.Name == "ids").ArrayElementType);

        // A subscript of an array function result has the element type.
        Assert.AreEqual(2, (await Scalar(executor, db, "SELECT rx_range_list(3)[2]")).LongValue);
    }

    [Test]
    public async Task TriggerRows_AndProcedureArguments_MapArrays()
    {
        // The boxed form uses nullable elements, so a NULL element is visible.
        object? boxed = ExtensionValueMapping.ToObject(ColumnValue.FromArray(ColumnType.Integer64, [new ColumnValue(ColumnType.Integer64, 5), ColumnValue.Null]));
        IReadOnlyList<long?> list = (IReadOnlyList<long?>)boxed!;
        CollectionAssert.AreEqual(new long?[] { 5, null }, list);

        ColumnValue back = ExtensionValueMapping.FromObject(new List<string> { "a", "b" });
        Assert.AreEqual(ColumnType.String, back.ArrayElementType);
        CollectionAssert.AreEqual(new[] { "a", "b" }, back.ArrayValues!.Select(v => v.StrValue));

        Assert.AreEqual(ColumnType.Null, ExtensionValueMapping.FromObject((IReadOnlyList<long>?)null).Type);

        // GetArray reads with the element type the caller chooses; SetArray writes through the setter path.
        TriggerRow row = new(
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                ["ids"] = ColumnValue.FromArray(ColumnType.Integer64, [new ColumnValue(ColumnType.Integer64, 1), ColumnValue.Null]),
            },
            [new TableColumnSchema("c1", "ids", ColumnType.Array, notNull: false, defaultValue: null, arrayElementType: ColumnType.Integer64)]);

        CollectionAssert.AreEqual(new long?[] { 1, null }, row.GetArray<long?>("ids"));
        Assert.Throws<InvalidCastException>(() => row.GetArray<long>("ids"), "a NULL element needs a nullable element type");
        Assert.Throws<InvalidCastException>(() => row.GetArray<string>("ids"), "the element type must match");

        row.SetArray("ids", new long[] { 7, 8 });
        CollectionAssert.AreEqual(new long?[] { 7, 8 }, row.GetArray<long?>("ids"));
        Assert.AreEqual("ids", row.ChangedColumns!.Single().Name);

        await Task.CompletedTask;
    }

    // ── Signatures the binder refuses ────────────────────────────────────────

    public static class BadSignatures
    {
        [CamusFunction("bad_nested")]
        public static long Nested(IReadOnlyList<IReadOnlyList<long>> values) => 0;

        [CamusFunction("bad_bytes_elements")]
        public static long BytesElements(IReadOnlyList<byte[]> values) => 0;

        [CamusFunction("bad_element")]
        public static long UnmappedElement(IReadOnlyList<int> values) => 0;

        [CamusFunction("bad_clr_array")]
        public static long ClrArray(long[] values) => 0;

        [CamusFunction("bad_return")]
        public static IReadOnlyList<IReadOnlyList<long>> NestedReturn(long value) => [];
    }

    [TestCase(nameof(BadSignatures.Nested), "nested arrays are not mapped")]
    [TestCase(nameof(BadSignatures.BytesElements), "arrays of byte[] are not mapped")]
    [TestCase(nameof(BadSignatures.UnmappedElement), "element type System.Int32")]
    [TestCase(nameof(BadSignatures.ClrArray), "declare an array as IReadOnlyList<T>")]
    [TestCase(nameof(BadSignatures.NestedReturn), "nested arrays are not mapped")]
    public void Binder_RefusesUnmappedArrays_AndNamesTheMember(string methodName, string reason)
    {
        MethodInfo method = typeof(BadSignatures).GetMethod(methodName)!;

        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            ExtensionFunctionBinder.Bind(method, method.GetCustomAttribute<CamusFunctionAttribute>()!))!;

        Assert.AreEqual(CamusDBErrorCodes.ExtensionInvalidSignature, ex.Code);
        StringAssert.Contains(methodName, ex.Message, "the error names the member");
        StringAssert.Contains(reason, ex.Message);
    }

    [Test]
    public async Task ShowExtensions_PrintsArraySignatures()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        Dictionary<string, ColumnValue> rich = (await ExecQuery(executor, db, "SHOW EXTENSIONS")).Single(r => r["name"].StrValue == "rich");
        string signatures = rich["signatures"].StrValue!;

        StringAssert.Contains("rx_sum_list(ARRAY(INT64)) -> INT64", signatures);
        StringAssert.Contains("rx_join(ARRAY(STRING), STRING) -> STRING", signatures);
        StringAssert.Contains("rx_range_list(INT64) -> ARRAY(INT64)", signatures);
    }
}
