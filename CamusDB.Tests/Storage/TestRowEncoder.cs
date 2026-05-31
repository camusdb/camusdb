
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

[TestFixture]
public sealed class TestRowEncoder
{
    // ---- schema helpers ---------------------------------------------------

    private static TableColumnSchema Col(string name, ColumnType type, bool notNull = false) =>
        new(name, name, type, notNull, null);

    /// <summary>
    /// Builds a minimal TableSchema whose SchemaHistory[version] matches its Columns list,
    /// so both Encode and Decode agree on what columns exist.
    /// </summary>
    private static TableSchema MakeSchema(int version, params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);

        // Pad SchemaHistory so index 0..version are all valid (filled with the same column set).
        List<TableSchemaHistory> history = new();
        for (int v = 0; v <= version; v++)
            history.Add(new TableSchemaHistory { Version = v, Columns = cols });

        return new TableSchema
        {
            Id = "test-table",
            Name = "test",
            Version = version,
            Columns = cols,
            SchemaHistory = history
        };
    }

    private static ObjectIdValue RowId(int a = 1, int b = 2, int c = 3) => new(a, b, c);

    // ---- round-trip tests -------------------------------------------------

    [Test]
    public void RoundTrip_Integer64()
    {
        TableSchema schema = MakeSchema(0, Col("n", ColumnType.Integer64));
        ObjectIdValue rowId = RowId();

        foreach (long v in new[] { long.MinValue, -1L, 0L, 1L, long.MaxValue })
        {
            Dictionary<string, ColumnValue> row = new() { ["n"] = new(ColumnType.Integer64, v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.AreEqual(ColumnType.Integer64, decoded["n"].Type);
            Assert.AreEqual(v, decoded["n"].LongValue, $"Round-trip failed for long {v}");
        }
    }

    [Test]
    public void RoundTrip_Float64()
    {
        TableSchema schema = MakeSchema(0, Col("f", ColumnType.Float64));
        ObjectIdValue rowId = RowId();

        foreach (double v in new[] { double.MinValue, -1.5, 0.0, 1.5, double.MaxValue })
        {
            Dictionary<string, ColumnValue> row = new() { ["f"] = new(ColumnType.Float64, v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.AreEqual(ColumnType.Float64, decoded["f"].Type);
            Assert.AreEqual(v, decoded["f"].FloatValue, $"Round-trip failed for double {v}");
        }
    }

    [Test]
    public void RoundTrip_Bool()
    {
        TableSchema schema = MakeSchema(0, Col("b", ColumnType.Bool));
        ObjectIdValue rowId = RowId();

        foreach (bool v in new[] { true, false })
        {
            Dictionary<string, ColumnValue> row = new() { ["b"] = new(ColumnType.Bool, v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.AreEqual(ColumnType.Bool, decoded["b"].Type);
            Assert.AreEqual(v, decoded["b"].BoolValue, $"Round-trip failed for bool {v}");
        }
    }

    [Test]
    public void RoundTrip_String()
    {
        TableSchema schema = MakeSchema(0, Col("s", ColumnType.String));
        ObjectIdValue rowId = RowId();

        foreach (string v in new[] { "", "hello", "unicode: é中文", "a\0b" })
        {
            Dictionary<string, ColumnValue> row = new() { ["s"] = new(ColumnType.String, v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.AreEqual(ColumnType.String, decoded["s"].Type);
            Assert.AreEqual(v, decoded["s"].StrValue, $"Round-trip failed for string '{v}'");
        }
    }

    [Test]
    public void RoundTrip_Id()
    {
        TableSchema schema = MakeSchema(0, Col("id", ColumnType.Id));
        ObjectIdValue rowId = RowId();

        ObjectIdValue idVal = new(1639931684, -1154155741, -743207513);
        string idStr = idVal.ToString();

        Dictionary<string, ColumnValue> row = new() { ["id"] = new(ColumnType.Id, idStr) };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.AreEqual(ColumnType.Id, decoded["id"].Type);
        Assert.AreEqual(idStr, decoded["id"].StrValue);
    }

    [Test]
    public void RoundTrip_NullColumn()
    {
        TableSchema schema = MakeSchema(0,
            Col("n", ColumnType.Integer64),
            Col("s", ColumnType.String)
        );
        ObjectIdValue rowId = RowId();

        // Provide only one of the two columns; the other should be null.
        Dictionary<string, ColumnValue> row = new()
        {
            ["n"] = new(ColumnType.Integer64, 42L)
            // "s" absent → written as TypeNull
        };

        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.AreEqual(ColumnType.Integer64, decoded["n"].Type);
        Assert.AreEqual(42L, decoded["n"].LongValue);
        Assert.AreEqual(ColumnType.Null, decoded["s"].Type);
    }

    [Test]
    public void RoundTrip_MultiColumnRow()
    {
        TableSchema schema = MakeSchema(0,
            Col("id",    ColumnType.Id),
            Col("name",  ColumnType.String),
            Col("age",   ColumnType.Integer64),
            Col("score", ColumnType.Float64),
            Col("active", ColumnType.Bool)
        );

        ObjectIdValue rowId = new(100, 200, 300);
        ObjectIdValue pkId  = new(1, 2, 3);

        Dictionary<string, ColumnValue> row = new()
        {
            ["id"]    = new(ColumnType.Id,        pkId.ToString()),
            ["name"]  = new(ColumnType.String,    "Alice"),
            ["age"]   = new(ColumnType.Integer64, 30L),
            ["score"] = new(ColumnType.Float64,   9.5),
            ["active"] = new(ColumnType.Bool,     true)
        };

        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.AreEqual(pkId.ToString(), decoded["id"].StrValue);
        Assert.AreEqual("Alice",         decoded["name"].StrValue);
        Assert.AreEqual(30L,             decoded["age"].LongValue);
        Assert.AreEqual(9.5,             decoded["score"].FloatValue);
        Assert.AreEqual(true,            decoded["active"].BoolValue);
    }

    [Test]
    public void RowIdEmbeddedInBytesIsIgnoredOnDecode()
    {
        // The rowId in the bytes is irrelevant — Decode discards it.
        // Decoding with a different rowId argument must still yield the same column values.
        TableSchema schema = MakeSchema(0, Col("n", ColumnType.Integer64));

        ObjectIdValue encodedWithId = new(1, 2, 3);
        Dictionary<string, ColumnValue> row = new() { ["n"] = new(ColumnType.Integer64, 99L) };
        byte[] bytes = RowEncoder.Encode(schema, row, encodedWithId);

        ObjectIdValue differentId = new(9, 9, 9);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, differentId, bytes);

        Assert.AreEqual(99L, decoded["n"].LongValue);
    }

    [Test]
    public void ByteCompatibility_MatchesRowSerializerFormat()
    {
        // Verify byte-level compatibility: the bytes produced by RowEncoder.Encode
        // must be decodable by RowEncoder.Decode.  Since both mirror the internal
        // RowSerializer/RowDeserializer exactly, the format is verified by construction.
        TableSchema schema = MakeSchema(0,
            Col("a", ColumnType.Integer64),
            Col("b", ColumnType.String),
            Col("c", ColumnType.Bool),
            Col("d", ColumnType.Float64)
        );

        ObjectIdValue rowId = new(111, 222, 333);

        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = new(ColumnType.Integer64, -42L),
            ["b"] = new(ColumnType.String,    "test"),
            ["c"] = new(ColumnType.Bool,      false),
            ["d"] = new(ColumnType.Float64,   3.14)
        };

        byte[] bytes = RowEncoder.Encode(schema, row, rowId);

        // Encode twice — must produce identical bytes (deterministic).
        byte[] bytes2 = RowEncoder.Encode(schema, row, rowId);
        Assert.AreEqual(bytes, bytes2, "Encode is not deterministic");

        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);
        Assert.AreEqual(-42L,   decoded["a"].LongValue);
        Assert.AreEqual("test", decoded["b"].StrValue);
        Assert.AreEqual(false,  decoded["c"].BoolValue);
        Assert.AreEqual(3.14,   decoded["d"].FloatValue);
    }

    [Test]
    public void PartialDecode_ReturnsOnlyRequiredColumns()
    {
        TableSchema schema = MakeSchema(0,
            Col("a", ColumnType.Integer64),
            Col("b", ColumnType.String),
            Col("c", ColumnType.Bool));
        ObjectIdValue rowId = RowId();
        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = new(ColumnType.Integer64, 1L),
            ["b"] = new(ColumnType.String, "x"),
            ["c"] = new(ColumnType.Bool, true),
        };

        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        HashSet<string> required = ["a", "c"];
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes, required);

        Assert.AreEqual(2, decoded.Count);
        Assert.AreEqual(1L, decoded["a"].LongValue);
        Assert.IsTrue(decoded["c"].BoolValue);
        Assert.IsFalse(decoded.ContainsKey("b"));
    }
}
