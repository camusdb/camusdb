
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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Tests.Storage;

[TestFixture]
public sealed class TestRowEncoder
{
    // ---- schema helpers ---------------------------------------------------

    private static TableColumnSchema Col(
        string name,
        ColumnType type,
        bool notNull = false,
        SchemaElementState state = SchemaElementState.Public
    ) => new(name, name, type, notNull, null, state);

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
    public void Decode_UsesCurrentStateForVisibilityWhenRowUsesOldSchemaHistory()
    {
        TableSchema schema = MakeSchema(0,
            Col("id", ColumnType.Id),
            Col("shadow", ColumnType.String));
        ObjectIdValue rowId = RowId();
        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["id"] = new(ColumnType.Id, rowId.ToString()),
            ["shadow"] = new(ColumnType.String, "hidden")
        }, rowId);

        schema.Version = 1;
        schema.Columns =
        [
            Col("id", ColumnType.Id),
            Col("shadow", ColumnType.String, state: SchemaElementState.DeleteOnly)
        ];

        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.True(decoded.ContainsKey("id"));
        Assert.False(decoded.ContainsKey("shadow"));
    }

    [Test]
    public void DecodeWritable_IncludesWriteOnlyColumnsButNotDeleteOnlyColumns()
    {
        TableSchema schema = MakeSchema(0,
            Col("public_col", ColumnType.String),
            Col("write_col", ColumnType.String, state: SchemaElementState.WriteOnly),
            Col("delete_col", ColumnType.String, state: SchemaElementState.DeleteOnly));
        ObjectIdValue rowId = RowId();
        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["public_col"] = new(ColumnType.String, "visible"),
            ["write_col"] = new(ColumnType.String, "internal"),
            ["delete_col"] = new(ColumnType.String, "ignored")
        }, rowId);

        Dictionary<string, ColumnValue> visible = RowEncoder.Decode(schema, rowId, bytes);
        Dictionary<string, ColumnValue> writable = RowEncoder.DecodeWritableAsync(
            schema,
            Kommander.Time.HLCTimestamp.Zero,
            rowId,
            bytes).AsTask().GetAwaiter().GetResult();

        Assert.True(visible.ContainsKey("public_col"));
        Assert.False(visible.ContainsKey("write_col"));
        Assert.False(visible.ContainsKey("delete_col"));

        Assert.True(writable.ContainsKey("public_col"));
        Assert.True(writable.ContainsKey("write_col"));
        Assert.False(writable.ContainsKey("delete_col"));
        Assert.AreEqual("internal", writable["write_col"].StrValue);
    }

    [Test]
    public void Decode_DoesNotMapDroppedColumnBytesToReaddedSameNameColumn()
    {
        TableSchema schema = MakeSchema(0, new TableColumnSchema(
            id: "old-x",
            name: "x",
            type: ColumnType.String,
            notNull: false,
            defaultValue: null));
        ObjectIdValue rowId = RowId();
        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["x"] = new(ColumnType.String, "old-bytes")
        }, rowId);

        schema.Version = 1;
        schema.Columns =
        [
            new TableColumnSchema("new-x", "x", ColumnType.String, false, null, SchemaElementState.Public)
        ];

        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.False(decoded.ContainsKey("x"));
    }

    [Test]
    public void Decode_NameFallbackStillWorksForLegacyIdlessHistoryColumns()
    {
        TableSchema schema = MakeSchema(0, new TableColumnSchema(
            id: "",
            name: "legacy",
            type: ColumnType.String,
            notNull: false,
            defaultValue: null));
        ObjectIdValue rowId = RowId();
        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["legacy"] = new(ColumnType.String, "kept")
        }, rowId);

        schema.Version = 1;
        schema.Columns =
        [
            new TableColumnSchema("legacy-id", "legacy", ColumnType.String, false, null, SchemaElementState.Public)
        ];

        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.True(decoded.ContainsKey("legacy"));
        Assert.AreEqual("kept", decoded["legacy"].StrValue);
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
    public void ByteCompatibility_EncodeDecodeRoundTrips()
    {
        // Verify byte-level compatibility: the bytes produced by RowEncoder.Encode
        // must be decodable by RowEncoder.Decode for the original column types — pins
        // the on-disk wire format so legacy data keeps decoding.
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

    // ---- new type round-trips -----------------------------------------------

    [Test]
    public void RoundTrip_Float32()
    {
        TableSchema schema = MakeSchema(0, Col("f", ColumnType.Float32));
        ObjectIdValue rowId = RowId();

        foreach (float v in new[] { float.MinValue, -1.5f, 0f, 1.5f, float.MaxValue })
        {
            Dictionary<string, ColumnValue> row = new() { ["f"] = new(ColumnType.Float32, (double)v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.That(decoded["f"].Type, Is.EqualTo(ColumnType.Float32));
            Assert.That((float)decoded["f"].FloatValue, Is.EqualTo(v), $"Float32 round-trip failed for {v}");
        }
    }

    [Test]
    public void RoundTrip_Float32_Zero_And_Negative()
    {
        TableSchema schema = MakeSchema(0, Col("f", ColumnType.Float32));
        ObjectIdValue rowId = RowId();

        foreach (double v in new[] { -999.0, 0.0, 1.0 })
        {
            Dictionary<string, ColumnValue> row = new() { ["f"] = new(ColumnType.Float32, v) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.That((float)decoded["f"].FloatValue, Is.EqualTo((float)v));
        }
    }

    [Test]
    public void RoundTrip_Date()
    {
        TableSchema schema = MakeSchema(0, Col("d", ColumnType.Date));
        ObjectIdValue rowId = RowId();

        long[] ticks =
        [
            new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc).Ticks,
            DateTime.MinValue.Ticks,
        ];

        foreach (long t in ticks)
        {
            Dictionary<string, ColumnValue> row = new() { ["d"] = new(ColumnType.Date, t) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.That(decoded["d"].Type, Is.EqualTo(ColumnType.Date));
            Assert.That(decoded["d"].LongValue, Is.EqualTo(t));
        }
    }

    [Test]
    public void RoundTrip_DateTime_MinMax()
    {
        TableSchema schema = MakeSchema(0, Col("dt", ColumnType.DateTime));
        ObjectIdValue rowId = RowId();

        long[] ticks = [DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks,
            new DateTime(2026, 1, 1, 12, 30, 0, DateTimeKind.Utc).Ticks];

        foreach (long t in ticks)
        {
            Dictionary<string, ColumnValue> row = new() { ["dt"] = new(ColumnType.DateTime, t) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.That(decoded["dt"].Type, Is.EqualTo(ColumnType.DateTime));
            Assert.That(decoded["dt"].LongValue, Is.EqualTo(t));
        }
    }

    [Test]
    public void RoundTrip_Bytes_Empty()
    {
        TableSchema schema = MakeSchema(0, Col("b", ColumnType.Bytes));
        ObjectIdValue rowId = RowId();

        Dictionary<string, ColumnValue> row = new() { ["b"] = new(Array.Empty<byte>()) };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["b"].Type, Is.EqualTo(ColumnType.Bytes));
        Assert.That(decoded["b"].BytesValue, Is.Not.Null);
        Assert.That(decoded["b"].BytesValue!.Length, Is.EqualTo(0));
    }

    [Test]
    public void RoundTrip_Bytes_Payload()
    {
        TableSchema schema = MakeSchema(0, Col("b", ColumnType.Bytes));
        ObjectIdValue rowId = RowId();

        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
        Dictionary<string, ColumnValue> row = new() { ["b"] = new(payload) };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["b"].Type, Is.EqualTo(ColumnType.Bytes));
        Assert.That(decoded["b"].BytesValue, Is.EqualTo(payload));
    }

    [Test]
    public void RoundTrip_Uuid()
    {
        TableSchema schema = MakeSchema(0, Col("u", ColumnType.Uuid));
        ObjectIdValue rowId = RowId();

        Guid[] samples =
        {
            Guid.Empty,
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
            Guid.NewGuid(),
            Guid.CreateVersion7(),
        };

        foreach (Guid g in samples)
        {
            Dictionary<string, ColumnValue> row = new() { ["u"] = ColumnValue.FromUuid(g) };
            byte[] bytes = RowEncoder.Encode(schema, row, rowId);
            Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

            Assert.That(decoded["u"].Type, Is.EqualTo(ColumnType.Uuid));
            Assert.That(decoded["u"].ToGuid(), Is.EqualTo(g), $"Uuid row round-trip failed for {g}");
        }
    }

    [Test]
    public void RoundTrip_Uuid_Array()
    {
        TableSchema schema = MakeSchema(0, Col("a", ColumnType.Array));
        ObjectIdValue rowId = RowId();

        Guid g1 = Guid.NewGuid();
        Guid g2 = Guid.CreateVersion7();
        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = ColumnValue.FromArray(ColumnType.Uuid, [ColumnValue.FromUuid(g1), ColumnValue.FromUuid(g2)])
        };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["a"].ArrayElementType, Is.EqualTo(ColumnType.Uuid));
        Assert.That(decoded["a"].ArrayValues![0].ToGuid(), Is.EqualTo(g1));
        Assert.That(decoded["a"].ArrayValues![1].ToGuid(), Is.EqualTo(g2));
    }

    [Test]
    public void RoundTrip_Array_Empty()
    {
        TableSchema schema = MakeSchema(0, Col("a", ColumnType.Array));
        ObjectIdValue rowId = RowId();

        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = ColumnValue.FromArray(ColumnType.Integer64, [])
        };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["a"].Type, Is.EqualTo(ColumnType.Array));
        Assert.That(decoded["a"].ArrayElementType, Is.EqualTo(ColumnType.Integer64));
        Assert.That(decoded["a"].ArrayValues!.Count, Is.EqualTo(0));
    }

    [Test]
    public void RoundTrip_Array_WithNullElement()
    {
        TableSchema schema = MakeSchema(0, Col("a", ColumnType.Array));
        ObjectIdValue rowId = RowId();

        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = ColumnValue.FromArray(ColumnType.Integer64, [
                new(ColumnType.Integer64, 10L),
                ColumnValue.Null,
                new(ColumnType.Integer64, 30L)
            ])
        };
        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["a"].ArrayValues![0].LongValue, Is.EqualTo(10L));
        Assert.That(decoded["a"].ArrayValues![1].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(decoded["a"].ArrayValues![2].LongValue, Is.EqualTo(30L));
    }

    [Test]
    public void RoundTrip_AllNewTypes_MultiColumnRow()
    {
        TableSchema schema = MakeSchema(0,
            Col("f32",  ColumnType.Float32),
            Col("dt",   ColumnType.DateTime),
            Col("d",    ColumnType.Date),
            Col("blob", ColumnType.Bytes),
            Col("arr",  ColumnType.Array)
        );
        ObjectIdValue rowId = RowId();

        long dateTimeTicks = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc).Ticks;
        long dateTicks     = new DateTime(2026, 6, 25, 0,  0, 0, DateTimeKind.Utc).Ticks;
        byte[] blob = [1, 2, 3, 4];

        Dictionary<string, ColumnValue> row = new()
        {
            ["f32"]  = new(ColumnType.Float32,  3.14),
            ["dt"]   = new(ColumnType.DateTime, dateTimeTicks),
            ["d"]    = new(ColumnType.Date,     dateTicks),
            ["blob"] = new(blob),
            ["arr"]  = ColumnValue.FromArray(ColumnType.Integer64, [new(ColumnType.Integer64, 7L)])
        };

        byte[] bytes   = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That((float)decoded["f32"].FloatValue,  Is.EqualTo((float)3.14));
        Assert.That(decoded["dt"].LongValue,           Is.EqualTo(dateTimeTicks));
        Assert.That(decoded["d"].LongValue,            Is.EqualTo(dateTicks));
        Assert.That(decoded["blob"].BytesValue,        Is.EqualTo(blob));
        Assert.That(decoded["arr"].ArrayValues![0].LongValue, Is.EqualTo(7L));
    }

    [Test]
    public void RoundTrip_OldSchemaVersion_UnchangedColumns()
    {
        // A row written with only Integer64/String (old schema) must still decode correctly
        // after schema version bumps — backward compat by construction.
        TableSchema schema = MakeSchema(0,
            Col("id",   ColumnType.Integer64),
            Col("name", ColumnType.String));
        ObjectIdValue rowId = RowId();

        Dictionary<string, ColumnValue> row = new()
        {
            ["id"]   = new(ColumnType.Integer64, 42L),
            ["name"] = new(ColumnType.String, "legacy")
        };

        byte[] bytes = RowEncoder.Encode(schema, row, rowId);
        Dictionary<string, ColumnValue> decoded = RowEncoder.Decode(schema, rowId, bytes);

        Assert.That(decoded["id"].LongValue,  Is.EqualTo(42L));
        Assert.That(decoded["name"].StrValue, Is.EqualTo("legacy"));
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

    // ---- DecodeToQueryRowAsync round-trip tests ----------------------------

    /// <summary>
    /// DecodeToQueryRowAsync must return a QueryRow whose IReadOnlyDictionary adapter yields
    /// the same values as the synchronous Decode path for every column in the row.
    /// </summary>
    [Test]
    public async Task DecodeToQueryRow_FullRow_AdapterValueEqualsDictionary()
    {
        TableSchema schema = MakeSchema(0,
            Col("id",    ColumnType.Id),
            Col("name",  ColumnType.String),
            Col("score", ColumnType.Integer64));
        ObjectIdValue rowId = RowId();

        string idStr = rowId.ToString();
        Dictionary<string, ColumnValue> source = new()
        {
            ["id"]    = new(ColumnType.Id,        idStr),
            ["name"]  = new(ColumnType.String,     "Alice"),
            ["score"] = new(ColumnType.Integer64,  42L),
        };

        byte[] bytes = RowEncoder.Encode(schema, source, rowId);

        // Synchronous decode — the reference result.
        Dictionary<string, ColumnValue> dictRow = RowEncoder.Decode(schema, rowId, bytes);

        // Async QueryRow decode — the new path under test.
        QueryRow qr = await RowEncoder.DecodeToQueryRowAsync(schema, default(HLCTimestamp), rowId, bytes);

        // The adapter must expose the same column set with identical values.
        Assert.AreEqual(dictRow.Count, qr.Count, "Column count must match");
        foreach (KeyValuePair<string, ColumnValue> kv in dictRow)
        {
            Assert.IsTrue(qr.ContainsKey(kv.Key), $"Column '{kv.Key}' missing from QueryRow");
            Assert.AreEqual(kv.Value.Type,     qr[kv.Key].Type,      $"Type mismatch for '{kv.Key}'");
            Assert.AreEqual(kv.Value.LongValue,  qr[kv.Key].LongValue,  $"LongValue mismatch for '{kv.Key}'");
            Assert.AreEqual(kv.Value.StrValue,   qr[kv.Key].StrValue,   $"StrValue mismatch for '{kv.Key}'");
        }
    }

    /// <summary>
    /// When requiredColumns is provided, DecodeToQueryRowAsync must return a QueryRow that
    /// contains only the requested columns, omitting the rest.
    /// </summary>
    [Test]
    public async Task DecodeToQueryRow_ProjectedColumns_ReturnsSubset()
    {
        TableSchema schema = MakeSchema(0,
            Col("a", ColumnType.Integer64),
            Col("b", ColumnType.String),
            Col("c", ColumnType.Bool));
        ObjectIdValue rowId = RowId(4, 5, 6);

        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["a"] = new(ColumnType.Integer64, 10L),
            ["b"] = new(ColumnType.String,    "hello"),
            ["c"] = new(ColumnType.Bool,      true),
        }, rowId);

        HashSet<string> required = ["a", "c"];
        QueryRow qr = await RowEncoder.DecodeToQueryRowAsync(
            schema, default(HLCTimestamp), rowId, bytes, required);

        Assert.AreEqual(2, qr.Count, "Only requested columns should be present");
        Assert.AreEqual(10L, qr["a"].LongValue);
        Assert.IsTrue(qr["c"].BoolValue);
        Assert.IsFalse(qr.ContainsKey("b"), "Non-requested column must be absent");
    }

    /// <summary>
    /// Nullable columns that are absent in the encoded row must appear as Null-typed values
    /// in the QueryRow returned by DecodeToQueryRowAsync, matching Decode behaviour.
    /// </summary>
    [Test]
    public async Task DecodeToQueryRow_NullableColumns_HandledCorrectly()
    {
        TableSchema schema = MakeSchema(0,
            Col("id",    ColumnType.Id),
            Col("notes", ColumnType.String, notNull: false));
        ObjectIdValue rowId = RowId(7, 8, 9);

        // Encode a row where "notes" is absent (null in the source dict).
        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["id"]    = new(ColumnType.Id, rowId.ToString()),
            ["notes"] = ColumnValue.Null,
        }, rowId);

        // Synchronous reference.
        Dictionary<string, ColumnValue> dictRow = RowEncoder.Decode(schema, rowId, bytes);

        QueryRow qr = await RowEncoder.DecodeToQueryRowAsync(schema, default(HLCTimestamp), rowId, bytes);

        // "notes" must be present and typed Null in both paths.
        Assert.IsTrue(qr.ContainsKey("notes"), "Null column must appear in QueryRow");
        Assert.AreEqual(dictRow["notes"].Type, qr["notes"].Type,
            "Null column type must match synchronous Decode");
    }

    /// <summary>
    /// When a column is added to the schema after a row was written (schema-history injection),
    /// DecodeToQueryRowAsync must inject the column's default value for the new column,
    /// matching the behaviour of DecodeAsync with schema-version forwarding.
    /// </summary>
    [Test]
    public async Task DecodeToQueryRow_SchemaHistory_InjectsMissingColumn()
    {
        // Version-0 schema: two columns.
        TableSchema schema = MakeSchema(0,
            Col("id",   ColumnType.Id),
            Col("name", ColumnType.String));
        ObjectIdValue rowId = RowId(10, 11, 12);

        byte[] bytes = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>()
        {
            ["id"]   = new(ColumnType.Id,     rowId.ToString()),
            ["name"] = new(ColumnType.String, "Bob"),
        }, rowId);

        // Upgrade schema to version 1: add "score" column with a default.
        TableColumnSchema scoreCol = new("score", "score", ColumnType.Integer64, false,
            new ColumnValue(ColumnType.Integer64, 99L), SchemaElementState.Public);

        schema.Version = 1;
        schema.Columns = [schema.SchemaHistory![0].Columns![0], schema.SchemaHistory[0].Columns![1], scoreCol];
        schema.SchemaHistory!.Add(new TableSchemaHistory
        {
            Version  = 1,
            Columns  = schema.Columns
        });

        // Decode using the v1 schema with visibilitySchemaVersion = 1 so injection fires.
        QueryRow qr = await RowEncoder.DecodeToQueryRowAsync(
            schema, default(HLCTimestamp), rowId, bytes,
            requiredColumns: null, visibilitySchemaVersion: 1);

        Assert.IsTrue(qr.ContainsKey("score"), "Injected column must appear in QueryRow");
        Assert.AreEqual(ColumnType.Integer64, qr["score"].Type,
            "Injected column must carry the column's default type");
        Assert.AreEqual(99L, qr["score"].LongValue,
            "Injected column must carry the column's default value");
    }
}
