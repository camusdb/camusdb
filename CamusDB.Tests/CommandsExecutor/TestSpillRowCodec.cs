/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
public sealed class TestSpillRowCodec
{
    // ──────────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private static QueryResultRow MakeRow(ObjectIdValue rowId, Dictionary<string, ColumnValue> cols) =>
        new(rowId, cols);

    private static QueryResultRow RoundTrip(QueryResultRow row)
    {
        byte[] frame = SpillRowCodec.Encode(row);
        int offset = 0;
        return SpillRowCodec.Decode(frame, ref offset);
    }

    private static void AssertColumnValueEqual(string label, ColumnValue expected, ColumnValue actual)
    {
        Assert.That(actual.Type, Is.EqualTo(expected.Type), $"{label}.Type");

        switch (expected.Type)
        {
            case ColumnType.Null:
                break;
            case ColumnType.Id:
            case ColumnType.String:
                Assert.That(actual.StrValue, Is.EqualTo(expected.StrValue), $"{label}.StrValue");
                break;
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                Assert.That(actual.LongValue, Is.EqualTo(expected.LongValue), $"{label}.LongValue");
                break;
            case ColumnType.Float64:
                Assert.That(actual.FloatValue, Is.EqualTo(expected.FloatValue), $"{label}.FloatValue");
                break;
            case ColumnType.Float32:
                Assert.That((float)actual.FloatValue, Is.EqualTo((float)expected.FloatValue), $"{label}.FloatValue(f32)");
                break;
            case ColumnType.Bool:
                Assert.That(actual.BoolValue, Is.EqualTo(expected.BoolValue), $"{label}.BoolValue");
                break;
            case ColumnType.Bytes:
                Assert.That(actual.BytesValue, Is.EqualTo(expected.BytesValue), $"{label}.BytesValue");
                break;
            case ColumnType.Array:
                Assert.That(actual.ArrayElementType, Is.EqualTo(expected.ArrayElementType), $"{label}.ArrayElementType");
                IReadOnlyList<ColumnValue> ea = expected.ArrayValues ?? [];
                IReadOnlyList<ColumnValue> aa = actual.ArrayValues ?? [];
                Assert.That(aa.Count, Is.EqualTo(ea.Count), $"{label}.ArrayValues.Count");
                for (int i = 0; i < ea.Count; i++)
                    AssertColumnValueEqual($"{label}[{i}]", ea[i], aa[i]);
                break;
        }
    }

    private static void AssertRowEqual(QueryResultRow expected, QueryResultRow actual)
    {
        Assert.That(actual.RowId, Is.EqualTo(expected.RowId), "RowId");
        Assert.That(actual.Row.Count, Is.EqualTo(expected.Row.Count), "Row.Count");
        foreach (KeyValuePair<string, ColumnValue> kv in expected.Row)
        {
            Assert.That(actual.Row.ContainsKey(kv.Key), Is.True, $"missing key '{kv.Key}'");
            AssertColumnValueEqual(kv.Key, kv.Value, actual.Row[kv.Key]);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // RowId round-trip
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void RoundTrip_NonDefaultRowId()
    {
        var rowId = new ObjectIdValue(1, 2, 3);
        var row = MakeRow(rowId, new Dictionary<string, ColumnValue>
        {
            ["x"] = new(ColumnType.Integer64, 42L)
        });

        QueryResultRow decoded = RoundTrip(row);

        Assert.That(decoded.RowId, Is.EqualTo(rowId));
    }

    [Test]
    public void RoundTrip_EmptyRowId_IsSentinel()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>
        {
            ["x"] = new(ColumnType.Integer64, 0L)
        });

        QueryResultRow decoded = RoundTrip(row);

        Assert.That(decoded.RowId, Is.EqualTo(ObjectIdValue.Empty));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Individual ColumnType round-trips
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void RoundTrip_Null()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = ColumnValue.Null });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Id()
    {
        string idStr = ObjectId.ToString(ObjectIdGenerator.Generate());
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Id, idStr) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Integer64()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Integer64, long.MinValue) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Integer64_MaxValue()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Integer64, long.MaxValue) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Float64()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Float64, Math.PI) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Float32()
    {
        float f = 3.14159f;
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Float32, (double)f) });
        QueryResultRow decoded = RoundTrip(row);
        Assert.That((float)decoded.Row["col"].FloatValue, Is.EqualTo(f));
    }

    [Test]
    public void RoundTrip_Bool_True()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = ColumnValue.True });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Bool_False()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = ColumnValue.False });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_String_Empty()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.String, "") });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_String_Unicode()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.String, "héllo wörld 日本語") });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Date()
    {
        long ticks = new DateTime(2025, 3, 15, 0, 0, 0, DateTimeKind.Utc).Ticks;
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.Date, ticks) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_DateTime()
    {
        long ticks = new DateTime(2025, 6, 29, 12, 34, 56, DateTimeKind.Utc).Ticks;
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new(ColumnType.DateTime, ticks) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Bytes_Empty()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new ColumnValue([]) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Bytes_NonEmpty()
    {
        byte[] data = [0x00, 0xFF, 0x42, 0x01, 0xDE, 0xAD];
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new ColumnValue(data) });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Bytes_Large()
    {
        byte[] data = new byte[1024 * 64];
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)(i & 0xFF);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["col"] = new ColumnValue(data) });
        AssertRowEqual(row, RoundTrip(row));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Array round-trips
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void RoundTrip_Array_Integer64()
    {
        var arr = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, -99L),
            new(ColumnType.Integer64, long.MaxValue),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_String()
    {
        var arr = ColumnValue.FromArray(ColumnType.String, [
            new(ColumnType.String, "alpha"),
            new(ColumnType.String, "β"),
            new(ColumnType.String, ""),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_WithNullElements()
    {
        var arr = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            ColumnValue.Null,
            new(ColumnType.Integer64, 3L),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Empty()
    {
        var arr = ColumnValue.FromArray(ColumnType.Float64, []);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Bool()
    {
        var arr = ColumnValue.FromArray(ColumnType.Bool, [
            ColumnValue.True, ColumnValue.False, ColumnValue.True
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Float32()
    {
        var arr = ColumnValue.FromArray(ColumnType.Float32, [
            new(ColumnType.Float32, (double)1.5f),
            new(ColumnType.Float32, (double)-99.9f),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        QueryResultRow decoded = RoundTrip(row);
        IReadOnlyList<ColumnValue> orig = arr.ArrayValues!;
        IReadOnlyList<ColumnValue> got = decoded.Row["arr"].ArrayValues!;
        Assert.That(got.Count, Is.EqualTo(orig.Count));
        for (int i = 0; i < orig.Count; i++)
            Assert.That((float)got[i].FloatValue, Is.EqualTo((float)orig[i].FloatValue));
    }

    [Test]
    public void RoundTrip_Array_Id()
    {
        // array<oid> is a declarable column type (DDL permits Id array elements);
        // the codec must round-trip it, not throw.
        var arr = ColumnValue.FromArray(ColumnType.Id, [
            new(ColumnType.Id, ObjectId.ToString(ObjectIdGenerator.Generate())),
            new(ColumnType.Id, ObjectId.ToString(ObjectIdGenerator.Generate())),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Float64_WithElements()
    {
        var arr = ColumnValue.FromArray(ColumnType.Float64, [
            new(ColumnType.Float64, Math.PI),
            new(ColumnType.Float64, -2.5),
            new(ColumnType.Float64, 0.0),
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Date_And_DateTime()
    {
        long d  = new DateTime(2023, 12, 25, 0, 0, 0, DateTimeKind.Utc).Ticks;
        long dt = new DateTime(2023, 12, 25, 18, 5, 1, DateTimeKind.Utc).Ticks;
        var dates = ColumnValue.FromArray(ColumnType.Date, [new(ColumnType.Date, d)]);
        var dts   = ColumnValue.FromArray(ColumnType.DateTime, [new(ColumnType.DateTime, dt)]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["dates"] = dates, ["dts"] = dts });
        AssertRowEqual(row, RoundTrip(row));
    }

    [Test]
    public void RoundTrip_Array_Bytes()
    {
        var arr = ColumnValue.FromArray(ColumnType.Bytes, [
            new ColumnValue(new byte[] { 0x01, 0x02 }),
            new ColumnValue(System.Array.Empty<byte>()),
            ColumnValue.Null,
        ]);
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["arr"] = arr });
        AssertRowEqual(row, RoundTrip(row));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Multi-column row
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void RoundTrip_AllTypes_SingleRow()
    {
        long dateTicks = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        long dtTicks   = new DateTime(2024, 6, 15, 9, 30, 0, DateTimeKind.Utc).Ticks;
        string idStr   = ObjectId.ToString(ObjectIdGenerator.Generate());

        var cols = new Dictionary<string, ColumnValue>
        {
            ["null_col"]     = ColumnValue.Null,
            ["id_col"]       = new(ColumnType.Id, idStr),
            ["int64_col"]    = new(ColumnType.Integer64, -12345678901234L),
            ["float64_col"]  = new(ColumnType.Float64, 2.718281828),
            ["float32_col"]  = new(ColumnType.Float32, (double)1.41421f),
            ["bool_true"]    = ColumnValue.True,
            ["bool_false"]   = ColumnValue.False,
            ["string_col"]   = new(ColumnType.String, "hello world"),
            ["date_col"]     = new(ColumnType.Date, dateTicks),
            ["datetime_col"] = new(ColumnType.DateTime, dtTicks),
            ["bytes_col"]    = new ColumnValue([0xDE, 0xAD, 0xBE, 0xEF]),
            ["array_col"]    = ColumnValue.FromArray(ColumnType.Integer64, [
                new(ColumnType.Integer64, 10L),
                new(ColumnType.Integer64, 20L),
            ]),
        };

        var row = MakeRow(new ObjectIdValue(7, 8, 9), cols);
        AssertRowEqual(row, RoundTrip(row));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Synthetic post-join row (qualified names, no TableSchema)
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void RoundTrip_PostJoin_QualifiedColumnNames()
    {
        // Simulates a row produced by a join: "o.id", "o.total", "c.name"
        // plus a computed column like "__expr_0" — none of these map to any TableSchema.
        var cols = new Dictionary<string, ColumnValue>
        {
            ["o.id"]      = new(ColumnType.Id, ObjectId.ToString(ObjectIdGenerator.Generate())),
            ["o.total"]   = new(ColumnType.Float64, 99.95),
            ["c.name"]    = new(ColumnType.String, "Acme Corp"),
            ["__expr_0"]  = new(ColumnType.Integer64, 42L),
        };

        var row = MakeRow(new ObjectIdValue(100, 200, 300), cols);
        QueryResultRow decoded = RoundTrip(row);

        Assert.That(decoded.Row.ContainsKey("o.id"),     Is.True);
        Assert.That(decoded.Row.ContainsKey("o.total"),  Is.True);
        Assert.That(decoded.Row.ContainsKey("c.name"),   Is.True);
        Assert.That(decoded.Row.ContainsKey("__expr_0"), Is.True);
        AssertRowEqual(row, decoded);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Multi-record streaming (DecodeAll)
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void DecodeAll_MultipleRecords()
    {
        var rows = new List<QueryResultRow>
        {
            MakeRow(new ObjectIdValue(1,0,0), new Dictionary<string, ColumnValue>() { ["a"] = new(ColumnType.Integer64, 1L) }),
            MakeRow(new ObjectIdValue(2,0,0), new Dictionary<string, ColumnValue>() { ["a"] = new(ColumnType.Integer64, 2L) }),
            MakeRow(new ObjectIdValue(3,0,0), new Dictionary<string, ColumnValue>() { ["a"] = new(ColumnType.Integer64, 3L) }),
        };

        using var ms = new System.IO.MemoryStream();
        foreach (QueryResultRow r in rows)
            SpillRowCodec.EncodeToStream(ms, r);

        byte[] buf = ms.ToArray();
        List<QueryResultRow> decoded = SpillRowCodec.DecodeAll(buf);

        Assert.That(decoded.Count, Is.EqualTo(rows.Count));
        for (int i = 0; i < rows.Count; i++)
            AssertRowEqual(rows[i], decoded[i]);
    }

    [Test]
    public void DecodeAll_EmptyBuffer_ReturnsEmpty()
    {
        List<QueryResultRow> result = SpillRowCodec.DecodeAll([]);
        Assert.That(result.Count, Is.EqualTo(0));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Frame integrity
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void Decode_TruncatedFrame_Throws()
    {
        var row = MakeRow(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>() { ["x"] = new(ColumnType.Integer64, 1L) });
        byte[] frame = SpillRowCodec.Encode(row);

        // Truncate the frame to half
        byte[] truncated = frame[..(frame.Length / 2)];
        int offset = 0;
        Assert.Throws<InvalidDataException>(() => SpillRowCodec.Decode(truncated, ref offset));
    }
}
