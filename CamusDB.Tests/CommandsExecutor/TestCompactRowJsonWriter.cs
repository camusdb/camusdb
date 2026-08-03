
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.App.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Parity tests for the streaming JSON row writer (<see cref="CompactRowJsonWriter"/> via
/// <see cref="PositionalRowSet"/>). The previous response path built a <c>List&lt;object?[]&gt;</c>
/// with <see cref="CompactRowEncoder.EncodeRow"/> and let MVC serialize it; the new path streams the
/// same rows straight to the <see cref="Utf8JsonWriter"/>. These tests assert the two produce
/// byte-identical JSON across every column type, NULL, arrays, and both row backings (layout-backed
/// <see cref="QueryRow"/> ordinal path and plain dictionary fallback).
/// </summary>
public sealed class TestCompactRowJsonWriter
{
    // MVC's default response options are camelCase; the row array itself has no property names so the
    // policy is irrelevant to the rows, but we match it exactly to be faithful to the real pipeline.
    private static readonly JsonSerializerOptions Options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    /// <summary>Serializes rows the OLD way: object?[] graph via CompactRowEncoder, then STJ.</summary>
    private static string OldJson(IReadOnlyList<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema> schema)
    {
        List<object?[]> encoded = rows.Select(r => CompactRowEncoder.EncodeRow(r.Row, schema)).ToList();
        return JsonSerializer.Serialize(encoded, Options);
    }

    /// <summary>Serializes rows the NEW way: PositionalRowSet → converter → Utf8JsonWriter.</summary>
    private static string NewJson(IReadOnlyList<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema> schema)
        => JsonSerializer.Serialize(new PositionalRowSet(rows, schema), Options);

    private static void AssertParity(IReadOnlyList<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema> schema)
        => Assert.AreEqual(OldJson(rows, schema), NewJson(rows, schema));

    // ── Row builders (dict-backed vs layout-backed QueryRow) ──

    private static QueryResultRow DictRow(IReadOnlyList<DerivedColumnSchema> schema, params ColumnValue[] values)
    {
        Dictionary<string, ColumnValue> dict = new(StringComparer.Ordinal);
        for (int i = 0; i < schema.Count; i++)
            dict[schema[i].Name] = values[i];
        return new QueryResultRow(new ObjectIdValue(1, 2, 3), dict);
    }

    private static QueryResultRow QueryRowBacked(RowLayout layout, IReadOnlyList<DerivedColumnSchema> schema, params ColumnValue[] values)
    {
        ColumnValue[] ordinal = new ColumnValue[layout.Count];
        for (int i = 0; i < schema.Count; i++)
            ordinal[layout.IndexOf(schema[i].Name)] = values[i];
        return new QueryResultRow(new ObjectIdValue(4, 5, 6), new QueryRow(new ObjectIdValue(4, 5, 6), layout, ordinal));
    }

    // ── All scalar types, dict-backed ──

    [Test]
    public void AllScalarTypes_DictRow_Parity()
    {
        DerivedColumnSchema[] schema =
        [
            new("i",  ColumnType.Integer64),
            new("s",  ColumnType.String),
            new("b",  ColumnType.Bool),
            new("f",  ColumnType.Float64),
            new("f32", ColumnType.Float32),
            new("d",  ColumnType.Date),
            new("dt", ColumnType.DateTime),
            new("id", ColumnType.Id),
        ];

        QueryResultRow row = DictRow(schema,
            new ColumnValue(ColumnType.Integer64, -9223372036854775808L),
            new ColumnValue(ColumnType.String, "héllo \"quoted\"\n\t"),
            ColumnValue.True,
            new ColumnValue(ColumnType.Float64, 3.14159265358979),
            new ColumnValue(ColumnType.Float32, 1.5f),
            new ColumnValue(ColumnType.Date, 638000000000000000L),
            new ColumnValue(ColumnType.DateTime, 638123456789012345L),
            new ColumnValue(ColumnType.Id, new ObjectIdValue(9, 8, 7).ToString()));

        AssertParity([row], schema);
    }

    // ── Uuid + Bytes, dict-backed ──

    [Test]
    public void UuidAndBytes_DictRow_Parity()
    {
        DerivedColumnSchema[] schema =
        [
            new("u", ColumnType.Uuid),
            new("by", ColumnType.Bytes),
        ];

        QueryResultRow row = DictRow(schema,
            ColumnValue.FromUuid(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")),
            new ColumnValue(new byte[] { 0, 1, 2, 250, 255 }));

        AssertParity([row], schema);
    }

    // ── NULL cells and an absent schema name (encodes null both ways) ──

    [Test]
    public void Nulls_And_AbsentColumn_Parity()
    {
        DerivedColumnSchema[] schema =
        [
            new("present", ColumnType.Integer64),
            new("nullcell", ColumnType.String),
            new("absent", ColumnType.Integer64),
        ];

        Dictionary<string, ColumnValue> dict = new(StringComparer.Ordinal)
        {
            ["present"] = new ColumnValue(ColumnType.Integer64, 5L),
            ["nullcell"] = ColumnValue.Null,
            // "absent" intentionally not in the row → null in both paths
        };
        QueryResultRow row = new(new ObjectIdValue(1, 1, 1), dict);

        AssertParity([row], schema);
    }

    // ── Arrays (including empty and NULL elements) ──

    [Test]
    public void Arrays_Parity()
    {
        DerivedColumnSchema[] schema =
        [
            new("ints", ColumnType.Array),
            new("strs", ColumnType.Array),
            new("empty", ColumnType.Array),
        ];

        ColumnValue intArray = ColumnValue.FromArray(ColumnType.Integer64,
        [
            new ColumnValue(ColumnType.Integer64, 1L),
            ColumnValue.Null,
            new ColumnValue(ColumnType.Integer64, 3L),
        ]);
        ColumnValue strArray = ColumnValue.FromArray(ColumnType.String,
        [
            new ColumnValue(ColumnType.String, "a"),
            new ColumnValue(ColumnType.String, "b"),
        ]);
        ColumnValue emptyArray = ColumnValue.FromArray(ColumnType.Integer64, []);

        QueryResultRow row = DictRow(schema, intArray, strArray, emptyArray);
        AssertParity([row], schema);
    }

    // ── Layout-backed QueryRow (ordinal path) must match the dict path byte-for-byte ──

    [Test]
    public void QueryRowOrdinalPath_MatchesDictPath()
    {
        DerivedColumnSchema[] schema =
        [
            new("id", ColumnType.Id),
            new("n",  ColumnType.Integer64),
            new("s",  ColumnType.String),
            new("u",  ColumnType.Uuid),
        ];

        RowLayout layout = new(schema.Select(c => c.Name));
        ColumnValue[] values =
        [
            new ColumnValue(ColumnType.Id, new ObjectIdValue(1, 2, 3).ToString()),
            new ColumnValue(ColumnType.Integer64, 77L),
            new ColumnValue(ColumnType.String, "row"),
            ColumnValue.FromUuid(Guid.NewGuid()),
        ];

        QueryResultRow dictRow = DictRow(schema, values);
        QueryResultRow qrRow = QueryRowBacked(layout, schema, values);

        // Old path over the dict row is the oracle; the new path over the QueryRow must equal it.
        Assert.AreEqual(OldJson([dictRow], schema), NewJson([qrRow], schema));
    }

    // ── Multiple rows sharing one layout exercise the ordinal-binding reuse across rows ──

    [Test]
    public void MultipleRows_SharedLayout_Parity()
    {
        DerivedColumnSchema[] schema =
        [
            new("n", ColumnType.Integer64),
            new("s", ColumnType.String),
        ];
        RowLayout layout = new(schema.Select(c => c.Name));

        List<QueryResultRow> qrRows = [];
        List<QueryResultRow> dictRows = [];
        for (int i = 0; i < 5; i++)
        {
            ColumnValue[] v = [new ColumnValue(ColumnType.Integer64, (long)i), new ColumnValue(ColumnType.String, "s" + i)];
            qrRows.Add(QueryRowBacked(layout, schema, v));
            dictRows.Add(DictRow(schema, v));
        }

        Assert.AreEqual(OldJson(dictRows, schema), NewJson(qrRows, schema));
    }

    // ── Empty result set ──

    [Test]
    public void EmptyRowSet_Parity()
    {
        DerivedColumnSchema[] schema = [new("a", ColumnType.Integer64)];
        AssertParity([], schema);
        Assert.AreEqual("[]", NewJson([], schema));
    }

    // ── Slot-backed QueryRow: the sink serializes straight from ValueSlots (no ColumnValue per cell);
    //    output must stay byte-identical to the ColumnValue oracle across every type. ──

    private static QueryResultRow SlotRowBacked(RowLayout layout, IReadOnlyList<DerivedColumnSchema> schema, params ColumnValue[] values)
    {
        ValueSlot[] slots = new ValueSlot[layout.Count];
        for (int i = 0; i < schema.Count; i++)
            slots[layout.IndexOf(schema[i].Name)] = ValueSlot.FromColumnValue(values[i]);
        ObjectIdValue id = new(7, 8, 9);
        return new QueryResultRow(id, QueryRow.FromSlots(id, layout, slots));
    }

    [Test]
    public void SlotBackedRow_AllTypes_MatchesDictPath()
    {
        DerivedColumnSchema[] schema =
        [
            new("i",   ColumnType.Integer64),
            new("s",   ColumnType.String),
            new("b",   ColumnType.Bool),
            new("f",   ColumnType.Float64),
            new("f32", ColumnType.Float32),
            new("d",   ColumnType.Date),
            new("dt",  ColumnType.DateTime),
            new("id",  ColumnType.Id),
            new("u",   ColumnType.Uuid),
            new("by",  ColumnType.Bytes),
            new("arr", ColumnType.Array),
            new("nul", ColumnType.String),
        ];
        RowLayout layout = new(schema.Select(c => c.Name));

        ColumnValue[] values =
        [
            new ColumnValue(ColumnType.Integer64, -42L),
            new ColumnValue(ColumnType.String, "héllo \"quoted\"\n"),
            ColumnValue.True,
            new ColumnValue(ColumnType.Float64, 2.718281828),
            new ColumnValue(ColumnType.Float32, -1.25f),
            new ColumnValue(ColumnType.Date, 638000000000000000L),
            new ColumnValue(ColumnType.DateTime, 638123456789012345L),
            new ColumnValue(ColumnType.Id, new ObjectIdValue(9, 8, 7).ToString()),
            ColumnValue.FromUuid(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")),
            new ColumnValue(new byte[] { 0, 1, 250, 255 }),
            ColumnValue.FromArray(ColumnType.Integer64,
            [
                new ColumnValue(ColumnType.Integer64, 1L),
                ColumnValue.Null,
                new ColumnValue(ColumnType.Integer64, 3L),
            ]),
            ColumnValue.Null,
        ];

        QueryResultRow slotRow = SlotRowBacked(layout, schema, values);
        QueryResultRow dictRow = DictRow(schema, values);

        Assert.AreEqual(OldJson([dictRow], schema), NewJson([slotRow], schema));
    }

    [Test]
    public void SlotBackedRow_StaysUnmaterialized_AfterSerialization()
    {
        DerivedColumnSchema[] schema = [new("n", ColumnType.Integer64), new("s", ColumnType.String)];
        RowLayout layout = new(schema.Select(c => c.Name));
        QueryResultRow row = SlotRowBacked(layout, schema,
            new ColumnValue(ColumnType.Integer64, 1L), new ColumnValue(ColumnType.String, "x"));

        NewJson([row], schema);

        // TryGetSlot still serves every cell → serialization went through the slot path and populated
        // neither the per-cell cache nor the eager backing.
        QueryRow queryRow = (QueryRow)row.Row;
        Assert.IsTrue(queryRow.IsSlotBacked);
        Assert.IsTrue(queryRow.TryGetSlot(0, out _));
        Assert.IsTrue(queryRow.TryGetSlot(1, out _));
    }

    [Test]
    public void TryGetSlot_DeclinesEagerRows_AndCachedCells()
    {
        DerivedColumnSchema[] schema = [new("n", ColumnType.Integer64), new("s", ColumnType.String)];
        RowLayout layout = new(schema.Select(c => c.Name));

        // Eager backing: always declined.
        QueryRow eager = new(new ObjectIdValue(1, 1, 1), layout,
            [new ColumnValue(ColumnType.Integer64, 1L), new ColumnValue(ColumnType.String, "x")]);
        Assert.IsFalse(eager.TryGetSlot(0, out _));

        // Slot backing: served until a cell is materialized, then that cell (and only it) is declined
        // so the cached ColumnValue is reused instead of re-decoding.
        QueryRow slotRow = (QueryRow)SlotRowBacked(layout, schema,
            new ColumnValue(ColumnType.Integer64, 5L), new ColumnValue(ColumnType.String, "y")).Row;
        Assert.IsTrue(slotRow.TryGetSlot(0, out ValueSlot n));
        Assert.AreEqual(5L, n.AsLong);

        slotRow.GetColumnValue(0);
        Assert.IsFalse(slotRow.TryGetSlot(0, out _));
        Assert.IsTrue(slotRow.TryGetSlot(1, out _));
    }

    // ── Full envelope under MVC's web JSON defaults: the converter must fire nested in the DTO,
    //    the field must serialize as camelCase "rows", and the positional shape must be intact. ──

    [Test]
    public void FullEnvelope_WebDefaults_RowsFieldPositional()
    {
        DerivedColumnSchema[] schema = [new("n", ColumnType.Integer64), new("s", ColumnType.String)];
        List<QueryResultRow> rows =
        [
            DictRow(schema, new ColumnValue(ColumnType.Integer64, 1L), new ColumnValue(ColumnType.String, "a")),
            DictRow(schema, new ColumnValue(ColumnType.Integer64, 2L), new ColumnValue(ColumnType.String, "b")),
        ];

        List<ColumnSchemaDto> columns = schema.Select(c => new ColumnSchemaDto { Name = c.Name, Type = c.Type }).ToList();
        ExecuteSQLQueryResponse response = new("ok", rows.Count, columns, new PositionalRowSet(rows, schema));

        JsonSerializerOptions webOptions = new(JsonSerializerDefaults.Web);
        string json = JsonSerializer.Serialize(response, webOptions);

        using JsonDocument doc = JsonDocument.Parse(json);
        JsonElement root = doc.RootElement;

        Assert.AreEqual("ok", root.GetProperty("status").GetString());
        Assert.AreEqual(2, root.GetProperty("total").GetInt32());

        JsonElement rowsEl = root.GetProperty("rows");
        Assert.AreEqual(JsonValueKind.Array, rowsEl.ValueKind);
        Assert.AreEqual(2, rowsEl.GetArrayLength());
        Assert.AreEqual(1, rowsEl[0][0].GetInt32());
        Assert.AreEqual("a", rowsEl[0][1].GetString());
        Assert.AreEqual(2, rowsEl[1][0].GetInt32());
        Assert.AreEqual("b", rowsEl[1][1].GetString());
    }
}
