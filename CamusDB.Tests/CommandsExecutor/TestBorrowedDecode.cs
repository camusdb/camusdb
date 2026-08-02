
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Locks the borrowed (zero-copy) decode backing: <see cref="RowEncoder.RowDecodeState.BorrowedDecode"/>
/// backs a decoded row with a <see cref="RowView"/> over the raw KV bytes instead of a
/// <c>ValueSlot[]</c>, and every value it yields must be identical to the eager and slot backings for the
/// same bytes — across all column types, nulls, projection, and schema-history default injection.
/// </summary>
public sealed class TestBorrowedDecode
{
    private static readonly ObjectIdValue RowId = new(7, 8, 9);
    private static readonly HLCTimestamp TxId = default;

    private static TableColumnSchema Col(string name, ColumnType type, ColumnType? elementType = null, SchemaElementState state = SchemaElementState.Public)
        => new(name, name, type, notNull: false, defaultValue: null, state: state, arrayElementType: elementType);

    private static TableSchema Schema(int version, params TableColumnSchema[] cols)
    {
        List<TableColumnSchema> list = new(cols);
        return new TableSchema
        {
            Id = "t", Name = "t", Version = version,
            Columns = list,
            SchemaHistory = [new TableSchemaHistory { Version = version, Columns = list }],
        };
    }

    private static ReadOnlyMemory<byte> Encode(TableSchema schema, Dictionary<string, ColumnValue> row)
        => BranchKvCodec.Decode(RowEncoder.EncodeStorageValue(schema, row, RowId)).Payload;

    /// <summary>Baseline configuration — these tests drive the decoder directly, with no engine.</summary>
    private static CamusDBOptions DecodeOptions => CamusDBOptions.Default;

    private static Task<QueryRow> DecodeAsync(
        TableSchema schema, ReadOnlyMemory<byte> payload, bool? borrowed,
        IReadOnlySet<string>? required = null, long? visibility = null, CamusDBOptions? options = null)
    {
        RowEncoder.RowDecodeState state = new() { BorrowedDecode = borrowed };
        return RowEncoder.DecodeToQueryRowAsync(schema, TxId, RowId, payload, options ?? DecodeOptions, requiredColumns: required, visibilitySchemaVersion: visibility, decodeState: state).AsTask();
    }

    /// <summary>Asserts the borrowed backing yields the same layout and cell values as the eager backing.</summary>
    private static async Task AssertParity(TableSchema schema, Dictionary<string, ColumnValue> row, IReadOnlySet<string>? required = null, long? visibility = null)
    {
        ReadOnlyMemory<byte> payload = Encode(schema, row);

        QueryRow borrowed = await DecodeAsync(schema, payload, borrowed: true, required, visibility);
        QueryRow eager = await DecodeAsync(schema, payload, borrowed: false, required, visibility);

        Assert.IsTrue(borrowed.IsBorrowedBacked, "expected borrowed backing");
        Assert.IsFalse(eager.IsBorrowedBacked);

        CollectionAssert.AreEqual(eager.Layout.OutputNames, borrowed.Layout.OutputNames, "layout mismatch");
        for (int i = 0; i < eager.Layout.Count; i++)
        {
            ColumnValue e = eager.GetColumnValue(i);
            ColumnValue b = borrowed.GetColumnValue(i);
            Assert.AreEqual(e.Type, b.Type, $"type mismatch at ordinal {i} ({eager.Layout.NameAt(i)})");
            // ColumnValue.CompareTo does not report NULL == NULL as 0, so equal types are enough for NULL.
            // (Message avoids ColumnValue.ToString — it throws for out-of-range Date/DateTime ticks.)
            if (e.Type != ColumnType.Null)
                Assert.AreEqual(0, e.CompareTo(b), $"value mismatch at ordinal {i} ({eager.Layout.NameAt(i)}), type {e.Type}");
        }
    }

    [Test]
    public async Task Parity_AllScalarTypesAndNulls()
    {
        TableSchema schema = Schema(0,
            Col("id", ColumnType.Id),
            Col("n", ColumnType.Integer64),
            Col("f", ColumnType.Float64),
            Col("f32", ColumnType.Float32),
            Col("flag", ColumnType.Bool),
            Col("s", ColumnType.String),
            Col("b", ColumnType.Bytes),
            Col("u", ColumnType.Uuid),
            Col("d", ColumnType.Date),
            Col("dt", ColumnType.DateTime),
            Col("missing", ColumnType.String));

        await AssertParity(schema, new Dictionary<string, ColumnValue>
        {
            ["id"] = new(ColumnType.Id, new ObjectIdValue(4, 5, 6).ToString()),
            ["n"] = new(ColumnType.Integer64, -42L),
            ["f"] = new(ColumnType.Float64, 3.14159),
            ["f32"] = new(ColumnType.Float32, 2.5),
            ["flag"] = ColumnValue.FromBool(true),
            ["s"] = new(ColumnType.String, "café 日本語"),
            ["b"] = new(new byte[] { 1, 2, 3, 255 }),
            ["u"] = new(ColumnType.Uuid, unchecked((long)0xFFEE0000), 0x1234),
            ["d"] = new(ColumnType.Date, 20260726L),
            ["dt"] = new(ColumnType.DateTime, new DateTime(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc).Ticks),
            // "missing" left absent → NULL
        });
    }

    [Test]
    public async Task Parity_EmptyVsNullString()
    {
        TableSchema schema = Schema(0, Col("s", ColumnType.String), Col("t", ColumnType.String));
        await AssertParity(schema, new Dictionary<string, ColumnValue>
        {
            ["s"] = new(ColumnType.String, ""),
            // "t" absent → NULL; empty and NULL must both round-trip distinctly on the borrowed path
        });
    }

    [Test]
    public async Task Parity_Array()
    {
        TableSchema schema = Schema(0, Col("arr", ColumnType.Array, ColumnType.Integer64));
        await AssertParity(schema, new Dictionary<string, ColumnValue>
        {
            ["arr"] = ColumnValue.FromArray(ColumnType.Integer64,
                [new(ColumnType.Integer64, 10L), ColumnValue.Null, new(ColumnType.Integer64, 30L)]),
        });
    }

    [Test]
    public async Task Parity_ProjectedSubset()
    {
        TableSchema schema = Schema(0,
            Col("a", ColumnType.Integer64),
            Col("big", ColumnType.String),
            Col("c", ColumnType.Bool));

        await AssertParity(schema, new Dictionary<string, ColumnValue>
        {
            ["a"] = new(ColumnType.Integer64, 1L),
            ["big"] = new(ColumnType.String, new string('x', 2048)),
            ["c"] = ColumnValue.FromBool(false),
        }, required: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "a", "c" });
    }

    [Test]
    public async Task Parity_SchemaHistoryInjectsDefaultForAddedColumn()
    {
        // Row written under v0 (id + n). Then a column is added at v1 with a default; the v1 read must
        // inject that default for the old row on the borrowed path exactly as on the eager path.
        TableSchema schema = Schema(0, Col("id", ColumnType.Id), Col("n", ColumnType.Integer64));
        ReadOnlyMemory<byte> payload = Encode(schema, new Dictionary<string, ColumnValue>
        {
            ["id"] = new(ColumnType.Id, new ObjectIdValue(4, 5, 6).ToString()),
            ["n"] = new(ColumnType.Integer64, 99L),
        });

        // Evolve schema to v1 with an added column carrying a default.
        List<TableColumnSchema> v1 = new()
        {
            Col("id", ColumnType.Id),
            Col("n", ColumnType.Integer64),
            new("added", "added", ColumnType.String, notNull: false, defaultValue: new ColumnValue(ColumnType.String, "def")),
        };
        schema.SchemaHistory!.Add(new TableSchemaHistory { Version = 1, Columns = v1 });
        schema.Version = 1;
        schema.Columns = v1;

        QueryRow borrowed = await DecodeAsync(schema, payload, borrowed: true, visibility: 1);
        QueryRow eager = await DecodeAsync(schema, payload, borrowed: false, visibility: 1);

        Assert.IsTrue(borrowed.IsBorrowedBacked);
        foreach (string name in new[] { "id", "n", "added" })
        {
            int be = borrowed.Layout.IndexOf(name);
            int ee = eager.Layout.IndexOf(name);
            Assert.GreaterOrEqual(be, 0, $"borrowed missing {name}");
            Assert.AreEqual(0, eager.GetColumnValue(ee).CompareTo(borrowed.GetColumnValue(be)), $"mismatch for {name}");
        }
        Assert.AreEqual("def", borrowed.GetColumnValue(borrowed.Layout.IndexOf("added")).StrValue);
    }

    [Test]
    public async Task Parity_WholeRowValuesAndRequalify()
    {
        TableSchema schema = Schema(0, Col("a", ColumnType.Integer64), Col("s", ColumnType.String));
        ReadOnlyMemory<byte> payload = Encode(schema, new Dictionary<string, ColumnValue>
        {
            ["a"] = new(ColumnType.Integer64, 5L),
            ["s"] = new(ColumnType.String, "z"),
        });

        QueryRow borrowed = await DecodeAsync(schema, payload, borrowed: true);

        // WithLayout must preserve the borrowed backing (no eager promotion) and the same values.
        RowLayout requalified = new(new[] { "x.a", "x.s" });
        QueryRow re = borrowed.WithLayout(requalified);
        Assert.AreEqual(5L, re.GetColumnValue(0).LongValue);
        Assert.AreEqual("z", re.GetColumnValue(1).StrValue);

        // Whole-row Values materializes every cell and promotes to eager.
        ColumnValue[] all = borrowed.Values;
        Assert.AreEqual(5L, all[0].LongValue);
        Assert.AreEqual("z", all[1].StrValue);
        Assert.IsFalse(borrowed.IsBorrowedBacked, "Values should promote to eager backing");
    }

    [Test]
    public async Task NoOverride_FollowsConfiguredPolicy()
    {
        TableSchema schema = Schema(0, Col("n", ColumnType.Integer64));
        ReadOnlyMemory<byte> payload = Encode(schema, new Dictionary<string, ColumnValue> { ["n"] = new(ColumnType.Integer64, 1L) });

        // With no per-row override the configured policy decides. This entry point has no scanner or
        // plan to consult, so only ForceBorrowed turns borrowing on; Adaptive falls back to eager.
        Task<QueryRow> DecodeUnder(BorrowedDecodePolicy policy)
            => DecodeAsync(schema, payload, borrowed: null,
                           options: DecodeOptions with { BorrowedDecode = policy });

        Assert.IsTrue((await DecodeUnder(BorrowedDecodePolicy.ForceBorrowed)).IsBorrowedBacked);
        Assert.IsFalse((await DecodeUnder(BorrowedDecodePolicy.ForceEager)).IsBorrowedBacked);
        Assert.IsFalse((await DecodeUnder(BorrowedDecodePolicy.Adaptive)).IsBorrowedBacked);
    }
}
