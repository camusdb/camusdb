/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The byte estimate a retaining operator charges per buffered row (<see cref="RowMemoryEstimator"/>).
/// It must grow with the payload a row holds, charge a borrowed row the whole buffer it keeps alive,
/// not count a payload twice when a cell is read, and never decode or materialize a cell itself.
/// </summary>
[TestFixture]
public sealed class TestRowMemoryEstimator
{
    private static QueryRow EagerRow(params ColumnValue[] values)
    {
        string[] names = new string[values.Length];
        for (int i = 0; i < names.Length; i++)
            names[i] = "c" + i;

        return new QueryRow(default, new RowLayout(names), values);
    }

    [Test]
    public void StringCell_IsChargedTwoBytesPerCharacter()
    {
        long empty = RowMemoryEstimator.EstimateRow(EagerRow(new ColumnValue(ColumnType.String, "")));
        long wide = RowMemoryEstimator.EstimateRow(EagerRow(new ColumnValue(ColumnType.String, new string('x', 1000))));

        Assert.AreEqual(2000, wide - empty);
    }

    [Test]
    public void BytesCell_IsChargedOneBytePerByte()
    {
        long small = RowMemoryEstimator.EstimateRow(EagerRow(new ColumnValue(new byte[10])));
        long large = RowMemoryEstimator.EstimateRow(EagerRow(new ColumnValue(new byte[4010])));

        Assert.AreEqual(4000, large - small);
    }

    [Test]
    public void SlotRow_ChargesItsStringOnce_WhenACellIsRead()
    {
        RowLayout layout = new(new[] { "s" });
        QueryRow row = QueryRow.FromSlots(default, layout, new[] { ValueSlot.FromString(new string('y', 5000)) });

        long before = RowMemoryEstimator.EstimateRow(row);
        Assert.Greater(before, 10_000, "the 5000-character string is 10,000 bytes");

        Assert.AreEqual(5000, row.GetColumnValue(0).StrValue!.Length);
        long after = RowMemoryEstimator.EstimateRow(row);

        // The cached cell shares the slot's string: only the cache array and one value object are new.
        Assert.Greater(after, before);
        Assert.Less(after - before, 1000, "a read cell must not charge the shared string a second time");
    }

    [Test]
    public void Estimate_DoesNotMaterializeACell()
    {
        RowLayout layout = new(new[] { "s" });
        QueryRow row = QueryRow.FromSlots(default, layout, new[] { ValueSlot.FromString("abc") });

        RowMemoryEstimator.EstimateRow(row);

        // TryGetSlot serves the slot only while the cell is not cached, so a true here proves the
        // estimate did not populate the cache.
        Assert.IsTrue(row.TryGetSlot(0, out ValueSlot slot));
        Assert.AreEqual("abc", slot.AsString);
    }

    [Test]
    public void BorrowedRow_IsChargedTheWholeBufferItKeepsAlive()
    {
        CompiledRowCodec codec = CompiledRowCodec.Build(1, new[]
        {
            new TableColumnSchema("n", "n", ColumnType.Integer64, notNull: false, defaultValue: null),
        });

        byte[] payload = codec.Encode(new[] { ValueSlot.FromLong(ColumnType.Integer64, 7) });

        // The row reads a few bytes at the start of a 64 KiB buffer, as a borrowed row reads its
        // slice of a larger scan buffer.
        byte[] buffer = new byte[64 * 1024];
        payload.CopyTo(buffer, 0);

        RowView view = new(default, new ReadOnlyMemory<byte>(buffer, 0, payload.Length), codec);
        QueryRow row = QueryRow.FromRowView(default, new RowLayout(new[] { "n" }), view, new[] { 0 }, Array.Empty<(int, ColumnValue)>());

        Assert.AreEqual(7, row.GetColumnValue(0).LongValue);
        Assert.GreaterOrEqual(RowMemoryEstimator.EstimateRow(row), buffer.Length,
            "the whole buffer stays alive while the row does, so the whole buffer is charged");
    }

    [Test]
    public void PairRow_ChargesBothInputs()
    {
        Dictionary<string, ColumnValue> left = new()
        {
            { "u.id",   new(ColumnType.Id, "abc") },
            { "u.name", new(ColumnType.String, new string('l', 300)) },
        };

        Dictionary<string, ColumnValue> right = new()
        {
            { "title",   new(ColumnType.String, new string('r', 700)) },
            { "user_id", new(ColumnType.Id, "abc") },
        };

        RowLayout layout = CamusDB.Core.CommandsExecutor.Controllers.Queries.QueryRowMerger.BuildJoinLayout(left, right, "p");
        Dictionary<string, int> rightMap = CamusDB.Core.CommandsExecutor.Controllers.Queries.QueryRowMerger.BuildRightKeyOrdinalMap(right, "p", layout);
        QueryRow pair = new CamusDB.Core.CommandsExecutor.Controllers.Queries.JoinRowPairer().Pair(left, right, layout, rightMap);

        Assert.GreaterOrEqual(
            RowMemoryEstimator.EstimateRow(pair),
            RowMemoryEstimator.EstimateRow(left) + RowMemoryEstimator.EstimateRow(right));
    }

    [Test]
    public void DictionaryRow_ChargesKeysAndValues()
    {
        Dictionary<string, ColumnValue> shortKey = new() { { "a", new(ColumnType.Integer64, 1L) } };
        Dictionary<string, ColumnValue> longKey = new() { { new string('k', 100), new(ColumnType.Integer64, 1L) } };

        Assert.AreEqual(198, RowMemoryEstimator.EstimateRow(longKey) - RowMemoryEstimator.EstimateRow(shortKey));
    }

    [Test]
    public void Key_IsChargedItsValues()
    {
        ColumnValue[] narrow = { new(ColumnType.Integer64, 1L) };
        ColumnValue[] wide = { new(ColumnType.String, new string('z', 500)) };

        Assert.Greater(RowMemoryEstimator.EstimateKey(wide), RowMemoryEstimator.EstimateKey(narrow) + 999);
    }
}
