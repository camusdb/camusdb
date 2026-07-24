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

/// <summary>
/// Round-trip tests for the secondary-index INCLUDE (stored/payload) value codec: encode a set of
/// column values into the trailing include tuple, then decode them back given the schema types.
/// Also pins the two backward-compatibility invariants — an empty include list produces the exact
/// historical 25-byte rowId-only value, and a short/absent tuple decodes its missing columns as NULL.
/// </summary>
[TestFixture]
public sealed class TestIndexIncludeValueCodec
{
    private static readonly ObjectIdValue SampleRowId = ObjectIdGenerator.Generate();

    private static byte[] Encode(string[] cols, Dictionary<string, ColumnValue> row)
        => IndexIncludeValueCodec.EncodeTuple(cols, row);

    private static CompositeColumnValue Decode(ColumnType[] types, byte[] tuple)
        => IndexIncludeValueCodec.DecodeTuple(types, tuple);

    [Test]
    public void RoundTrip_AllCommonTypes()
    {
        string[] cols = ["s", "i", "f", "b"];
        Dictionary<string, ColumnValue> row = new()
        {
            ["s"] = new ColumnValue(ColumnType.String, "héllo"),
            ["i"] = new ColumnValue(ColumnType.Integer64, -9_223_000_111L),
            ["f"] = new ColumnValue(ColumnType.Float64, -3.5),
            ["b"] = ColumnValue.FromBool(true),
        };

        byte[] tuple = Encode(cols, row);
        CompositeColumnValue decoded = Decode([ColumnType.String, ColumnType.Integer64, ColumnType.Float64, ColumnType.Bool], tuple);

        Assert.AreEqual("héllo", decoded.Values[0].StrValue);
        Assert.AreEqual(-9_223_000_111L, decoded.Values[1].LongValue);
        Assert.AreEqual(-3.5, decoded.Values[2].FloatValue);
        Assert.AreEqual(true, decoded.Values[3].BoolValue);
    }

    [Test]
    public void RoundTrip_NullIncludeValue()
    {
        string[] cols = ["a", "b"];
        Dictionary<string, ColumnValue> row = new()
        {
            ["a"] = new ColumnValue(ColumnType.Integer64, 7),
            // "b" absent → NULL payload
        };

        byte[] tuple = Encode(cols, row);
        CompositeColumnValue decoded = Decode([ColumnType.Integer64, ColumnType.Integer64], tuple);

        Assert.AreEqual(7, decoded.Values[0].LongValue);
        Assert.AreEqual(ColumnType.Null, decoded.Values[1].Type);
    }

    [Test]
    public void EmptyIncludeList_ProducesRowIdOnlyValue_ByteIdentical()
    {
        byte[] tuple = Encode([], new Dictionary<string, ColumnValue>());
        Assert.AreEqual(0, tuple.Length);

        // The fused index value with an empty tuple must equal the historical rowId-only 25-byte value.
        byte[] withEmpty = BranchKvCodec.EncodeIndexRowId(SampleRowId, tuple);
        byte[] legacy = BranchKvCodec.EncodeIndexRowId(SampleRowId);

        Assert.AreEqual(legacy, withEmpty);
        Assert.AreEqual(25, withEmpty.Length);
    }

    [Test]
    public void ShortTuple_DecodesMissingColumnsAsNull()
    {
        // A tuple written when the index had only one include column, decoded by a schema that now
        // expects two, must yield the first value and NULL for the absent trailing column.
        string[] cols = ["a"];
        Dictionary<string, ColumnValue> row = new() { ["a"] = new ColumnValue(ColumnType.Integer64, 42) };
        byte[] tuple = Encode(cols, row);

        CompositeColumnValue decoded = Decode([ColumnType.Integer64, ColumnType.String], tuple);

        Assert.AreEqual(42, decoded.Values[0].LongValue);
        Assert.AreEqual(ColumnType.Null, decoded.Values[1].Type);
    }

    [Test]
    public void DecodeTupleInto_DecodesOnlyProjectedPositions_SkippingOthers()
    {
        // Tuple = [status:String, total:Float64, note:String]; project only positions 0 and 2.
        string[] cols = ["status", "total", "note"];
        Dictionary<string, ColumnValue> row = new()
        {
            ["status"] = new ColumnValue(ColumnType.String, "shipped"),
            ["total"] = new ColumnValue(ColumnType.Float64, -2.5),
            ["note"] = new ColumnValue(ColumnType.String, "hello"),
        };
        byte[] tuple = Encode(cols, row);

        ColumnType[] types = [ColumnType.String, ColumnType.Float64, ColumnType.String];
        // Skip 'total' (position 1 → -1); 'status' → output[1], 'note' → output[0].
        int[] outputForPosition = [1, -1, 0];
        ColumnValue[] output = new ColumnValue[2];

        IndexIncludeValueCodec.DecodeTupleInto(types, outputForPosition, output, tuple);

        Assert.AreEqual("hello", output[0].StrValue);   // note (decoded after skipping total)
        Assert.AreEqual("shipped", output[1].StrValue);  // status
    }

    [Test]
    public void DecodeTupleInto_ShortTuple_FillsProjectedTailWithNull()
    {
        // Tuple carries only the first column; a plan projecting position 1 must yield NULL.
        string[] cols = ["a"];
        Dictionary<string, ColumnValue> row = new() { ["a"] = new ColumnValue(ColumnType.Integer64, 5) };
        byte[] tuple = Encode(cols, row);

        ColumnType[] types = [ColumnType.Integer64, ColumnType.String];
        int[] outputForPosition = [0, 1];
        ColumnValue[] output = new ColumnValue[2];

        IndexIncludeValueCodec.DecodeTupleInto(types, outputForPosition, output, tuple);

        Assert.AreEqual(5, output[0].LongValue);
        Assert.AreEqual(ColumnType.Null, output[1].Type);
    }

    [Test]
    public void FusedValue_SplitsRowIdFromTuple()
    {
        string[] cols = ["x"];
        Dictionary<string, ColumnValue> row = new() { ["x"] = new ColumnValue(ColumnType.Integer64, 123) };
        byte[] tuple = Encode(cols, row);

        byte[] value = BranchKvCodec.EncodeIndexRowId(SampleRowId, tuple);
        BranchKvValue decoded = BranchKvCodec.Decode(value);

        // The rowId is the fixed 24-byte prefix; the include tuple follows.
        Assert.IsTrue(decoded.HasPayload);
        Assert.Greater(decoded.Payload.Length, BranchKvCodec.IndexRowIdPayloadLength);

        System.ReadOnlySpan<byte> payload = decoded.Payload.Span;
        string rowIdText = System.Text.Encoding.UTF8.GetString(payload.Slice(0, BranchKvCodec.IndexRowIdPayloadLength));
        Assert.AreEqual(SampleRowId.ToString(), rowIdText);

        CompositeColumnValue tail = Decode([ColumnType.Integer64], payload.Slice(BranchKvCodec.IndexRowIdPayloadLength).ToArray());
        Assert.AreEqual(123, tail.Values[0].LongValue);
    }
}
