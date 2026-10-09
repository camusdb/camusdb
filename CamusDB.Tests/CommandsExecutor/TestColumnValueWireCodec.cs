/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Every ColumnType must round-trip the wire codec exactly — partial aggregation states cross
/// nodes through it, and a lossy value (a truncated double, a re-encoded UUID) would corrupt
/// results silently. Comparison uses CompareTo (the engine's own equality) plus per-type
/// payload checks where CompareTo is too coarse.
/// </summary>
public sealed class TestColumnValueWireCodec
{
    private static ColumnValue RoundTrip(ColumnValue value)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
            ColumnValueWireCodec.Write(writer, value);

        using JsonDocument doc = JsonDocument.Parse(Encoding.UTF8.GetString(stream.ToArray()));
        return ColumnValueWireCodec.Read(doc.RootElement);
    }

    [Test]
    public void AllScalarTypes_RoundTripExactly()
    {
        (ColumnValue value, string label)[] cases =
        [
            (ColumnValue.Null, "null"),
            (ColumnValue.True, "true"),
            (ColumnValue.False, "false"),
            (new ColumnValue(ColumnType.Integer64, long.MinValue), "int-min"),
            (new ColumnValue(ColumnType.Integer64, 42L), "int"),
            (new ColumnValue(ColumnType.Float64, 0.1 + 0.2), "double-imprecise"),
            (new ColumnValue(ColumnType.Float64, double.MaxValue), "double-max"),
            (new ColumnValue(ColumnType.Float32, 1.5), "float32"),
            (new ColumnValue(ColumnType.String, "café — 数据库 'q'"), "string-unicode"),
            (new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3"), "id"),
            (new ColumnValue(ColumnType.Date, 19850L), "date"),
            (new ColumnValue(ColumnType.DateTime, 1723852800123L), "datetime"),
        ];

        foreach ((ColumnValue value, string label) in cases)
        {
            ColumnValue restored = RoundTrip(value);
            Assert.AreEqual(value.Type, restored.Type, $"{label}: type");
            Assert.AreEqual(0, value.CompareTo(restored), $"{label}: value");
        }
    }

    [Test]
    public void BytesUuidAndArray_RoundTripExactly()
    {
        ColumnValue bytes = new(new byte[] { 0, 1, 2, 255, 128 });
        ColumnValue restoredBytes = RoundTrip(bytes);
        Assert.AreEqual(bytes.BytesValue, restoredBytes.BytesValue, "bytes payload");

        ColumnValue uuid = ColumnValue.FromUuidString("01890a5d-ac96-774b-b9aa-9f0c12345678");
        ColumnValue restoredUuid = RoundTrip(uuid);
        Assert.AreEqual(uuid.UuidValue, restoredUuid.UuidValue, "uuid string form");
        Assert.AreEqual(uuid.UuidHigh, restoredUuid.UuidHigh, "uuid high");
        Assert.AreEqual(uuid.LongValue, restoredUuid.LongValue, "uuid low");

        ColumnValue array = new(
            ColumnType.Array,
            strValue: null, longValue: 0, floatValue: 0, boolValue: false, bytesValue: null,
            arrayValues:
            [
                new ColumnValue(ColumnType.Integer64, 7L),
                ColumnValue.Null,
                new ColumnValue(ColumnType.Integer64, -9L),
            ],
            arrayElementType: ColumnType.Integer64);

        ColumnValue restoredArray = RoundTrip(array);
        Assert.AreEqual(ColumnType.Array, restoredArray.Type);
        Assert.AreEqual(ColumnType.Integer64, restoredArray.ArrayElementType);
        Assert.AreEqual(3, restoredArray.ArrayValues!.Count);
        Assert.AreEqual(0, array.ArrayValues![0].CompareTo(restoredArray.ArrayValues[0]));
        Assert.AreEqual(ColumnType.Null, restoredArray.ArrayValues[1].Type);
        Assert.AreEqual(0, array.ArrayValues[2].CompareTo(restoredArray.ArrayValues[2]));
    }

    [TestCase("12345678901234567890.123456789")]
    [TestCase("99999999999999999999999999999.999999999")]
    [TestCase("-99999999999999999999999999999.999999999")]
    [TestCase("-0.000000001")]
    [TestCase("18446744073709551616")]
    [TestCase("0")]
    public void Numeric_RoundTripsExactly(string text)
    {
        ColumnValue value = ColumnValue.FromNumericString(text);
        ColumnValue restored = RoundTrip(value);

        Assert.AreEqual(ColumnType.Numeric, restored.Type);
        Assert.AreEqual(value.NumericUnscaled, restored.NumericUnscaled);
        Assert.AreEqual(text, restored.NumericValue);
    }

    [Test]
    public void Numeric_FramePastTheRange_IsRefused()
    {
        // A corrupt or hostile frame must not carry a NUMERIC past the range into the merge.
        Int128 past = NumericMath.MaxUnscaled + 1;
        long high = (long)(past >> 64);
        long low = (long)(ulong)(past & ulong.MaxValue);

        using JsonDocument doc = JsonDocument.Parse($"{{\"t\":{(int)ColumnType.Numeric},\"u\":{high},\"l\":{low}}}");

        CamusDBException error = Assert.Throws<CamusDBException>(() => ColumnValueWireCodec.Read(doc.RootElement))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, error.Code);
    }

    [Test]
    public void Numeric_PartialAggregateCells_RoundTrip()
    {
        // Partial aggregate rows cross nodes as a cells object; a NUMERIC SUM and the AVG sum/count
        // pair must arrive unchanged, so the merge and the AVG finalizer see the exact values.
        Dictionary<string, ColumnValue> row = new(StringComparer.OrdinalIgnoreCase)
        {
            ["s"] = ColumnValue.FromNumericString("-12345678901234567890.123456789"),
            ["c"] = new ColumnValue(ColumnType.Integer64, 3L),
            ["m"] = ColumnValue.Null,
        };

        Dictionary<string, ColumnValue> back = QueryExecutor.ParseCells(QueryExecutor.EncodeCells(row));

        Assert.AreEqual(ColumnType.Numeric, back["s"].Type);
        Assert.AreEqual("-12345678901234567890.123456789", back["s"].NumericValue);
        Assert.AreEqual(3L, back["c"].LongValue);
        Assert.AreEqual(ColumnType.Null, back["m"].Type);
    }
}
