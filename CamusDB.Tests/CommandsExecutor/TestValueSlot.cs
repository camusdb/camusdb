
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
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="ValueSlot"/>: the boundary round trip (ColumnValue ⇄ ValueSlot ⇄
/// ColumnValue) must be value-identical for every column type and edge case, and slot ordering/hash
/// must agree with <see cref="ColumnValue.CompareTo"/> (and <c>string.CompareOrdinal</c> for text keys).
/// </summary>
public sealed class TestValueSlot
{
    // ── round-trip equality helper (ColumnValue has no value Equals) ──

    private static void AssertColumnValueEqual(ColumnValue expected, ColumnValue actual)
    {
        Assert.AreEqual(expected.Type, actual.Type, "Type");
        switch (expected.Type)
        {
            case ColumnType.Null:
                break;
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                Assert.AreEqual(expected.LongValue, actual.LongValue);
                break;
            case ColumnType.Bool:
                Assert.AreEqual(expected.BoolValue, actual.BoolValue);
                break;
            case ColumnType.Float64:
            case ColumnType.Float32:
                Assert.AreEqual(expected.FloatValue, actual.FloatValue);
                break;
            case ColumnType.String:
            case ColumnType.Id:
                Assert.AreEqual(expected.StrValue, actual.StrValue);
                break;
            case ColumnType.Uuid:
                Assert.AreEqual(expected.UuidHigh, actual.UuidHigh);
                Assert.AreEqual(expected.LongValue, actual.LongValue);
                break;
            case ColumnType.Bytes:
                CollectionAssert.AreEqual(expected.BytesValue, actual.BytesValue);
                break;
            case ColumnType.Array:
                Assert.AreEqual(expected.ArrayElementType, actual.ArrayElementType);
                Assert.AreEqual(expected.ArrayValues!.Count, actual.ArrayValues!.Count);
                for (int i = 0; i < expected.ArrayValues.Count; i++)
                    AssertColumnValueEqual(expected.ArrayValues[i], actual.ArrayValues[i]);
                break;
            default:
                Assert.Fail("unhandled type " + expected.Type);
                break;
        }
    }

    private static void AssertRoundTrips(ColumnValue original)
        => AssertColumnValueEqual(original, ValueSlot.FromColumnValue(original).ToColumnValue());

    // ── Round trip: every type + edge cases ──

    [Test]
    public void RoundTrip_AllScalarTypes_AndEdges()
    {
        AssertRoundTrips(ColumnValue.Null);
        AssertRoundTrips(ColumnValue.True);
        AssertRoundTrips(ColumnValue.False);

        AssertRoundTrips(new ColumnValue(ColumnType.Integer64, 0L));
        AssertRoundTrips(new ColumnValue(ColumnType.Integer64, long.MinValue));
        AssertRoundTrips(new ColumnValue(ColumnType.Integer64, long.MaxValue));
        AssertRoundTrips(new ColumnValue(ColumnType.Integer64, -12345L));

        AssertRoundTrips(new ColumnValue(ColumnType.Float64, 0.0));
        AssertRoundTrips(new ColumnValue(ColumnType.Float64, -3.14159265358979));
        AssertRoundTrips(new ColumnValue(ColumnType.Float64, double.MaxValue));
        AssertRoundTrips(new ColumnValue(ColumnType.Float32, 1.5));
        AssertRoundTrips(new ColumnValue(ColumnType.Float32, -0.25));

        AssertRoundTrips(new ColumnValue(ColumnType.Date, 638000000000000000L));
        AssertRoundTrips(new ColumnValue(ColumnType.DateTime, 638123456789012345L));

        AssertRoundTrips(new ColumnValue(ColumnType.String, ""));
        AssertRoundTrips(new ColumnValue(ColumnType.String, "héllo \"q\"\n\t"));
        AssertRoundTrips(new ColumnValue(ColumnType.Id, new ObjectIdValue(9, 8, 7).ToString()));
        // Non-canonical Id string (e.g. a raw SQL literal) must still round-trip exactly.
        AssertRoundTrips(new ColumnValue(ColumnType.Id, "not-a-24hex-oid"));

        AssertRoundTrips(ColumnValue.FromUuid(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")));
        AssertRoundTrips(ColumnValue.FromUuid(Guid.Empty));

        AssertRoundTrips(new ColumnValue(Array.Empty<byte>()));
        AssertRoundTrips(new ColumnValue(new byte[] { 0, 1, 2, 250, 255 }));
    }

    [Test]
    public void RoundTrip_Arrays_IncludingNestedNullsAndEmpty()
    {
        AssertRoundTrips(ColumnValue.FromArray(ColumnType.Integer64, []));
        AssertRoundTrips(ColumnValue.FromArray(ColumnType.Integer64,
        [
            new ColumnValue(ColumnType.Integer64, 1L),
            ColumnValue.Null,
            new ColumnValue(ColumnType.Integer64, -3L),
        ]));
        AssertRoundTrips(ColumnValue.FromArray(ColumnType.String,
        [
            new ColumnValue(ColumnType.String, "a"),
            new ColumnValue(ColumnType.String, ""),
        ]));
    }

    // ── Ordering parity with ColumnValue.CompareTo ──

    private static void AssertOrderParity(ColumnValue a, ColumnValue b)
    {
        int cv = Math.Sign(a.CompareTo(b));
        int slot = Math.Sign(ValueSlot.FromColumnValue(a).CompareTo(ValueSlot.FromColumnValue(b)));
        Assert.AreEqual(cv, slot, $"order parity for {a} vs {b}");
    }

    [Test]
    public void Ordering_MatchesColumnValue_PerType()
    {
        AssertOrderParity(new ColumnValue(ColumnType.Integer64, -5L), new ColumnValue(ColumnType.Integer64, 5L));
        AssertOrderParity(new ColumnValue(ColumnType.Integer64, 5L), new ColumnValue(ColumnType.Integer64, 5L));
        AssertOrderParity(new ColumnValue(ColumnType.Float64, 1.1), new ColumnValue(ColumnType.Float64, 1.2));
        AssertOrderParity(new ColumnValue(ColumnType.Float32, 1.5), new ColumnValue(ColumnType.Float32, 1.4));
        AssertOrderParity(ColumnValue.False, ColumnValue.True);
        AssertOrderParity(new ColumnValue(ColumnType.Date, 10L), new ColumnValue(ColumnType.Date, 20L));

        AssertOrderParity(new ColumnValue(ColumnType.String, "apple"), new ColumnValue(ColumnType.String, "banana"));
        AssertOrderParity(new ColumnValue(ColumnType.String, "Z"), new ColumnValue(ColumnType.String, "a")); // ordinal: 'Z'(90) < 'a'(97)
        AssertOrderParity(new ColumnValue(ColumnType.Id, "aaa"), new ColumnValue(ColumnType.Id, "aab"));

        AssertOrderParity(new ColumnValue(new byte[] { 1, 2 }), new ColumnValue(new byte[] { 1, 3 }));
        AssertOrderParity(new ColumnValue(new byte[] { 1, 2 }), new ColumnValue(new byte[] { 1, 2, 0 }));

        AssertOrderParity(ColumnValue.FromUuid(Guid.Parse("00000000-0000-0000-0000-000000000001")),
                          ColumnValue.FromUuid(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")));
    }

    [Test]
    public void Ordering_StringKeys_MatchOrdinal()
    {
        string[] xs = ["", "A", "Z", "a", "ab", "b", "é"];
        for (int i = 0; i < xs.Length; i++)
            for (int j = 0; j < xs.Length; j++)
            {
                int expected = Math.Sign(string.CompareOrdinal(xs[i], xs[j]));
                int slot = Math.Sign(
                    ValueSlot.FromString(xs[i]).CompareTo(ValueSlot.FromString(xs[j])));
                Assert.AreEqual(expected, slot, $"ordinal parity '{xs[i]}' vs '{xs[j]}'");
            }
    }

    [Test]
    public void Ordering_Null_SortsFirst()
    {
        ValueSlot nul = ValueSlot.Null;
        ValueSlot n = ValueSlot.FromLong(ColumnType.Integer64, 1L);

        Assert.AreEqual(0, nul.CompareTo(ValueSlot.Null));
        Assert.AreEqual(-1, Math.Sign(nul.CompareTo(n)));
        Assert.AreEqual(1, Math.Sign(n.CompareTo(nul)));
    }

    [Test]
    public void CompareTo_IncompatibleTypes_Throws()
    {
        ValueSlot i = ValueSlot.FromLong(ColumnType.Integer64, 1L);
        ValueSlot s = ValueSlot.FromString("x");
        Assert.Throws<ArgumentException>(() => i.CompareTo(s));
    }

    // ── Hash consistency with equality ──

    [Test]
    public void Hash_EqualValues_HashEqual()
    {
        Assert.AreEqual(ValueSlot.FromLong(ColumnType.Integer64, 42L).GetSlotHashCode(),
                        ValueSlot.FromLong(ColumnType.Integer64, 42L).GetSlotHashCode());
        Assert.AreEqual(ValueSlot.FromString("abc").GetSlotHashCode(),
                        ValueSlot.FromString("abc").GetSlotHashCode());
        Assert.AreEqual(ValueSlot.FromBytes([1, 2, 3]).GetSlotHashCode(),
                        ValueSlot.FromBytes([1, 2, 3]).GetSlotHashCode());
        Assert.AreEqual(ValueSlot.FromUuid(7, 9).GetSlotHashCode(),
                        ValueSlot.FromUuid(7, 9).GetSlotHashCode());

        ValueSlot a = ValueSlot.FromColumnValue(ColumnValue.FromArray(ColumnType.Integer64,
            [new ColumnValue(ColumnType.Integer64, 1L), ColumnValue.Null]));
        ValueSlot b = ValueSlot.FromColumnValue(ColumnValue.FromArray(ColumnType.Integer64,
            [new ColumnValue(ColumnType.Integer64, 1L), ColumnValue.Null]));
        Assert.AreEqual(a.GetSlotHashCode(), b.GetSlotHashCode());
    }

    // ── Accessors ──

    [Test]
    public void Accessors_ReturnPackedPayload()
    {
        Assert.AreEqual(123L, ValueSlot.FromLong(ColumnType.Integer64, 123L).AsLong);
        Assert.IsTrue(ValueSlot.True.AsBool);
        Assert.IsFalse(ValueSlot.False.AsBool);
        Assert.AreEqual(2.5, ValueSlot.FromDouble(ColumnType.Float64, 2.5).AsDouble);
        Assert.AreEqual("hi", ValueSlot.FromString("hi").AsString);
        CollectionAssert.AreEqual(new byte[] { 9, 8 }, ValueSlot.FromBytes([9, 8]).AsBytes);
        ValueSlot u = ValueSlot.FromUuid(high: 11, low: 22);
        Assert.AreEqual(11, u.UuidHigh);
        Assert.AreEqual(22, u.UuidLow);
        Assert.IsTrue(ValueSlot.Null.IsNull);
    }
}
