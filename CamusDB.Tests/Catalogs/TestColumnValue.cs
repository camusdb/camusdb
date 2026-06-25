
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Unit tests for ColumnValue new types (Float32, Date, DateTime, Bytes, Array):
/// construction, comparison, NULL ordering, and JSON round-trip through MetaJsonContext.
/// </summary>
[TestFixture]
public sealed class TestColumnValue
{
    // -------------------------------------------------------------------------
    // Float32
    // -------------------------------------------------------------------------

    [Test]
    public void Float32_ConstructAndCompare()
    {
        ColumnValue a = new(ColumnType.Float32, 1.5);
        ColumnValue b = new(ColumnType.Float32, 2.5);
        ColumnValue c = new(ColumnType.Float32, 1.5);

        Assert.That(a.CompareTo(b), Is.LessThan(0));
        Assert.That(b.CompareTo(a), Is.GreaterThan(0));
        Assert.That(a.CompareTo(c), Is.EqualTo(0));
        Assert.That(a.Type, Is.EqualTo(ColumnType.Float32));
    }

    [Test]
    public void Float32_NullSortsBeforePresent()
    {
        ColumnValue present = new(ColumnType.Float32, -999.0);
        Assert.That(ColumnValue.Null.CompareTo(present), Is.LessThan(0));
        Assert.That(present.CompareTo(ColumnValue.Null), Is.GreaterThan(0));
    }

    [Test]
    public void Float32_JsonRoundTrip()
    {
        ColumnValue original = new(ColumnType.Float32, 3.14);
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Float32));
        Assert.That((float)restored.FloatValue, Is.EqualTo((float)3.14));
    }

    // -------------------------------------------------------------------------
    // Date  (stored as UTC DateTime.Ticks truncated to midnight)
    // -------------------------------------------------------------------------

    [Test]
    public void Date_ConstructAndCompare()
    {
        long ticks2020 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        long ticks2021 = new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        ColumnValue d1 = new(ColumnType.Date, ticks2020);
        ColumnValue d2 = new(ColumnType.Date, ticks2021);
        ColumnValue d3 = new(ColumnType.Date, ticks2020);

        Assert.That(d1.CompareTo(d2), Is.LessThan(0));
        Assert.That(d2.CompareTo(d1), Is.GreaterThan(0));
        Assert.That(d1.CompareTo(d3), Is.EqualTo(0));
        Assert.That(d1.Type, Is.EqualTo(ColumnType.Date));
    }

    [Test]
    public void Date_NullSortsBeforePresent()
    {
        ColumnValue present = new(ColumnType.Date, new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc).Ticks);
        Assert.That(ColumnValue.Null.CompareTo(present), Is.LessThan(0));
        Assert.That(present.CompareTo(ColumnValue.Null), Is.GreaterThan(0));
    }

    [Test]
    public void Date_JsonRoundTrip()
    {
        long ticks = new DateTime(2026, 3, 15, 0, 0, 0, DateTimeKind.Utc).Ticks;
        ColumnValue original = new(ColumnType.Date, ticks);
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Date));
        Assert.That(restored.LongValue, Is.EqualTo(ticks));
    }

    // -------------------------------------------------------------------------
    // DateTime  (stored as UTC DateTime.Ticks)
    // -------------------------------------------------------------------------

    [Test]
    public void DateTime_ConstructAndCompare()
    {
        long t1 = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc).Ticks;
        long t2 = new DateTime(2026, 1, 1, 13, 0, 0, DateTimeKind.Utc).Ticks;

        ColumnValue dt1 = new(ColumnType.DateTime, t1);
        ColumnValue dt2 = new(ColumnType.DateTime, t2);

        Assert.That(dt1.CompareTo(dt2), Is.LessThan(0));
        Assert.That(dt2.CompareTo(dt1), Is.GreaterThan(0));
        Assert.That(dt1.Type, Is.EqualTo(ColumnType.DateTime));
    }

    [Test]
    public void DateTime_NullSortsBeforePresent()
    {
        ColumnValue present = new(ColumnType.DateTime, DateTime.MaxValue.Ticks);
        Assert.That(ColumnValue.Null.CompareTo(present), Is.LessThan(0));
    }

    [Test]
    public void DateTime_MinMax_JsonRoundTrip()
    {
        ColumnValue minDt = new(ColumnType.DateTime, DateTime.MinValue.Ticks);
        ColumnValue maxDt = new(ColumnType.DateTime, DateTime.MaxValue.Ticks);

        foreach (ColumnValue original in new[] { minDt, maxDt })
        {
            byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
            ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

            Assert.That(restored.Type, Is.EqualTo(ColumnType.DateTime));
            Assert.That(restored.LongValue, Is.EqualTo(original.LongValue));
        }
    }

    // -------------------------------------------------------------------------
    // Bytes
    // -------------------------------------------------------------------------

    [Test]
    public void Bytes_ConstructAndCompare()
    {
        ColumnValue a = new(new byte[] { 0x01, 0x02 });
        ColumnValue b = new(new byte[] { 0x01, 0x03 });
        ColumnValue c = new(new byte[] { 0x01, 0x02 });
        ColumnValue empty = new(Array.Empty<byte>());

        Assert.That(a.CompareTo(b), Is.LessThan(0));
        Assert.That(b.CompareTo(a), Is.GreaterThan(0));
        Assert.That(a.CompareTo(c), Is.EqualTo(0));
        Assert.That(empty.CompareTo(a), Is.LessThan(0));  // shorter-is-less on prefix
        Assert.That(a.Type, Is.EqualTo(ColumnType.Bytes));
    }

    [Test]
    public void Bytes_NullSortsBeforePresent()
    {
        ColumnValue present = new(new byte[] { 0xFF });
        Assert.That(ColumnValue.Null.CompareTo(present), Is.LessThan(0));
        Assert.That(present.CompareTo(ColumnValue.Null), Is.GreaterThan(0));
    }

    [Test]
    public void Bytes_EmptyRoundTrip()
    {
        ColumnValue original = new(Array.Empty<byte>());
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Bytes));
        Assert.That(restored.BytesValue, Is.Not.Null);
        Assert.That(restored.BytesValue!.Length, Is.EqualTo(0));
    }

    [Test]
    public void Bytes_NonTrivialJsonRoundTrip()
    {
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01];
        ColumnValue original = new(payload);
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Bytes));
        Assert.That(restored.BytesValue, Is.EqualTo(payload));
    }

    // -------------------------------------------------------------------------
    // Array
    // -------------------------------------------------------------------------

    [Test]
    public void Array_ConstructAndCompare_ElementWiseLexicographic()
    {
        ColumnValue a = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 2L)
        ]);
        ColumnValue b = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 3L)
        ]);
        ColumnValue c = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 2L)
        ]);
        ColumnValue shorter = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L)
        ]);

        Assert.That(a.CompareTo(b), Is.LessThan(0));
        Assert.That(b.CompareTo(a), Is.GreaterThan(0));
        Assert.That(a.CompareTo(c), Is.EqualTo(0));
        Assert.That(shorter.CompareTo(a), Is.LessThan(0));  // prefix common, shorter-is-less
        Assert.That(a.Type, Is.EqualTo(ColumnType.Array));
        Assert.That(a.ArrayElementType, Is.EqualTo(ColumnType.Integer64));
    }

    [Test]
    public void Array_NullSortsBeforePresent()
    {
        ColumnValue arr = ColumnValue.FromArray(ColumnType.Integer64, [new(ColumnType.Integer64, 42L)]);
        Assert.That(ColumnValue.Null.CompareTo(arr), Is.LessThan(0));
        Assert.That(arr.CompareTo(ColumnValue.Null), Is.GreaterThan(0));
    }

    [Test]
    public void Array_WithNullElement_ComparesCorrectly()
    {
        ColumnValue withNull = ColumnValue.FromArray(ColumnType.Integer64, [
            ColumnValue.Null,
            new(ColumnType.Integer64, 5L)
        ]);
        ColumnValue withValue = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 5L)
        ]);

        // Null sorts before any present value, so withNull < withValue
        Assert.That(withNull.CompareTo(withValue), Is.LessThan(0));
    }

    [Test]
    public void Array_CoPositionedNulls_AreEqual()
    {
        // Both directions must return 0 — the comparator-contract bug where
        // CompareTo's top-level null guard would return 1 for (Null, Null).
        ColumnValue a = ColumnValue.FromArray(ColumnType.Integer64, [ColumnValue.Null]);
        ColumnValue b = ColumnValue.FromArray(ColumnType.Integer64, [ColumnValue.Null]);

        Assert.That(a.CompareTo(b), Is.EqualTo(0), "a.CompareTo(b) must be 0 for co-positioned NULLs");
        Assert.That(b.CompareTo(a), Is.EqualTo(0), "b.CompareTo(a) must be 0 for co-positioned NULLs");
    }

    [Test]
    public void Array_MultipleCoPositionedNulls_AreEqual()
    {
        ColumnValue a = ColumnValue.FromArray(ColumnType.Integer64, [
            ColumnValue.Null,
            new(ColumnType.Integer64, 7L),
            ColumnValue.Null
        ]);
        ColumnValue b = ColumnValue.FromArray(ColumnType.Integer64, [
            ColumnValue.Null,
            new(ColumnType.Integer64, 7L),
            ColumnValue.Null
        ]);

        Assert.That(a.CompareTo(b), Is.EqualTo(0));
        Assert.That(b.CompareTo(a), Is.EqualTo(0));
    }

    [Test]
    public void Array_Empty_JsonRoundTrip()
    {
        ColumnValue original = ColumnValue.FromArray(ColumnType.Integer64, []);
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Array));
        Assert.That(restored.ArrayElementType, Is.EqualTo(ColumnType.Integer64));
        Assert.That(restored.ArrayValues, Is.Not.Null);
        Assert.That(restored.ArrayValues!.Count, Is.EqualTo(0));
    }

    [Test]
    public void Array_NonTrivial_JsonRoundTrip()
    {
        ColumnValue original = ColumnValue.FromArray(ColumnType.Integer64, [
            new(ColumnType.Integer64, 10L),
            ColumnValue.Null,
            new(ColumnType.Integer64, 30L)
        ]);
        byte[] json = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.ColumnValue);
        ColumnValue restored = MetaJsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnValue);

        Assert.That(restored.Type, Is.EqualTo(ColumnType.Array));
        Assert.That(restored.ArrayValues!.Count, Is.EqualTo(3));
        Assert.That(restored.ArrayValues[0].LongValue, Is.EqualTo(10L));
        Assert.That(restored.ArrayValues[1].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(restored.ArrayValues[2].LongValue, Is.EqualTo(30L));
    }

    [Test]
    public void Array_HeterogeneousElement_Throws()
    {
        Assert.Throws<CamusDB.Core.CamusDBException>(() =>
            ColumnValue.FromArray(ColumnType.Integer64, [new(ColumnType.String, "x")]));
    }

    [Test]
    public void Array_NullElementWithWrongSiblings_Allowed()
    {
        // Null elements are always allowed regardless of declared element type.
        Assert.DoesNotThrow(() =>
            ColumnValue.FromArray(ColumnType.Integer64, [ColumnValue.Null, new(ColumnType.Integer64, 1L)]));
    }

    // -------------------------------------------------------------------------
    // Cross-type CompareTo still throws
    // -------------------------------------------------------------------------

    [Test]
    public void CrossType_CompareThrows()
    {
        ColumnValue i = new(ColumnType.Integer64, 1L);
        ColumnValue f = new(ColumnType.Float32, 1.0);

        Assert.Throws<ArgumentException>(() => i.CompareTo(f));
    }

    [Test]
    public void CrossType_Float32VsFloat64_Throws()
    {
        ColumnValue f32 = new(ColumnType.Float32, 1.0);
        ColumnValue f64 = new(ColumnType.Float64, 1.0);

        Assert.Throws<ArgumentException>(() => f32.CompareTo(f64));
    }
}
