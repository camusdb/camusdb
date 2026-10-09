/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Order and round-trip tests for the NUMERIC index-key encoding: the sign-flipped Int128 written as
/// 19 fixed base-125 digits. Ordinal order of the keys must equal <see cref="ColumnValue.CompareTo"/>
/// order in both directions, a key must decode to the value that made it, and a key must never hold
/// the '/' that ends a Kahuna key space.
/// </summary>
[TestFixture]
internal sealed class TestNumericKeyEncoding
{
    private static readonly OrderType[] Asc = { OrderType.Ascending };
    private static readonly OrderType[] Desc = { OrderType.Descending };

    private static List<ColumnValue> SampleValues()
    {
        List<Int128> unscaled =
        [
            NumericMath.MinUnscaled, NumericMath.MinUnscaled + 1, -(Int128)ulong.MaxValue - 1, -(Int128)ulong.MaxValue,
            long.MinValue, -1_000_000_000, -1, 0, 1, 999_999_999, 1_000_000_000, long.MaxValue,
            (Int128)ulong.MaxValue, (Int128)ulong.MaxValue + 1, NumericMath.MaxUnscaled - 1, NumericMath.MaxUnscaled,
        ];

        Random random = new(20261008);
        Span<byte> bytes = stackalloc byte[16];
        for (int i = 0; i < 500; i++)
        {
            random.NextBytes(bytes);
            Int128 value = (Int128)(new UInt128(BitConverter.ToUInt64(bytes[..8]), BitConverter.ToUInt64(bytes[8..])) % (UInt128)NumericMath.MaxUnscaled);
            unscaled.Add(random.Next(2) == 0 ? value : -value);
        }

        return unscaled.Select(ColumnValue.FromNumeric).ToList();
    }

    private static string Key(ColumnValue value, OrderType[] directions) =>
        KeyEncoder.Encode(new CompositeColumnValue(new[] { value }), directions);

    [Test]
    public void AscendingKeys_OrderLikeCompareTo()
    {
        List<ColumnValue> values = SampleValues();

        List<ColumnValue> byValue = values.OrderBy(v => v, Comparer<ColumnValue>.Create((a, b) => a.CompareTo(b))).ToList();
        List<ColumnValue> byKey = values.OrderBy(v => Key(v, Asc), StringComparer.Ordinal).ToList();

        CollectionAssert.AreEqual(byValue.Select(v => v.NumericUnscaled), byKey.Select(v => v.NumericUnscaled));
    }

    [Test]
    public void DescendingKeys_OrderInReverse()
    {
        List<ColumnValue> values = SampleValues();

        List<ColumnValue> byValueDesc = values.OrderByDescending(v => v, Comparer<ColumnValue>.Create((a, b) => a.CompareTo(b))).ToList();
        List<ColumnValue> byKey = values.OrderBy(v => Key(v, Desc), StringComparer.Ordinal).ToList();

        CollectionAssert.AreEqual(byValueDesc.Select(v => v.NumericUnscaled), byKey.Select(v => v.NumericUnscaled));
    }

    [Test]
    public void Keys_RoundTrip_HaveFixedWidth_AndNoSlash()
    {
        foreach (ColumnValue value in SampleValues())
        {
            foreach (OrderType[] directions in new[] { Asc, Desc })
            {
                string key = Key(value, directions);

                Assert.AreEqual(1 + 19, key.Length, "marker + 19 base-125 digits");
                Assert.AreEqual(key.Length, KeyEncoder.Measure(new CompositeColumnValue(new[] { value }), directions));
                Assert.IsFalse(key.Contains('/'), "a '/' would split the Kahuna key space");

                ColumnValue decoded = KeyEncoder.Decode(key, new[] { ColumnType.Numeric }, directions).Values[0];
                Assert.AreEqual(ColumnType.Numeric, decoded.Type);
                Assert.AreEqual(value.NumericUnscaled, decoded.NumericUnscaled);
            }
        }
    }

    [Test]
    public void Null_SortsFirstAscending_AndLastDescending()
    {
        ColumnValue min = ColumnValue.FromNumeric(NumericMath.MinUnscaled);
        ColumnValue max = ColumnValue.FromNumeric(NumericMath.MaxUnscaled);

        Assert.Less(string.CompareOrdinal(Key(ColumnValue.Null, Asc), Key(min, Asc)), 0);
        Assert.Less(string.CompareOrdinal(Key(min, Desc), Key(ColumnValue.Null, Desc)), 0);
        Assert.Less(string.CompareOrdinal(Key(max, Desc), Key(min, Desc)), 0);
    }

    [Test]
    public void CompositeKey_OrdersByNumericThenString()
    {
        static string Composite(string numeric, string text) => KeyEncoder.Encode(new CompositeColumnValue(new[]
        {
            ColumnValue.FromNumericString(numeric), new ColumnValue(ColumnType.String, text),
        }));

        string[] ordered =
        [
            Composite("-1.5", "z"), Composite("-1.5", "zz"), Composite("0", "a"), Composite("0.000000001", ""), Composite("2", "a"),
        ];

        for (int i = 1; i < ordered.Length; i++)
            Assert.Less(string.CompareOrdinal(ordered[i - 1], ordered[i]), 0, $"position {i}");
    }
}
