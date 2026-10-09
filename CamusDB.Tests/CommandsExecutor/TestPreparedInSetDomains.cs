/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <see cref="PreparedInSet"/> against the reference rule, <c>x IN (a, b, …)</c> = <c>x = a OR x = b OR …</c>
/// with <see cref="MixedNumericComparison.EqualsForMembership"/>. Each list runs at its own length
/// (eight items or fewer: a linear scan) and padded past eight (hashed), in both orders. Mixed numeric
/// equality is not transitive, so a set that deduplicates items through it gives a different answer
/// from the reference; these cases are the ones where it did.
/// </summary>
[TestFixture]
[Parallelizable(ParallelScope.All)]
public sealed class TestPreparedInSetDomains
{
    private static ColumnValue Num(string text) => ColumnValue.FromNumericString(text);
    private static ColumnValue Int(long value) => new(ColumnType.Integer64, value);
    private static ColumnValue F64(double value) => new(ColumnType.Float64, value);
    private static ColumnValue F32(float value) => new(ColumnType.Float32, value);

    /// <summary>Strings that equal no numeric probe, to push a list past the hash threshold.</summary>
    private static readonly ColumnValue[] Padding = Enumerable.Range(0, 9).Select(i => new ColumnValue(ColumnType.String, "pad" + i)).ToArray();

    private static bool Reference(ColumnValue probe, IEnumerable<ColumnValue> items) =>
        items.Any(item => MixedNumericComparison.EqualsForMembership(probe, item));

    private static IEnumerable<TestCaseData> Cases()
    {
        ColumnValue big = Num("9007199254740992");
        ColumnValue bigPlusOne = Num("9007199254740993");
        ColumnValue bigFloat = F64(9007199254740992.0);

        yield return new TestCaseData(bigPlusOne, new[] { big, bigFloat }).SetName("NUMERIC probe matches the float, not the equal-double NUMERIC item");
        yield return new TestCaseData(Int(9007199254740993), new[] { Int(9007199254740992), bigFloat }).SetName("INT64 probe matches the float, not the equal-double INT64 item");
        yield return new TestCaseData(Int(9007199254740993), new[] { big, bigFloat }).SetName("INT64 probe against NUMERIC and float items");
        yield return new TestCaseData(bigPlusOne, new[] { Int(9007199254740992), bigFloat }).SetName("NUMERIC probe against INT64 and float items");
        yield return new TestCaseData(bigFloat, new[] { bigPlusOne }).SetName("float probe matches an exact item by its double");
        yield return new TestCaseData(F64(1.5), new[] { Num("1.5"), Int(1) }).SetName("float probe matches a NUMERIC item");
        yield return new TestCaseData(Num("2"), new[] { Int(2) }).SetName("NUMERIC probe matches an INT64 item exactly");
        yield return new TestCaseData(Num("2.000000001"), new[] { Int(2), F64(2.5) }).SetName("NUMERIC probe matches no near item");
        yield return new TestCaseData(Int(2), new[] { Num("2.000000001") }).SetName("INT64 probe never matches a fractional NUMERIC");
        yield return new TestCaseData(F64(double.NaN), new[] { F64(double.NaN) }).SetName("NaN equals NaN, as CompareTo does");
        yield return new TestCaseData(F64(-0.0), new[] { F64(0.0) }).SetName("negative zero equals zero");
        yield return new TestCaseData(F32(0.1f), new[] { new ColumnValue(ColumnType.Float32, 0.1) }).SetName("FLOAT32 at single precision");
        yield return new TestCaseData(F64(0.1), new[] { F32(0.1f) }).SetName("FLOAT64 probe against a FLOAT32 item widens the item");
        yield return new TestCaseData(new ColumnValue(ColumnType.String, "2"), new[] { Int(2), Num("2") }).SetName("a string never equals a number");
    }

    [TestCaseSource(nameof(Cases))]
    public void Contains_AgreesWithTheReference_AtAnyLength_InEitherOrder(ColumnValue probe, ColumnValue[] items)
    {
        foreach (ColumnValue[] ordered in new[] { items, items.Reverse().ToArray() })
        {
            bool expected = Reference(probe, ordered);

            Assert.AreEqual(expected, new PreparedInSet(ordered).Contains(probe), $"short list, {probe}");

            ColumnValue[] padded = [.. ordered, .. Padding];
            Assert.AreEqual(expected, new PreparedInSet(padded).Contains(probe), $"hashed list, {probe}");

            ColumnValue[] paddedFirst = [.. Padding, .. ordered];
            Assert.AreEqual(expected, new PreparedInSet(paddedFirst).Contains(probe), $"hashed list, padding first, {probe}");
        }
    }

    [Test]
    public void Contains_StillMatchesTheNonNumericItems_OfAMixedList()
    {
        ColumnValue[] items = [.. Padding, Int(1), F64(2.5), Num("3.5")];
        PreparedInSet set = new(items);

        Assert.IsTrue(set.Contains(new ColumnValue(ColumnType.String, "pad3")));
        Assert.IsFalse(set.Contains(new ColumnValue(ColumnType.String, "pad9")));
        Assert.IsTrue(set.Contains(Num("1")));
        Assert.IsFalse(set.Contains(Int(3)));
        Assert.IsTrue(set.Contains(F64(3.5)));
        Assert.IsTrue(set.Contains(Num("2.5")));
    }

    /// <summary>
    /// Closely spaced NUMERIC values near 10²⁰ all widen to one double. A set hashed through the double
    /// puts them in one bucket, and building it is quadratic: about 0.4 s for 4,000 values, so tens of
    /// seconds for 20,000. Hashed at full width it is linear. The bound is loose, so it never flakes on
    /// a slow machine, but a quadratic build is far past it.
    /// </summary>
    [Test]
    public void ExactValues_NearOneDouble_BuildAndProbeInLinearTime()
    {
        const int count = 20_000;
        Int128 start = ColumnValue.FromNumericString("100000000000000000000").NumericUnscaled;
        ColumnValue[] items = new ColumnValue[count];
        for (int i = 0; i < count; i++)
            items[i] = ColumnValue.FromNumeric(start + i);

        Stopwatch watch = Stopwatch.StartNew();
        PreparedInSet set = new(items);
        for (int i = 0; i < count; i++)
            Assert.IsTrue(set.Contains(items[i]));
        Assert.IsFalse(set.Contains(ColumnValue.FromNumeric(start + count)));
        watch.Stop();

        Assert.Less(watch.Elapsed, TimeSpan.FromSeconds(3), "build and probe of 20,000 close NUMERIC values");
    }
}
