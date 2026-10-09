/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Text.Json;

using Grpc.Core;
using NUnit.Framework;

using CamusDB.App.Grpc;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="NumericMath"/>, the one implementation of the NUMERIC type: exact
/// parsing, canonical formatting, half-away-from-zero rounding at nine fractional digits, the
/// ±(10²⁹ − 10⁻⁹) range, the conversions to and from <see cref="long"/> and <see cref="double"/>, and
/// the arithmetic. The expected values follow Spanner GoogleSQL NUMERIC.
/// </summary>
[TestFixture]
internal sealed class TestNumericMath
{
    private const string MaxText = "99999999999999999999999999999.999999999";
    private const string MinText = "-99999999999999999999999999999.999999999";

    private static Int128 Parse(string text)
    {
        Assert.AreEqual(NumericConversionStatus.Ok, NumericMath.TryParse(text, out Int128 unscaled), text);
        return unscaled;
    }

    private static string Canonical(string text) => NumericMath.Format(Parse(text));

    [TestCase("0", "0")]
    [TestCase("-0", "0")]
    [TestCase("1", "1")]
    [TestCase("1.10", "1.1")]
    [TestCase("5.000000000", "5")]
    [TestCase("+2.5", "2.5")]
    [TestCase("-2.5", "-2.5")]
    [TestCase(".5", "0.5")]
    [TestCase("5.", "5")]
    [TestCase("0.000000001", "0.000000001")]
    [TestCase("1.5e3", "1500")]
    [TestCase("15E-1", "1.5")]
    [TestCase("  7.25  ", "7.25")]
    [TestCase("12345678901234567890.123456789", "12345678901234567890.123456789")]
    [TestCase(MaxText, MaxText)]
    [TestCase(MinText, MinText)]
    public void Parse_ThenFormat_GivesCanonicalText(string input, string expected)
    {
        Assert.AreEqual(expected, Canonical(input));
    }

    [TestCase("1.0000000005", "1.000000001")]
    [TestCase("-1.0000000005", "-1.000000001")]
    [TestCase("1.0000000004999", "1")]
    [TestCase("2.0000000005", "2.000000001")]
    [TestCase("0.00000000049", "0")]
    [TestCase("0.0000000005", "0.000000001")]
    [TestCase("-0.0000000005", "-0.000000001")]
    [TestCase("0.000000000000000000001", "0")]
    public void Parse_MoreThanNineFractionalDigits_RoundsHalfAwayFromZero(string input, string expected)
    {
        Assert.AreEqual(expected, Canonical(input));
    }

    [Test]
    public void Parse_MaxAndMin_AreTheRangeBounds()
    {
        Assert.AreEqual(NumericMath.MaxUnscaled, Parse(MaxText));
        Assert.AreEqual(NumericMath.MinUnscaled, Parse(MinText));
    }

    [TestCase("100000000000000000000000000000")]
    [TestCase("-100000000000000000000000000000")]
    [TestCase("99999999999999999999999999999.9999999995")] // rounds up past the maximum
    [TestCase("1e29")]
    [TestCase("1e1000000000")]
    public void Parse_OutsideTheRange_IsOverflow(string input)
    {
        Assert.AreEqual(NumericConversionStatus.Overflow, NumericMath.TryParse(input, out _), input);
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("-")]
    [TestCase(".")]
    [TestCase("1.2.3")]
    [TestCase("abc")]
    [TestCase("1e")]
    [TestCase("1e+")]
    [TestCase("NaN")]
    [TestCase("Infinity")]
    [TestCase("1,5")]
    public void Parse_NotANumber_IsInvalid(string input)
    {
        Assert.AreEqual(NumericConversionStatus.Invalid, NumericMath.TryParse(input, out _), input);
    }

    [Test]
    public void Parse_ZeroWithAHugeExponent_IsZero()
    {
        Assert.AreEqual(Int128.Zero, Parse("0e1000000000"));
    }

    /// <summary>
    /// A long run of fractional digits must not cancel a huge exponent. The exponent here is
    /// 10,000,000 and the mantissa has 1,000,002 fractional digits, so the value is
    /// 10^(10,000,000 − 1,000,002): far past the range. A clamp near the fraction length once
    /// parsed it as 0.01.
    /// </summary>
    [Test]
    public void Parse_LongFractionDoesNotCancelAHugeExponent()
    {
        string tiny = "0." + new string('0', 1_000_001) + "1";

        Assert.AreEqual(NumericConversionStatus.Overflow, NumericMath.TryParse(tiny + "e10000000", out _));
        Assert.AreEqual(Int128.Zero, Parse(tiny + "e-10000000"));
        Assert.AreEqual("1", Canonical(tiny + "e1000002"));
    }

    // ── double ──────────────────────────────────────────────────────────────

    [TestCase(0.1, "0.1")]
    [TestCase(-0.1, "-0.1")]
    [TestCase(1.5, "1.5")]
    // The double nearest 123456789.123 is 123456789.1229999959…: a computed double converts from its
    // exact binary value (the Spanner FLOAT64 cast), so the error shows at the ninth digit. Only a
    // literal converts from its source text.
    [TestCase(123456789.123, "123456789.122999996")]
    [TestCase(0.1 + 0.2, "0.3")]                // 0.30000000000000004 rounds to 9 digits
    [TestCase(1e-10, "0")]
    [TestCase(5e-10, "0.000000001")]            // the double is 5.0000000000000003e-10, so it rounds up
    [TestCase(1e28, "9999999999999999583119736832")]
    public void FromDouble_ExpandsExactlyThenRounds(double value, string expected)
    {
        Assert.AreEqual(NumericConversionStatus.Ok, NumericMath.TryFromDouble(value, out Int128 unscaled));
        Assert.AreEqual(expected, NumericMath.Format(unscaled));
    }

    [Test]
    public void FromDouble_NonFinite_IsInvalid_AndHugeIsOverflow()
    {
        Assert.AreEqual(NumericConversionStatus.Invalid, NumericMath.TryFromDouble(double.NaN, out _));
        Assert.AreEqual(NumericConversionStatus.Invalid, NumericMath.TryFromDouble(double.PositiveInfinity, out _));
        Assert.AreEqual(NumericConversionStatus.Invalid, NumericMath.TryFromDouble(double.NegativeInfinity, out _));
        // The double 1e29 is 99999999999999991433150857216, still inside the range; 2e29 is not.
        Assert.AreEqual(NumericConversionStatus.Ok, NumericMath.TryFromDouble(1e29, out _));
        Assert.AreEqual(NumericConversionStatus.Overflow, NumericMath.TryFromDouble(2e29, out _));
        Assert.AreEqual(NumericConversionStatus.Overflow, NumericMath.TryFromDouble(-1e300, out _));
    }

    [TestCase("0.1", 0.1)]
    [TestCase("-2.5", -2.5)]
    [TestCase("123456789.123456789", 123456789.123456789)]
    [TestCase(MaxText, 1e29)]
    public void ToDouble_GivesTheClosestDouble(string text, double expected)
    {
        Assert.AreEqual(expected, NumericMath.ToDouble(Parse(text)));
    }

    [TestCase("100000004.000000001", 100000008f)] // just above a float midpoint: one rounding, not two
    [TestCase("100000004", 100000000f)]           // exactly on the midpoint: ties to even
    [TestCase("-100000004.000000001", -100000008f)]
    [TestCase("0.1", 0.1f)]
    [TestCase("16777217", 16777216f)]
    [TestCase(MaxText, 1e29f)]
    public void ToSingle_RoundsOnceFromTheExactValue(string text, float expected)
    {
        Assert.AreEqual(expected, NumericMath.ToSingle(Parse(text)));
    }

    // ── long ────────────────────────────────────────────────────────────────

    [TestCase("2.5", 3L)]
    [TestCase("-2.5", -3L)]
    [TestCase("2.499999999", 2L)]
    [TestCase("-0.5", -1L)]
    [TestCase("0.499999999", 0L)]
    [TestCase("9223372036854775807", long.MaxValue)]
    [TestCase("-9223372036854775808", long.MinValue)]
    public void ToInt64_RoundsHalfAwayFromZero(string text, long expected)
    {
        Assert.IsTrue(NumericMath.TryToInt64(Parse(text), out long value));
        Assert.AreEqual(expected, value);
    }

    [TestCase("9223372036854775807.5")]
    [TestCase("-9223372036854775808.5")]
    [TestCase(MaxText)]
    public void ToInt64_OutOfRange_Fails(string text)
    {
        Assert.IsFalse(NumericMath.TryToInt64(Parse(text), out _));
    }

    [Test]
    public void FromInt64_IsExact_AtTheExtremes()
    {
        Assert.AreEqual("9223372036854775807", NumericMath.Format(NumericMath.FromInt64(long.MaxValue)));
        Assert.AreEqual("-9223372036854775808", NumericMath.Format(NumericMath.FromInt64(long.MinValue)));
    }

    // ── Arithmetic ──────────────────────────────────────────────────────────

    [Test]
    public void Add_IsExact_WhereFloatIsNot()
    {
        Assert.AreEqual("0.3", NumericMath.Format(NumericMath.Add(Parse("0.1"), Parse("0.2"))));
        Assert.AreEqual(MaxText, NumericMath.Format(NumericMath.Add(Parse(MaxText), 0)));
        Assert.AreEqual("0", NumericMath.Format(NumericMath.Add(Parse(MaxText), Parse(MinText))));
    }

    [Test]
    public void AddAndSubtract_PastTheRange_Throw()
    {
        CamusDBException up = Assert.Throws<CamusDBException>(() => NumericMath.Add(Parse(MaxText), Parse("0.000000001")))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, up.Code);

        CamusDBException down = Assert.Throws<CamusDBException>(() => NumericMath.Subtract(Parse(MinText), Parse("0.000000001")))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, down.Code);
    }

    [TestCase("1.5", "2", "3")]
    [TestCase("-1.5", "2", "-3")]
    [TestCase("0.000000001", "0.5", "0.000000001")]   // 5e-10 rounds half away from zero
    [TestCase("-0.000000001", "0.5", "-0.000000001")]
    [TestCase("0.000000001", "0.4", "0")]
    [TestCase("12345678901234567890", "1000000000", "12345678901234567890000000000")] // wide path
    [TestCase("99999999999999999999999999999", "0.5", "49999999999999999999999999999.5")]
    public void Multiply_RoundsTheExactProduct(string left, string right, string expected)
    {
        Assert.AreEqual(expected, NumericMath.Format(NumericMath.Multiply(Parse(left), Parse(right))));
        Assert.AreEqual(expected, NumericMath.Format(NumericMath.Multiply(Parse(right), Parse(left))));
    }

    [Test]
    public void Multiply_WideOperandWithANarrowProduct_IsExactAndDoesNotAllocate()
    {
        Int128 wide = Parse("20000000000");     // unscaled 2 × 10¹⁹, past 64 bits
        Int128 one = Parse("1");

        Assert.AreEqual("20000000000", NumericMath.Format(NumericMath.Multiply(wide, one)));
        Assert.AreEqual("-30000000000", NumericMath.Format(NumericMath.Multiply(wide, Parse("-1.5"))));
        Assert.AreEqual("0", NumericMath.Format(NumericMath.Multiply(Parse(MaxText), 0)));

        NumericMath.Multiply(wide, one);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
            NumericMath.Multiply(wide, one);
        // The BigInteger path allocated about 160 bytes per call. A few bytes in total can come from
        // the runtime (tiered compilation) during the loop, so the bound is per call, not exactly zero.
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Less(allocated, 1_000, "a product that fits in 128 bits must not take the BigInteger path");
    }

    [Test]
    public void Multiply_PastTheRange_Throws()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            NumericMath.Multiply(Parse("10000000000000000000000000000"), Parse("10")))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, ex.Code);
    }

    [TestCase("1", "3", "0.333333333")]
    [TestCase("2", "3", "0.666666667")]
    [TestCase("-2", "3", "-0.666666667")]
    [TestCase("10", "4", "2.5")]
    [TestCase("1", "0.000000001", "1000000000")]
    [TestCase(MaxText, "1", MaxText)]                  // wide path: the numerator does not fit in 128 bits
    [TestCase(MaxText, "-3", "-33333333333333333333333333333.333333333")]
    public void Divide_RoundsTheExactQuotient(string left, string right, string expected)
    {
        Assert.AreEqual(expected, NumericMath.Format(NumericMath.Divide(Parse(left), Parse(right))));
    }

    [Test]
    public void Divide_ByZero_AndPastTheRange_Throw()
    {
        CamusDBException zero = Assert.Throws<CamusDBException>(() => NumericMath.Divide(Parse("1"), 0))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, zero.Code);

        CamusDBException range = Assert.Throws<CamusDBException>(() =>
            NumericMath.Divide(Parse(MaxText), Parse("0.5")))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, range.Code);
    }

    [TestCase("7.5", "2", "1.5")]
    [TestCase("-7.5", "2", "-1.5")]   // the sign of the dividend
    [TestCase("7.5", "-2", "1.5")]
    public void Remainder_HasTheSignOfTheDividend(string left, string right, string expected)
    {
        Assert.AreEqual(expected, NumericMath.Format(NumericMath.Remainder(Parse(left), Parse(right))));
    }

    // ── Error code ──────────────────────────────────────────────────────────

    [Test]
    public void OutOfRangeCode_MapsToHttp400AndGrpcOutOfRange()
    {
        Assert.AreEqual("CADB0417", CamusDBErrorCodes.NumericValueOutOfRange);
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.NumericValueOutOfRange));
        Assert.AreEqual(StatusCode.OutOfRange, GrpcErrorMapper.GetGrpcStatus(CamusDBErrorCodes.NumericValueOutOfRange));
    }

    // ── ColumnValue ─────────────────────────────────────────────────────────

    [Test]
    public void ColumnValue_RoundTripsTheInt128_AndOrdersBySignedValue()
    {
        string[] ordered = [MinText, "-1", "-0.000000001", "0", "0.000000001", "1", "18446744073.709551616", MaxText];

        ColumnValue? previous = null;
        foreach (string text in ordered)
        {
            ColumnValue value = ColumnValue.FromNumericString(text);
            Assert.AreEqual(ColumnType.Numeric, value.Type);
            Assert.AreEqual(Parse(text), value.NumericUnscaled, text);
            Assert.AreEqual(NumericMath.Format(Parse(text)), value.NumericValue);

            if (previous is not null)
                Assert.Less(previous.CompareTo(value), 0, $"{previous.NumericValue} < {text}");

            previous = value;
        }
    }

    [Test]
    public void ColumnValue_JsonConstructor_AcceptsTextOrHalves()
    {
        ColumnValue fromText = new(ColumnType.Numeric, "1.25", 0, 0, false, null, null, ColumnType.Null);
        Assert.AreEqual("1.25", fromText.NumericValue);

        ColumnValue negative = ColumnValue.FromNumericString("-1.25");
        ColumnValue fromHalves = new(ColumnType.Numeric, null, negative.LongValue, 0, false, null, null,
            ColumnType.Null, negative.UuidHigh);
        Assert.AreEqual(0, negative.CompareTo(fromHalves));
    }

    // ── Round ───────────────────────────────────────────────────────────────

    [TestCase("2.5", 0, NumericRounding.HalfAwayFromZero, "3")]
    [TestCase("-2.5", 0, NumericRounding.HalfAwayFromZero, "-3")]
    [TestCase("2.4999", 0, NumericRounding.HalfAwayFromZero, "2")]
    [TestCase("1.25", 1, NumericRounding.HalfAwayFromZero, "1.3")]
    [TestCase("-1.25", 1, NumericRounding.HalfAwayFromZero, "-1.3")]
    [TestCase("1250", -2, NumericRounding.HalfAwayFromZero, "1300")]
    [TestCase("1.000000001", 9, NumericRounding.HalfAwayFromZero, "1.000000001")]
    [TestCase("1.000000001", 20, NumericRounding.HalfAwayFromZero, "1.000000001")]
    [TestCase("-2.7", 0, NumericRounding.TowardZero, "-2")]
    [TestCase("1299.99", -2, NumericRounding.TowardZero, "1200")]
    [TestCase("-1.25", 0, NumericRounding.Floor, "-2")]
    [TestCase("-1.25", 0, NumericRounding.Ceiling, "-1")]
    [TestCase("1.000000001", 0, NumericRounding.Ceiling, "2")]
    [TestCase("-0.000000001", 0, NumericRounding.Floor, "-1")]
    [TestCase(MaxText, -40, NumericRounding.HalfAwayFromZero, "0")]
    [TestCase(MaxText, -40, NumericRounding.TowardZero, "0")]
    [TestCase("5", -29, NumericRounding.Floor, "0")]
    public void Round_ByMode(string text, long digits, NumericRounding mode, string expected)
    {
        Assert.AreEqual(expected, NumericMath.Format(NumericMath.Round(Parse(text), digits, mode, "round")));
    }

    [TestCase(MaxText, 0, NumericRounding.Ceiling)]
    [TestCase(MaxText, 0, NumericRounding.HalfAwayFromZero)]
    [TestCase(MinText, 0, NumericRounding.Floor)]
    [TestCase("1", -40, NumericRounding.Ceiling)]
    [TestCase("-1", -40, NumericRounding.Floor)]
    public void Round_PastTheRange_Throws(string text, long digits, NumericRounding mode)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => NumericMath.Round(Parse(text), digits, mode, "round"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, ex.Code);
    }

    [Test]
    public void ArithmeticResultType_FollowsSpanner()
    {
        Assert.AreEqual(ColumnType.Numeric, MixedNumericComparison.ArithmeticResultType(ColumnType.Numeric, ColumnType.Integer64));
        Assert.AreEqual(ColumnType.Numeric, MixedNumericComparison.ArithmeticResultType(ColumnType.Integer64, ColumnType.Numeric));
        Assert.AreEqual(ColumnType.Numeric, MixedNumericComparison.ArithmeticResultType(ColumnType.Numeric, ColumnType.Numeric));
        Assert.AreEqual(ColumnType.Float64, MixedNumericComparison.ArithmeticResultType(ColumnType.Numeric, ColumnType.Float64));
        Assert.AreEqual(ColumnType.Float64, MixedNumericComparison.ArithmeticResultType(ColumnType.Float32, ColumnType.Numeric));
        Assert.AreEqual(ColumnType.Float32, MixedNumericComparison.ArithmeticResultType(ColumnType.Float32, ColumnType.Integer64));
        Assert.AreEqual(ColumnType.Integer64, MixedNumericComparison.ArithmeticResultType(ColumnType.Integer64, ColumnType.Integer64));
        Assert.IsNull(MixedNumericComparison.ArithmeticResultType(ColumnType.Numeric, ColumnType.String));
    }

    // ── Range enforcement on every construction path ───────────────────────

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    [Test]
    public void Construction_OutsideTheRange_Throws()
    {
        Int128 past = NumericMath.MaxUnscaled + 1;

        CamusDBException fromInt128 = Assert.Throws<CamusDBException>(() => ColumnValue.FromNumeric(past))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, fromInt128.Code);

        CamusDBException fromHalves = Assert.Throws<CamusDBException>(() =>
            new ColumnValue(ColumnType.Numeric, (long)(past >> 64), (long)(ulong)(past & ulong.MaxValue)))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, fromHalves.Code);

        Assert.DoesNotThrow(() => ColumnValue.FromNumeric(NumericMath.MaxUnscaled));
        Assert.DoesNotThrow(() => ColumnValue.FromNumeric(NumericMath.MinUnscaled));
    }

    /// <summary>
    /// HTTP request bodies deserialize <see cref="ColumnValue"/> parameters and values, so the JSON
    /// constructor is an input boundary: raw halves past the range and an explicitly empty string
    /// are refused, while an absent string with in-range halves (a persisted default) still loads.
    /// </summary>
    [Test]
    public void JsonInput_IsValidated()
    {
        CamusDBException halves = Assert.Throws<CamusDBException>(() =>
            JsonSerializer.Deserialize<ColumnValue>("{\"Type\":12,\"UuidHigh\":9223372036854775807,\"LongValue\":-1}", Json))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, halves.Code);

        CamusDBException empty = Assert.Throws<CamusDBException>(() =>
            JsonSerializer.Deserialize<ColumnValue>("{\"Type\":12,\"StrValue\":\"\"}", Json))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, empty.Code);

        CamusDBException text = Assert.Throws<CamusDBException>(() =>
            JsonSerializer.Deserialize<ColumnValue>("{\"Type\":12,\"StrValue\":\"1e40\"}", Json))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, text.Code);

        Assert.AreEqual("1.5", JsonSerializer.Deserialize<ColumnValue>("{\"Type\":12,\"StrValue\":\"1.5\"}", Json)!.NumericValue);

        ColumnValue persisted = ColumnValue.FromNumericString("-12345678901234567890.5");
        string roundTrip = JsonSerializer.Serialize(persisted);
        Assert.AreEqual(0, persisted.CompareTo(JsonSerializer.Deserialize<ColumnValue>(roundTrip, Json)!));
    }

    // ── Hashes ──────────────────────────────────────────────────────────────

    [Test]
    public void Hashes_UseTheValue_NotOnlyTheType()
    {
        ColumnValue[] values = Enumerable.Range(0, 1_000).Select(i => ColumnValue.FromNumeric(i * 1_000_000_007L)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.Greater(values.Select(v => SqlColumnValueComparer.Instance.GetHashCode(v)).Distinct().Count(), 990, nameof(SqlColumnValueComparer));
            Assert.Greater(values.Select(v => CompositeColumnValueComparer.Instance.GetHashCode(new CompositeColumnValue(new[] { v }))).Distinct().Count(), 990, nameof(CompositeColumnValueComparer));
            Assert.Greater(values.Select(QueryDistincter.DistinctValueHash).Distinct().Count(), 990, nameof(QueryDistincter));
        });

        // Equal values hash equally in every comparer.
        ColumnValue a = ColumnValue.FromNumericString("2.50"), b = ColumnValue.FromNumericString("2.5");
        Assert.AreEqual(SqlColumnValueComparer.Instance.GetHashCode(a), SqlColumnValueComparer.Instance.GetHashCode(b));
        Assert.AreEqual(QueryDistincter.DistinctValueHash(a), QueryDistincter.DistinctValueHash(b));
    }

    [Test]
    public void ValueSlot_RoundTripsAndComparesLikeColumnValue()
    {
        ColumnValue a = ColumnValue.FromNumericString("-5.5");
        ColumnValue b = ColumnValue.FromNumericString("3");

        ValueSlot sa = ValueSlot.FromColumnValue(a);
        ValueSlot sb = ValueSlot.FromColumnValue(b);

        Assert.AreEqual(a.NumericUnscaled, sa.NumericUnscaled);
        Assert.AreEqual(0, a.CompareTo(sa.ToColumnValue()));
        Assert.Less(sa.CompareTo(sb), 0);
        Assert.AreEqual(sa.GetSlotHashCode(), ValueSlot.FromColumnValue(ColumnValue.FromNumericString("-5.50")).GetSlotHashCode());
    }
}
