/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="IntervalMath"/>, the one implementation of the interval value: the parse
/// of the PostgreSQL and the ISO-8601 text forms, the ISO-8601 output, the order where a month is 30
/// days, and the arithmetic. Each expected value follows the PostgreSQL rules: the fraction rules of
/// <c>DecodeInterval</c>, the <c>iso_8601</c> output style, <c>interval_cmp_value</c>,
/// <c>interval_mul</c> and <c>interval_div</c>, and <c>timestamp_pl_interval</c>.
/// </summary>
[TestFixture]
internal sealed class TestIntervalMath
{
    private static string RoundTrip(string text) => IntervalMath.FormatIso(IntervalMath.Parse(text));

    // ─── Parse and format ──────────────────────────────────────────────────

    [TestCase("1 year 2 mons 3 days 04:05:06.789", "P1Y2M3DT4H5M6.789S")]
    [TestCase("1 day 2 hours", "P1DT2H")]
    [TestCase("1day", "P1D")]
    [TestCase("1 DAY", "P1D")]
    [TestCase("-1 day +2 hours", "P-1DT2H")]
    [TestCase("3 hours ago", "PT-3H")]
    [TestCase("@ 1 day ago", "P-1D")]
    [TestCase("2 weeks", "P14D")]
    [TestCase("90 minutes", "PT1H30M")]
    [TestCase("1 hour 30 mins 15 secs", "PT1H30M15S")]
    [TestCase("1500 ms", "PT1.5S")]
    [TestCase("1 us", "PT0.000001S")]
    [TestCase("1 decade 1 century 1 millennium", "P1110Y")]
    [TestCase("1 quarter", "P3M")]
    [TestCase("13 months", "P1Y1M")]
    [TestCase("-13 months", "P-1Y-1M")]
    [TestCase("0", "PT0S")]
    [TestCase("1.5", "PT1.5S")]
    [TestCase("-00:00:01.5", "PT-1.5S")]
    [TestCase("10:30", "PT10H30M")]
    [TestCase("1 day -10:30:00", "P1DT-10H-30M")]
    [TestCase("P1Y2M3DT4H5M6.5S", "P1Y2M3DT4H5M6.5S")]
    [TestCase("p1dt2h", "P1DT2H")]
    [TestCase("P1W", "P7D")]
    [TestCase("PT0.000001S", "PT0.000001S")]
    [TestCase("P-1D", "P-1D")]
    [TestCase("PT36H", "PT36H")]
    [TestCase("P0D", "PT0S")]
    public void Parse_ThenFormat_GivesThePostgreSqlIsoText(string text, string iso)
    {
        Assert.AreEqual(iso, RoundTrip(text), text);
    }

    /// <summary>
    /// The fraction rules of PostgreSQL: a fraction of a year rounds to whole months, a fraction of a
    /// month moves into days (30 a month), and a fraction of a week or a day moves into the time.
    /// </summary>
    [TestCase("1.5 years", "P1Y6M")]
    [TestCase("1.1 years", "P1Y1M")]
    [TestCase("0.5 decades", "P5Y")]
    [TestCase("1.5 months", "P1M15D")]
    [TestCase("0.1 months", "P3D")]
    [TestCase("0.05 months", "P1DT12H")]
    [TestCase("1.5 weeks", "P10DT12H")]
    [TestCase("1.5 days", "P1DT12H")]
    [TestCase("-1.5 days", "P-1DT-12H")]
    [TestCase("0.5 hours", "PT30M")]
    [TestCase("1.5 microseconds", "PT0.000002S")]
    [TestCase("2.5 microseconds", "PT0.000002S")]
    [TestCase("P1.5D", "P1DT12H")]
    [TestCase("P0.5M", "P15D")]
    public void AFraction_MovesIntoTheSmallerParts(string text, string iso)
    {
        Assert.AreEqual(iso, RoundTrip(text), text);
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("abc")]
    [TestCase("1 fortnight")]
    [TestCase("day")]
    [TestCase("1 day ago 2 hours")]
    [TestCase("ago")]
    [TestCase("1 2")]
    [TestCase("P")]
    [TestCase("PT")]
    [TestCase("P1H")]
    [TestCase("PT1D")]
    [TestCase("P1DT")]
    [TestCase("P1D2")]
    [TestCase("12:60")]
    [TestCase("12:30:60")]
    [TestCase("1..5 days")]
    [TestCase(". days")]
    public void InvalidText_IsAnInvalidInputError(string text)
    {
        CamusDBException e = Assert.Throws<CamusDBException>(() => IntervalMath.Parse(text))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code);
        StringAssert.Contains("invalid input syntax for type interval", e.Message);
    }

    [TestCase("2147483648 months")]
    [TestCase("-2147483649 days")]
    [TestCase("178956971 years")]
    [TestCase("9223372036854775807 hours")]
    [TestCase("99999999999999999999999999999999 days")]
    public void APartPastItsRange_IsAnOutOfRangeError(string text)
    {
        CamusDBException e = Assert.Throws<CamusDBException>(() => IntervalMath.Parse(text))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code);
        Assert.AreEqual(IntervalMath.IntervalOutOfRangeMessage, e.Message);
    }

    [Test]
    public void TheExtremes_FormatWithoutAnOverflow()
    {
        Assert.AreEqual("P-178956970Y-8M-2147483648DT-2562047788H-54.775808S",
            IntervalMath.FormatIso(new IntervalParts(int.MinValue, int.MinValue, long.MinValue)));
        Assert.AreEqual("P178956970Y7M2147483647DT2562047788H54.775807S",
            IntervalMath.FormatIso(new IntervalParts(int.MaxValue, int.MaxValue, long.MaxValue)));
    }

    // ─── Order ─────────────────────────────────────────────────────────────

    [TestCase("1 month", "30 days", 0)]
    [TestCase("1 day", "24 hours", 0)]
    [TestCase("1 month", "29 days", 1)]
    [TestCase("1 month", "30 days 1 us", -1)]
    [TestCase("1 year", "360 days", 0)]
    [TestCase("-1 day", "0", -1)]
    [TestCase("1 day -1 hour", "23 hours", 0)]
    public void Compare_CountsAMonthAs30DaysAndADayAs24Hours(string left, string right, int expected)
    {
        Assert.AreEqual(expected, Math.Sign(IntervalMath.Compare(IntervalMath.Parse(left), IntervalMath.Parse(right))), $"{left} vs {right}");
    }

    // ─── Arithmetic between intervals ──────────────────────────────────────

    [Test]
    public void AddSubtractAndNegate_ActOnEachPart()
    {
        IntervalParts a = IntervalMath.Parse("1 mon 2 days 03:00:00");
        IntervalParts b = IntervalMath.Parse("2 mons -1 day 01:30:00");

        Assert.AreEqual("P3M1DT4H30M", IntervalMath.FormatIso(IntervalMath.Add(a, b)));
        Assert.AreEqual("P-1M3DT1H30M", IntervalMath.FormatIso(IntervalMath.Subtract(a, b)));
        Assert.AreEqual("P-1M-2DT-3H", IntervalMath.FormatIso(IntervalMath.Negate(a)));
    }

    [Test]
    public void AddPastTheRange_IsAnError()
    {
        IntervalParts max = new(int.MaxValue, 0, 0);
        Assert.Throws<CamusDBException>(() => IntervalMath.Add(max, new IntervalParts(1, 0, 0)));
        Assert.Throws<CamusDBException>(() => IntervalMath.Negate(new IntervalParts(int.MinValue, 0, 0)));
        Assert.Throws<CamusDBException>(() => IntervalMath.Negate(new IntervalParts(0, 0, long.MinValue)));
    }

    [TestCase("1 month", 0.5, "P15D")]
    [TestCase("1 month", 1.5, "P1M15D")]
    [TestCase("1 day", 1.5, "P1DT12H")]
    [TestCase("1 mon 1 day", 2.0, "P2M2D")]
    [TestCase("1 hour", 2.5, "PT2H30M")]
    [TestCase("1 day", -1.0, "P-1D")]
    [TestCase("1 hour", 0.0, "PT0S")]
    [TestCase("1 mon 1 day 01:00:00", 1.0 / 3.0, "P10DT8H20M")]
    public void Multiply_FollowsIntervalMul(string text, double factor, string iso)
    {
        Assert.AreEqual(iso, IntervalMath.FormatIso(IntervalMath.Multiply(IntervalMath.Parse(text), factor)), $"{text} * {factor}");
    }

    [TestCase("1 month", 3.0, "P10D")]
    [TestCase("1 day", 2.0, "PT12H")]
    [TestCase("3 hours", 2.0, "PT1H30M")]
    [TestCase("1 mon", 0.5, "P2M")]
    public void Divide_FollowsIntervalDiv(string text, double divisor, string iso)
    {
        Assert.AreEqual(iso, IntervalMath.FormatIso(IntervalMath.Divide(IntervalMath.Parse(text), divisor)), $"{text} / {divisor}");
    }

    [Test]
    public void MultiplyAndDivide_RefuseNaNInfinityZeroAndOverflow()
    {
        IntervalParts day = IntervalMath.Parse("1 day");

        Assert.Throws<CamusDBException>(() => IntervalMath.Multiply(day, double.NaN));
        Assert.Throws<CamusDBException>(() => IntervalMath.Multiply(day, double.PositiveInfinity));
        Assert.Throws<CamusDBException>(() => IntervalMath.Multiply(day, 3e9));
        Assert.Throws<CamusDBException>(() => IntervalMath.Multiply(new IntervalParts(0, 0, long.MaxValue), 2.0));

        CamusDBException zero = Assert.Throws<CamusDBException>(() => IntervalMath.Divide(day, 0.0))!;
        Assert.AreEqual(NumericMath.DivisionByZeroMessage, zero.Message);
        Assert.Throws<CamusDBException>(() => IntervalMath.Divide(day, double.NaN));
    }

    // ─── Arithmetic with datetimes ─────────────────────────────────────────

    private static long Ticks(string iso) => DateTime.Parse(iso, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal).Ticks;

    private static string Iso(long ticks) => new DateTime(ticks, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ss.ffffff");

    [TestCase("2024-01-31T00:00:00", "1 month", "2024-02-29T00:00:00.000000")]
    [TestCase("2023-01-31T00:00:00", "1 month", "2023-02-28T00:00:00.000000")]
    [TestCase("2024-02-29T00:00:00", "1 year", "2025-02-28T00:00:00.000000")]
    [TestCase("2024-01-31T00:00:00", "1 mon 1 day", "2024-03-01T00:00:00.000000")]
    [TestCase("2024-03-31T00:00:00", "-1 month", "2024-02-29T00:00:00.000000")]
    [TestCase("2024-01-01T10:00:00", "1 day 2 hours", "2024-01-02T12:00:00.000000")]
    [TestCase("2024-01-01T00:00:00", "-1 us", "2023-12-31T23:59:59.999999")]
    [TestCase("2024-01-01T00:00:00", "36 hours", "2024-01-02T12:00:00.000000")]
    public void AddToTicks_AddsMonthsThenDaysThenTime(string start, string interval, string expected)
    {
        Assert.AreEqual(expected, Iso(IntervalMath.AddToTicks(Ticks(start), IntervalMath.Parse(interval))), $"{start} + {interval}");
    }

    [TestCase("9999-12-31T00:00:00", "1 day")]
    [TestCase("0001-01-01T00:00:00", "-1 us")]
    [TestCase("2024-01-01T00:00:00", "2147483647 months")]
    [TestCase("2024-01-01T00:00:00", "2147483647 days")]
    public void AddToTicks_PastTheDatetimeRange_IsAnError(string start, string interval)
    {
        CamusDBException e = Assert.Throws<CamusDBException>(() => IntervalMath.AddToTicks(Ticks(start), IntervalMath.Parse(interval)))!;
        Assert.AreEqual(IntervalMath.TimestampOutOfRangeMessage, e.Message);
    }

    [TestCase("2024-01-02T03:00:00", "2024-01-01T00:00:00", "P1DT3H")]
    [TestCase("2024-01-01T00:00:00", "2024-01-02T03:00:00", "P-1DT-3H")]
    [TestCase("2024-03-01T00:00:00", "2024-02-01T00:00:00", "P29D")]
    [TestCase("2024-01-01T00:00:00", "2024-01-01T00:00:00", "PT0S")]
    [TestCase("2024-01-01T00:00:00.5", "2024-01-01T00:00:00", "PT0.5S")]
    public void BetweenTicks_GivesDaysAndTime_AndNoMonths(string left, string right, string iso)
    {
        Assert.AreEqual(iso, IntervalMath.FormatIso(IntervalMath.BetweenTicks(Ticks(left), Ticks(right))), $"{left} - {right}");
    }

    [Test]
    public void BetweenTicks_OfTheWholeRange_Fits()
    {
        IntervalParts span = IntervalMath.BetweenTicks(DateTime.MaxValue.Ticks, DateTime.MinValue.Ticks);
        Assert.AreEqual(3652058, span.Days);
        Assert.AreEqual(0, span.Months);
    }
}
