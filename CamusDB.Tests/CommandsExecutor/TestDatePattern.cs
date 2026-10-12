/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Globalization;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for the compiled date pattern engine. Each PostgreSQL template token and each MySQL
/// specifier has a format case and a parse case, and each parse error has a case that checks the
/// message names the text and the pattern. The reference value is Sunday 2026-03-15 08:05:09.123456
/// UTC: day of year 74, ISO week 11, quarter 1.
/// </summary>
public sealed class TestDatePattern
{
    private static readonly DateTime Reference = new DateTime(2026, 3, 15, 8, 5, 9, DateTimeKind.Utc).AddTicks(1234560);

    private static string Pg(string pattern, DateTime value)
        => DatePattern.Get("to_char", pattern, DatePatternDialect.PostgreSql).Format(value);

    private static string My(string pattern, DateTime value)
        => DatePattern.Get("date_format", pattern, DatePatternDialect.MySql).Format(value);

    private static DateTime PgParse(string text, string pattern)
        => DatePattern.Get("to_timestamp", pattern, DatePatternDialect.PostgreSql).Parse("to_timestamp", text);

    private static DateTime MyParse(string text, string pattern)
        => DatePattern.Get("str_to_date", pattern, DatePatternDialect.MySql).Parse("str_to_date", text);

    private static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0, int second = 0)
        => new(year, month, day, hour, minute, second, DateTimeKind.Utc);

    private static void AssertPgParseFails(string text, string pattern, string reason)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => PgParse(text, pattern))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains($"could not parse '{text}' with pattern '{pattern}'", ex.Message);
        StringAssert.Contains(reason, ex.Message);
    }

    private static void AssertMyParseFails(string text, string pattern, string reason)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => MyParse(text, pattern))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains($"could not parse '{text}' with pattern '{pattern}'", ex.Message);
        StringAssert.Contains(reason, ex.Message);
    }

    // ── PostgreSQL: format, one case per token ─────────────────────────────────

    [TestCase("YYYY", "2026")]
    [TestCase("yyyy", "2026")]
    [TestCase("YYY", "026")]
    [TestCase("YY", "26")]
    [TestCase("Y", "6")]
    [TestCase("IYYY", "2026")]
    [TestCase("MM", "03")]
    [TestCase("Mon", "Mar")]
    [TestCase("MON", "MAR")]
    [TestCase("mon", "mar")]
    [TestCase("Month", "March    ")]
    [TestCase("MONTH", "MARCH    ")]
    [TestCase("month", "march    ")]
    [TestCase("DD", "15")]
    [TestCase("DDD", "074")]
    [TestCase("D", "1")]
    [TestCase("ID", "7")]
    [TestCase("Dy", "Sun")]
    [TestCase("DY", "SUN")]
    [TestCase("Day", "Sunday   ")]
    [TestCase("DAY", "SUNDAY   ")]
    [TestCase("WW", "11")]
    [TestCase("IW", "11")]
    [TestCase("Q", "1")]
    [TestCase("HH", "08")]
    [TestCase("HH12", "08")]
    [TestCase("HH24", "08")]
    [TestCase("hh24", "08")]
    [TestCase("MI", "05")]
    [TestCase("SS", "09")]
    [TestCase("MS", "123")]
    [TestCase("US", "123456")]
    [TestCase("AM", "AM")]
    [TestCase("PM", "AM")]
    [TestCase("pm", "am")]
    [TestCase("TZ", "UTC")]
    [TestCase("tz", "utc")]
    [TestCase("OF", "+00")]
    public void PostgreSql_FormatsEachToken(string pattern, string expected)
    {
        Assert.AreEqual(expected, Pg(pattern, Reference));
    }

    [TestCase("FMMM", "3")]
    [TestCase("FMDD", "15")]
    [TestCase("FMMonth", "March")]
    [TestCase("FMDay", "Sunday")]
    [TestCase("FMDDD", "74")]
    [TestCase("FMHH24", "8")]
    [TestCase("FMMI", "5")]
    [TestCase("fmMonth", "March")]
    [TestCase("FMMonth DD", "March 15")]
    [TestCase("FMMonth MM", "March 03")]
    [TestCase("FM-YYYY", "-2026")]
    public void PostgreSql_FillModeChangesTheNextFieldOnly(string pattern, string expected)
    {
        Assert.AreEqual(expected, Pg(pattern, Reference));
    }

    [Test]
    public void PostgreSql_YearPadsToFourDigitsUnlessFillMode()
    {
        DateTime year99 = Utc(99, 1, 2);
        Assert.AreEqual("0099", Pg("YYYY", year99));
        Assert.AreEqual("99", Pg("FMYYYY", year99));
        Assert.AreEqual("099", Pg("YYY", year99));
    }

    [Test]
    public void PostgreSql_QuotedTextAndOtherCharactersAreLiteral()
    {
        Assert.AreEqual("2026 year 03", Pg("YYYY \"year\" MM", Reference));
        Assert.AreEqual("YYYY=2026", Pg("\"YYYY\"=YYYY", Reference));
        Assert.AreEqual("say \"hi\" 2026", Pg("\"say \\\"hi\\\" \"YYYY", Reference));
        Assert.AreEqual("\"2026\"", Pg("\\\"YYYY\\\"", Reference));
        Assert.AreEqual("2026-03-15T08:05:09", Pg("YYYY-MM-DD\"T\"HH24:MI:SS", Reference));
        Assert.AreEqual("[15/03]", Pg("[DD/MM]", Reference));
        Assert.AreEqual("", Pg("", Reference));
    }

    [Test]
    public void PostgreSql_TwelveAndTwentyFourHourClocks()
    {
        Assert.AreEqual("12 AM 00", Pg("HH12 AM HH24", Utc(2026, 3, 15, 0)));
        Assert.AreEqual("11 AM 11", Pg("HH12 AM HH24", Utc(2026, 3, 15, 11)));
        Assert.AreEqual("12 PM 12", Pg("HH12 PM HH24", Utc(2026, 3, 15, 12)));
        Assert.AreEqual("08 pm 20", Pg("HH am HH24", Utc(2026, 3, 15, 20)));
    }

    [Test]
    public void PostgreSql_IsoWeekAndIsoYearAtYearEdges()
    {
        // 2021-01-01 (a Friday) is in ISO week 53 of ISO year 2020.
        Assert.AreEqual("2020-53-5 2021 01", Pg("IYYY-IW-ID YYYY WW", Utc(2021, 1, 1)));

        // 2024-12-30 (a Monday) opens ISO week 1 of 2025.
        Assert.AreEqual("2025-01-1 2024 53", Pg("IYYY-IW-ID YYYY WW", Utc(2024, 12, 30)));
    }

    [Test]
    public void PostgreSql_FormatIgnoresTheCurrentCulture()
    {
        CultureInfo saved = CultureInfo.CurrentCulture;

        try
        {
            // The Thai culture uses the Buddhist calendar, where 2026 is the year 2569.
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");
            Assert.AreEqual("2026 March Sunday", Pg("YYYY FMMonth FMDay", Reference));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Test]
    public void PostgreSql_UnterminatedQuoteIsAPatternError()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Pg("YYYY \"abc", Reference))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("could not use pattern 'YYYY \"abc'", ex.Message);
        StringAssert.Contains("no closing", ex.Message);
    }

    // ── PostgreSQL: parse, one case per token ──────────────────────────────────

    [TestCase("2026-03-15", "YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("2026-03-15", "yyyy-mm-dd", 2026, 3, 15)]
    [TestCase("3/5/2026", "MM/DD/YYYY", 2026, 3, 5)]
    [TestCase("03/15/2026", "MM/DD/YYYY", 2026, 3, 15)]
    [TestCase("20260315", "YYYYMMDD", 2026, 3, 15)]
    [TestCase("026-03-15", "YYY-MM-DD", 2026, 3, 15)]
    [TestCase("999-03-15", "YYY-MM-DD", 1999, 3, 15)]
    [TestCase("26-03-15", "YY-MM-DD", 2026, 3, 15)]
    [TestCase("69-03-15", "YY-MM-DD", 2069, 3, 15)]
    [TestCase("70-03-15", "YY-MM-DD", 1970, 3, 15)]
    [TestCase("6-03-15", "Y-MM-DD", 2006, 3, 15)]
    [TestCase("15-Mar-2026", "DD-Mon-YYYY", 2026, 3, 15)]
    [TestCase("15-MAR-2026", "DD-Mon-YYYY", 2026, 3, 15)]
    [TestCase("15-mar-2026", "DD-MON-YYYY", 2026, 3, 15)]
    [TestCase("March 15, 2026", "Month DD, YYYY", 2026, 3, 15)]
    [TestCase("March     15, 2026", "Month DD, YYYY", 2026, 3, 15)]
    [TestCase("MARCH 15, 2026", "MONTH DD, YYYY", 2026, 3, 15)]
    [TestCase("May,2026", "Month,YYYY", 2026, 5, 1)]
    [TestCase("May      ,2026", "Month,YYYY", 2026, 5, 1)]
    [TestCase("2026 May      ", "YYYY Month", 2026, 5, 1)]
    [TestCase("2026 May", "YYYY FMMonth", 2026, 5, 1)]
    [TestCase("2026-074", "YYYY-DDD", 2026, 3, 15)]
    [TestCase("2024-366", "YYYY-DDD", 2024, 12, 31)]
    [TestCase("1 2026-03-15", "D YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("7 2026-03-15", "ID YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("Sun 2026-03-15", "Dy YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("SUNDAY 2026-03-15", "Day YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("Sunday, 2026-03-15", "FMDay, YYYY-MM-DD", 2026, 3, 15)]
    [TestCase("2026-03-15 11", "YYYY-MM-DD WW", 2026, 3, 15)]
    [TestCase("2026-03-15 11", "YYYY-MM-DD IW", 2026, 3, 15)]
    [TestCase("2026-03-15 1", "YYYY-MM-DD Q", 2026, 3, 15)]
    [TestCase("2026", "YYYY", 2026, 1, 1)]
    [TestCase("2026-03", "YYYY-MM", 2026, 3, 1)]
    [TestCase("2026y03", "YYYY\"y\"MM", 2026, 3, 1)]
    [TestCase("2026-03-15 UTC", "YYYY-MM-DD TZ", 2026, 3, 15)]
    [TestCase("2026-03-15 utc", "YYYY-MM-DD TZ", 2026, 3, 15)]
    [TestCase("2026-03-15+00", "YYYY-MM-DDOF", 2026, 3, 15)]
    public void PostgreSql_ParsesEachDateToken(string text, string pattern, int year, int month, int day)
    {
        Assert.AreEqual(Utc(year, month, day), PgParse(text, pattern));
    }

    [TestCase("2026-03-15 08:05 PM", "YYYY-MM-DD HH12:MI AM", 20, 5, 0)]
    [TestCase("2026-03-15 08:05 am", "YYYY-MM-DD HH12:MI PM", 8, 5, 0)]
    [TestCase("2026-03-15 12:00 AM", "YYYY-MM-DD HH:MI AM", 0, 0, 0)]
    [TestCase("2026-03-15 12:00 PM", "YYYY-MM-DD HH:MI AM", 12, 0, 0)]
    [TestCase("2026-03-15 12:30", "YYYY-MM-DD HH12:MI", 0, 30, 0)]
    [TestCase("2026-03-15 20:05:09", "YYYY-MM-DD HH24:MI:SS", 20, 5, 9)]
    [TestCase("2026-03-15 8:5:9", "YYYY-MM-DD HH24:MI:SS", 8, 5, 9)]
    [TestCase("2026-03-15 20 08 PM", "YYYY-MM-DD HH24 HH12 PM", 20, 0, 0)]
    public void PostgreSql_ParsesTwelveAndTwentyFourHourClocks(string text, string pattern, int hour, int minute, int second)
    {
        Assert.AreEqual(Utc(2026, 3, 15, hour, minute, second), PgParse(text, pattern));
    }

    [TestCase("2026-03-15 09.123", "YYYY-MM-DD SS.MS", 1230000)]
    [TestCase("2026-03-15 09.5", "YYYY-MM-DD SS.MS", 5000000)]
    [TestCase("2026-03-15 09.123456", "YYYY-MM-DD SS.US", 1234560)]
    [TestCase("2026-03-15 09.5", "YYYY-MM-DD SS.US", 5000000)]
    [TestCase("2026-03-15 09.123.123456", "YYYY-MM-DD SS.MS.US", 1234560)]
    public void PostgreSql_ParsesFractionsAsDigitsAfterThePoint(string text, string pattern, long fractionTicks)
    {
        Assert.AreEqual(Utc(2026, 3, 15, 0, 0, 9).AddTicks(fractionTicks), PgParse(text, pattern));
    }

    [Test]
    public void PostgreSql_ParsesAnIsoWeekDate()
    {
        Assert.AreEqual(Utc(2021, 1, 1), PgParse("2020-53-5", "IYYY-IW-ID"));
        Assert.AreEqual(Utc(2024, 12, 30), PgParse("2025", "IYYY"));
        Assert.AreEqual(Utc(2026, 3, 15), PgParse("2026-11-7", "IYYY-IW-ID"));
        Assert.AreEqual(Utc(2026, 3, 9), PgParse("2026-11", "IYYY-IW"));
    }

    // ── PostgreSQL: parse errors ───────────────────────────────────────────────

    [Test]
    public void PostgreSql_ParseErrorsNameTheTextAndThePattern()
    {
        AssertPgParseFails("2026/03/15", "YYYY-MM-DD", "expected '-' at position 5, found '/03/15'");
        AssertPgParseFails("2026-Ma-15", "YYYY-MM-DD", "expected a number for 'MM' at position 6");
        AssertPgParseFails("2026-03", "YYYY-MM-DD", "expected '-' at position 8, found the end of the text");
        AssertPgParseFails("2026-03-15x", "YYYY-MM-DD", "unexpected text 'x' at position 11");
        AssertPgParseFails("2026-03-15 ", "YYYY-MM-DD", "unexpected text ' '");
        AssertPgParseFails("2026-0315", "YYYY-MM DD", "expected a space at position 8");
    }

    [TestCase("2026-13-01", "YYYY-MM-DD", "'MM' value 13 is out of range 1-12")]
    [TestCase("2026-00-01", "YYYY-MM-DD", "'MM' value 0 is out of range 1-12")]
    [TestCase("2026-03-32", "YYYY-MM-DD", "'DD' value 32 is out of range 1-31")]
    [TestCase("2026-02-29", "YYYY-MM-DD", "day 29 is out of range for February 2026, which has 28 days")]
    [TestCase("2026-03-15 24", "YYYY-MM-DD HH24", "'HH24' value 24 is out of range 0-23")]
    [TestCase("2026-03-15 13", "YYYY-MM-DD HH12", "'HH12' value 13 is out of range 1-12")]
    [TestCase("2026-03-15 00", "YYYY-MM-DD HH", "'HH' value 0 is out of range 1-12")]
    [TestCase("2026-03-15 10:60", "YYYY-MM-DD HH24:MI", "'MI' value 60 is out of range 0-59")]
    [TestCase("2026-03-15 10:00:60", "YYYY-MM-DD HH24:MI:SS", "'SS' value 60 is out of range 0-59")]
    [TestCase("2026-366", "YYYY-DDD", "year 2026 has 365 days, so day of year 366 is out of range")]
    [TestCase("0000-01-01", "YYYY-MM-DD", "year 0 is outside the years 1-9999")]
    [TestCase("2026-03-15 8", "YYYY-MM-DD D", "'D' value 8 is out of range 1-7")]
    [TestCase("2026-03-15 5", "YYYY-MM-DD Q", "'Q' value 5 is out of range 1-4")]
    public void PostgreSql_OutOfRangeValuesAreErrors(string text, string pattern, string reason)
    {
        AssertPgParseFails(text, pattern, reason);
    }

    [TestCase("03-15", "MM-DD", "the pattern has no year field")]
    [TestCase("10:30", "HH24:MI", "the pattern has no year field")]
    [TestCase("2026 25", "YYYY YY", "'YY' gives 2025, which conflicts with an earlier field that gives 2026")]
    [TestCase("03 Apr 2026", "MM Mon YYYY", "'Mon' gives 4, which conflicts with an earlier field that gives 3")]
    [TestCase("2026-03-15 Mon", "YYYY-MM-DD Dy", "'Dy' gives Monday, but 2026-03-15 is a Sunday")]
    [TestCase("2026-03-15 2", "YYYY-MM-DD D", "'D' gives Monday, but 2026-03-15 is a Sunday")]
    [TestCase("2026-03-15 1", "YYYY-MM-DD ID", "'ID' gives Monday, but 2026-03-15 is a Sunday")]
    [TestCase("2026-03-15 12", "YYYY-MM-DD WW", "'WW' gives week 12, but 2026-03-15 is in week 11")]
    [TestCase("2026-03-15 12", "YYYY-MM-DD IW", "'IW' gives ISO week 12, but 2026-03-15 is in ISO week 11")]
    [TestCase("2026-03-15 2", "YYYY-MM-DD Q", "'Q' gives quarter 2, but 2026-03-15 is in quarter 1")]
    [TestCase("2026-03-15 075", "YYYY-MM-DD DDD", "'DDD' gives day of year 75, but 2026-03-15 is day 74")]
    [TestCase("2026-03", "IYYY-MM", "mixes ISO week date fields")]
    [TestCase("2025-53", "IYYY-IW", "ISO year 2025 has 52 weeks, so week 53 is out of range")]
    [TestCase("2026-03-15 10 AM", "YYYY-MM-DD HH24 AM", "'AM' needs a 12-hour field")]
    [TestCase("2026-03-15 PM", "YYYY-MM-DD PM", "'PM' needs a 12-hour field")]
    [TestCase("2026-03-15 10 08 PM", "YYYY-MM-DD HH24 HH12 PM", "the 12-hour field gives hour 20, which conflicts with the 24-hour field, which gives 10")]
    [TestCase("2026-03-15 09.124.123456", "YYYY-MM-DD SS.MS.US", "the millisecond field gives 124, which conflicts with the microsecond field")]
    [TestCase("15-Mxr-2026", "DD-Mon-YYYY", "expected a three-letter month name for 'Mon' at position 4")]
    [TestCase("Marc 15 2026", "Month DD YYYY", "expected a month name for 'Month' at position 1")]
    [TestCase("Xyz 2026-03-15", "Dy YYYY-MM-DD", "expected a three-letter day name for 'Dy' at position 1")]
    [TestCase("2026-03-15 XM", "YYYY-MM-DD HH12 AM", "expected a number for 'HH12'")]
    [TestCase("2026-03-15 10 XM", "YYYY-MM-DD HH12 AM", "expected AM or PM for 'AM' at position 15")]
    [TestCase("2026-03-15 EST", "YYYY-MM-DD TZ", "expected 'UTC' for 'TZ' at position 12")]
    [TestCase("2026-03-15+05", "YYYY-MM-DDOF", "expected '+00' for 'OF' at position 11")]
    [TestCase("2026x03", "YYYY\"y\"MM", "expected 'y' at position 5")]
    public void PostgreSql_InconsistentTextIsAnError(string text, string pattern, string reason)
    {
        AssertPgParseFails(text, pattern, reason);
    }

    // ── Round trip ─────────────────────────────────────────────────────────────

    [TestCase("YYYY-MM-DD HH24:MI:SS.US")]
    [TestCase("FMDay, DD Month YYYY HH12:MI:SS.US AM")]
    [TestCase("Dy DD-MON-YYYY HH:MI:SS.US PM TZ")]
    [TestCase("YYYYMMDD\"T\"HH24MISSUS")]
    [TestCase("IYYY-IW-ID HH24:MI:SS.US")]
    [TestCase("YYYY-DDD HH24:MI:SS.MS.US Q WW IW D")]
    public void PostgreSql_ParseReadsBackWhatFormatPrinted(string pattern)
    {
        DateTime[] values =
        [
            Reference,
            Utc(2021, 1, 1),
            Utc(2024, 2, 29, 23, 59, 59).AddTicks(9999990),
            Utc(2024, 12, 30, 12, 0, 0),
            Utc(1, 1, 1),
            Utc(9999, 12, 31, 0, 0, 1).AddTicks(10),
            Utc(1999, 9, 22, 0, 30, 0),
        ];

        foreach (DateTime value in values)
        {
            string text = Pg(pattern, value);
            Assert.AreEqual(value, PgParse(text, pattern), $"{pattern} -> '{text}'");
        }
    }

    [TestCase("%Y-%m-%d %H:%i:%s.%f")]
    [TestCase("%W, %M %e, %Y %h:%i:%s.%f %p")]
    [TestCase("%a %d-%b-%y %I:%i:%S.%f%p %j %u %v")]
    public void MySql_ParseReadsBackWhatFormatPrinted(string pattern)
    {
        DateTime[] values =
        [
            Reference,
            Utc(2021, 1, 1),
            Utc(2024, 2, 29, 23, 59, 59).AddTicks(9999990),
            Utc(2027, 1, 1, 12, 0, 0),
            Utc(1999, 9, 22, 0, 30, 0),
        ];

        foreach (DateTime value in values)
        {
            string text = My(pattern, value);
            Assert.AreEqual(value, MyParse(text, pattern), $"{pattern} -> '{text}'");
        }
    }

    [Test]
    public void Format_DropsTheSeventhFractionDigit()
    {
        DateTime value = Utc(2026, 3, 15).AddTicks(1234567);
        Assert.AreEqual("123456", Pg("US", value));
        Assert.AreEqual(Utc(2026, 3, 15).AddTicks(1234560), PgParse(Pg("YYYY-MM-DD US", value), "YYYY-MM-DD US"));
    }

    // ── MySQL: format and parse, one case per specifier ─────────────────────────

    [TestCase("%Y", "2026")]
    [TestCase("%y", "26")]
    [TestCase("%m", "03")]
    [TestCase("%c", "3")]
    [TestCase("%d", "15")]
    [TestCase("%e", "15")]
    [TestCase("%H", "08")]
    [TestCase("%h", "08")]
    [TestCase("%I", "08")]
    [TestCase("%i", "05")]
    [TestCase("%s", "09")]
    [TestCase("%S", "09")]
    [TestCase("%f", "123456")]
    [TestCase("%p", "AM")]
    [TestCase("%M", "March")]
    [TestCase("%b", "Mar")]
    [TestCase("%W", "Sunday")]
    [TestCase("%a", "Sun")]
    [TestCase("%j", "074")]
    [TestCase("%u", "11")]
    [TestCase("%v", "11")]
    [TestCase("%%", "%")]
    [TestCase("100%% %Y", "100% 2026")]
    [TestCase("", "")]
    public void MySql_FormatsEachSpecifier(string pattern, string expected)
    {
        Assert.AreEqual(expected, My(pattern, Reference));
    }

    [Test]
    public void MySql_UnpaddedSpecifiersAndAfternoon()
    {
        DateTime value = Utc(2026, 3, 5, 20, 7, 0);
        Assert.AreEqual("3/5 03/05 08 08 20 PM", My("%c/%e %m/%d %h %I %H %p", value));
        Assert.AreEqual("12 AM", My("%h %p", Utc(2026, 3, 5, 0)));
    }

    [Test]
    public void MySql_MondayWeekDiffersFromIsoWeekAtTheYearStart()
    {
        // 2027-01-01 is a Friday: ISO puts it in week 53 of 2026, and WEEK(x, 1) counts it as week 0.
        Assert.AreEqual("00 53", My("%u %v", Utc(2027, 1, 1)));
        Assert.AreEqual("01 01", My("%u %v", Utc(2027, 1, 4)));

        // 2026-01-01 is a Thursday, so its week has four days in 2026 and is week 1 in both.
        Assert.AreEqual("01 01", My("%u %v", Utc(2026, 1, 1)));
    }

    [TestCase("2026-03-15", "%Y-%m-%d", 2026, 3, 15)]
    [TestCase("26-03-15", "%y-%m-%d", 2026, 3, 15)]
    [TestCase("99-03-15", "%y-%m-%d", 1999, 3, 15)]
    [TestCase("3/5/2026", "%c/%e/%Y", 2026, 3, 5)]
    [TestCase("03/05/2026", "%m/%d/%Y", 2026, 3, 5)]
    [TestCase("March 15, 2026", "%M %e, %Y", 2026, 3, 15)]
    [TestCase("15 Mar 2026", "%d %b %Y", 2026, 3, 15)]
    [TestCase("Sunday 2026-03-15", "%W %Y-%m-%d", 2026, 3, 15)]
    [TestCase("Sun 2026-03-15", "%a %Y-%m-%d", 2026, 3, 15)]
    [TestCase("2026 074", "%Y %j", 2026, 3, 15)]
    [TestCase("2026-03-15 11", "%Y-%m-%d %u", 2026, 3, 15)]
    [TestCase("2026-03-15 11", "%Y-%m-%d %v", 2026, 3, 15)]
    [TestCase("100% 2026", "100%% %Y", 2026, 1, 1)]
    public void MySql_ParsesEachDateSpecifier(string text, string pattern, int year, int month, int day)
    {
        Assert.AreEqual(Utc(year, month, day), MyParse(text, pattern));
    }

    [TestCase("2026-03-15 20:05:09", "%Y-%m-%d %H:%i:%s", 20, 5, 9, 0)]
    [TestCase("2026-03-15 08:05:09", "%Y-%m-%d %h:%i:%S", 8, 5, 9, 0)]
    [TestCase("2026-03-15 08:05 PM", "%Y-%m-%d %I:%i %p", 20, 5, 0, 0)]
    [TestCase("2026-03-15 12:05 am", "%Y-%m-%d %h:%i %p", 0, 5, 0, 0)]
    [TestCase("2026-03-15 00:00:09.123456", "%Y-%m-%d %H:%i:%s.%f", 0, 0, 9, 1234560)]
    [TestCase("2026-03-15 00:00:09.5", "%Y-%m-%d %H:%i:%s.%f", 0, 0, 9, 5000000)]
    public void MySql_ParsesEachTimeSpecifier(string text, string pattern, int hour, int minute, int second, long fractionTicks)
    {
        Assert.AreEqual(Utc(2026, 3, 15, hour, minute, second).AddTicks(fractionTicks), MyParse(text, pattern));
    }

    [TestCase("2026/03/15", "%Y-%m-%d", "expected '-' at position 5")]
    [TestCase("2026-03-15 Monday", "%Y-%m-%d %W", "'%W' gives Monday, but 2026-03-15 is a Sunday")]
    [TestCase("2026-03-15 12", "%Y-%m-%d %u", "'%u' gives week 12, but 2026-03-15 is in week 11")]
    [TestCase("2026-03-15 12", "%Y-%m-%d %v", "'%v' gives ISO week 12, but 2026-03-15 is in ISO week 11")]
    [TestCase("2026-03-15 10 PM", "%Y-%m-%d %H %p", "'%p' needs a 12-hour field")]
    [TestCase("2026-02-30", "%Y-%m-%d", "day 30 is out of range for February 2026")]
    [TestCase("10:30:00", "%H:%i:%s", "the pattern has no year field")]
    public void MySql_InconsistentTextIsAnError(string text, string pattern, string reason)
    {
        AssertMyParseFails(text, pattern, reason);
    }

    [TestCase("%Y-%q", "unknown specifier '%q' at position 4")]
    [TestCase("%Y-%T", "unknown specifier '%T' at position 4")]
    [TestCase("%Y%", "ends with a '%' that has no specifier letter")]
    public void MySql_UnknownSpecifierIsAPatternError(string pattern, string reason)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => My(pattern, Reference))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains($"could not use pattern '{pattern}'", ex.Message);
        StringAssert.Contains(reason, ex.Message);
    }

    // ── Compile cache ──────────────────────────────────────────────────────────

    [Test]
    public void Get_CompilesEachPatternOncePerDialect()
    {
        DatePattern first = DatePattern.Get("to_char", "YYYY-MM-DD", DatePatternDialect.PostgreSql);
        DatePattern second = DatePattern.Get("to_char", "YYYY-MM-DD", DatePatternDialect.PostgreSql);
        DatePattern mysql = DatePattern.Get("date_format", "YYYY-MM-DD", DatePatternDialect.MySql);

        Assert.AreSame(first, second);
        Assert.AreNotSame(first, mysql);

        // The same text is a template in one dialect and plain literal text in the other.
        Assert.AreEqual("2026-03-15", first.Format(Reference));
        Assert.AreEqual("YYYY-MM-DD", mysql.Format(Reference));
    }

    [Test]
    public void Get_StaysCorrectWhenTheCacheOverflows()
    {
        for (int i = 0; i < 600; i++)
            Assert.AreEqual(i.ToString(CultureInfo.InvariantCulture) + " 2026", Pg("\"" + i.ToString(CultureInfo.InvariantCulture) + "\" YYYY", Reference));

        Assert.AreEqual("2026", Pg("YYYY", Reference));
    }
}
