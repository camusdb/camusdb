/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The time zone argument of <c>date_trunc</c>, <c>date_part</c> and <c>to_char</c>. Every case goes
/// through SQL. The instants sit on the edges of a zone's rules: both daylight saving changes of
/// America/New_York in 2026 (March 8 and November 1), a zone with a half-hour offset (Asia/Kolkata), a
/// zone whose clock skips midnight (America/Havana, America/Santiago), and a zone whose clock moves
/// by half an hour (Australia/Lord_Howe). The expected values were checked against the OS tzdata
/// (2026f) through <c>TimeZoneInfo</c>, a source independent of the engine's.
/// </summary>
[NonParallelizable]
public sealed class TestDateTimeZoneArgument : SharedNodeBaseTest
{
    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> Scalar(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT " + expression);
        Assert.AreEqual(1, rows.Count, expression);
        return rows[0].Row["0"];
    }

    private static async Task AssertIso(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, string expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.DateTime, value.Type, expression);
        Assert.AreEqual(expected, value.IsoValue, expression);
    }

    private static async Task AssertLong(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, long expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.Integer64, value.Type, expression);
        Assert.AreEqual(expected, value.LongValue, expression);
    }

    private static async Task AssertString(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, string expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.String, value.Type, expression);
        Assert.AreEqual(expected, value.StrValue, expression);
    }

    private static CamusDBException AssertThrows(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression)
    {
        return Assert.ThrowsAsync<CamusDBException>(async () => await Scalar(executor, database, dbname, expression))!;
    }

    // ── date_trunc ──────────────────────────────────────────────────────────────

    [Test]
    public async Task DateTrunc_Day_StartsAtLocalMidnight_OnBothSidesOfTheSpringChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 2026-03-08 starts in EST (-05); the clock moves from 02:00 to 03:00 that morning.
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-03-08T12:00:00Z', 'America/New_York')", "2026-03-08T05:00:00.0000000Z");
        // 2026-03-09 starts in EDT (-04).
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-03-09T12:00:00Z', 'America/New_York')", "2026-03-09T04:00:00.0000000Z");
        // 03:00Z is still 22:00 of March 7 in New York.
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-03-08T03:00:00Z', 'America/New_York')", "2026-03-07T05:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_Hour_AroundTheSpringChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 06:59Z is 01:59 EST, the last minute before the change.
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-03-08T06:59:00Z', 'America/New_York')", "2026-03-08T06:00:00.0000000Z");
        // 07:30Z is 03:30 EDT. The local hour 02 does not exist that day.
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-03-08T07:30:00Z', 'America/New_York')", "2026-03-08T07:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_TheRepeatedHourOfTheFallChange_GivesOneBucketPerOffset()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // On 2026-11-01 the clock moves from 02:00 EDT back to 01:00 EST, so 01:30 happens twice:
        // 05:30Z (EDT) and 06:30Z (EST).
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-11-01T05:30:00Z', 'America/New_York')", "2026-11-01T05:00:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-11-01T06:30:00Z', 'America/New_York')", "2026-11-01T06:00:00.0000000Z");

        // A minute in the second occurrence stays in the second occurrence.
        await AssertIso(executor, database, dbname,
            "date_trunc('minute', '2026-11-01T06:30:45Z', 'America/New_York')", "2026-11-01T06:30:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('second', '2026-11-01T06:30:45.5Z', 'America/New_York')", "2026-11-01T06:30:45.0000000Z");
    }

    [Test]
    public async Task DateTrunc_Day_TheLongDayOfTheFallChange_StartsOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Every instant of the 25-hour local day 2026-11-01 truncates to its EDT midnight.
        foreach (string instant in new[] { "2026-11-01T04:00:00Z", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z", "2026-11-02T04:59:59Z" })
            await AssertIso(executor, database, dbname,
                $"date_trunc('day', '{instant}', 'America/New_York')", "2026-11-01T04:00:00.0000000Z");

        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-11-02T05:00:00Z', 'America/New_York')", "2026-11-02T05:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_HalfHourOffset_AsiaKolkata()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 20:10Z is 01:40 of the next day in India (+05:30).
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", "2026-03-15T18:30:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", "2026-03-15T19:30:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('month', '2026-03-31T19:00:00Z', 'Asia/Kolkata')", "2026-03-31T18:30:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('year', '2026-12-31T18:30:00Z', 'Asia/Kolkata')", "2026-12-31T18:30:00.0000000Z");
        // Monday 2026-03-16 00:00 IST starts the ISO week.
        await AssertIso(executor, database, dbname,
            "date_trunc('week', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", "2026-03-15T18:30:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_Day_WhenTheClockSkipsMidnight_StartsAtTheChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Havana moves from 00:00 CST to 01:00 CDT on 2026-03-08, at 05:00Z. Local midnight does not
        // exist, so the day starts at 01:00 CDT.
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-03-08T12:00:00Z', 'America/Havana')", "2026-03-08T05:00:00.0000000Z");

        // Santiago moves from 00:00 (-04) to 01:00 (-03) on 2026-09-06, at 04:00Z.
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-09-06T12:00:00Z', 'America/Santiago')", "2026-09-06T04:00:00.0000000Z");
        await AssertIso(executor, database, dbname,
            "date_trunc('day', '2026-09-05T12:00:00Z', 'America/Santiago')", "2026-09-05T04:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_Hour_WhenTheClockSkipsHalfAnHour_StartsAtTheChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Lord Howe moves from 02:00 (+10:30) to 02:30 (+11) on 2026-10-04, at 15:30Z the day before.
        // 15:45Z is 02:45, and the local hour 02 starts at the change.
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-10-03T15:45:00Z', 'Australia/Lord_Howe')", "2026-10-03T15:30:00.0000000Z");
        // 15:29Z is 01:59 (+10:30).
        await AssertIso(executor, database, dbname,
            "date_trunc('hour', '2026-10-03T15:29:00Z', 'Australia/Lord_Howe')", "2026-10-03T14:30:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_WhenTheClockMovesBackHalfAnHour_TheSecondOccurrenceStartsAtTheChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Lord Howe moves from 02:00 (+11) back to 01:30 (+10:30) on 2026-04-05, at 2026-04-04T15:00Z.
        // So 01:30-02:00 happens twice, and 01:00 happens only at +11.
        const string zone = "Australia/Lord_Howe";

        // 01:45 and 01:59 at +11: the hour 01 that started at 01:00 +11.
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T14:45:00Z', '{zone}')", "2026-04-04T14:00:00.0000000Z");
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T14:59:00Z', '{zone}')", "2026-04-04T14:00:00.0000000Z");
        // 01:30, 01:45 and 01:59 at +10:30: the second occurrence of hour 01, which starts at the change.
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T15:00:00Z', '{zone}')", "2026-04-04T15:00:00.0000000Z");
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T15:15:00Z', '{zone}')", "2026-04-04T15:00:00.0000000Z");
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T15:29:00Z', '{zone}')", "2026-04-04T15:00:00.0000000Z");
        // 02:00 at +10:30 starts the next hour.
        await AssertIso(executor, database, dbname, $"date_trunc('hour', '2026-04-04T15:30:00Z', '{zone}')", "2026-04-04T15:30:00.0000000Z");

        // A repeated minute stays in its own occurrence.
        await AssertIso(executor, database, dbname, $"date_trunc('minute', '2026-04-04T14:45:30Z', '{zone}')", "2026-04-04T14:45:00.0000000Z");
        await AssertIso(executor, database, dbname, $"date_trunc('minute', '2026-04-04T15:15:30Z', '{zone}')", "2026-04-04T15:15:00.0000000Z");

        // The day still starts once, at local midnight +11.
        await AssertIso(executor, database, dbname, $"date_trunc('day', '2026-04-04T15:15:00Z', '{zone}')", "2026-04-04T13:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_IsNeverLaterThanItsValue_AndIsIdempotent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string[] zones = ["America/New_York", "Asia/Kolkata", "America/Havana", "America/Santiago", "Australia/Lord_Howe", "UTC"];
        string[] units = ["microsecond", "millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year", "decade"];
        string[] instants =
        [
            "2026-03-08T04:59:59Z", "2026-03-08T05:00:00Z", "2026-03-08T06:59:59Z", "2026-03-08T07:00:00Z",
            "2026-11-01T04:30:00Z", "2026-11-01T05:59:59Z", "2026-11-01T06:00:00Z", "2026-11-01T06:30:00Z",
            "2026-09-06T03:59:59Z", "2026-09-06T04:00:00Z", "2026-10-03T15:29:59Z", "2026-10-03T15:30:00Z",
            "2026-04-04T14:45:00Z", "2026-04-04T14:59:59Z", "2026-04-04T15:00:00Z", "2026-04-04T15:15:00Z",
        ];

        foreach (string zone in zones)
        foreach (string unit in units)
        foreach (string instant in instants)
        {
            string expression = $"date_trunc('{unit}', '{instant}', '{zone}')";
            ColumnValue once = await Scalar(executor, database, dbname, expression);
            ColumnValue twice = await Scalar(executor, database, dbname, $"date_trunc('{unit}', {expression}, '{zone}')");
            ColumnValue value = await Scalar(executor, database, dbname, $"CAST('{instant}' AS datetime)");

            Assert.LessOrEqual(once.LongValue, value.LongValue, expression);
            Assert.AreEqual(once.LongValue, twice.LongValue, "idempotent: " + expression);
        }
    }

    [Test]
    public async Task DateTrunc_ZoneUtc_MatchesTheTwoArgumentForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string unit in new[] { "hour", "day", "week", "month", "year" })
        {
            ColumnValue withZone = await Scalar(executor, database, dbname, $"date_trunc('{unit}', '2026-06-17T10:30:45Z', 'UTC')");
            ColumnValue withoutZone = await Scalar(executor, database, dbname, $"date_trunc('{unit}', '2026-06-17T10:30:45Z')");
            Assert.AreEqual(withoutZone.LongValue, withZone.LongValue, unit);
        }
    }

    // ── date_part ───────────────────────────────────────────────────────────────

    [Test]
    public async Task DatePart_ReadsTheWallClockOfTheZone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('hour', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", 1);
        await AssertLong(executor, database, dbname, "date_part('minute', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", 40);
        await AssertLong(executor, database, dbname, "date_part('day', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", 16);
        // Monday in India, still Sunday in UTC.
        await AssertLong(executor, database, dbname, "date_part('dow', '2026-03-15T20:10:00Z', 'Asia/Kolkata')", 1);
        await AssertLong(executor, database, dbname, "date_part('dow', '2026-03-15T20:10:00Z')", 0);

        // Both occurrences of 01:30 on 2026-11-01 read hour 1 in New York.
        await AssertLong(executor, database, dbname, "date_part('hour', '2026-11-01T05:30:00Z', 'America/New_York')", 1);
        await AssertLong(executor, database, dbname, "date_part('hour', '2026-11-01T06:30:00Z', 'America/New_York')", 1);
        // 03:00Z on 2026-03-08 is 22:00 of March 7 in New York.
        await AssertLong(executor, database, dbname, "date_part('day', '2026-03-08T03:00:00Z', 'America/New_York')", 7);
        // 2026-12-31T20:00Z is already 2027 in India.
        await AssertLong(executor, database, dbname, "date_part('year', '2026-12-31T20:00:00Z', 'Asia/Kolkata')", 2027);
    }

    [Test]
    public async Task DatePart_Epoch_DoesNotChangeWithTheZone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue utc = await Scalar(executor, database, dbname, "date_part('epoch', '2026-03-15T20:10:00Z')");
        ColumnValue zoned = await Scalar(executor, database, dbname, "date_part('epoch', '2026-03-15T20:10:00Z', 'Asia/Kolkata')");
        Assert.AreEqual(utc.LongValue, zoned.LongValue);

        // The zone is still checked.
        CamusDBException ex = AssertThrows(executor, database, dbname, "date_part('epoch', '2026-03-15T20:10:00Z', 'Nowhere/City')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task DatePart_WithAZone_RefusesAnInterval()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertThrows(executor, database, dbname, "date_part('hour', INTERVAL '3 hours', 'Asia/Kolkata')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("interval", ex.Message);
    }

    [Test]
    public async Task DateArgument_IsMidnightUtc_ThenReadInTheZone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Midnight UTC of March 15 is 19:00 of March 14 in Bogota and 05:30 of March 15 in India.
        await AssertLong(executor, database, dbname, "date_part('day', DATE '2026-03-15', 'America/Bogota')", 14);
        await AssertIso(executor, database, dbname,
            "date_trunc('day', DATE '2026-03-15', 'Asia/Kolkata')", "2026-03-14T18:30:00.0000000Z");
    }

    // ── to_char ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task ToChar_PrintsTheWallClockAbbreviationAndOffsetOfTheZone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, database, dbname,
            "to_char('2026-11-01T05:30:00Z', 'YYYY-MM-DD HH24:MI TZ OF', 'America/New_York')", "2026-11-01 01:30 EDT -04");
        await AssertString(executor, database, dbname,
            "to_char('2026-11-01T06:30:00Z', 'YYYY-MM-DD HH24:MI TZ OF', 'America/New_York')", "2026-11-01 01:30 EST -05");
        await AssertString(executor, database, dbname,
            "to_char('2026-03-15T20:10:00Z', 'YYYY-MM-DD HH24:MI tz OF', 'Asia/Kolkata')", "2026-03-16 01:40 ist +05:30");
        await AssertString(executor, database, dbname,
            "to_char('2026-01-15T12:00:00Z', 'HH24:MI TZ OF', 'America/St_Johns')", "08:30 NST -03:30");
        // tzdb names this zone by its offset.
        await AssertString(executor, database, dbname,
            "to_char('2026-03-15T12:00:00Z', 'HH24:MI TZ OF', 'America/Bogota')", "07:00 -05 -05");
        // Without a zone the fields stay UTC.
        await AssertString(executor, database, dbname,
            "to_char('2026-03-15T12:00:00Z', 'HH24:MI TZ OF')", "12:00 UTC +00");
    }

    [Test]
    public async Task ToChar_FmOffset_DropsTheHourPaddingOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, database, dbname, "to_char('2026-03-15T20:10:00Z', 'OF FMOF', 'Asia/Kolkata')", "+05:30 +5:30");
        await AssertString(executor, database, dbname, "to_char('2026-03-15T12:00:00Z', 'OF FMOF', 'America/Bogota')", "-05 -5");
        await AssertString(executor, database, dbname, "to_char('2026-01-15T12:00:00Z', 'OF FMOF', 'America/St_Johns')", "-03:30 -3:30");
        await AssertString(executor, database, dbname, "to_char('2026-03-15T20:10:00Z', 'OF FMOF', 'Pacific/Kiritimati')", "+14 +14");
        await AssertString(executor, database, dbname, "to_char('2026-03-15T12:00:00Z', 'OF FMOF')", "+00 +0");
    }

    // ── which calls depend on the zone rules ────────────────────────────────────

    [TestCase("SELECT * FROM t WHERE date_part('hour', ts, 'Asia/Kolkata') = 1", true)]
    [TestCase("SELECT * FROM t WHERE DATE_TRUNC('day', ts, 'UTC') > ts", true)]
    [TestCase("SELECT to_char(ts, 'HH24', 'UTC') FROM t", true)]
    [TestCase("SELECT * FROM t WHERE CASE WHEN num > 1 THEN date_trunc('day', ts, @zone) ELSE ts END > ts", true)]
    [TestCase("SELECT * FROM t WHERE date_part('hour', ts) = 1", false)]
    [TestCase("SELECT * FROM t WHERE EXTRACT(hour FROM ts) = 1", false)]
    [TestCase("SELECT to_char(ts, 'HH24') FROM t", false)]
    [TestCase("SELECT date_add(ts, 1, 'day') FROM t", false)]
    [TestCase("SELECT * FROM t WHERE num > 5", false)]
    public void ContainsTimeZoneDependentCall_FindsCallsThatPassTheZone(string sql, bool expected)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, ScalarFunctionEvaluator.ContainsTimeZoneDependentCall(ast), sql);
        Assert.AreEqual(expected ? TimeZoneRules.DataVersion : null, TimeZoneRules.DataVersionIfUsedBy([null, ast]), sql);
    }

    // ── zone names ──────────────────────────────────────────────────────────────

    [Test]
    public async Task ZoneNames_IgnoreCase_AndAcceptAliases()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string zone in new[] { "america/new_york", "AMERICA/NEW_YORK", "US/Eastern" })
            await AssertIso(executor, database, dbname,
                $"date_trunc('day', '2026-03-09T12:00:00Z', '{zone}')", "2026-03-09T04:00:00.0000000Z");

        await AssertLong(executor, database, dbname, "date_part('minute', '2026-03-15T20:10:00Z', 'Asia/Calcutta')", 40);
    }

    [Test]
    public async Task UnknownZone_IsInvalidInput_NamingTheZone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string expression in new[]
                 {
                     "date_trunc('day', '2026-03-09T12:00:00Z', 'Mars/Olympus_Mons')",
                     "date_part('hour', '2026-03-09T12:00:00Z', 'Mars/Olympus_Mons')",
                     "to_char('2026-03-09T12:00:00Z', 'HH24', 'Mars/Olympus_Mons')",
                 })
        {
            CamusDBException ex = AssertThrows(executor, database, dbname, expression);
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, expression);
            StringAssert.Contains("'Mars/Olympus_Mons'", ex.Message, expression);
        }
    }

    [Test]
    public async Task NumericOffset_IsRefused_WithAHint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertThrows(executor, database, dbname, "date_trunc('day', '2026-03-09T12:00:00Z', '+05:30')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("numeric offset", ex.Message);
    }

    [Test]
    public async Task NullZone_GivesNull_AndANonStringZone_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await Scalar(executor, database, dbname, "date_trunc('day', '2026-03-09T12:00:00Z', NULL)");
        Assert.AreEqual(ColumnType.Null, value.Type);

        CamusDBException ex = AssertThrows(executor, database, dbname, "date_trunc('day', '2026-03-09T12:00:00Z', 5)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task Zone_CanBeAParameter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT date_trunc('day', '2026-03-15T20:10:00Z', @zone)",
            new Dictionary<string, ColumnValue> { ["@zone"] = new(ColumnType.String, "Asia/Kolkata") });

        Assert.AreEqual("2026-03-15T18:30:00.0000000Z", rows.Single().Row["0"].IsoValue);
    }

    // ── over a table ────────────────────────────────────────────────────────────

    [Test]
    public async Task GroupBy_LocalDay_AcrossTheFallChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE events (id int64 PRIMARY KEY, created datetime)");

        // Local New York times in the comments.
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO events (id, created) VALUES " +
            "(1, '2026-10-31T23:00:00Z'), " + // Oct 31 19:00 EDT
            "(2, '2026-11-01T03:59:59Z'), " + // Oct 31 23:59:59 EDT
            "(3, '2026-11-01T04:00:00Z'), " + // Nov 1 00:00 EDT
            "(4, '2026-11-01T05:30:00Z'), " + // Nov 1 01:30 EDT
            "(5, '2026-11-01T06:30:00Z'), " + // Nov 1 01:30 EST
            "(6, '2026-11-02T04:59:59Z'), " + // Nov 1 23:59:59 EST
            "(7, '2026-11-02T05:00:00Z')");   // Nov 2 00:00 EST

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT date_trunc('day', created, 'America/New_York') AS day, count(*) AS n " +
            "FROM events GROUP BY date_trunc('day', created, 'America/New_York') ORDER BY day");

        Assert.AreEqual(
            new[] { "2026-10-31T04:00:00.0000000Z:2", "2026-11-01T04:00:00.0000000Z:4", "2026-11-02T05:00:00.0000000Z:1" },
            rows.Select(r => r.Row["day"].IsoValue + ":" + r.Row["n"].LongValue).ToArray());

        // A filter on a local part.
        List<QueryResultRow> oneThirty = await ExecQuery(executor, database, dbname,
            "SELECT id FROM events WHERE date_part('hour', created, 'America/New_York') = 1 ORDER BY id");
        Assert.AreEqual(new long[] { 4, 5 }, oneThirty.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [TestCase("date_trunc('day', created, 'America/New_York')")]
    [TestCase("date_part('hour', created, 'Asia/Kolkata')")]
    [TestCase("to_char(created, 'YYYY-MM-DD', 'UTC')")]
    [TestCase("lower(to_char(created, 'YYYY-MM-DD', 'UTC'))")]
    public async Task IndexExpression_WithAZone_IsRefused(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE events (id int64 PRIMARY KEY, created datetime)");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, database, dbname, $"CREATE INDEX bad ON events (({expression}))"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidIndexExpression, ex!.Code, ex.Message);
        StringAssert.Contains("time zone argument", ex.Message);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "events"));
        Assert.IsFalse(table.AllIndexes.ContainsKey("bad"));
    }

    [Test]
    public async Task IndexExpression_WithoutAZone_IsAccepted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE events (id int64 PRIMARY KEY, created datetime)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO events (id, created) VALUES (1, '2026-11-01T05:30:00Z'), (2, '2026-11-02T06:30:00Z')");
        await ExecDdl(executor, database, dbname, "CREATE INDEX events_day ON events ((date_trunc('day', created)))");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT id FROM events WHERE date_trunc('day', created) = '2026-11-01T00:00:00Z'");
        Assert.AreEqual(new long[] { 1 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task View_CanGroupByLocalDay()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE orders (id int64 PRIMARY KEY, created datetime)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO orders (id, created) VALUES (1, '2026-03-15T18:00:00Z'), (2, '2026-03-15T19:00:00Z')");
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW orders_by_india_day AS SELECT date_trunc('day', created, 'Asia/Kolkata') AS day, count(*) AS n " +
            "FROM orders GROUP BY date_trunc('day', created, 'Asia/Kolkata')");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT day, n FROM orders_by_india_day ORDER BY day");

        // 18:00Z is 23:30 of March 15 in India and 19:00Z is 00:30 of March 16.
        Assert.AreEqual(
            new[] { "2026-03-14T18:30:00.0000000Z:1", "2026-03-15T18:30:00.0000000Z:1" },
            rows.Select(r => r.Row["day"].IsoValue + ":" + r.Row["n"].LongValue).ToArray());
    }
}
