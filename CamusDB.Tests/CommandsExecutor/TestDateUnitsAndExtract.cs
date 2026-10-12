
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The date units beyond year..millisecond (week, quarter, dow, isodow, doy, isoyear, epoch,
/// microsecond, decade, century, millennium), the <c>EXTRACT(field FROM x)</c> form, and the MySQL
/// one-argument shortcuts. Every case goes through SQL, so the parser, the function registry and the
/// evaluator are all on the path. The dates sit on the edges that break a naive rule: ISO week 53,
/// the ISO year that differs from the calendar year, leap days, quarter limits and Sundays.
/// </summary>
[NonParallelizable]
public sealed class TestDateUnitsAndExtract : SharedNodeBaseTest
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
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
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

    private static async Task AssertLong(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, long expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.Integer64, value.Type, expression);
        Assert.AreEqual(expected, value.LongValue, expression);
    }

    private static async Task AssertIso(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, string expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.DateTime, value.Type, expression);
        Assert.AreEqual(expected, value.IsoValue, expression);
    }

    private static async Task<CamusDBException> AssertThrows(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupEvents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE events (id int64 PRIMARY KEY, created datetime, day date)");

        // 2024-06-16 is a Sunday, 2024-06-17 a Monday, 2023-12-31 a Sunday in ISO week 52.
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO events (id, created, day) VALUES " +
            "(1, '2024-06-16T10:30:45Z', '2024-06-16'), " +
            "(2, '2024-06-17T08:00:00Z', '2024-06-17'), " +
            "(3, '2023-12-31T23:59:59Z', '2023-12-31')");

        return (dbname, database, executor);
    }

    // ── date_part: the new units ────────────────────────────────────────────────

    [Test]
    public async Task DatePart_IsoWeekAndIsoYear_AtYearEdges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 2020 has 53 ISO weeks; 2021-01-01 (a Friday) still belongs to week 53 of ISO year 2020.
        await AssertLong(executor, database, dbname, "date_part('week', '2020-12-31')", 53);
        await AssertLong(executor, database, dbname, "date_part('week', '2021-01-01')", 53);
        await AssertLong(executor, database, dbname, "date_part('isoyear', '2021-01-01')", 2020);
        await AssertLong(executor, database, dbname, "date_part('week', '2021-01-04')", 1);
        await AssertLong(executor, database, dbname, "date_part('isoyear', '2021-01-04')", 2021);

        // 2024-12-30 (a Monday) opens ISO week 1 of 2025.
        await AssertLong(executor, database, dbname, "date_part('weeks', '2024-12-30')", 1);
        await AssertLong(executor, database, dbname, "date_part('isoyear', '2024-12-30')", 2025);
        await AssertLong(executor, database, dbname, "date_part('year', '2024-12-30')", 2024);
    }

    [Test]
    public async Task DatePart_QuarterLimits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('quarter', '2024-01-01')", 1);
        await AssertLong(executor, database, dbname, "date_part('quarter', '2024-03-31T23:59:59Z')", 1);
        await AssertLong(executor, database, dbname, "date_part('quarter', '2024-04-01')", 2);
        await AssertLong(executor, database, dbname, "date_part('quarter', '2024-09-30')", 3);
        await AssertLong(executor, database, dbname, "date_part('quarter', '2024-12-31')", 4);
    }

    [Test]
    public async Task DatePart_DowAndIsoDow_OnSundayAndMonday()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('dow', '2024-06-16')", 0);
        await AssertLong(executor, database, dbname, "date_part('isodow', '2024-06-16')", 7);
        await AssertLong(executor, database, dbname, "date_part('dow', '2024-06-17')", 1);
        await AssertLong(executor, database, dbname, "date_part('isodow', '2024-06-17')", 1);
        await AssertLong(executor, database, dbname, "date_part('dow', '2024-06-15')", 6);
        await AssertLong(executor, database, dbname, "date_part('isodow', '2024-06-15')", 6);
    }

    [Test]
    public async Task DatePart_DayOfYear_LeapYear()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('doy', '2024-01-01')", 1);
        await AssertLong(executor, database, dbname, "date_part('doy', '2024-02-29')", 60);
        await AssertLong(executor, database, dbname, "date_part('doy', '2024-12-31')", 366);
        await AssertLong(executor, database, dbname, "date_part('doy', '2023-12-31')", 365);
    }

    [Test]
    public async Task DatePart_Epoch_WholeSecondsRoundedDown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('epoch', '1970-01-01T00:00:00Z')", 0);
        await AssertLong(executor, database, dbname, "date_part('epoch', '2024-06-15T10:30:45.999Z')", 1718447445);
        // Before 1970 the fraction rounds down too, so half a second before the epoch is -1.
        await AssertLong(executor, database, dbname, "date_part('epoch', '1969-12-31T23:59:59.5Z')", -1);
        await AssertLong(executor, database, dbname, "date_part('epoch', '2024-06-15')", 1718409600);
    }

    [Test]
    public async Task DatePart_SubSecondUnits_AreTheFractionOfTheSecond()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('millisecond', '2024-06-15T10:30:45.1234567Z')", 123);
        await AssertLong(executor, database, dbname, "date_part('microsecond', '2024-06-15T10:30:45.1234567Z')", 123456);
        await AssertLong(executor, database, dbname, "date_part('microseconds', '2024-06-15T10:30:45.000001Z')", 1);
        await AssertLong(executor, database, dbname, "date_part('us', '2024-06-15T10:30:45Z')", 0);
    }

    [Test]
    public async Task DatePart_DecadeCenturyMillennium_FollowPostgreSql()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_part('decade', '2024-06-15')", 202);
        await AssertLong(executor, database, dbname, "date_part('century', '2000-12-31')", 20);
        await AssertLong(executor, database, dbname, "date_part('century', '2001-01-01')", 21);
        await AssertLong(executor, database, dbname, "date_part('millennium', '2000-12-31')", 2);
        await AssertLong(executor, database, dbname, "date_part('millennium', '2001-01-01')", 3);
    }

    // ── date_trunc ──────────────────────────────────────────────────────────────

    [Test]
    public async Task DateTrunc_Week_StartsOnMonday()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // A Sunday belongs to the week that started the Monday before.
        await AssertIso(executor, database, dbname, "date_trunc('week', '2024-06-16T10:30:45Z')", "2024-06-10T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('week', '2024-06-17T10:30:45Z')", "2024-06-17T00:00:00.0000000Z");
        // The week crosses a year boundary: 2021-01-01 is a Friday.
        await AssertIso(executor, database, dbname, "date_trunc('week', '2021-01-01')", "2020-12-28T00:00:00.0000000Z");
        // ...and a leap day: 2024-03-03 is a Sunday.
        await AssertIso(executor, database, dbname, "date_trunc('week', '2024-03-03')", "2024-02-26T00:00:00.0000000Z");
    }

    [Test]
    public async Task DateTrunc_QuarterAndLongerUnits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertIso(executor, database, dbname, "date_trunc('quarter', '2024-05-20T10:00:00Z')", "2024-04-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('quarter', '2024-12-31T23:59:59Z')", "2024-10-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('quarter', '2024-03-31')", "2024-01-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('decade', '2024-06-15')", "2020-01-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('century', '2024-06-15')", "2001-01-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('century', '2000-06-15')", "1901-01-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('millennium', '2024-06-15')", "2001-01-01T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_trunc('microsecond', '2024-06-15T10:30:45.1234567Z')", "2024-06-15T10:30:45.1234560Z");
    }

    [Test]
    public async Task DateTrunc_Week_GroupsAWeeklyReport()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        // The Sunday 2024-06-16 belongs to the week of Monday 2024-06-10; the Monday after opens a new week.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT date_trunc('week', created) AS week, count(*) AS n FROM events " +
            "GROUP BY date_trunc('week', created) ORDER BY week");

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual("2023-12-25T00:00:00.0000000Z", rows[0].Row["week"].IsoValue);
        Assert.AreEqual("2024-06-10T00:00:00.0000000Z", rows[1].Row["week"].IsoValue);
        Assert.AreEqual("2024-06-17T00:00:00.0000000Z", rows[2].Row["week"].IsoValue);
        CollectionAssert.AreEqual(new long[] { 1, 1, 1 }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    public async Task DateTrunc_DecadeBeforeYearTen_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT date_trunc('decade', '0005-06-15')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("before year 1", ex.Message);
    }

    // ── date_add / date_diff ────────────────────────────────────────────────────

    [Test]
    public async Task DateAdd_NewUnits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertIso(executor, database, dbname, "date_add('2024-02-26', 1, 'week')", "2024-03-04T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-03-04', -2, 'weeks')", "2024-02-19T00:00:00.0000000Z");
        // A quarter is three months, so a day past the end of the target month clamps like a month does.
        await AssertIso(executor, database, dbname, "date_add('2024-11-30', 1, 'quarter')", "2025-02-28T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-01-31', 1, 'quarter')", "2024-04-30T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-02-29', 1, 'decade')", "2034-02-28T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-06-15', 1, 'century')", "2124-06-15T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-06-15', -1, 'millennium')", "1024-06-15T00:00:00.0000000Z");
        await AssertIso(executor, database, dbname, "date_add('2024-06-15T10:30:45Z', 1, 'microsecond')", "2024-06-15T10:30:45.0000010Z");
    }

    [Test]
    public async Task DateAdd_UnitOverflow_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT date_add('2024-06-15', 2000000000, 'quarter')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("overflow", ex.Message);
    }

    [Test]
    public async Task DateDiff_NewUnits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "date_diff('2024-01-01', '2024-01-14', 'week')", 1);
        await AssertLong(executor, database, dbname, "date_diff('2024-01-01', '2024-01-15', 'week')", 2);
        await AssertLong(executor, database, dbname, "date_diff('2024-01-15', '2024-01-01', 'week')", -2);
        await AssertLong(executor, database, dbname, "date_diff('2024-01-15', '2024-04-14', 'quarter')", 0);
        await AssertLong(executor, database, dbname, "date_diff('2024-01-15', '2024-04-15', 'quarter')", 1);
        await AssertLong(executor, database, dbname, "date_diff('2000-01-01', '2009-12-31', 'decade')", 0);
        await AssertLong(executor, database, dbname, "date_diff('2000-01-01', '2010-01-01', 'decade')", 1);
        await AssertLong(executor, database, dbname, "date_diff('1900-01-01', '2024-01-01', 'century')", 1);
        await AssertLong(executor, database, dbname, "date_diff('1000-01-01', '2024-01-01', 'millennium')", 1);
        await AssertLong(executor, database, dbname, "date_diff('2024-06-15T10:30:45Z', '2024-06-15T10:30:45.000123Z', 'microsecond')", 123);
    }

    [TestCase("date_trunc('dow', '2024-06-15')", "dow")]
    [TestCase("date_trunc('epoch', '2024-06-15')", "epoch")]
    [TestCase("date_add('2024-06-15', 1, 'isodow')", "isodow")]
    [TestCase("date_add('2024-06-15', 1, 'doy')", "doy")]
    [TestCase("date_diff('2024-06-15', '2024-06-16', 'isoyear')", "isoyear")]
    [TestCase("date_diff('2024-06-15', '2024-06-16', 'epoch')", "epoch")]
    [TestCase("date_part('fortnight', '2024-06-15')", "fortnight")]
    public async Task PositionOnlyUnits_AreRejectedOutsideDatePart(string expression, string unit)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT " + expression);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("expects a supported date/time unit but received '" + unit + "'", ex.Message);
    }

    // ── EXTRACT ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Extract_MatchesDatePart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string unit in new[] { "year", "quarter", "month", "week", "day", "dow", "isodow", "doy", "isoyear", "hour", "minute", "second", "millisecond", "microsecond", "epoch", "decade", "century", "millennium" })
        {
            const string value = "'2024-06-16T10:30:45.1234567Z'";
            ColumnValue viaExtract = await Scalar(executor, database, dbname, $"EXTRACT({unit} FROM {value})");
            ColumnValue viaDatePart = await Scalar(executor, database, dbname, $"date_part('{unit}', {value})");

            Assert.AreEqual(ColumnType.Integer64, viaExtract.Type, unit);
            Assert.AreEqual(viaDatePart.LongValue, viaExtract.LongValue, unit);
        }
    }

    [Test]
    public async Task Extract_AcceptsAnyCaseAndAQuotedField()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "extract(YEAR from '2024-06-16')", 2024);
        await AssertLong(executor, database, dbname, "Extract(Dow From '2024-06-16')", 0);
        await AssertLong(executor, database, dbname, "EXTRACT('quarter' FROM '2024-06-16')", 2);
        // The call form names the same function.
        await AssertLong(executor, database, dbname, "extract('month', '2024-06-16')", 6);
        // The source is a full expression.
        await AssertLong(executor, database, dbname, "EXTRACT(day FROM date_add('2024-02-28', 1, 'day'))", 29);
    }

    [Test]
    public async Task Extract_NullSource_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await Scalar(executor, database, dbname, "EXTRACT(year FROM NULL)");
        Assert.AreEqual(ColumnType.Null, value.Type);
    }

    [Test]
    public async Task Extract_UnknownField_ReportsTheField()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT EXTRACT(fortnight FROM '2024-06-16')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("received 'fortnight'", ex.Message);
    }

    [Test]
    public async Task FromInsideAnotherFunction_IsASyntaxError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT upper(year FROM '2024-06-16')");
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
        StringAssert.Contains("EXTRACT(field FROM value)", ex.Message);
    }

    [Test]
    public async Task Extract_InProjectionWhereGroupByAndOrderBy()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> filtered = await ExecQuery(executor, database, dbname,
            "SELECT id, EXTRACT(dow FROM created) AS dow FROM events WHERE EXTRACT(year FROM created) = 2024 ORDER BY id");
        Assert.AreEqual(2, filtered.Count);
        Assert.AreEqual(1L, filtered[0].Row["id"].LongValue);
        Assert.AreEqual(0L, filtered[0].Row["dow"].LongValue);
        Assert.AreEqual(2L, filtered[1].Row["id"].LongValue);
        Assert.AreEqual(1L, filtered[1].Row["dow"].LongValue);

        List<QueryResultRow> grouped = await ExecQuery(executor, database, dbname,
            "SELECT EXTRACT(year FROM created) AS y, count(*) AS n FROM events GROUP BY EXTRACT(year FROM created) ORDER BY y");
        Assert.AreEqual(2, grouped.Count);
        Assert.AreEqual(2023L, grouped[0].Row["y"].LongValue);
        Assert.AreEqual(1L, grouped[0].Row["n"].LongValue);
        Assert.AreEqual(2024L, grouped[1].Row["y"].LongValue);
        Assert.AreEqual(2L, grouped[1].Row["n"].LongValue);

        List<QueryResultRow> ordered = await ExecQuery(executor, database, dbname,
            "SELECT id FROM events ORDER BY EXTRACT(isodow FROM day), id");
        CollectionAssert.AreEqual(new long[] { 2, 1, 3 }, ordered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task Extract_ColumnAndTableNamedExtract_StillWork()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE extract (id int64 PRIMARY KEY, extract int64)");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO extract (id, extract) VALUES (1, 7)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT extract, EXTRACT(year FROM '2024-06-16') AS y FROM extract WHERE extract > 0");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(7L, rows[0].Row["extract"].LongValue);
        Assert.AreEqual(2024L, rows[0].Row["y"].LongValue);
    }

    [Test]
    public async Task Extract_ViewBody_RendersAsExtractAndReadsBack()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW sundays AS SELECT id, EXTRACT(year FROM created) AS y FROM events WHERE EXTRACT(dow FROM created) = 0");

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW sundays");
        string body = shown[0].Row["create view"].StrValue!;
        StringAssert.Contains("EXTRACT(year FROM created)", body);
        StringAssert.Contains("EXTRACT(dow FROM created)", body);

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT id, y FROM sundays ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 3 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
        CollectionAssert.AreEqual(new long[] { 2024, 2023 }, rows.Select(r => r.Row["y"].LongValue).ToArray());
    }

    [Test]
    public async Task Extract_FieldThatIsAKeyword_RendersQuotedInAView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        // 'from' is not a unit, but CREATE VIEW stores the body before any row is read. The stored text
        // must still parse, so the field is printed as a string literal and not as the keyword FROM.
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW odd AS SELECT EXTRACT('from' FROM created) AS v FROM events");

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW odd");
        StringAssert.Contains("EXTRACT('from' FROM created)", shown[0].Row["create view"].StrValue!);

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT v FROM odd");
        StringAssert.Contains("received 'from'", ex.Message);
    }

    [Test]
    public async Task Extract_CheckConstraint_IsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE shifts (id int64 PRIMARY KEY, starts datetime, CHECK (EXTRACT(isodow FROM starts) <= 5))");

        await ExecNonQuery(executor, database, dbname, "INSERT INTO shifts (id, starts) VALUES (1, '2024-06-17T08:00:00Z')");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, database, dbname, "INSERT INTO shifts (id, starts) VALUES (2, '2024-06-16T08:00:00Z')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [Test]
    public async Task Extract_Explain_RendersTheExtractForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "EXPLAIN SELECT id FROM events WHERE EXTRACT(year FROM created) = 2024");

        string plan = string.Join("\n", rows.Select(r => r.Row["detail"].StrValue));
        StringAssert.Contains("EXTRACT(year FROM created)", plan);
    }

    // ── MySQL shortcuts ─────────────────────────────────────────────────────────

    [Test]
    public async Task MySqlShortcuts_ReadOnePart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        const string value = "'2024-06-16T10:30:45Z'";
        await AssertLong(executor, database, dbname, $"year({value})", 2024);
        await AssertLong(executor, database, dbname, $"month({value})", 6);
        await AssertLong(executor, database, dbname, $"day({value})", 16);
        await AssertLong(executor, database, dbname, $"dayofmonth({value})", 16);
        await AssertLong(executor, database, dbname, $"hour({value})", 10);
        await AssertLong(executor, database, dbname, $"minute({value})", 30);
        await AssertLong(executor, database, dbname, $"second({value})", 45);
        await AssertLong(executor, database, dbname, $"quarter({value})", 2);
        await AssertLong(executor, database, dbname, "dayofyear('2024-02-29')", 60);
    }

    [Test]
    public async Task MySqlShortcuts_DayOfWeekCountsFromSunday_WeekdayFromMonday()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // MySQL: DAYOFWEEK is 1 = Sunday .. 7 = Saturday; WEEKDAY is 0 = Monday .. 6 = Sunday.
        await AssertLong(executor, database, dbname, "dayofweek('2024-06-16')", 1);
        await AssertLong(executor, database, dbname, "dayofweek('2024-06-15')", 7);
        await AssertLong(executor, database, dbname, "dayofweek('2024-06-17')", 2);
        await AssertLong(executor, database, dbname, "weekday('2024-06-16')", 6);
        await AssertLong(executor, database, dbname, "weekday('2024-06-17')", 0);
        await AssertLong(executor, database, dbname, "weekday('2024-06-15')", 5);
    }

    [Test]
    public async Task MySqlShortcuts_DayNameAndMonthName_AreEnglish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue dayName = await Scalar(executor, database, dbname, "dayname('2024-06-16')");
        ColumnValue monthName = await Scalar(executor, database, dbname, "monthname('2024-02-29')");

        Assert.AreEqual(ColumnType.String, dayName.Type);
        Assert.AreEqual("Sunday", dayName.StrValue);
        Assert.AreEqual(ColumnType.String, monthName.Type);
        Assert.AreEqual("February", monthName.StrValue);
    }

    [Test]
    public async Task MySqlWeek_Mode0StartsOnSunday_Mode3IsIso()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 2024-01-01 is a Monday: the days before the first Sunday (Jan 7) are week 0 in mode 0.
        await AssertLong(executor, database, dbname, "week('2024-01-01')", 0);
        await AssertLong(executor, database, dbname, "week('2024-01-06')", 0);
        await AssertLong(executor, database, dbname, "week('2024-01-07')", 1);
        await AssertLong(executor, database, dbname, "week('2024-12-31')", 52);
        // 2023-01-01 is a Sunday, so it opens week 1.
        await AssertLong(executor, database, dbname, "week('2023-01-01', 0)", 1);

        await AssertLong(executor, database, dbname, "week('2021-01-01', 3)", 53);
        await AssertLong(executor, database, dbname, "week('2024-01-01', 3)", 1);
    }

    [Test]
    public async Task MySqlWeek_OtherModes_AreRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT week('2024-01-01', 1)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("mode 0", ex.Message);
    }

    [Test]
    public async Task MySqlShortcuts_TypedColumnsAndNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT year(created) AS y, dayofweek(day) AS d, monthname(created) AS m FROM events WHERE id = 3");
        Assert.AreEqual(2023L, rows[0].Row["y"].LongValue);
        Assert.AreEqual(1L, rows[0].Row["d"].LongValue);
        Assert.AreEqual("December", rows[0].Row["m"].StrValue);

        foreach (string call in new[] { "year(NULL)", "week(NULL)", "week('2024-01-01', NULL)", "dayname(NULL)", "weekday(NULL)" })
        {
            ColumnValue value = await Scalar(executor, database, dbname, call);
            Assert.AreEqual(ColumnType.Null, value.Type, call);
        }
    }
}
