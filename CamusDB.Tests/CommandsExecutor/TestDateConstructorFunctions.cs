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
/// The functions that build a date from its parts (<c>make_date</c>, <c>make_timestamp</c>,
/// <c>makedate</c>, <c>last_day</c>) and the millisecond epoch <c>unix_timestamp_ms</c>. Every case
/// goes through SQL, so the parser, the registry and the evaluator are all on the path. The parts
/// sit on the edges that break a naive rule: month 13, February 30, February 29 in a leap year and
/// in a common year, a fractional second that has no exact binary form, and dates before 1970.
/// </summary>
[NonParallelizable]
public sealed class TestDateConstructorFunctions : SharedNodeBaseTest
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

    private static async Task AssertDate(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, string expected)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.Date, value.Type, expression);
        Assert.AreEqual(expected, value.IsoValue, expression);
    }

    private static async Task AssertDateTime(
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

    private static async Task AssertTrue(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.Bool, value.Type, expression);
        Assert.IsTrue(value.BoolValue, expression);
    }

    private static async Task AssertNull(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression)
    {
        ColumnValue value = await Scalar(executor, database, dbname, expression);
        Assert.AreEqual(ColumnType.Null, value.Type, expression);
    }

    private static async Task AssertInvalid(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string expression, string messagePart)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: "SELECT " + expression, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, expression);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code, expression);
        Assert.That(ex.Message, Does.Contain(messagePart), expression);
    }

    // ── make_date ───────────────────────────────────────────────────────────────

    [Test]
    public async Task MakeDate_ValidParts_GiveADate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDate(executor, database, dbname, "make_date(2026, 10, 14)", "2026-10-14");
        await AssertDate(executor, database, dbname, "make_date(1, 1, 1)", "0001-01-01");
        await AssertDate(executor, database, dbname, "make_date(9999, 12, 31)", "9999-12-31");
        await AssertDate(executor, database, dbname, "MAKE_DATE(1969, 12, 31)", "1969-12-31");
    }

    [Test]
    public async Task MakeDate_February29_OnlyInALeapYear()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDate(executor, database, dbname, "make_date(2024, 2, 29)", "2024-02-29");
        await AssertDate(executor, database, dbname, "make_date(2000, 2, 29)", "2000-02-29");
        await AssertInvalid(executor, database, dbname, "make_date(2023, 2, 29)", "day 29 is out of range 1-28 for 2023-02");

        // A century year is a leap year only when it divides by 400.
        await AssertInvalid(executor, database, dbname, "make_date(1900, 2, 29)", "day 29 is out of range 1-28 for 1900-02");
    }

    [Test]
    public async Task MakeDate_InvalidParts_NameThePart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertInvalid(executor, database, dbname, "make_date(2026, 13, 1)", "month 13 is out of range 1-12");
        await AssertInvalid(executor, database, dbname, "make_date(2026, 0, 1)", "month 0 is out of range 1-12");
        await AssertInvalid(executor, database, dbname, "make_date(2026, 2, 30)", "day 30 is out of range 1-28 for 2026-02");
        await AssertInvalid(executor, database, dbname, "make_date(2026, 4, 31)", "day 31 is out of range 1-30 for 2026-04");
        await AssertInvalid(executor, database, dbname, "make_date(2026, 1, 0)", "day 0 is out of range 1-31");
        await AssertInvalid(executor, database, dbname, "make_date(0, 1, 1)", "year 0 is out of range 1-9999");
        await AssertInvalid(executor, database, dbname, "make_date(10000, 1, 1)", "year 10000 is out of range 1-9999");
        await AssertInvalid(executor, database, dbname, "make_date(-44, 3, 15)", "year -44 is out of range 1-9999");
        await AssertInvalid(executor, database, dbname, "make_date('2026', 1, 1)", "argument 1 of type Integer64");
    }

    [Test]
    public async Task MakeDate_NullPart_GivesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertNull(executor, database, dbname, "make_date(NULL, 1, 1)");
        await AssertNull(executor, database, dbname, "make_date(2026, NULL, 1)");
        await AssertNull(executor, database, dbname, "make_date(2026, 1, NULL)");

        // NULL wins over an invalid part, as in PostgreSQL's strict functions.
        await AssertNull(executor, database, dbname, "make_date(2026, 13, NULL)");
    }

    [Test]
    public async Task MakeDate_ComparesWithADateColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE days (id int64 PRIMARY KEY, day date)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO days (id, day) VALUES (1, '2024-02-28'), (2, '2024-02-29'), (3, '2024-03-01')");

        // The result is typed date, so it inserts without CAST and compares with the column.
        await ExecNonQuery(executor, database, dbname, "INSERT INTO days (id, day) VALUES (4, make_date(2024, 3, 2))");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT id FROM days WHERE day >= make_date(2024, 2, 29) ORDER BY id");

        Assert.That(rows.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(new long[] { 2, 3, 4 }));
    }

    // ── make_timestamp ──────────────────────────────────────────────────────────

    [Test]
    public async Task MakeTimestamp_ValidParts_GiveAUtcDateTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 10, 14, 13, 45, 30)", "2026-10-14T13:45:30.0000000Z");
        await AssertDateTime(executor, database, dbname, "make_timestamp(2024, 2, 29, 0, 0, 0)", "2024-02-29T00:00:00.0000000Z");
        await AssertDateTime(executor, database, dbname, "make_timestamp(9999, 12, 31, 23, 59, 59)", "9999-12-31T23:59:59.0000000Z");
    }

    [Test]
    public async Task MakeTimestamp_FractionalSecond_RoundsToTheMicrosecond()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 30.5)", "2026-01-01T00:00:30.5000000Z");

        // 0.123 has no exact binary form; a tick-precision conversion would give .1229999.
        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 0.123)", "2026-01-01T00:00:00.1230000Z");
        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 1.0000004)", "2026-01-01T00:00:01.0000000Z");
        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 1.0000005)", "2026-01-01T00:00:01.0000010Z");

        // A NUMERIC second is exact before rounding.
        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, NUMERIC '12.3456785')", "2026-01-01T00:00:12.3456790Z");
        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, NUMERIC '12.3456784')", "2026-01-01T00:00:12.3456780Z");
    }

    [Test]
    public async Task MakeTimestamp_SecondThatRoundsTo60_CarriesIntoTheNextMinute()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDateTime(executor, database, dbname, "make_timestamp(2026, 12, 31, 23, 59, 59.9999999)", "2027-01-01T00:00:00.0000000Z");
        await AssertInvalid(executor, database, dbname, "make_timestamp(9999, 12, 31, 23, 59, 59.9999999)", "result is after year 9999");
    }

    [Test]
    public async Task MakeTimestamp_InvalidParts_NameThePart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 13, 1, 0, 0, 0)", "month 13 is out of range 1-12");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 2, 30, 0, 0, 0)", "day 30 is out of range 1-28 for 2026-02");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2023, 2, 29, 0, 0, 0)", "day 29 is out of range 1-28 for 2023-02");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 24, 0, 0)", "hour 24 is out of range 0-23");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, -1, 0, 0)", "hour -1 is out of range 0-23");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 60, 0)", "minute 60 is out of range 0-59");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 60)", "second 60 is out of range");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, 60.0)", "second 60 is out of range");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, -0.5)", "second -0.5 is out of range");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, NUMERIC '60')", "second 60 is out of range");
        await AssertInvalid(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, '30')", "argument 6 of type Integer64, Float64 or Numeric");
        await AssertInvalid(executor, database, dbname, "make_timestamp(0, 1, 1, 0, 0, 0)", "year 0 is out of range 1-9999");
    }

    [Test]
    public async Task MakeTimestamp_NullPart_GivesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertNull(executor, database, dbname, "make_timestamp(NULL, 1, 1, 0, 0, 0)");
        await AssertNull(executor, database, dbname, "make_timestamp(2026, 1, 1, NULL, 0, 0)");
        await AssertNull(executor, database, dbname, "make_timestamp(2026, 1, 1, 0, 0, NULL)");
    }

    // ── makedate ────────────────────────────────────────────────────────────────

    [Test]
    public async Task MakeDateFromDayOfYear_CountsFromJanuaryFirst()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDate(executor, database, dbname, "makedate(2026, 1)", "2026-01-01");
        await AssertDate(executor, database, dbname, "makedate(2026, 32)", "2026-02-01");
        await AssertDate(executor, database, dbname, "makedate(2026, 365)", "2026-12-31");

        // Day 60 is February 29 in a leap year and March 1 in a common year.
        await AssertDate(executor, database, dbname, "makedate(2024, 60)", "2024-02-29");
        await AssertDate(executor, database, dbname, "makedate(2023, 60)", "2023-03-01");
        await AssertDate(executor, database, dbname, "makedate(2024, 366)", "2024-12-31");
    }

    [Test]
    public async Task MakeDateFromDayOfYear_PastTheEndOfTheYear_CarriesAsMySqlDoes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // MySQL: MAKEDATE(2011, 366) = 2012-01-01.
        await AssertDate(executor, database, dbname, "makedate(2011, 366)", "2012-01-01");
        await AssertDate(executor, database, dbname, "makedate(2026, 730)", "2027-12-31");
        await AssertDate(executor, database, dbname, "makedate(9999, 365)", "9999-12-31");
    }

    [Test]
    public async Task MakeDateFromDayOfYear_InvalidParts_NameThePart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertInvalid(executor, database, dbname, "makedate(2026, 0)", "day of year 0 is out of range 1-");
        await AssertInvalid(executor, database, dbname, "makedate(2026, -5)", "day of year -5 is out of range 1-");
        await AssertInvalid(executor, database, dbname, "makedate(9999, 366)", "day of year 366 is out of range 1-365 for year 9999");
        await AssertInvalid(executor, database, dbname, "makedate(2026, 9223372036854775807)", "day of year 9223372036854775807 is out of range");
        await AssertInvalid(executor, database, dbname, "makedate(0, 1)", "year 0 is out of range 1-9999");
        await AssertInvalid(executor, database, dbname, "makedate(10000, 1)", "year 10000 is out of range 1-9999");
    }

    [Test]
    public async Task MakeDateFromDayOfYear_NullPart_GivesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertNull(executor, database, dbname, "makedate(NULL, 1)");
        await AssertNull(executor, database, dbname, "makedate(2026, NULL)");
    }

    // ── last_day ────────────────────────────────────────────────────────────────

    [Test]
    public async Task LastDay_GivesTheLastDayOfTheMonth()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDate(executor, database, dbname, "last_day('2026-01-15')", "2026-01-31");
        await AssertDate(executor, database, dbname, "last_day('2026-04-01')", "2026-04-30");
        await AssertDate(executor, database, dbname, "last_day('2026-12-31')", "2026-12-31");
        await AssertDate(executor, database, dbname, "last_day('9999-12-01')", "9999-12-31");

        // A datetime argument: the time of day goes away.
        await AssertDate(executor, database, dbname, "last_day(make_timestamp(2026, 6, 10, 23, 59, 59.5))", "2026-06-30");
        await AssertDate(executor, database, dbname, "last_day('2026-06-10T23:30:00Z')", "2026-06-30");

        // An offset moves the value into another UTC month before the month is read.
        await AssertDate(executor, database, dbname, "last_day('2026-06-30T23:30:00-02:00')", "2026-07-31");
    }

    [Test]
    public async Task LastDay_February_DependsOnTheLeapYear()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertDate(executor, database, dbname, "last_day('2024-02-10')", "2024-02-29");
        await AssertDate(executor, database, dbname, "last_day(make_date(2024, 2, 29))", "2024-02-29");
        await AssertDate(executor, database, dbname, "last_day('2023-02-10')", "2023-02-28");
        await AssertDate(executor, database, dbname, "last_day('2000-02-01')", "2000-02-29");
        await AssertDate(executor, database, dbname, "last_day('1900-02-01')", "1900-02-28");
    }

    [Test]
    public async Task LastDay_ReadsDateAndDateTimeColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE events (id int64 PRIMARY KEY, created datetime, day date)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO events (id, created, day) VALUES " +
            "(1, '2024-02-03T10:00:00Z', '2023-02-03'), " +
            "(2, '2026-11-30T23:59:59Z', '2026-11-01'), " +
            "(3, NULL, NULL)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT id, last_day(created) AS c, last_day(day) AS d FROM events ORDER BY id");

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual("2024-02-29", rows[0].Row["c"].IsoValue);
        Assert.AreEqual("2023-02-28", rows[0].Row["d"].IsoValue);
        Assert.AreEqual(ColumnType.Date, rows[0].Row["c"].Type);
        Assert.AreEqual("2026-11-30", rows[1].Row["c"].IsoValue);
        Assert.AreEqual("2026-11-30", rows[1].Row["d"].IsoValue);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["c"].Type);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["d"].Type);

        // The month-end rows of a monthly report.
        List<QueryResultRow> monthEnd = await ExecQuery(executor, database, dbname,
            "SELECT id FROM events WHERE CAST(created AS date) = last_day(created)");
        Assert.That(monthEnd.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(new long[] { 2 }));
    }

    [Test]
    public async Task LastDay_InvalidAndNullArguments()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertNull(executor, database, dbname, "last_day(NULL)");
        await AssertInvalid(executor, database, dbname, "last_day('2026-13-01')", "could not parse date/time value '2026-13-01'");
        await AssertInvalid(executor, database, dbname, "last_day('2026-02-30')", "could not parse date/time value '2026-02-30'");
        await AssertInvalid(executor, database, dbname, "last_day(20260215)", "expects a date/datetime value");
    }

    // ── deterministic use ───────────────────────────────────────────────────────

    [Test]
    public async Task Constructors_AreDeterministic_InCheckAndDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE bookings (id int64 PRIMARY KEY, " +
            "day date DEFAULT (make_date(2026, 1, 1)) CHECK (day >= make_date(2020, 1, 1)), " +
            "starts datetime DEFAULT (make_timestamp(2026, 1, 1, 9, 30, 0)), " +
            "closes date DEFAULT (last_day(makedate(2026, 32))))");

        await ExecNonQuery(executor, database, dbname, "INSERT INTO bookings (id) VALUES (1)");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO bookings (id, day) VALUES (2, make_date(2024, 2, 29))");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT id, day, starts, closes FROM bookings ORDER BY id");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("2026-01-01", rows[0].Row["day"].IsoValue);
        Assert.AreEqual("2026-01-01T09:30:00.0000000Z", rows[0].Row["starts"].IsoValue);
        Assert.AreEqual("2026-02-28", rows[0].Row["closes"].IsoValue);
        Assert.AreEqual("2024-02-29", rows[1].Row["day"].IsoValue);

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
            sql: "INSERT INTO bookings (id, day) VALUES (3, make_date(2019, 12, 31))", parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);
    }

    // ── unix_timestamp_ms ───────────────────────────────────────────────────────

    [Test]
    public async Task UnixTimestampMs_OfAValue_IsWholeMillisecondsRoundedDown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, database, dbname, "unix_timestamp_ms('1970-01-01T00:00:00Z')", 0);
        await AssertLong(executor, database, dbname, "unix_timestamp_ms('2026-10-14T12:00:00.1239999Z')", 1_791_979_200_123);
        await AssertLong(executor, database, dbname, "unix_timestamp_ms(make_date(2026, 10, 14))", 1_791_936_000_000);

        // Before 1970 the value rounds down, away from zero, as date_trunc('millisecond') does.
        await AssertLong(executor, database, dbname, "unix_timestamp_ms('1969-12-31T23:59:59.9995Z')", -1);
        await AssertLong(executor, database, dbname, "unix_timestamp_ms('1969-12-31T23:59:59.999Z')", -1);
        await AssertLong(executor, database, dbname, "unix_timestamp_ms('1969-12-31T23:59:59.998Z')", -2);

        await AssertNull(executor, database, dbname, "unix_timestamp_ms(NULL)");
        await AssertInvalid(executor, database, dbname, "unix_timestamp_ms(42)", "expects a date/datetime value");
    }

    [Test]
    public async Task UnixTimestampMs_RoundTripsThroughFromUnixtime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE ticks (id int64 PRIMARY KEY, t datetime)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO ticks (id, t) VALUES " +
            "(1, '2026-10-14T12:34:56.7891234Z'), " +
            "(2, '2026-10-14T12:34:56.0010000Z'), " +
            "(3, '2001-09-09T01:46:40.1230000Z'), " +
            "(4, '1969-07-20T20:17:40.9999999Z'), " +
            "(5, '1970-01-01T00:00:00.0000001Z'), " +
            "(6, '2200-12-31T23:59:59.9999999Z')");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT id, from_unixtime(unix_timestamp_ms(t) / 1000.0) = date_trunc('millisecond', t) AS same, " +
            "from_unixtime(unix_timestamp_ms(t) / 1000.0) AS back, date_trunc('millisecond', t) AS truncated " +
            "FROM ticks ORDER BY id");

        Assert.AreEqual(6, rows.Count);

        foreach (QueryResultRow row in rows)
            Assert.IsTrue(
                row.Row["same"].BoolValue,
                $"row {row.Row["id"].LongValue}: {row.Row["back"].IsoValue} <> {row.Row["truncated"].IsoValue}");

        // A float cannot hold each microsecond after about 2255; a NUMERIC divisor stays exact there.
        await ExecNonQuery(executor, database, dbname, "INSERT INTO ticks (id, t) VALUES (7, '9999-12-31T23:59:59.9999999Z')");

        List<QueryResultRow> late = await ExecQuery(executor, database, dbname,
            "SELECT from_unixtime(unix_timestamp_ms(t) / NUMERIC '1000') = date_trunc('millisecond', t) AS same " +
            "FROM ticks WHERE id = 7");

        Assert.AreEqual(1, late.Count);
        Assert.IsTrue(late[0].Row["same"].BoolValue);
    }

    [Test]
    public async Task FromUnixtime_Fraction_RoundsToTheMicrosecond()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 1.001 as a float is 1.000999999...; a tick conversion would give .0009999.
        await AssertDateTime(executor, database, dbname, "from_unixtime(1.001)", "1970-01-01T00:00:01.0010000Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(1791979200123 / 1000.0)", "2026-10-14T12:00:00.1230000Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(0.0000004)", "1970-01-01T00:00:00.0000000Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(-0.0000005)", "1969-12-31T23:59:59.9999990Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(-1)", "1969-12-31T23:59:59.0000000Z");

        // A NUMERIC argument is exact before rounding, also where a float cannot hold a microsecond.
        await AssertDateTime(executor, database, dbname, "from_unixtime(NUMERIC '253402300799.999')", "9999-12-31T23:59:59.9990000Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(NUMERIC '1.0000015')", "1970-01-01T00:00:01.0000020Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(NUMERIC '-1.0000015')", "1969-12-31T23:59:58.9999980Z");

        // The limits of a datetime.
        await AssertDateTime(executor, database, dbname, "from_unixtime(253402300799)", "9999-12-31T23:59:59.0000000Z");
        await AssertDateTime(executor, database, dbname, "from_unixtime(-62135596800)", "0001-01-01T00:00:00.0000000Z");
        await AssertInvalid(executor, database, dbname, "from_unixtime(253402300800)", "unix timestamp out of range: 253402300800");
        await AssertInvalid(executor, database, dbname, "from_unixtime(-62135596801)", "unix timestamp out of range: -62135596801");
        await AssertInvalid(executor, database, dbname, "from_unixtime(1e300)", "unix timestamp out of range");
        await AssertInvalid(executor, database, dbname, "from_unixtime(NUMERIC '253402300800')", "unix timestamp out of range: 253402300800");
    }

    [Test]
    public async Task UnixTimestampMs_WithNoArgument_IsTheStartOfTheTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await AssertTrue(executor, database, dbname, "unix_timestamp_ms() = unix_timestamp_ms(now())");
        await AssertTrue(executor, database, dbname, "from_unixtime(unix_timestamp_ms() / 1000.0) = date_trunc('millisecond', now())");
        await AssertTrue(executor, database, dbname, "unix_timestamp_ms() >= unix_timestamp() * 1000");
        await AssertTrue(executor, database, dbname, "unix_timestamp_ms() < (unix_timestamp() + 1) * 1000");

        // Two statements of one transaction see one value.
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            long first = await ScalarIn(executor, tx, dbname, "SELECT unix_timestamp_ms()");
            await Task.Delay(20);
            long second = await ScalarIn(executor, tx, dbname, "SELECT unix_timestamp_ms()");
            Assert.AreEqual(first, second);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> ScalarIn(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count, sql);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["0"].Type, sql);
        return rows[0].Row["0"].LongValue;
    }
}
