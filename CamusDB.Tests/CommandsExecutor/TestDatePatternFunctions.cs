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
/// End-to-end tests for <c>to_char</c>, <c>to_timestamp</c>, the two-argument <c>to_date</c>,
/// <c>date_format</c> and <c>str_to_date</c> through SQL: in a projection, a <c>WHERE</c>, an
/// <c>INSERT ... VALUES</c>, a view body, a column <c>DEFAULT</c> and a stored <c>CHECK</c>. The
/// pattern engine itself has unit tests in <see cref="TestDatePattern"/>.
/// </summary>
[NonParallelizable]
public sealed class TestDatePatternFunctions : SharedNodeBaseTest
{
    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, database.Name, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, database.Name, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, database.Name, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, DatabaseDescriptor database, string expression)
    {
        List<QueryResultRow> rows = await Query(executor, database, "SELECT " + expression);
        Assert.AreEqual(1, rows.Count, expression);
        return rows[0].Row["0"];
    }

    private static async Task<CamusDBException> AssertQueryThrows(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, database.Name, sql, null));
            await cursor.ToListAsync();
        })!;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<CamusDBException> AssertExecThrows(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, database.Name, sql, null)))!;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private async Task<(DatabaseDescriptor database, CommandExecutor executor)> SetupEvents()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, "CREATE TABLE events (id int64 PRIMARY KEY, created datetime, day date)");

        // 2024-06-16 is a Sunday and 2024-06-17 a Monday.
        await Exec(executor, database,
            "INSERT INTO events (id, created, day) VALUES " +
            "(1, '2024-06-16T10:30:45.123456Z', '2024-06-16'), " +
            "(2, '2024-06-17T20:00:00Z', '2024-06-17'), " +
            "(3, '2023-12-31T23:59:59Z', '2023-12-31')");

        return (database, executor);
    }

    [Test]
    public async Task ToChar_InAProjection()
    {
        (DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> rows = await Query(executor, database,
            "SELECT to_char(created, 'FMDay, DD Mon YYYY HH12:MI AM') AS label, to_char(day, 'IYYY-\"W\"IW') AS week " +
            "FROM events ORDER BY id");

        Assert.AreEqual(
            new[] { "Sunday, 16 Jun 2024 10:30 AM", "Monday, 17 Jun 2024 08:00 PM", "Sunday, 31 Dec 2023 11:59 PM" },
            rows.Select(r => r.Row["label"].StrValue).ToArray());

        Assert.AreEqual(ColumnType.String, rows[0].Row["label"].Type);
        Assert.AreEqual(new[] { "2024-W24", "2024-W25", "2023-W52" }, rows.Select(r => r.Row["week"].StrValue).ToArray());
    }

    [Test]
    public async Task DateFormat_InAProjection()
    {
        (DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> rows = await Query(executor, database,
            "SELECT date_format(created, '%W %M %e, %Y %H:%i:%s.%f') AS label FROM events ORDER BY id");

        Assert.AreEqual(
            new[] { "Sunday June 16, 2024 10:30:45.123456", "Monday June 17, 2024 20:00:00.000000", "Sunday December 31, 2023 23:59:59.000000" },
            rows.Select(r => r.Row["label"].StrValue).ToArray());
    }

    [Test]
    public async Task PatternFunctions_InAWhere()
    {
        (DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> sundays = await Query(executor, database,
            "SELECT id FROM events WHERE to_char(day, 'Dy') = 'Sun' ORDER BY id");
        Assert.AreEqual(new[] { 1L, 3L }, sundays.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> exact = await Query(executor, database,
            "SELECT id FROM events WHERE created = to_timestamp('17/06/2024 08:00 PM', 'DD/MM/YYYY HH12:MI AM')");
        Assert.AreEqual(new[] { 2L }, exact.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> mysql = await Query(executor, database,
            "SELECT id FROM events WHERE created < str_to_date('June 17, 2024', '%M %e, %Y') ORDER BY id");
        Assert.AreEqual(new[] { 1L, 3L }, mysql.Select(r => r.Row["id"].LongValue).ToArray());

        // str_to_date always returns datetime, so a date column compares with it through a cast.
        List<QueryResultRow> mysqlDate = await Query(executor, database,
            "SELECT id FROM events WHERE day = CAST(str_to_date('June 17, 2024', '%M %e, %Y') AS date)");
        Assert.AreEqual(new[] { 2L }, mysqlDate.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> byDate = await Query(executor, database,
            "SELECT id FROM events WHERE day = to_date('12/31/2023', 'MM/DD/YYYY')");
        Assert.AreEqual(new[] { 3L }, byDate.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task ToDate_InAnInsertValues()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, database, "CREATE TABLE imports (id int64 PRIMARY KEY, day date, at datetime)");

        await Exec(executor, database,
            "INSERT INTO imports (id, day, at) VALUES " +
            "(1, to_date('03/15/2026', 'MM/DD/YYYY'), to_timestamp('15-Mar-2026 14:05', 'DD-Mon-YYYY HH24:MI')), " +
            "(2, to_date('7/4/2026', 'MM/DD/YYYY'), str_to_date('04/07/2026 9:30 PM', '%d/%m/%Y %h:%i %p'))");

        List<QueryResultRow> rows = await Query(executor, database, "SELECT day, at FROM imports ORDER BY id");

        Assert.AreEqual(ColumnType.Date, rows[0].Row["day"].Type);
        Assert.AreEqual("2026-03-15", rows[0].Row["day"].IsoValue);
        Assert.AreEqual("2026-03-15T14:05:00.0000000Z", rows[0].Row["at"].IsoValue);
        Assert.AreEqual("2026-07-04", rows[1].Row["day"].IsoValue);
        Assert.AreEqual("2026-07-04T21:30:00.0000000Z", rows[1].Row["at"].IsoValue);
    }

    [Test]
    public async Task ToChar_InAViewBody()
    {
        (DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        await ExecDdl(executor, database,
            "CREATE VIEW monthly AS SELECT id, to_char(day, 'FMMonth YYYY') AS label FROM events");

        List<QueryResultRow> rows = await Query(executor, database, "SELECT label FROM monthly ORDER BY id");
        Assert.AreEqual(new[] { "June 2024", "June 2024", "December 2023" }, rows.Select(r => r.Row["label"].StrValue).ToArray());

        List<QueryResultRow> filtered = await Query(executor, database, "SELECT id FROM monthly WHERE label = 'December 2023'");
        Assert.AreEqual(new[] { 3L }, filtered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task PatternFunctions_InAStoredCheckAndADefault()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database,
            "CREATE TABLE shifts (id int64 PRIMARY KEY, day date, " +
            "opened date DEFAULT (to_date('01/02/2026', 'MM/DD/YYYY')), " +
            "CONSTRAINT weekdays_only CHECK (to_char(day, 'ID') <> '6' AND to_char(day, 'ID') <> '7'))");

        await Exec(executor, database, "INSERT INTO shifts (id, day) VALUES (1, '2026-03-16')");

        CamusDBException ex = await AssertExecThrows(executor, database, "INSERT INTO shifts (id, day) VALUES (2, '2026-03-15')");
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        List<QueryResultRow> rows = await Query(executor, database, "SELECT id, opened FROM shifts");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("2026-01-02", rows[0].Row["opened"].IsoValue);
    }

    [Test]
    public async Task ToTimestamp_ReadsBackWhatToCharPrinted()
    {
        (DatabaseDescriptor database, CommandExecutor executor) = await SetupEvents();

        const string pattern = "YYYY-MM-DD HH24:MI:SS.US";
        List<QueryResultRow> rows = await Query(executor, database,
            $"SELECT id FROM events WHERE to_timestamp(to_char(created, '{pattern}'), '{pattern}') = created ORDER BY id");
        Assert.AreEqual(new[] { 1L, 2L, 3L }, rows.Select(r => r.Row["id"].LongValue).ToArray());

        const string mysqlPattern = "%Y-%m-%d %H:%i:%s.%f";
        List<QueryResultRow> mysqlRows = await Query(executor, database,
            $"SELECT id FROM events WHERE str_to_date(date_format(created, '{mysqlPattern}'), '{mysqlPattern}') = created ORDER BY id");
        Assert.AreEqual(new[] { 1L, 2L, 3L }, mysqlRows.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task OneArgumentForms_KeepTheirMeaning()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // to_date(x) is still the cast to date.
        ColumnValue cast = await Scalar(executor, database, "to_date('2026-03-15')");
        Assert.AreEqual(ColumnType.Date, cast.Type);
        Assert.AreEqual("2026-03-15", cast.IsoValue);

        // to_timestamp(number) is from_unixtime(number).
        ColumnValue fromNumber = await Scalar(executor, database, "to_timestamp(1773532800)");
        ColumnValue fromUnixtime = await Scalar(executor, database, "from_unixtime(1773532800)");
        Assert.AreEqual(ColumnType.DateTime, fromNumber.Type);
        Assert.AreEqual(fromUnixtime.LongValue, fromNumber.LongValue);
        Assert.AreEqual("2026-03-15T00:00:00.0000000Z", fromNumber.IsoValue);

        ColumnValue fraction = await Scalar(executor, database, "to_timestamp(1773532800.5)");
        Assert.AreEqual("2026-03-15T00:00:00.5000000Z", fraction.IsoValue);
    }

    [Test]
    public async Task ResultTypes_AreFixedByTheFunction()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // str_to_date returns datetime even when the pattern has no time fields.
        ColumnValue mysqlDate = await Scalar(executor, database, "str_to_date('2026-03-15', '%Y-%m-%d')");
        Assert.AreEqual(ColumnType.DateTime, mysqlDate.Type);
        Assert.AreEqual("2026-03-15T00:00:00.0000000Z", mysqlDate.IsoValue);

        // to_date keeps the date only, after it reads and checks the time fields.
        ColumnValue dateOnly = await Scalar(executor, database, "to_date('2026-03-15 23:59', 'YYYY-MM-DD HH24:MI')");
        Assert.AreEqual(ColumnType.Date, dateOnly.Type);
        Assert.AreEqual("2026-03-15", dateOnly.IsoValue);

        ColumnValue text = await Scalar(executor, database, "to_char(CAST('2026-03-15' AS date), 'YYYY')");
        Assert.AreEqual(ColumnType.String, text.Type);
        Assert.AreEqual("2026", text.StrValue);

        // A string argument is read with the same ISO rule as the other date functions.
        ColumnValue fromString = await Scalar(executor, database, "date_format('2026-03-15T10:00:00Z', '%H')");
        Assert.AreEqual("10", fromString.StrValue);
    }

    [Test]
    public async Task NullArguments_GiveNull()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string expression in new[]
                 {
                     "to_char(NULL, 'YYYY')", "to_char(CAST('2026-03-15' AS date), NULL)",
                     "to_timestamp(NULL, 'YYYY')", "to_timestamp('2026', NULL)", "to_timestamp(NULL)",
                     "to_date(NULL, 'YYYY')", "to_date(NULL)",
                     "date_format(NULL, '%Y')", "str_to_date(NULL, '%Y')", "str_to_date('2026', NULL)",
                 })
        {
            ColumnValue value = await Scalar(executor, database, expression);
            Assert.AreEqual(ColumnType.Null, value.Type, expression);
        }
    }

    [Test]
    public async Task BadInput_GivesAnErrorThatNamesTheTextAndThePattern()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException parse = await AssertQueryThrows(executor, database, "SELECT to_date('02/30/2026', 'MM/DD/YYYY')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, parse.Code);
        StringAssert.Contains("Function 'to_date' could not parse '02/30/2026' with pattern 'MM/DD/YYYY'", parse.Message);
        StringAssert.Contains("day 30 is out of range for February 2026", parse.Message);

        CamusDBException mysql = await AssertQueryThrows(executor, database, "SELECT str_to_date('2026-03-15', '%Y/%m/%d')");
        StringAssert.Contains("Function 'str_to_date' could not parse '2026-03-15' with pattern '%Y/%m/%d'", mysql.Message);

        CamusDBException pattern = await AssertQueryThrows(executor, database, "SELECT date_format(CAST('2026-03-15' AS date), '%Y-%q')");
        StringAssert.Contains("Function 'date_format' could not use pattern '%Y-%q': unknown specifier '%q'", pattern.Message);

        CamusDBException notADate = await AssertQueryThrows(executor, database, "SELECT to_char(123, '999')");
        StringAssert.Contains("expects a date/datetime value", notADate.Message);

        CamusDBException notText = await AssertQueryThrows(executor, database, "SELECT to_timestamp(20260315, 'YYYYMMDD')");
        StringAssert.Contains("Function 'to_timestamp' expects argument 1 of type String", notText.Message);

        CamusDBException arity = await AssertQueryThrows(executor, database, "SELECT to_char(CAST('2026-03-15' AS date))");
        StringAssert.Contains("Function 'to_char' expects 2 argument(s) but received 1", arity.Message);
    }
}
