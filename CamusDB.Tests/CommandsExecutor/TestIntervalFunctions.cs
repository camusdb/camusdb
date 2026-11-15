/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
/// The PostgreSQL interval functions through SQL (<c>age</c>, <c>make_interval</c>, <c>justify_days</c>,
/// <c>justify_hours</c>, <c>justify_interval</c>, <c>date_bin</c>, and <c>EXTRACT</c> / <c>date_part</c>
/// from an interval), and the range scan for <c>col &gt; now() - INTERVAL '7 days'</c> on an indexed
/// column. Each expected value follows PostgreSQL, except where the class summary of the function says
/// that CamusDB differs.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIntervalFunctions : BaseTest
{
    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.String => value.StrValue!,
        ColumnType.DateTime => new DateTime(value.LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
        ColumnType.Interval => IntervalMath.FormatIso(value.IntervalParts),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    /// <summary>The rows of <paramref name="sql"/> (cells join with <c>|</c>, rows with <c> ; </c>), or the error code.</summary>
    private static async Task<string> Run(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, List<ColumnType>? types = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            foreach (QueryResultRow row in rows)
                types?.AddRange(row.Row.Values.Select(v => v.Type));

            return string.Join(" ; ", rows.Select(r => string.Join("|", r.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private async Task<string> Scalar(string expression, List<ColumnType>? types = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return await Run(database, executor, dbname, $"SELECT {expression}", types);
    }

    // ── age ──────────────────────────────────────────────────────────────────

    [TestCase("age(CAST('2001-04-10' AS date), CAST('1957-06-13' AS date))", "P43Y9M27D")]
    [TestCase("age(CAST('2024-03-01T00:00:00Z' AS datetime), CAST('2024-01-31T12:00:00Z' AS datetime))", "P1MT12H")]
    [TestCase("age(CAST('2024-01-31T12:00:00Z' AS datetime), CAST('2024-03-01T00:00:00Z' AS datetime))", "P-1MT-12H")]
    [TestCase("age(CAST('2024-03-01' AS date), CAST('2024-02-29' AS date))", "P1D")]
    [TestCase("age(CAST('2025-02-28' AS date), CAST('2024-02-29' AS date))", "P11M28D")]
    [TestCase("age(CAST('2024-02-29' AS date), CAST('2024-02-29' AS date))", "PT0S")]
    [TestCase("age(CAST('2024-01-01T00:00:01.5Z' AS datetime), CAST('2024-01-01T00:00:00.75Z' AS datetime))", "PT0.75S")]
    [TestCase("age(current_date())", "PT0S")]
    [TestCase("age(current_date() - 1)", "P1D")]
    [TestCase("age(NULL, CAST('2024-01-01' AS date))", "NULL")]
    public async Task Age_GivesTheSymbolicDifference(string expression, string expected)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(expected), expression);
    }

    [Test]
    public async Task Age_IsAnInterval()
    {
        List<ColumnType> types = [];
        await Scalar("age(CAST('2001-04-10' AS date), CAST('1957-06-13' AS date))", types);
        Assert.That(types, Is.EqualTo(new[] { ColumnType.Interval }));
    }

    // ── make_interval ────────────────────────────────────────────────────────

    [TestCase("make_interval()", "PT0S")]
    [TestCase("make_interval(1)", "P1Y")]
    [TestCase("make_interval(1, 2, 3, 4, 5, 6, 7.5)", "P1Y2M25DT5H6M7.5S")]
    [TestCase("make_interval(0, 0, 0, 0, 0, 0, 90)", "PT1M30S")]
    [TestCase("make_interval(0, -14)", "P-1Y-2M")]
    [TestCase("make_interval(0, 0, 0, 0, 0, 0, NULL)", "NULL")]
    public async Task MakeInterval_BuildsAnIntervalFromItsParts(string expression, string expected)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(expected), expression);
    }

    [TestCase("make_interval(2147483647, 1)")]
    [TestCase("make_interval(0, 0, 1, 2147483647)")]
    [TestCase("make_interval(1.5)")]
    [TestCase("make_interval('1')")]
    [TestCase("make_interval(0, 0, 0, 0, 0, 0, 10000000000000000.0)")]
    /// <remarks>PostgreSQL 16 wraps an overflow of a part without an error: <c>make_interval(2147483647, 1)</c>
    /// is <c>-11 mons</c> there. CamusDB refuses it, as it refuses every other integer overflow.</remarks>
    public async Task MakeInterval_RefusesAPartOutOfRangeOrOfTheWrongType(string expression)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    // ── justify_* ────────────────────────────────────────────────────────────

    [TestCase("justify_days(INTERVAL '35 days')", "P1M5D")]
    [TestCase("justify_days(INTERVAL '-35 days')", "P-1M-5D")]
    [TestCase("justify_days(INTERVAL '1 month -1 day')", "P29D")]
    [TestCase("justify_days(INTERVAL '30 days 50 hours')", "P1MT50H")]
    [TestCase("justify_hours(INTERVAL '27 hours')", "P1DT3H")]
    [TestCase("justify_hours(INTERVAL '1 day -1 hour')", "PT23H")]
    [TestCase("justify_hours(INTERVAL '-50 hours')", "P-2DT-2H")]
    [TestCase("justify_hours(INTERVAL '1 month 27 hours')", "P1M1DT3H")]
    [TestCase("justify_interval(INTERVAL '1 mon -1 hour')", "P29DT23H")]
    [TestCase("justify_interval(INTERVAL '-1 mon 1 hour')", "P-29DT-23H")]
    [TestCase("justify_interval(INTERVAL '40 days 50 hours')", "P1M12DT2H")]
    [TestCase("justify_interval(INTERVAL '0' SECOND)", "PT0S")]
    public async Task Justify_MovesWholeDaysAndMonthsUp(string expression, string expected)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(expected), expression);
    }

    [TestCase("justify_days(1)")]
    [TestCase("justify_hours('1 day')")]
    public async Task Justify_RefusesAValueThatIsNotAnInterval(string expression)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    // ── date_bin ─────────────────────────────────────────────────────────────

    [TestCase("date_bin(INTERVAL '15 minutes', CAST('2020-02-11T15:44:17Z' AS datetime), CAST('2001-01-01T00:00:00Z' AS datetime))", "2020-02-11 15:30:00")]
    [TestCase("date_bin(INTERVAL '15 minutes', CAST('2020-02-11T15:44:17Z' AS datetime), CAST('2001-01-01T00:02:30Z' AS datetime))", "2020-02-11 15:32:30")]
    [TestCase("date_bin(INTERVAL '1 hour', CAST('2000-12-31T22:30:00Z' AS datetime), CAST('2001-01-01T00:00:00Z' AS datetime))", "2000-12-31 22:00:00")]
    [TestCase("date_bin(INTERVAL '1 hour', CAST('2001-01-01T00:00:00Z' AS datetime), CAST('2001-01-01T00:00:00Z' AS datetime))", "2001-01-01 00:00:00")]
    [TestCase("date_bin(INTERVAL '1 day', CAST('2024-02-29' AS date), CAST('2024-01-01' AS date))", "2024-02-29 00:00:00")]
    [TestCase("date_bin(INTERVAL '7 days', CAST('2024-03-03' AS date), CAST('2024-01-01' AS date))", "2024-02-26 00:00:00")]
    [TestCase("date_bin(INTERVAL '1 hour', NULL, CAST('2001-01-01' AS date))", "NULL")]
    public async Task DateBin_GivesTheStartOfTheBin(string expression, string expected)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(expected), expression);
    }

    [TestCase("date_bin(INTERVAL '1 month', CAST('2024-01-01' AS date), CAST('2001-01-01' AS date))", "months or years")]
    [TestCase("date_bin(INTERVAL '0' SECOND, CAST('2024-01-01' AS date), CAST('2001-01-01' AS date))", "greater than zero")]
    [TestCase("date_bin(INTERVAL '-1 hour', CAST('2024-01-01' AS date), CAST('2001-01-01' AS date))", "greater than zero")]
    [TestCase("date_bin(INTERVAL '1 day', CAST('0001-01-01T06:00:00Z' AS datetime), CAST('0001-01-01T12:00:00Z' AS datetime))", "timestamp out of range")]
    public async Task DateBin_RefusesAStrideWithMonths_ZeroOrNegative(string expression, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT {expression}", null));
                await cursor.ToListAsync();
            })!;

            Assert.Multiple(() =>
            {
                Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
                Assert.That(e.Message, Does.Contain(message), expression);
            });
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    // ── EXTRACT / date_part from an interval ─────────────────────────────────

    [TestCase("EXTRACT(year FROM INTERVAL '1 year 14 months')", "2")]
    [TestCase("EXTRACT(month FROM INTERVAL '1 year 14 months')", "2")]
    [TestCase("EXTRACT(day FROM INTERVAL '36 hours')", "0")]
    [TestCase("EXTRACT(hour FROM INTERVAL '36 hours')", "36")]
    [TestCase("EXTRACT(minute FROM INTERVAL '1 hour 30 minutes')", "30")]
    [TestCase("EXTRACT(second FROM INTERVAL '90.5 seconds')", "30")]
    [TestCase("EXTRACT(millisecond FROM INTERVAL '30.25 seconds')", "250")]
    [TestCase("EXTRACT(microsecond FROM INTERVAL '0.000123 seconds')", "123")]
    [TestCase("EXTRACT(hour FROM INTERVAL '-3 hours -20 minutes')", "-3")]
    [TestCase("EXTRACT(minute FROM INTERVAL '-3 hours -20 minutes')", "-20")]
    [TestCase("EXTRACT(quarter FROM INTERVAL '0 months')", "1")]
    [TestCase("EXTRACT(quarter FROM INTERVAL '4 months')", "2")]
    [TestCase("EXTRACT(quarter FROM INTERVAL '-4 months')", "0")]
    [TestCase("EXTRACT(decade FROM INTERVAL '25 years')", "2")]
    [TestCase("EXTRACT(century FROM INTERVAL '250 years')", "2")]
    [TestCase("EXTRACT(millennium FROM INTERVAL '2500 years')", "2")]
    [TestCase("EXTRACT(epoch FROM INTERVAL '1 year')", "31557600")]
    [TestCase("EXTRACT(epoch FROM INTERVAL '1 month 1 day 1 second')", "2678401")]
    [TestCase("EXTRACT(epoch FROM INTERVAL '-1.5 seconds')", "-2")]
    [TestCase("date_part('hour', INTERVAL '5 hours')", "5")]
    [TestCase("date_part('day', age(CAST('2024-03-01' AS date), CAST('2024-02-29' AS date)))", "1")]
    public async Task Extract_ReadsOneFieldOfAnInterval(string expression, string expected)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(expected), expression);
    }

    [TestCase("EXTRACT(week FROM INTERVAL '1 day')")]
    [TestCase("EXTRACT(dow FROM INTERVAL '1 day')")]
    [TestCase("EXTRACT(doy FROM INTERVAL '1 day')")]
    [TestCase("EXTRACT(isoyear FROM INTERVAL '1 year')")]
    public async Task Extract_RefusesACalendarPositionForAnInterval(string expression)
    {
        Assert.That(await Scalar(expression), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    // ── Range scan for a bound with an interval ──────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateLogs()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE logs (id INT64 PRIMARY KEY, created DATETIME)");
        await Commit(database, executor, dbname, "CREATE INDEX created_idx ON logs (created)");
        await Commit(database, executor, dbname,
            "INSERT INTO logs (id, created) VALUES (1, now()), (2, '2000-01-01T00:00:00Z'), (3, now() - INTERVAL '3 days')");

        return (dbname, database, executor);
    }

    private static async Task<string> PlanText(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "EXPLAIN " + sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return string.Join("\n", rows.Select(r =>
                (r.Row.TryGetValue("node", out ColumnValue? node) ? node.StrValue : "") + " " +
                (r.Row.TryGetValue("detail", out ColumnValue? detail) ? detail.StrValue : "")));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    [TestCase("SELECT id FROM logs WHERE created > now() - INTERVAL '7 days' ORDER BY id")]
    [TestCase("SELECT id FROM logs WHERE created >= now() - INTERVAL 7 DAY ORDER BY id")]
    [TestCase("SELECT id FROM logs WHERE now() - INTERVAL '7 days' < created ORDER BY id")]
    [TestCase("SELECT id FROM logs WHERE created > DATE_SUB(now(), INTERVAL 1 WEEK) ORDER BY id")]
    [TestCase("SELECT id FROM logs WHERE created > current_date() - 7 ORDER BY id")]
    public async Task ABoundWithAnInterval_UsesAnIndexRangeScan(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateLogs();

        string plan = await PlanText(database, executor, dbname, sql);
        string rows = await Run(database, executor, dbname, sql);

        Assert.Multiple(() =>
        {
            Assert.That(plan, Does.Contain("index-range-scan").IgnoreCase, plan);
            Assert.That(plan, Does.Contain("created_idx"), plan);
            Assert.That(rows, Is.EqualTo("1 ; 3"), sql);
        });
    }

    /// <summary>
    /// The plan check above can fail: when the column is inside the expression, the bound is not a
    /// constant, so the plan does not use the index on the column (it scans the primary key), and the
    /// rows are still correct.
    /// </summary>
    [Test]
    public async Task AColumnInsideTheIntervalExpression_IsNotARangeBound()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateLogs();

        const string sql = "SELECT id FROM logs WHERE created + INTERVAL '7 days' > now() ORDER BY id";
        string plan = await PlanText(database, executor, dbname, sql);
        string rows = await Run(database, executor, dbname, sql);

        Assert.Multiple(() =>
        {
            Assert.That(plan, Does.Not.Contain("created_idx"), plan);
            Assert.That(rows, Is.EqualTo("1 ; 3"), sql);
        });
    }
}
