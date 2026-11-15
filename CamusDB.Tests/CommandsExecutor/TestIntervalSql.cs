/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The interval value through SQL: the literal forms (<c>INTERVAL '1 day'</c>, <c>INTERVAL '3' HOUR</c>,
/// <c>INTERVAL 3 HOUR</c>, <c>CAST(x AS interval)</c>, <c>x::interval</c>), the operators with dates,
/// datetimes, numbers and other intervals, the order where a month is 30 days, and the rule that an
/// interval is an expression value only: no table stores one, and a client receives its ISO-8601 text as
/// a string. Each expected value follows the PostgreSQL rules.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIntervalSql : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE ev (id INT64 PRIMARY KEY, ts DATETIME, d DATE, n INT64)");
        await Commit(database, executor, dbname,
            "INSERT INTO ev (id, ts, d, n) VALUES " +
            "(1, '2024-01-31T10:00:00Z', '2024-01-31', 2), " +
            "(2, '2024-02-29T00:30:00Z', '2024-02-29', 1), " +
            "(3, NULL, NULL, NULL)");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal) || sql.StartsWith("DROP", StringComparison.Ordinal))
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

    /// <summary>The error code of a statement that must fail, or "ok".</summary>
    private static async Task<string> CommitCode(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        try
        {
            await Commit(database, executor, dbname, sql);
            return "ok";
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.String => value.StrValue!,
        ColumnType.Date => value.IsoValue!,
        ColumnType.DateTime => new DateTime(value.LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
        ColumnType.Interval => IntervalMath.FormatIso(value.IntervalParts),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    /// <summary>
    /// The rows of <paramref name="sql"/>: cells join with <c>|</c> and rows with <c> ; </c>, or the error
    /// code. The type of the last cell of each row goes to <paramref name="lastCellTypes"/>.
    /// </summary>
    private static async Task<string> Run(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        List<ColumnType>? lastCellTypes = null, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            foreach (QueryResultRow row in rows)
                lastCellTypes?.Add(row.Row.Values.Last().Type);

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

    // ── Literals and casts ───────────────────────────────────────────────────

    [TestCase("INTERVAL '1 day 2 hours'", "P1DT2H")]
    [TestCase("interval '1 day'", "P1D")]
    [TestCase("INTERVAL'1 day'", "P1D")]
    [TestCase("INTERVAL 'P1W'", "P7D")]
    [TestCase("INTERVAL '1 year 2 mons 3 days 04:05:06.5'", "P1Y2M3DT4H5M6.5S")]
    [TestCase("INTERVAL '3 hours ago'", "PT-3H")]
    [TestCase("INTERVAL '3' HOUR", "PT3H")]
    [TestCase("INTERVAL '2' year", "P2Y")]
    [TestCase("INTERVAL '-1' DAY", "P-1D")]
    [TestCase("INTERVAL 3 HOUR", "PT3H")]
    [TestCase("INTERVAL 1.5 HOUR", "PT1H30M")]
    [TestCase("INTERVAL 1 QUARTER", "P3M")]
    [TestCase("INTERVAL 10 MINUTE", "PT10M")]
    [TestCase("CAST('1 day' AS interval)", "P1D")]
    [TestCase("CAST('PT90M' AS INTERVAL)", "PT1H30M")]
    [TestCase("'36 hours'::interval", "PT36H")]
    [TestCase("CAST(INTERVAL '2 days' AS interval)", "P2D")]
    [TestCase("-INTERVAL '1 day 2 hours'", "P-1DT-2H")]
    [TestCase("- -INTERVAL '1 day'", "P1D")]
    public async Task ALiteralOrACast_GivesAnInterval(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Interval }), expression);
        });
    }

    [TestCase("CAST(INTERVAL '1 day 2 hours' AS string)", "P1DT2H")]
    [TestCase("'every ' || INTERVAL '1 day'", "every P1D")]
    [TestCase("CAST(INTERVAL '0' SECOND AS string)", "PT0S")]
    public async Task TheTextOfAnInterval_IsItsIsoForm(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(expected), expression);
    }

    [TestCase("INTERVAL 'abc'")]
    [TestCase("INTERVAL '1 fortnight'")]
    [TestCase("INTERVAL 3 fortnight")]
    [TestCase("INTERVAL '1 day' HOUR")]
    [TestCase("INTERVAL '2147483648 months'")]
    [TestCase("CAST(1 AS interval)")]
    [TestCase("CAST(INTERVAL '1 day' AS int64)")]
    [TestCase("CAST(CAST('2024-01-01' AS date) AS interval)")]
    public async Task InvalidIntervalText_OrAnInvalidCast_IsAnError(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    // ── Operators ────────────────────────────────────────────────────────────

    [TestCase("CAST('2024-01-31T10:00:00Z' AS datetime) + INTERVAL '1 month'", "2024-02-29 10:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-01-31T10:00:00Z' AS datetime) + INTERVAL '1 mon 1 day'", "2024-03-01 10:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-03-31T00:00:00Z' AS datetime) - INTERVAL '1 month'", "2024-02-29 00:00:00", ColumnType.DateTime)]
    [TestCase("INTERVAL '2 hours' + CAST('2024-01-01T23:00:00Z' AS datetime)", "2024-01-02 01:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-01-31' AS date) + INTERVAL '1 month'", "2024-02-29 00:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-02-29' AS date) + INTERVAL '1 year'", "2025-02-28 00:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-03-01' AS date) - INTERVAL '1 hour'", "2024-02-29 23:00:00", ColumnType.DateTime)]
    [TestCase("INTERVAL '1 day' + CAST('2023-12-31' AS date)", "2024-01-01 00:00:00", ColumnType.DateTime)]
    [TestCase("CAST('2024-01-02T03:00:00Z' AS datetime) - CAST('2024-01-01T00:00:00Z' AS datetime)", "P1DT3H", ColumnType.Interval)]
    [TestCase("CAST('2024-01-01T00:00:00Z' AS datetime) - CAST('2024-01-02T03:00:00Z' AS datetime)", "P-1DT-3H", ColumnType.Interval)]
    [TestCase("CAST('2024-03-01' AS date) - CAST('2024-02-01T12:00:00Z' AS datetime)", "P28DT12H", ColumnType.Interval)]
    [TestCase("CAST('2024-03-01T06:00:00Z' AS datetime) - CAST('2024-03-01' AS date)", "PT6H", ColumnType.Interval)]
    [TestCase("INTERVAL '1 day' + INTERVAL '2 hours'", "P1DT2H", ColumnType.Interval)]
    [TestCase("INTERVAL '1 mon' - INTERVAL '1 day'", "P1M-1D", ColumnType.Interval)]
    [TestCase("INTERVAL '1 day' * 2", "P2D", ColumnType.Interval)]
    [TestCase("3 * INTERVAL '1 hour'", "PT3H", ColumnType.Interval)]
    [TestCase("INTERVAL '1 day' * 1.5", "P1DT12H", ColumnType.Interval)]
    [TestCase("INTERVAL '1 month' * CAST('0.5' AS numeric)", "P15D", ColumnType.Interval)]
    [TestCase("INTERVAL '1 month' / 3", "P10D", ColumnType.Interval)]
    [TestCase("INTERVAL '3 hours' / 2.0", "PT1H30M", ColumnType.Interval)]
    [TestCase("INTERVAL '1 month' = INTERVAL '30 days'", "t", ColumnType.Bool)]
    [TestCase("INTERVAL '1 day' = INTERVAL '24 hours'", "t", ColumnType.Bool)]
    [TestCase("INTERVAL '1 month' > INTERVAL '29 days'", "t", ColumnType.Bool)]
    [TestCase("INTERVAL '-1 day' < INTERVAL '0'", "t", ColumnType.Bool)]
    [TestCase("CAST('2024-01-02T00:00:00Z' AS datetime) - CAST('2024-01-01T00:00:00Z' AS datetime) = INTERVAL '24 hours'", "t", ColumnType.Bool)]
    public async Task AnOperator_GivesThePostgreSqlResult(string expression, string expected, ColumnType type)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { type }), expression);
        });
    }

    [TestCase("INTERVAL '1 day' + 1")]
    [TestCase("1 - INTERVAL '1 day'")]
    [TestCase("1 / INTERVAL '1 day'")]
    [TestCase("INTERVAL '1 day' * INTERVAL '1 day'")]
    [TestCase("INTERVAL '1 day' / INTERVAL '1 day'")]
    [TestCase("INTERVAL '1 day' - CAST('2024-01-01' AS date)")]
    [TestCase("CAST('2024-01-01' AS date) * INTERVAL '1 day'")]
    [TestCase("CAST('2024-01-01T00:00:00Z' AS datetime) + 1")]
    [TestCase("CAST('2024-01-01T00:00:00Z' AS datetime) + CAST('2024-01-01T00:00:00Z' AS datetime)")]
    [TestCase("INTERVAL '1 day' + 'x'")]
    [TestCase("INTERVAL '1 day' * TRUE")]
    [TestCase("INTERVAL '1 day' = 1")]
    [TestCase("INTERVAL '1 day' / 0")]
    [TestCase("INTERVAL '1 day' * CAST('NaN' AS float64)")]
    [TestCase("CAST('9999-12-31' AS date) + INTERVAL '1 day'")]
    [TestCase("CAST('0001-01-01T00:00:00Z' AS datetime) - INTERVAL '1 us'")]
    [TestCase("INTERVAL '2147483647 months' + INTERVAL '1 month'")]
    [TestCase("-INTERVAL '-2147483648 days'")]
    public async Task APairWithNoRule_OrAResultOutOfRange_IsAnError(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    [TestCase("CAST(NULL AS interval) + INTERVAL '1 day'")]
    [TestCase("NULL + INTERVAL '1 day'")]
    [TestCase("INTERVAL '1 day' * NULL")]
    [TestCase("CAST(NULL AS datetime) - INTERVAL '1 day'")]
    [TestCase("-CAST(NULL AS interval)")]
    public async Task ANullOperand_GivesNull(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(""), expression);
    }

    // ── DATE_ADD and DATE_SUB ────────────────────────────────────────────────

    [TestCase("date_add(CAST('2024-01-31' AS date), INTERVAL 1 MONTH)", "2024-02-29 00:00:00")]
    [TestCase("DATE_ADD(CAST('2024-01-31T10:00:00Z' AS datetime), INTERVAL '1 day')", "2024-02-01 10:00:00")]
    [TestCase("date_add('2024-01-01', INTERVAL 2 HOUR)", "2024-01-01 02:00:00")]
    [TestCase("date_add(CAST('2024-01-01' AS date), INTERVAL '1 day' * 3)", "2024-01-04 00:00:00")]
    [TestCase("date_sub(CAST('2024-03-01' AS date), INTERVAL 1 DAY)", "2024-02-29 00:00:00")]
    [TestCase("DATE_SUB(CAST('2024-03-31T12:00:00Z' AS datetime), INTERVAL '1 month 12 hours')", "2024-02-29 00:00:00")]
    [TestCase("date_sub(CAST('2024-03-01' AS date), 1, 'day')", "2024-02-29 00:00:00")]
    [TestCase("date_sub(CAST('2024-03-01' AS date), -1, 'month')", "2024-04-01 00:00:00")]
    [TestCase("date_add(CAST('2024-01-01' AS date), 1, 'day')", "2024-01-02 00:00:00")]
    public async Task DateAddAndDateSub_TakeAnInterval(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { ColumnType.DateTime }), expression);
        });
    }

    [TestCase("date_add(CAST('2024-01-01' AS date), 1)", "CADB0400")]
    [TestCase("date_sub(CAST('2024-01-01' AS date), 'x')", "CADB0400")]
    [TestCase("date_add(CAST('9999-12-31' AS date), INTERVAL 1 DAY)", "CADB0400")]
    [TestCase("date_sub(CAST('2024-01-01' AS date), INTERVAL '-2147483648 days')", "CADB0400")]
    [TestCase("date_add(NULL, INTERVAL 1 DAY)", "")]
    [TestCase("date_sub(CAST('2024-01-01' AS date), CAST(NULL AS interval))", "")]
    public async Task DateAddAndDateSub_RefuseAnAmountThatIsNotAnInterval(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(expected), expression);
    }

    // ── Rows ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task ColumnOperands_GiveOneValuePerRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string rows = await Run(database, executor, dbname,
            "SELECT id, ts + INTERVAL '1 month', d - INTERVAL 1 DAY, ts - CAST('2024-01-01T00:00:00Z' AS datetime), n * INTERVAL '1 hour' FROM ev ORDER BY id");

        Assert.That(rows, Is.EqualTo(
            "1|2024-02-29 10:00:00|2024-01-30 00:00:00|P30DT10H|PT2H ; " +
            "2|2024-03-29 00:30:00|2024-02-28 00:00:00|P59DT30M|PT1H ; " +
            "3||||"));
    }

    [Test]
    public async Task AWhereBound_OrderBy_GroupBy_DistinctAndMinMax_UseTheIntervalOrder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string filtered = await Run(database, executor, dbname,
            "SELECT id FROM ev WHERE ts > CAST('2024-03-01T00:00:00Z' AS datetime) - INTERVAL '2 days' ORDER BY id");
        string ordered = await Run(database, executor, dbname,
            "SELECT id FROM ev WHERE ts IS NOT NULL ORDER BY ts - CAST('2024-01-01T00:00:00Z' AS datetime) DESC");
        string minMax = await Run(database, executor, dbname,
            "SELECT MIN(ts - CAST('2024-01-01T00:00:00Z' AS datetime)), MAX(ts - CAST('2024-01-01T00:00:00Z' AS datetime)) FROM ev");

        // One month and 30 days compare equal, so they form one group and one distinct value.
        string grouped = await Run(database, executor, dbname,
            "SELECT COUNT(*) FROM ev GROUP BY CASE WHEN id = 1 THEN INTERVAL '1 month' ELSE INTERVAL '30 days' END");
        string distinct = await Run(database, executor, dbname,
            "SELECT COUNT(*) FROM (SELECT DISTINCT CASE WHEN id = 1 THEN INTERVAL '1 month' ELSE INTERVAL '30 days' END AS x FROM ev) s");

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Is.EqualTo("2"));
            Assert.That(ordered, Is.EqualTo("2 ; 1"));
            Assert.That(minMax, Is.EqualTo("P30DT10H|P59DT30M"));
            Assert.That(grouped, Is.EqualTo("3"));
            Assert.That(distinct, Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task AParameter_CanBeTheAmountOfAUnit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> parameters = new() { ["@n"] = new ColumnValue(ColumnType.Integer64, 3) };

        string unitForm = await Run(database, executor, dbname, "SELECT INTERVAL @n DAY", parameters: parameters);
        string product = await Run(database, executor, dbname, "SELECT @n * INTERVAL '1 hour'", parameters: parameters);

        Assert.Multiple(() =>
        {
            Assert.That(unitForm, Is.EqualTo("P3D"));
            Assert.That(product, Is.EqualTo("PT3H"));
        });
    }

    /// <summary>
    /// A subquery result becomes a literal in the outer statement. An interval becomes
    /// CAST('&lt;ISO text&gt;' AS interval), which keeps its three parts.
    /// </summary>
    [Test]
    public async Task ASubquery_CanReturnAnInterval()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string scalar = await Run(database, executor, dbname, "SELECT (SELECT INTERVAL '1 mon 2 days')");
        string bound = await Run(database, executor, dbname,
            "SELECT id FROM ev WHERE ts - CAST('2024-01-01T00:00:00Z' AS datetime) > (SELECT INTERVAL '31 days') ORDER BY id");
        string member = await Run(database, executor, dbname,
            "SELECT id FROM ev WHERE n * INTERVAL '1 hour' IN (SELECT INTERVAL '60 minutes') ORDER BY id");

        Assert.Multiple(() =>
        {
            Assert.That(scalar, Is.EqualTo("P1M2D"));
            Assert.That(bound, Is.EqualTo("2"));
            Assert.That(member, Is.EqualTo("2"));
        });
    }

    /// <summary>
    /// The result cache keys a statement by its parameters. Two intervals with the same time part but
    /// other days, or that compare equal but print differently, must not share a key.
    /// </summary>
    [Test]
    public void TheResultCacheFingerprint_SeesAllThreeParts()
    {
        static string Key(string interval) => ResultFingerprintBuilder.Build(
            "db1", "c", "shape", new() { ["@p"] = ColumnValue.FromInterval(IntervalMath.Parse(interval)) }, null, new CacheHintOptions("test"));

        Assert.Multiple(() =>
        {
            Assert.That(Key("1 day"), Is.Not.EqualTo(Key("2 days")));
            Assert.That(Key("1 month"), Is.Not.EqualTo(Key("30 days")));
            Assert.That(Key("1 day"), Is.EqualTo(Key("24:00:00 0 days 1 day -24 hours")));
        });
    }

    // ── An interval is an expression value only ─────────────────────────────

    [Test]
    public async Task NoTableStoresAnInterval()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE s (id INT64 PRIMARY KEY, txt STRING)");

        string columnType = await CommitCode(database, executor, dbname, "CREATE TABLE t2 (id INT64 PRIMARY KEY, x interval)");
        string addColumn = await CommitCode(database, executor, dbname, "ALTER TABLE s ADD COLUMN x interval");
        string createAs = await CommitCode(database, executor, dbname, "CREATE TABLE c AS SELECT id, ts - ts AS span FROM ev");
        string materialized = await CommitCode(database, executor, dbname, "CREATE MATERIALIZED VIEW mv AS SELECT id, ts - ts AS span FROM ev");
        string plainView = await CommitCode(database, executor, dbname, "CREATE VIEW pv AS SELECT id, ts - ts AS span FROM ev");
        string insert = await CommitCode(database, executor, dbname, "INSERT INTO s (id, txt) VALUES (1, INTERVAL '1 day')");
        string insertSelect = await CommitCode(database, executor, dbname, "INSERT INTO s (id, txt) SELECT id, ts - ts FROM ev WHERE id = 1");
        string update = await CommitCode(database, executor, dbname, "UPDATE ev SET ts = INTERVAL '1 day' WHERE id = 1");
        string stored = await CommitCode(database, executor, dbname, "INSERT INTO s (id, txt) VALUES (2, CAST(INTERVAL '1 day' AS string))");
        string storedRow = await Run(database, executor, dbname, "SELECT txt FROM s");

        Assert.Multiple(() =>
        {
            Assert.That(columnType, Is.EqualTo(CamusDBErrorCodes.InvalidInput), "a column of type interval");
            Assert.That(addColumn, Is.EqualTo(CamusDBErrorCodes.InvalidInput), "ADD COLUMN of type interval");
            Assert.That(createAs, Is.EqualTo(CamusDBErrorCodes.InvalidInput), "CREATE TABLE AS with an interval column");
            Assert.That(materialized, Is.EqualTo(CamusDBErrorCodes.InvalidInput), "a materialized view stores its rows");
            Assert.That(plainView, Is.EqualTo("ok"), "a plain view stores no rows");
            Assert.That(insert, Is.EqualTo(CamusDBErrorCodes.UnknownType), "INSERT of an interval into a string column");
            Assert.That(insertSelect, Is.EqualTo(CamusDBErrorCodes.UnknownType), "INSERT … SELECT of an interval");
            Assert.That(update, Is.EqualTo(CamusDBErrorCodes.UnknownType), "UPDATE to an interval");
            Assert.That(stored, Is.EqualTo("ok"), "the text form stores");
            Assert.That(storedRow, Is.EqualTo("P1D"));
        });
    }

    [Test]
    public async Task AColumnNamedInterval_StillWorks()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE iv (id INT64 PRIMARY KEY, interval INT64)");
        await Commit(database, executor, dbname, "INSERT INTO iv (id, interval) VALUES (1, 5), (2, 7)");

        Assert.That(
            await Run(database, executor, dbname, "SELECT interval + 1 FROM iv WHERE interval = 5 ORDER BY interval"),
            Is.EqualTo("6"));
    }

    /// <summary>
    /// A view body is rendered to SQL text and parsed again on each load. Each literal form must survive
    /// that, and a close and reopen of the database.
    /// </summary>
    [Test]
    public async Task AViewWithIntervalLiterals_SurvivesAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname,
            "CREATE VIEW v AS SELECT id, ts + INTERVAL '1 month' AS a, ts - INTERVAL '3' HOUR AS b, " +
            "d + INTERVAL 2 DAY AS c, -INTERVAL '1 day' AS e FROM ev WHERE id = 1");

        const string expected = "1|2024-02-29 10:00:00|2024-01-31 07:00:00|2024-02-02 00:00:00|P-1D";
        string before = await Run(database, executor, dbname, "SELECT id, a, b, c, e FROM v");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        string after = await Run(reopened, executor, dbname, "SELECT id, a, b, c, e FROM v");

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(expected));
            Assert.That(after, Is.EqualTo(expected));
        });
    }

    // ── Clients receive the ISO-8601 text ────────────────────────────────────

    [Test]
    public void AClient_SeesAStringColumnOfIsoText()
    {
        ColumnValue value = ColumnValue.FromInterval(IntervalMath.Parse("1 day 2 hours"));

        List<ColumnSchemaDto> columns = ColumnSchemaDto.FromSchema([new DerivedColumnSchema("x", ColumnType.Interval)], []);

        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
            CompactRowJsonWriter.WriteValue(writer, value);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0].Type, Is.EqualTo(ColumnType.String), "REST column type");
            Assert.That(Encoding.UTF8.GetString(stream.ToArray()), Is.EqualTo("\"P1DT2H\""), "REST cell");
            Assert.That(CompactRowEncoder.EncodeValue(value), Is.EqualTo("P1DT2H"), "REST positional cell");
            Assert.That(GrpcValueCodec.ToProto(value).StringValue, Is.EqualTo("P1DT2H"), "gRPC cell");
            Assert.That((int)CamusSqlService.BuildResultSchema([new DerivedColumnSchema("x", ColumnType.Interval)]).Columns[0].Type,
                Is.EqualTo((int)ColumnType.String), "gRPC column type");
        });
    }
}
