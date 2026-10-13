/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Which time each clock function reports. <c>now()</c>, <c>current_timestamp()</c>,
/// <c>transaction_timestamp()</c>, <c>current_date()</c> and a no-argument <c>unix_timestamp()</c>
/// report the start of the transaction; <c>statement_timestamp()</c> the start of the statement;
/// <c>clock_timestamp()</c> and <c>sysdate()</c> the clock at each call. Every test drives SQL through
/// the executor, so the times are attached by the real entry points, not by the test.
/// </summary>
[NonParallelizable]
internal sealed class TestStatementClockFunctions : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await QueryIn(executor, tx, dbname, sql);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> QueryIn(
        CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        List<Dictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));

        return rows;
    }

    private static async Task NonQueryIn(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
        => await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

    /// <summary>A multi-row INSERT of <paramref name="count"/> rows with ids from <paramref name="firstId"/>.</summary>
    private static string InsertRows(string table, string columns, int firstId, int count, Func<int, string> values)
    {
        StringBuilder sql = new($"INSERT INTO {table} ({columns}) VALUES ");

        for (int i = 0; i < count; i++)
        {
            if (i > 0)
                sql.Append(", ");

            sql.Append('(').Append(values(firstId + i)).Append(')');
        }

        return sql.ToString();
    }

    private static List<long> Ticks(IEnumerable<Dictionary<string, ColumnValue>> rows, string column)
        => rows.Select(r =>
        {
            Assert.That(r[column].Type, Is.EqualTo(ColumnType.DateTime), column);
            return r[column].LongValue;
        }).ToList();

    // -----------------------------------------------------------------------
    // Transaction time
    // -----------------------------------------------------------------------

    [Test]
    public async Task Now_AndItsAliases_ReportOneValueWithinAStatement()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname,
            "SELECT now() AS a, current_timestamp() AS b, transaction_timestamp() AS c, now() = now() AS same");

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0]["same"].BoolValue, Is.True, "now() = now() must hold within one statement");
        Assert.That(rows[0]["b"].LongValue, Is.EqualTo(rows[0]["a"].LongValue));
        Assert.That(rows[0]["c"].LongValue, Is.EqualTo(rows[0]["a"].LongValue));
    }

    [Test]
    public async Task Now_IsTheStartOfTheTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            // The statement runs well after the transaction began, so a value read at call time
            // could not equal the start time.
            await Task.Delay(30);

            List<Dictionary<string, ColumnValue>> rows = await QueryIn(executor, tx, dbname,
                "SELECT now() AS n, current_date() AS d, unix_timestamp() AS u");

            DateTime start = tx.StartTime;
            Assert.That(rows[0]["n"].LongValue, Is.EqualTo(start.Ticks));
            Assert.That(rows[0]["d"].Type, Is.EqualTo(ColumnType.Date));
            Assert.That(rows[0]["d"].LongValue, Is.EqualTo(start.Date.Ticks));
            Assert.That(rows[0]["u"].LongValue, Is.EqualTo(new DateTimeOffset(start).ToUnixTimeSeconds()));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task Now_IsStableAcrossStatements_WhileStatementTimestampIsNot()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            List<Dictionary<string, ColumnValue>> first = await QueryIn(executor, tx, dbname,
                "SELECT now() AS n, statement_timestamp() AS s, statement_timestamp() AS s2");

            await Task.Delay(30);

            List<Dictionary<string, ColumnValue>> second = await QueryIn(executor, tx, dbname,
                "SELECT now() AS n, statement_timestamp() AS s");

            Assert.That(second[0]["n"].LongValue, Is.EqualTo(first[0]["n"].LongValue), "now() is fixed for the transaction");
            Assert.That(first[0]["s2"].LongValue, Is.EqualTo(first[0]["s"].LongValue), "statement_timestamp() is fixed for the statement");
            Assert.That(second[0]["s"].LongValue, Is.GreaterThan(first[0]["s"].LongValue), "each statement gets its own statement_timestamp()");
            Assert.That(first[0]["s"].LongValue, Is.GreaterThanOrEqualTo(first[0]["n"].LongValue));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task Now_DiffersBetweenTransactions()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        long first = (await Query(executor, dbname, "SELECT now() AS n"))[0]["n"].LongValue;
        await Task.Delay(30);
        long second = (await Query(executor, dbname, "SELECT now() AS n"))[0]["n"].LongValue;

        Assert.That(second, Is.GreaterThan(first));
    }

    // -----------------------------------------------------------------------
    // Clock time
    // -----------------------------------------------------------------------

    [Test]
    public async Task ClockTimestamp_ReadsTheClockOnEachCall()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            // Two calls of one row can fall in the same clock tick, so the statement is run until they
            // do not. A function fixed per statement would never differ, however many runs.
            bool differed = false;

            for (int attempt = 0; attempt < 500 && !differed; attempt++)
            {
                Dictionary<string, ColumnValue> row = (await QueryIn(executor, tx, dbname,
                    "SELECT clock_timestamp() AS a, sysdate() AS b, clock_timestamp() AS c, now() AS n"))[0];

                Assert.That(row["b"].Type, Is.EqualTo(ColumnType.DateTime));
                Assert.That(row["c"].LongValue, Is.GreaterThanOrEqualTo(row["a"].LongValue));
                differed = row["c"].LongValue != row["a"].LongValue;
            }

            Assert.That(differed, Is.True, "clock_timestamp() must be read on each call");

            await Task.Delay(30);

            Dictionary<string, ColumnValue> later = (await QueryIn(executor, tx, dbname,
                "SELECT clock_timestamp() AS a, now() AS n"))[0];

            Assert.That(later["a"].LongValue, Is.GreaterThan(later["n"].LongValue),
                "clock_timestamp() moves on while now() stays at the transaction start");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    [Test]
    public async Task Select_Now_ReportsOneValueForTenThousandRows()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE src (id INT64 PRIMARY KEY)");

        for (int first = 1; first <= 10_000; first += 2_000)
            await ExecuteNonQuery(executor, dbname, InsertRows("src", "id", first, 2_000, id => id.ToString()));

        List<Dictionary<string, ColumnValue>> all = await Query(executor, dbname,
            "SELECT id, now() AS ts, clock_timestamp() AS clock FROM src");

        Assert.That(all, Has.Count.EqualTo(10_000));
        Assert.That(Ticks(all, "ts").Distinct().Count(), Is.EqualTo(1), "now() is one value for the whole statement");
        Assert.That(Ticks(all, "clock").Distinct().Count(), Is.GreaterThan(1), "clock_timestamp() is read per row");
    }

    // Serializable takes an exclusive lock over the source range and Read Committed does not; the time
    // each row gets must not depend on that.
    [TestCase(CamusIsolationLevel.Serializable, 10_000)]
    [TestCase(CamusIsolationLevel.ReadCommitted, 10_000)]
    public async Task InsertSelect_Now_WritesOneValueForEveryRow(CamusIsolationLevel isolation, int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE src (id INT64 PRIMARY KEY)");
        await ExecuteDdl(executor, dbname, "CREATE TABLE dst (id INT64 PRIMARY KEY, ts DATETIME, clock DATETIME)");

        for (int first = 1; first <= rows; first += 2_000)
            await ExecuteNonQuery(executor, dbname, InsertRows("src", "id", first, Math.Min(2_000, rows - first + 1), id => id.ToString()));

        KvTransaction tx = await database.Transactions.BeginAsync(isolation);
        try
        {
            await NonQueryIn(executor, tx, dbname, "INSERT INTO dst (id, ts, clock) SELECT id, now(), clock_timestamp() FROM src");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        List<Dictionary<string, ColumnValue>> all = await Query(executor, dbname, "SELECT ts, clock FROM dst");
        Assert.That(all, Has.Count.EqualTo(rows));
        Assert.That(Ticks(all, "ts").Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
        Assert.That(Ticks(all, "clock").Distinct().Count(), Is.GreaterThan(1));

        List<Dictionary<string, ColumnValue>> distinct = await Query(executor, dbname, "SELECT DISTINCT ts FROM dst");
        Assert.That(distinct, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Update_Now_WritesOneValueForEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, ts DATETIME)");
        await ExecuteNonQuery(executor, dbname, InsertRows("t", "id", 1, 500, id => id.ToString()));

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await NonQueryIn(executor, tx, dbname, "UPDATE t SET ts = now() WHERE id > 0");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        List<long> written = Ticks(await Query(executor, dbname, "SELECT ts FROM t"), "ts");
        Assert.That(written, Has.Count.EqualTo(500));
        Assert.That(written.Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
    }

    [Test]
    public async Task DefaultNow_OnAMultiRowInsert_WritesOneValue_ThatEqualsAnExplicitNow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE TABLE events (id INT64 PRIMARY KEY, explicit_ts DATETIME, ts DATETIME DEFAULT (now()), " +
            "d DATE DEFAULT (current_date()), st DATETIME DEFAULT (statement_timestamp()), " +
            "explicit_st DATETIME)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await NonQueryIn(executor, tx, dbname,
                InsertRows("events", "id, explicit_ts, explicit_st", 1, 300, id => $"{id}, now(), statement_timestamp()"));

            await Task.Delay(30);

            // The second statement names no clock function, so its defaults alone ask for the times.
            await NonQueryIn(executor, tx, dbname, InsertRows("events", "id", 301, 300, id => id.ToString()));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id, explicit_ts, ts, d, st, explicit_st FROM events");
        Assert.That(rows, Has.Count.EqualTo(600));

        Assert.That(Ticks(rows, "ts").Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks), "one default time for both statements");
        Assert.That(rows.Select(r => r["d"].LongValue).Distinct().Single(), Is.EqualTo(tx.StartTime.Date.Ticks));

        List<Dictionary<string, ColumnValue>> firstStatement = rows.Where(r => r["id"].LongValue <= 300).ToList();
        List<Dictionary<string, ColumnValue>> secondStatement = rows.Where(r => r["id"].LongValue > 300).ToList();

        Assert.That(Ticks(firstStatement, "explicit_ts").Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));

        // A statement-time default agrees with an explicit statement_timestamp() of its own statement,
        // and is one value per statement.
        long firstStatementTime = Ticks(firstStatement, "st").Distinct().Single();
        Assert.That(Ticks(firstStatement, "explicit_st").Distinct().Single(), Is.EqualTo(firstStatementTime));
        Assert.That(Ticks(secondStatement, "st").Distinct().Single(), Is.GreaterThan(firstStatementTime));
    }

    [Test]
    public async Task DefaultNow_OnATypedInsert_WritesTheTransactionTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE logs (id OID PRIMARY KEY, msg STRING, ts DATETIME DEFAULT (now()))");

        List<Dictionary<string, ColumnValue>> values = [];
        for (int i = 0; i < 50; i++)
        {
            values.Add(new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "msg", new(ColumnType.String, "m" + i) },
            });
        }

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.Insert(new InsertTicket(tx, dbname, "logs", values));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        List<long> written = Ticks(await Query(executor, dbname, "SELECT ts FROM logs"), "ts");
        Assert.That(written, Has.Count.EqualTo(50));
        Assert.That(written.Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
    }

    [Test]
    public async Task DefaultNow_OnInsertSelect_WritesOneValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE src (id INT64 PRIMARY KEY)");
        await ExecuteDdl(executor, dbname, "CREATE TABLE dst (id INT64 PRIMARY KEY, ts DATETIME DEFAULT (now()))");
        await ExecuteNonQuery(executor, dbname, InsertRows("src", "id", 1, 400, id => id.ToString()));

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await NonQueryIn(executor, tx, dbname, "INSERT INTO dst (id) SELECT id FROM src");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        List<long> written = Ticks(await Query(executor, dbname, "SELECT ts FROM dst"), "ts");
        Assert.That(written, Has.Count.EqualTo(400));
        Assert.That(written.Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
    }

    [Test]
    public async Task AddColumnDefaultNow_GivesEveryExistingRowOneTime_AcrossFillBatches()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY)");

        // More rows than one fill batch holds, so the fill commits more than one transaction.
        await ExecuteNonQuery(executor, dbname, InsertRows("items", "id", 1, 600, id => id.ToString()));

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN created DATETIME NOT NULL DEFAULT (now())");

        List<long> filled = Ticks(await Query(executor, dbname, "SELECT created FROM items"), "created");
        Assert.That(filled, Has.Count.EqualTo(600));
        Assert.That(filled.Distinct().Count(), Is.EqualTo(1));
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    [TestCase(false)]
    [TestCase(true)]
    public async Task EqualityOnNow_FindsTheRowsTheSameTransactionWrote(bool indexed)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, ts DATETIME)");

        // With the index, the planner reads now() once as the seek key; without it, the filter reads it
        // once per row. Both must see the value the inserts wrote.
        if (indexed)
            await ExecuteDdl(executor, dbname, "CREATE INDEX t_ts ON t (ts)");

        await ExecuteNonQuery(executor, dbname, InsertRows("t", "id, ts", 1, 5, id => $"{id}, '2020-01-01T00:00:00Z'"));

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await NonQueryIn(executor, tx, dbname, InsertRows("t", "id, ts", 100, 20, id => $"{id}, now()"));

            await Task.Delay(30);

            List<Dictionary<string, ColumnValue>> rows = await QueryIn(executor, tx, dbname, "SELECT id FROM t WHERE ts = now()");
            Assert.That(rows.Select(r => r["id"].LongValue).OrderBy(id => id), Is.EqualTo(Enumerable.Range(100, 20).Select(i => (long)i)));

            List<Dictionary<string, ColumnValue>> older = await QueryIn(executor, tx, dbname, "SELECT id FROM t WHERE ts < now()");
            Assert.That(older, Has.Count.EqualTo(5));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task RangeOnNow_IsAnIndexRange_InASingleTableQueryAndInAJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, ts DATETIME, n INT64)");
        await ExecuteDdl(executor, dbname, "CREATE INDEX t_ts ON t (ts)");
        await ExecuteNonQuery(executor, dbname, InsertRows("t", "id, ts, n", 1, 2000, id => $"{id}, '2020-01-01T00:00:00Z', {id}"));
        await ExecuteNonQuery(executor, dbname, "INSERT INTO t (id, ts, n) VALUES (100000, '2999-01-01T00:00:00Z', 1)");

        // The access path is chosen by cost, so the table needs statistics that show a range on ts
        // is selective.
        await Query(executor, dbname, "ANALYZE t");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            List<Dictionary<string, ColumnValue>> plan = await QueryIn(executor, tx, dbname, "EXPLAIN SELECT id FROM t WHERE ts > now()");
            string[] nodes = plan.Select(r => r["node"].StrValue!).ToArray();
            Assert.That(nodes, Does.Contain("index-range-scan"), string.Join(", ", nodes));

            List<Dictionary<string, ColumnValue>> rows = await QueryIn(executor, tx, dbname, "SELECT id FROM t WHERE ts > now()");
            Assert.That(rows.Select(r => r["id"].LongValue).ToArray(), Is.EqualTo(new[] { 100000L }));

            // A join leaf takes its bound from now() as it takes one from a literal.
            const string join = "SELECT a.id, b.id AS other FROM t a JOIN t b ON a.n = b.id WHERE a.ts > now()";
            List<Dictionary<string, ColumnValue>> joinPlan = await QueryIn(executor, tx, dbname, "EXPLAIN " + join);
            string[] joinNodes = joinPlan.Select(r => r["node"].StrValue!).ToArray();
            Assert.That(joinNodes, Does.Contain("index-range-scan"), string.Join(", ", joinNodes));
            Assert.That(joinNodes, Does.Not.Contain("table-scan"), string.Join(", ", joinNodes));

            List<Dictionary<string, ColumnValue>> joined = await QueryIn(executor, tx, dbname, join);
            Assert.That(joined, Has.Count.EqualTo(1));
            Assert.That(joined[0]["other"].LongValue, Is.EqualTo(1L));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task AsOfSystemTime_Now_IsStillTheTransactionStart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecuteNonQuery(executor, dbname, InsertRows("t", "id", 1, 10, id => id.ToString()));

        // The snapshot must be later than the insert, or the table reads as empty.
        await Task.Delay(600);

        // A time-travel read runs only as an autocommit read-only SELECT.
        KvTransaction tx = KvTransaction.CreateReadOnly();
        try
        {
            List<Dictionary<string, ColumnValue>> rows = await QueryIn(executor, tx, dbname,
                "SELECT id, now() AS n FROM t AS OF SYSTEM TIME '-200ms'");

            Assert.That(rows, Has.Count.EqualTo(10));
            Assert.That(Ticks(rows, "n").Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task ViewThatCallsNow_ReportsTheTransactionTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecuteNonQuery(executor, dbname, InsertRows("t", "id", 1, 50, id => id.ToString()));
        await ExecuteDdl(executor, dbname, "CREATE VIEW stamped AS SELECT id, now() AS ts, statement_timestamp() AS st FROM t");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await Task.Delay(30);

            // The statement names no clock function; only the view body does.
            List<Dictionary<string, ColumnValue>> rows = await QueryIn(executor, tx, dbname, "SELECT id, ts, st FROM stamped");
            Assert.That(rows, Has.Count.EqualTo(50));
            Assert.That(Ticks(rows, "ts").Distinct().Single(), Is.EqualTo(tx.StartTime.Ticks));
            Assert.That(Ticks(rows, "st").Distinct().Count(), Is.EqualTo(1));

            // The view's now() and the outer statement's now() are one value.
            List<Dictionary<string, ColumnValue>> matching = await QueryIn(executor, tx, dbname, "SELECT id FROM stamped WHERE ts = now()");
            Assert.That(matching, Has.Count.EqualTo(50));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
