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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for statement statistics and <c>SHOW STATEMENT STATS</c>, driven through
/// <see cref="ExecuteSQLTicket"/> so every statement takes the path a console session takes.
///
/// <para><b>Every test builds its own engine with the configuration it needs.</b> An engine fixes
/// its settings when it is constructed; the live enable is covered separately, through
/// <c>SET CLUSTER SETTING</c>.</para>
///
/// <para>The slow query log stays off in these engines unless a test says otherwise, so the counters
/// prove the probe exists for statement statistics on its own.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestStatementStatsStatements : BaseTest
{
    private CamusDBOptions Recording(int maxShapes = 2000) =>
        Options with { StatementStatsEnabled = true, StatementStatsMaxShapes = maxShapes, SlowQueryLogEnabled = false };

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql, KvTransaction? tx = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx ?? KvTransaction.CreateReadOnly(), database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static Task<List<QueryResultRow>> ServerQueryAsync(CommandExecutor executor, string sql)
        => QueryAsync(executor, "", sql, tx: null!);

    private static async Task NonQueryAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task DdlAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static Task<List<QueryResultRow>> StatsAsync(CommandExecutor executor, string sql = "SHOW STATEMENT STATS")
        => ServerQueryAsync(executor, sql);

    private static List<QueryResultRow> WithSql(IEnumerable<QueryResultRow> rows, string fragment)
        => rows.Where(row => row.Row["sample_sql"].StrValue?.Contains(fragment, StringComparison.OrdinalIgnoreCase) == true).ToList();

    private static long Number(QueryResultRow row, string column) => row.Row[column].LongValue;

    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> SetupAsync(CamusDBOptions options)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await DdlAsync(executor, database, db, "CREATE TABLE robots (id INT64 PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL)");
        await NonQueryAsync(executor, database, db,
            "INSERT INTO robots (id, name, year) VALUES " + string.Join(", ", Enumerable.Range(1, 20).Select(i => $"({i}, 'robot{i}', {2000 + i})")));

        return (db, database, executor);
    }

    [Test]
    public async Task DisabledByDefault_ReturnsNoRowsWithoutError()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Options);

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE year = 2003");

        Assert.IsEmpty(await StatsAsync(executor));
    }

    /// <summary>Twenty runs of one shape with two different literals are one row with twenty calls.</summary>
    [Test]
    public async Task OneShapeWithDifferentLiteralsIsOneRow()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording());

        for (int i = 0; i < 20; i++)
            await QueryAsync(executor, db, $"SELECT name FROM robots WHERE year = {(i % 2 == 0 ? 2003 : 2017)}");

        List<QueryResultRow> rows = WithSql(await StatsAsync(executor), "SELECT name FROM robots WHERE year =");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(20, Number(rows[0], "calls"));
        Assert.AreEqual("select", rows[0].Row["kind"].StrValue);
        Assert.AreEqual(db, rows[0].Row["database"].StrValue);
        Assert.AreEqual(20, Number(rows[0], "rows_returned"));
        Assert.AreEqual(16, rows[0].Row["shape_id"].StrValue!.Length);
        Assert.Greater(rows[0].Row["total_ms"].FloatValue, 0);
        Assert.GreaterOrEqual(rows[0].Row["sum_sq_ms"].FloatValue, 0);
        Assert.LessOrEqual(rows[0].Row["min_ms"].FloatValue, rows[0].Row["max_ms"].FloatValue);
    }

    /// <summary>
    /// The probe is created for statement statistics even with the slow query log off, so the
    /// execution facts are there: a full scan of twenty rows reads twenty rows and counts a full scan.
    /// </summary>
    [Test]
    public async Task RowsReadAndFullScansAreCountedWithTheSlowLogOff()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording());

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE name LIKE 'robot1%'");

        QueryResultRow row = WithSql(await StatsAsync(executor), "LIKE 'robot1%'").Single();
        Assert.AreEqual(20, Number(row, "rows_read"));
        Assert.AreEqual(1, Number(row, "full_scans"));
        Assert.AreEqual(0, Number(row, "spills"));
    }

    [Test]
    public async Task WritesAreRecordedWithTheirKind()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Recording());

        await NonQueryAsync(executor, database, db, "INSERT INTO robots (id, name, year) VALUES (100, 'r100', 2100)");
        await NonQueryAsync(executor, database, db, "UPDATE robots SET name = 'renamed' WHERE id = 100");
        await NonQueryAsync(executor, database, db, "DELETE FROM robots WHERE id = 100");

        List<QueryResultRow> rows = await StatsAsync(executor);

        // The single-row INSERT has the shape of the setup's batch insert — a VALUES list of constants
        // is one shape at any length — so it is counted on that row, whose sample is the setup's text.
        QueryResultRow insert = WithSql(rows, "INSERT INTO robots (id, name, year) VALUES").Single();
        Assert.AreEqual("insert", insert.Row["kind"].StrValue);
        Assert.AreEqual(2, Number(insert, "calls"));
        Assert.AreEqual("update", WithSql(rows, "UPDATE robots SET name").Single().Row["kind"].StrValue);
        Assert.AreEqual("delete", WithSql(rows, "DELETE FROM robots").Single().Row["kind"].StrValue);
        Assert.AreEqual(1, Number(WithSql(rows, "UPDATE robots SET name").Single(), "rows_returned"), "rows affected");
    }

    /// <summary>
    /// A multi-row VALUES list of constants is one shape whatever its length, so batch inserts of
    /// different sizes do not each take a row of their own.
    /// </summary>
    [Test]
    public async Task BatchInsertsOfDifferentSizesShareOneShape()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Recording());

        await NonQueryAsync(executor, database, db, "INSERT INTO robots (id, name, year) VALUES (200, 'a', 1), (201, 'b', 2)");
        await NonQueryAsync(executor, database, db, "INSERT INTO robots (id, name, year) VALUES (202, 'c', 3), (203, 'd', 4), (204, 'e', 5)");

        // The setup's twenty-row insert, the two-row batch and the three-row batch are one shape. The
        // sample is the first text seen for it, which is the setup's.
        QueryResultRow batch = (await StatsAsync(executor)).Single(r => r.Row["kind"].StrValue == "insert");

        Assert.AreEqual(3, Number(batch, "calls"));
        StringAssert.Contains("(1, 'robot1', 2001)", batch.Row["sample_sql"].StrValue);
    }

    [Test]
    public async Task AFailedStatementCountsAsAnError()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording());

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 1");
        Assert.ThrowsAsync<CamusDBException>(async () => await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 1 AND no_such_column = 2"));

        QueryResultRow failed = WithSql(await StatsAsync(executor), "no_such_column").Single();
        Assert.AreEqual(1, Number(failed, "calls"));
        Assert.AreEqual(1, Number(failed, "errors"));
        Assert.AreEqual(0, Number(failed, "retries"), "an unknown column is not retryable");
    }

    /// <summary>
    /// A write that loses a lock conflict fails with a retryable code; it counts as an error and as a
    /// retry. The older transaction holds the row, so the younger one's write dies at once.
    /// </summary>
    [Test]
    public async Task ARetryableFailureCountsAsAnErrorAndARetry()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Recording());

        KvTransaction holder = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(holder, db, "UPDATE robots SET name = 'held' WHERE id = 5", null));

        KvTransaction loser = await database.Transactions.BeginAsync();
        CamusDBException conflict = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(loser, db, "UPDATE robots SET name = 'contended' WHERE id = 5", null)))!;

        await database.Transactions.RollbackAsync(loser);
        await database.Transactions.RollbackAsync(holder);

        Assert.IsTrue(SerializableRetryHelper.IsRetryable(conflict), $"expected a retryable code, got {conflict.Code}");

        QueryResultRow row = WithSql(await StatsAsync(executor), "UPDATE robots SET name =").Single();
        Assert.AreEqual(2, Number(row, "calls"));
        Assert.AreEqual(1, Number(row, "errors"));
        Assert.AreEqual(1, Number(row, "retries"));
    }

    /// <summary>
    /// A new index flips the access path of a shape from a full scan to an index scan; the next
    /// execution counts one plan change, and executions with the same plan count none.
    /// </summary>
    [Test]
    public async Task ANewIndexThatFlipsTheAccessPathCountsOnePlanChange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Recording());
        const string select = "SELECT name FROM robots WHERE year = 2005";

        await QueryAsync(executor, db, select);
        await QueryAsync(executor, db, select);

        QueryResultRow stable = WithSql(await StatsAsync(executor), select).Single();
        Assert.AreEqual(0, Number(stable, "plan_changes"));
        string planBefore = stable.Row["last_plan"].StrValue!;

        await DdlAsync(executor, database, db, "CREATE INDEX year_idx ON robots (year)");
        CollectionAssert.Contains((await QueryAsync(executor, db, "EXPLAIN " + select)).Select(r => r.Row["node"].StrValue), "index-range-scan");

        await QueryAsync(executor, db, select);
        await QueryAsync(executor, db, select);

        QueryResultRow flipped = WithSql(await StatsAsync(executor), select).Single();
        Assert.AreEqual(4, Number(flipped, "calls"));
        Assert.AreEqual(1, Number(flipped, "plan_changes"));
        Assert.AreNotEqual(planBefore, flipped.Row["last_plan"].StrValue);
        Assert.AreEqual(2, Number(flipped, "full_scans"), "only the two runs before the index scanned the table");
    }

    /// <summary>Reading the statistics never adds to them: the statement is not a recorded kind.</summary>
    [Test]
    public async Task TheStatementIsNotInItsOwnOutput()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording());

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 3");
        await StatsAsync(executor);
        List<QueryResultRow> rows = await StatsAsync(executor);

        Assert.IsEmpty(WithSql(rows, "STATEMENT STATS"));
        Assert.IsTrue(rows.All(r => r.Row["kind"].StrValue is "select" or "insert" or "update" or "delete"));
    }

    [Test]
    public async Task LikeFiltersOnTheSampleText()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording());

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 3");
        await QueryAsync(executor, db, "SELECT name FROM robots WHERE year > 2010");

        List<QueryResultRow> rows = await StatsAsync(executor, "SHOW STATEMENT STATS LIKE '%year >%'");

        Assert.AreEqual(1, rows.Count);
        StringAssert.Contains("year > 2010", rows[0].Row["sample_sql"].StrValue);
    }

    /// <summary>
    /// A full table admits no new shape, counts it in <c>statement_stats.dropped</c>, and keeps
    /// counting the shapes it already holds.
    /// </summary>
    [Test]
    public async Task AFullTableDropsNewShapesAndKeepsCountingOldOnes()
    {
        // Three slots: the setup's INSERT takes one, the first two SELECT shapes take the rest.
        (string db, _, CommandExecutor executor) = await SetupAsync(Recording(maxShapes: 3));

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 1");
        await QueryAsync(executor, db, "SELECT name FROM robots WHERE id = 1");
        await QueryAsync(executor, db, "SELECT year FROM robots WHERE id = 1");
        await QueryAsync(executor, db, "SELECT * FROM robots WHERE id = 2");

        List<QueryResultRow> rows = await StatsAsync(executor);
        Assert.AreEqual(3, rows.Count);
        Assert.IsEmpty(WithSql(rows, "SELECT year"));
        Assert.AreEqual(2, Number(WithSql(rows, "SELECT * FROM robots").Single(), "calls"));

        List<QueryResultRow> engine = await ServerQueryAsync(executor, "SHOW ENGINE STATS LIKE 'statement_stats.%'");
        QueryResultRow dropped = engine.Single(r => r.Row["metric"].StrValue == "statement_stats.dropped");
        QueryResultRow shapes = engine.Single(r => r.Row["metric"].StrValue == "statement_stats.shapes");

        Assert.AreEqual("camusdb", dropped.Row["source"].StrValue);
        Assert.AreEqual(1, Number(dropped, "count"));
        Assert.AreEqual(3, (long)shapes.Row["last"].FloatValue);
    }
}
