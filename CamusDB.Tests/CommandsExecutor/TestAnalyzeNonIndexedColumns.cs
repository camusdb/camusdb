/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>ANALYZE</c> collects a distinct count, bounds and a histogram for the columns without an
/// index too, up to <see cref="CamusDBOptions.StatsAnalyzeMaxColumns"/> columns, the indexed
/// leading columns first. The estimator then reads them for a filter on such a column instead of
/// the default selectivity. The table here has 1,000 rows: <c>region</c> holds 50 values of 20
/// rows, <c>score</c> runs from 0 to 999, and only <c>year</c> has an index.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestAnalyzeNonIndexedColumns : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    [Test]
    public async Task Analyze_CollectsStatisticsForANonIndexedColumn_AndTheEstimateReadsThem()
    {
        Db db = await CreateCities(Options);
        TableDescriptor table = await db.Database.TableDescriptors["cities"];

        QueryResultRow result = await Analyze(db, "cities");
        Assert.AreEqual(4, result.Row["columns"].LongValue, "id, year, region and score");

        Assert.AreEqual(50, db.Executor.Statistics.GetColumnNdv(db.Database, table, "region"));
        Assert.IsNotNull(db.Executor.Statistics.GetColumnHistogram(db.Database, table, "score"), "a histogram for the column without an index");
        ColumnMinMax? score = db.Executor.Statistics.GetColumnMinMax(db.Database, table, "score");
        Assert.AreEqual(0, score!.Min!.LongValue);
        Assert.AreEqual(999, score.Max!.LongValue);

        // One region of fifty: 20 rows, where the default of ten percent would say 100.
        long equality = await FilterEstimate(db, "SELECT id FROM cities WHERE region = 'region 7'");
        Assert.That(equality, Is.InRange(10, 40), "the estimate reads the distinct count of region");

        // Half the scores: 500 rows from the histogram, where the default would say 100.
        long range = await FilterEstimate(db, "SELECT id FROM cities WHERE score < 500");
        Assert.That(range, Is.InRange(400, 600), "the estimate reads the histogram of score");
    }

    /// <summary>
    /// A list on a joined table names the column through its alias. The estimate must read the
    /// same distinct count as a single comparison: three regions of fifty are 60 rows, where the
    /// default of ten percent for each value would say 300.
    /// </summary>
    [Test]
    public async Task Analyze_AnInListOnAQualifiedColumn_ReadsTheDistinctCount()
    {
        Db db = await CreateCities(Options);
        await Analyze(db, "cities");

        long estimate = await FilterEstimate(db, "SELECT c.id FROM cities c WHERE c.region IN ('region 1', 'region 2', 'region 3')");
        Assert.That(estimate, Is.InRange(40, 90), "three values over fifty distinct regions");
    }

    [Test]
    public async Task Analyze_StopsAtTheColumnCap_AfterTheIndexedColumns()
    {
        Db db = await CreateCities(Options with { StatsAnalyzeMaxColumns = 3 });
        TableDescriptor table = await db.Database.TableDescriptors["cities"];

        QueryResultRow result = await Analyze(db, "cities");
        Assert.AreEqual(3, result.Row["columns"].LongValue);

        // The primary key and the indexed year come first, then region, the first declared column
        // without an index; score is past the cap.
        Assert.IsNotNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "id"));
        Assert.IsNotNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "year"));
        Assert.IsNotNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "region"));
        Assert.IsNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "score"));
        Assert.IsNull(db.Executor.Statistics.GetColumnHistogram(db.Database, table, "score"));
    }

    [Test]
    public async Task Analyze_CapZero_CollectsTheIndexedColumnsOnly()
    {
        Db db = await CreateCities(Options with { StatsAnalyzeMaxColumns = 0 });
        TableDescriptor table = await db.Database.TableDescriptors["cities"];

        QueryResultRow result = await Analyze(db, "cities");
        Assert.AreEqual(2, result.Row["columns"].LongValue);

        Assert.IsNotNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "year"));
        Assert.IsNull(db.Executor.Statistics.GetColumnNdv(db.Database, table, "region"));

        long equality = await FilterEstimate(db, "SELECT id FROM cities WHERE region = 'region 7'");
        Assert.AreEqual(100, equality, "without statistics the filter takes the default of ten percent");
    }

    [Test]
    public async Task BackgroundAnalyze_CollectsTheNonIndexedColumnsToo()
    {
        Db db = await CreateCities(Options);
        TableDescriptor table = await db.Database.TableDescriptors["cities"];

        await db.Executor.RunBackgroundAnalyzeForTestsAsync(db.Database, table, shouldPause: null, CancellationToken.None);

        long? regions = db.Executor.Statistics.GetColumnNdv(db.Database, table, "region");
        Assert.That(regions, Is.Not.Null.And.InRange(45, 55), "the sketch counts the fifty regions within its error");
        Assert.IsNotNull(db.Executor.Statistics.GetColumnHistogram(db.Database, table, "score"));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateCities(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE cities (id int64 primary key, region string, score int64, year int64)");
        await Ddl(db, "CREATE INDEX cities_year ON cities (year)");

        List<string> values = new(1000);
        for (int i = 0; i < 1000; i++)
            values.Add($"({i}, 'region {i % 50}', {i}, {2000 + i % 25})");

        for (int start = 0; start < values.Count; start += 250)
            await Exec(db, "INSERT INTO cities (id, region, score, year) VALUES " + string.Join(", ", values.Skip(start).Take(250)));

        return db;
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task<QueryResultRow> Analyze(Db db, string table)
    {
        List<QueryResultRow> rows = await Query(db, "ANALYZE " + table);
        return rows.Single();
    }

    /// <summary>The largest <c>estimated_rows</c> of the filter nodes of the plan of <paramref name="sql"/>.</summary>
    private static async Task<long> FilterEstimate(Db db, string sql)
    {
        List<QueryResultRow> plan = await Query(db, "EXPLAIN " + sql);
        List<long> filters = plan
            .Where(r => r.Row.TryGetValue("node", out ColumnValue? node) && node.StrValue == "filter")
            .Where(r => r.Row.TryGetValue("estimated_rows", out ColumnValue? rows) && rows.Type == ColumnType.Integer64)
            .Select(r => r.Row["estimated_rows"].LongValue)
            .ToList();

        Assert.IsNotEmpty(filters, "the plan must filter the column without an index:\n" + string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue)));
        return filters.Max();
    }

    private static async Task<List<QueryResultRow>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
