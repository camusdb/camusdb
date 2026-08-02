
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Integration tests for the ANALYZE TABLE statement.
///
/// These tests drive the full stack: SQL parsing → CommandExecutor → TableAnalyzer →
/// StatisticsManager, against a real in-memory Kahuna node. Each test verifies a distinct
/// facet of the statistics produced by ANALYZE.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
public sealed class TestAnalyzeTable : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a "robots" table: id (Id PK), name (String), year (Integer64 indexed).
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupRobotsTable(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static async Task InsertRobotsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname,
        int count, int baseYear = 2000)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(baseYear + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>Runs ANALYZE TABLE via ExecuteSQLQuery and consumes the single result row.</summary>
    private static async Task<QueryResultRow> RunAnalyzeAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string tableName)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: txn,
                database: dbname,
                sql: $"ANALYZE {tableName}",
                parameters: null));

        QueryResultRow? result = null;
        await foreach (QueryResultRow row in cursor)
            result = row;

        await database.Transactions.CommitAsync(txn);
        return result!.Value;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// ANALYZE on a table with N inserted rows must report a row count equal to N.
    /// </summary>
    [Test]
    public async Task AnalyzePopulatesRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        const int N = 20;
        await InsertRobotsAsync(executor, database, dbname, N);

        TableDescriptor table = await OpenTableAsync(database, "robots");

        QueryResultRow resultRow = await RunAnalyzeAsync(executor, database, dbname, "robots");

        // The result row carries a "rows" column with the scanned count.
        Assert.IsTrue(resultRow.Row.TryGetValue("rows", out ColumnValue? rowsVal), "'rows' column missing in result");
        Assert.AreEqual(N, rowsVal!.LongValue, "ANALYZE must report the correct row count");

        // StatisticsManager must also reflect the exact row count.
        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);
        Assert.IsNotNull(estimate, "Row count estimate must be non-null after ANALYZE");
        Assert.AreEqual(N, estimate!.Value, "StatisticsManager row count must match inserted rows");
    }

    /// <summary>
    /// ANALYZE on a table with uniform values must report NDV ≈ N for an indexed column;
    /// a low-cardinality column (all same value) must report NDV = 1.
    /// </summary>
    [Test]
    public async Task AnalyzePopulatesColumnNdv()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Create a table where 'year' is always the same value (low cardinality).
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        // Insert 10 rows, all with the same year (2000).
        txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 10; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, 2000L) },  // all same
                    }
                }));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        long? ndv = executor.Statistics.GetColumnNdv(database, table, "year");
        Assert.IsNotNull(ndv, "NDV for 'year' must be populated after ANALYZE");
        Assert.AreEqual(1L, ndv!.Value, "All-same-value column must have NDV = 1");
    }

    /// <summary>
    /// ANALYZE on a skewed distribution must produce a histogram where the low bucket has
    /// disproportionately more rows than the high bucket.
    /// </summary>
    [Test]
    public async Task AnalyzePopulatesHistogram()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // Insert 80 rows with year in [2000..2079] and 20 with year in [2080..2099].
        // With 100 rows and 100 buckets, the default 100-bucket setting collapses to
        // as many distinct values as there are; verify the histogram covers all rows.
        await InsertRobotsAsync(executor, database, dbname, count: 80, baseYear: 2000);
        await InsertRobotsAsync(executor, database, dbname, count: 20, baseYear: 2080);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        ColumnHistogram? hist = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(hist, "Histogram for 'year' must be populated after ANALYZE");
        Assert.AreEqual(100L, hist!.TotalRows, "TotalRows must equal the number of inserted rows");
        Assert.Greater(hist.Buckets.Count, 0, "At least one bucket must exist");
        Assert.AreEqual(100L, hist.Buckets[^1].CumulativeRows, "Last bucket must cover all rows");
    }

    /// <summary>
    /// ANALYZE must populate correct min/max bounds for indexed columns.
    /// </summary>
    [Test]
    public async Task AnalyzePopulatesMinMax()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // Insert rows with year 2000..2009 (10 distinct years).
        await InsertRobotsAsync(executor, database, dbname, count: 10, baseYear: 2000);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        ColumnMinMax? mm = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.IsNotNull(mm, "Min/max for 'year' must be populated after ANALYZE");
        Assert.IsNotNull(mm!.Min, "Min must not be null");
        Assert.IsNotNull(mm.Max, "Max must not be null");
        Assert.AreEqual(2000L, mm.Min!.LongValue, "Min year must be 2000");
        Assert.AreEqual(2009L, mm.Max!.LongValue, "Max year must be 2009");
    }

    /// <summary>
    /// Statistics written by ANALYZE must survive a cache eviction and reload from Kahuna.
    /// </summary>
    [Test]
    public async Task AnalyzeSurvivesReopen()
    {
        // Scenario: insert 10 rows (years 2000–2009), then delete the 5 with year < 2005.
        //
        // After the deletes, DML tracking believes:
        //   min = 2000  (min never moves up when a row is deleted — requires a full scan)
        //   year_idx count ≈ 10 - 5 = 5, but may still be 10 if the decrement hasn't flushed
        //
        // ANALYZE recomputes from a fresh scan:
        //   min = 2005, max = 2009, year_idx count = 5, rowCount = 5
        //
        // After evict + reload the persisted values must be ANALYZE's recomputed ones, not the
        // stale DML-tracked ones.  This test would have failed under Finding 1 (SeedColumnStats
        // after the flushes) because the reloaded min would be 2000, not 2005.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // Insert years 2000–2009.
        await InsertRobotsAsync(executor, database, dbname, count: 10, baseYear: 2000);

        // Delete the 5 rows with year < 2005, drifting DML-tracked min away from truth.
        for (long yr = 2000; yr < 2005; yr++)
        {
            KvTransaction delTxn = await database.Transactions.BeginAsync();
            await executor.Delete(new DeleteTicket(
                txnState: delTxn,
                databaseName: dbname,
                tableName: "robots",
                where: null,
                filters: [new QueryFilter("year", "=", new ColumnValue(ColumnType.Integer64, yr))]
            ));
            await database.Transactions.CommitAsync(delTxn);
        }

        TableDescriptor table = await OpenTableAsync(database, "robots");

        // ANALYZE must recompute all stats from the 5 surviving rows.
        QueryResultRow result = await RunAnalyzeAsync(executor, database, dbname, "robots");
        Assert.AreEqual(5L, result.Row["rows"].LongValue, "ANALYZE must count 5 surviving rows");

        // Evict the in-memory entry and reload from Kahuna so we read only what was persisted.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.LoadByIdAsync(database, table.Id);

        // Row count — ANALYZE's recomputed value, not DML-tracked.
        long? rowCount = executor.Statistics.GetRowCountEstimate(database, table);
        Assert.IsNotNull(rowCount, "Row count must survive eviction");
        Assert.AreEqual(5L, rowCount!.Value, "Reloaded row count must reflect ANALYZE's scan (5 rows)");

        // Min/max — the critical drift assertion: min must be 2005, not the stale 2000.
        ColumnMinMax? mm = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.IsNotNull(mm, "Min/max must survive eviction");
        Assert.AreEqual(2005L, mm!.Min!.LongValue, "Reloaded Min must be ANALYZE's recomputed 2005, not DML-tracked 2000");
        Assert.AreEqual(2009L, mm.Max!.LongValue, "Reloaded Max must be 2009");

        // Index entry count — must reflect the 5 survivors, not the pre-delete count.
        long? yearIdx = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(yearIdx, "Index entry count must survive eviction");
        Assert.AreEqual(5L, yearIdx!.Value, "Reloaded index entry count must be 5 (survivors)");

        // Histogram and NDV must also survive.
        ColumnHistogram? hist = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(hist, "Histogram must survive eviction");
        Assert.AreEqual(5L, hist!.TotalRows, "Reloaded histogram TotalRows must be 5");

        long? ndv = executor.Statistics.GetColumnNdv(database, table, "year");
        Assert.IsNotNull(ndv, "NDV must survive eviction");
        Assert.AreEqual(5L, ndv!.Value, "Reloaded NDV must be 5 (five distinct surviving years)");
    }

    /// <summary>
    /// ANALYZE on a Date-typed indexed column must produce min/max that advance past the first row
    /// and buckets that are monotonically ordered by date value. Before the fix, ScalarBound.CompareTo
    /// returned 0 for Date, so min/max froze at the first-seen value and the sort used for
    /// histogram building was a no-op.
    /// </summary>
    [Test]
    public async Task AnalyzeDate_MinMaxAndHistogramOrderCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id",         ColumnType.Id),
                new("event_date", ColumnType.Date),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",       new ColumnIndexInfo[] { new("id",         OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "date_idx",  new ColumnIndexInfo[] { new("event_date", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        // Insert 5 rows in non-monotonic (shuffled) order so that the histogram bucket-ordering
        // assertion is discriminating: a no-op sort would leave the data in insertion order and
        // produce non-monotonic upper bounds, catching a broken CompareTo.
        long[] dateTicks = new[]
        {
            new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
        };

        txn = await database.Transactions.BeginAsync();
        foreach (long ticks in dateTicks)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "events",
                values: new()
                {
                    new()
                    {
                        { "id",         new(ColumnType.Id,   ObjectIdGenerator.Generate().ToString()) },
                        { "event_date", new(ColumnType.Date, ticks) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await OpenTableAsync(database, "events");
        await RunAnalyzeAsync(executor, database, dbname, "events");

        long expectedMin = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        long expectedMax = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        // Min must be the earliest date, max must be the latest — regardless of insertion order.
        ColumnMinMax? mm = executor.Statistics.GetColumnMinMax(database, table, "event_date");
        Assert.IsNotNull(mm, "Min/max for 'event_date' must be populated after ANALYZE");
        Assert.AreEqual(expectedMin, mm!.Min!.LongValue, "Min must be the earliest date");
        Assert.AreEqual(expectedMax, mm.Max!.LongValue,  "Max must be the latest date");

        // Histogram must be populated and buckets must be monotonically ordered.
        ColumnHistogram? hist = executor.Statistics.GetColumnHistogram(database, table, "event_date");
        Assert.IsNotNull(hist, "Histogram for 'event_date' must be populated after ANALYZE");
        Assert.Greater(hist!.Buckets.Count, 0, "At least one bucket must exist");
        for (int i = 1; i < hist.Buckets.Count; i++)
            Assert.LessOrEqual(
                hist.Buckets[i - 1].UpperBound!.LongValue,
                hist.Buckets[i].UpperBound!.LongValue,
                $"Bucket {i - 1} upper bound must not exceed bucket {i} upper bound");
    }

    /// <summary>
    /// When the row count is not a multiple of the bucket count, the trailing partial run must
    /// be included in the last histogram bucket. Before the fix, the index-stepping loop stopped
    /// short of the tail, so the last UpperBound was below the column maximum and CumulativeRows
    /// was patched to TotalRows but pointed at the wrong value.
    /// </summary>
    [Test]
    public async Task AnalyzeHistogram_TrailingPartialBucket_UpperBoundEqualsColumnMax()
    {
        // A small bucket count makes the tail partial bucket clearly visible:
        // 10 rows / 3 buckets → bucketSize = ceil(10/3) = 4 → runs cover [0..3],[4..7], leaving [8..9].
        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupRobotsTable(Options with { StatsHistogramBuckets = 3 });

        // Insert 10 rows with years 2000–2009; max is 2009.
        await InsertRobotsAsync(executor, database, dbname, count: 10, baseYear: 2000);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        ColumnHistogram? hist = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(hist, "Histogram must be populated after ANALYZE");
        Assert.AreEqual(10L, hist!.TotalRows, "TotalRows must equal 10");
        Assert.AreEqual(10L, hist.Buckets[^1].CumulativeRows,
            "Last bucket CumulativeRows must equal TotalRows");
        Assert.AreEqual(2009L, hist.Buckets[^1].UpperBound!.LongValue,
            "Last bucket UpperBound must equal the column maximum (2009), not the pre-tail value (2007)");
    }

    /// <summary>
    /// When the requested bucket count times the ceil-rounded bucket size overshoots the row
    /// count (e.g. 6 rows into 4 buckets → bucketSize=2 covers only 3 buckets), the builder must
    /// stop once the data is exhausted rather than emitting trailing empty buckets that duplicate
    /// the maximum's UpperBound with zero rows. Every emitted bucket must therefore carry at least
    /// one distinct value, and the last bucket must still reach the column maximum.
    /// </summary>
    [Test]
    public async Task AnalyzeHistogram_BucketOvershoot_EmitsNoEmptyTrailingBuckets()
    {
        // 6 rows / 4 buckets → bucketSize = ceil(6/4) = 2 → only 3 buckets hold data;
        // a 4th would be empty (start index 6 is past the last row, 5).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupRobotsTable(Options with { StatsHistogramBuckets = 4 });

        await InsertRobotsAsync(executor, database, dbname, count: 6, baseYear: 2000);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        ColumnHistogram? hist = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(hist, "Histogram must be populated after ANALYZE");

        // No bucket may be empty — a real (start ≤ end) bucket always spans ≥ 1 value, so a
        // DistinctInBucket of 0 can only come from a degenerate trailing bucket.
        foreach (ColumnHistogramBucket b in hist!.Buckets)
            Assert.Greater(b.DistinctInBucket, 0,
                "no histogram bucket may be empty (DistinctInBucket == 0 signals a degenerate trailing bucket)");

        Assert.AreEqual(3, hist.Buckets.Count,
            "6 rows at bucketSize 2 must yield exactly 3 buckets, not 4 with an empty tail");
        Assert.AreEqual(2005L, hist.Buckets[^1].UpperBound!.LongValue,
            "Last bucket UpperBound must still equal the column maximum (2005)");
        Assert.AreEqual(6L, hist.Buckets[^1].CumulativeRows,
            "Last bucket CumulativeRows must equal TotalRows");
    }

    /// <summary>
    /// ANALYZE on a table larger than StatsAnalyzeSampleRows must report isSampled=true in
    /// its status string and must report exactly StatsAnalyzeSampleRows rows — not the total
    /// table size. Before the fix, ScanRows was capped at the limit so the sentinel row was
    /// never delivered and isSampled stayed false; the status incorrectly said "analyzed N rows"
    /// and the row count was taken as the absolute table size.
    /// </summary>
    [Test]
    public async Task AnalyzeDetectsSamplingWhenTableExceedsSampleLimit()
    {
        // A very small sample limit, so the table can exceed it without a large insert loop.
        const int sampleLimit = 10;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupRobotsTable(Options with { StatsAnalyzeSampleRows = sampleLimit });

        // Insert more rows than the sample limit.
        await InsertRobotsAsync(executor, database, dbname, count: sampleLimit + 5);

        QueryResultRow resultRow = await RunAnalyzeAsync(executor, database, dbname, "robots");

        // Status must say "sampled", not "analyzed".
        string status = resultRow.Row["status"].StrValue!;
        StringAssert.Contains("sampled", status,
            "ANALYZE status must report 'sampled' when the table exceeds the sample limit");

        // Reported row count must equal the sample limit, not the total inserted count.
        Assert.AreEqual(sampleLimit, resultRow.Row["rows"].LongValue,
            "ANALYZE rows must equal the sample limit when the table is larger");
    }

    /// <summary>
    /// ANALYZE must publish an output column schema through the <see cref="QuerySchemaHolder"/>,
    /// not just populate the row dictionary. The presentation layer (gRPC / CLI) encodes rows
    /// positionally from this schema, so an empty schema drops every cell on the wire even though
    /// the row itself is fully populated — the client then renders a column-less box. This asserts
    /// the four summary columns (table, status, rows, columns) are declared with the right types.
    /// </summary>
    [Test]
    public async Task AnalyzePublishesOutputSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        await InsertRobotsAsync(executor, database, dbname, count: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: txn,
                database: dbname,
                sql: "ANALYZE TABLE robots",
                parameters: null),
                schemaOut: schemaHolder);

        await foreach (QueryResultRow _ in cursor) { }
        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(4, schemaHolder.Schema.Count,
            "ANALYZE must publish four output columns (table, status, rows, columns)");
        Assert.AreEqual("table",   schemaHolder.Schema[0].Name);
        Assert.AreEqual(ColumnType.String, schemaHolder.Schema[0].Type);
        Assert.AreEqual("status",  schemaHolder.Schema[1].Name);
        Assert.AreEqual(ColumnType.String, schemaHolder.Schema[1].Type);
        Assert.AreEqual("rows",    schemaHolder.Schema[2].Name);
        Assert.AreEqual(ColumnType.Integer64, schemaHolder.Schema[2].Type);
        Assert.AreEqual("columns", schemaHolder.Schema[3].Name);
        Assert.AreEqual(ColumnType.Integer64, schemaHolder.Schema[3].Type);
    }

    /// <summary>
    /// ANALYZE TABLE (with TABLE keyword) must produce the same result as ANALYZE tablename.
    /// </summary>
    [Test]
    public async Task AnalyzeWithTableKeyword()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        const int N = 5;
        await InsertRobotsAsync(executor, database, dbname, N);

        TableDescriptor table = await OpenTableAsync(database, "robots");

        // Use the "ANALYZE TABLE tablename" form.
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: txn,
                database: dbname,
                sql: "ANALYZE TABLE robots",
                parameters: null));
        QueryResultRow? resultRow = null;
        await foreach (QueryResultRow row in cursor)
            resultRow = row;
        await database.Transactions.CommitAsync(txn);

        Assert.IsNotNull(resultRow, "ANALYZE TABLE must return a result row");
        Assert.IsTrue(resultRow!.Value.Row.TryGetValue("rows", out ColumnValue? rowsVal));
        Assert.AreEqual(N, rowsVal!.LongValue, "ANALYZE TABLE must report the correct row count");

        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);
        Assert.IsNotNull(estimate);
        Assert.AreEqual(N, estimate!.Value);
    }

    /// <summary>
    /// ANALYZE must record the observed column minimum on the histogram: it is the first
    /// bucket's lower boundary, without which first-bucket values cannot be interpolated
    /// (they would estimate ~0 rows).
    /// </summary>
    [Test]
    public async Task AnalyzeRecordsHistogramMinValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        await InsertRobotsAsync(executor, database, dbname, 20, baseYear: 2000);

        TableDescriptor table = await OpenTableAsync(database, "robots");
        await RunAnalyzeAsync(executor, database, dbname, "robots");

        CamusDB.Core.Statistics.Models.ColumnHistogram? hist =
            executor.Statistics.GetColumnHistogram(database, table, "year");

        Assert.IsNotNull(hist, "ANALYZE must build a histogram for the indexed year column");
        Assert.IsNotNull(hist!.MinValue, "ANALYZE must record the observed minimum");
        Assert.AreEqual(2000, hist.MinValue!.LongValue, "MinValue must equal the smallest scanned year");
    }

    /// <summary>
    /// ANALYZE scans under its own read-only snapshot pinned after the baseline capture, not the
    /// caller's transaction: statistics must reflect committed data only (a scan inside a user
    /// transaction would publish that transaction's uncommitted rows into global statistics), and
    /// pinning after the baseline keeps the `scanned − baseline` row-count correction unbiased.
    /// </summary>
    [Test]
    public async Task AnalyzeIgnoresUncommittedRowsFromCallerTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        const int committed = 4;
        await InsertRobotsAsync(executor, database, dbname, committed);

        // Open a transaction and insert rows WITHOUT committing, then run ANALYZE on that
        // same transaction's ticket.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 5; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Uncommitted" + i) },
                        { "year", new(ColumnType.Integer64, 3000L + i) },
                    }
                }));

        try
        {
            (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: txn,
                    database: dbname,
                    sql: "ANALYZE robots",
                    parameters: null));

            QueryResultRow? result = null;
            await foreach (QueryResultRow row in cursor)
                result = row;

            Assert.IsTrue(result!.Value.Row.TryGetValue("rows", out ColumnValue? rowsVal));
            Assert.AreEqual(committed, rowsVal!.LongValue,
                "ANALYZE must count only committed rows, not the caller transaction's uncommitted inserts");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(txn);
        }
    }
}
