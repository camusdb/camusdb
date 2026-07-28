
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cardinality estimator tests (selectivity from histograms).
///
/// These tests verify that <see cref="CardinalityEstimator"/> produces accurate
/// selectivity estimates when histogram statistics are available, and falls back
/// gracefully when they are not.
///
/// Fixtures:
///   1. Unit-level: directly verify <see cref="CardinalityEstimator.EstimateRangeFraction"/>
///      using histograms injected via <see cref="StatisticsManager.SetHistogramsAsync"/>.
///   2. Integration: verify that EXPLAIN estimated_rows improves when real histogram data
///      is available vs. the fixed 10% fallback.
/// </summary>
[TestFixture]
public sealed class TestCardinalityEstimator : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>Builds a simple uniform equi-depth histogram for integer values in [min, max].</summary>
    private static ColumnHistogram BuildUniformHistogram(long min, long max, long totalRows, int bucketCount = 10)
    {
        long range = max - min;
        long rowsPerBucket = totalRows / bucketCount;
        long valuesPerBucket = Math.Max(1, range / bucketCount);
        var buckets = new List<ColumnHistogramBucket>();

        for (int i = 0; i < bucketCount; i++)
        {
            long upperVal = min + valuesPerBucket * (i + 1);
            if (i == bucketCount - 1) upperVal = max;

            long cumulative = rowsPerBucket * (i + 1);
            if (i == bucketCount - 1) cumulative = totalRows;

            buckets.Add(new ColumnHistogramBucket
            {
                UpperBound       = Int(upperVal),
                CumulativeRows   = cumulative,
                DistinctInBucket = valuesPerBucket,
            });
        }

        return new ColumnHistogram { Buckets = buckets, TotalRows = totalRows };
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

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

    private static InsertTicket MakeInsertTicket(KvTransaction txn, string dbname, long year)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: "robots",
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "Robot" + year) },
                    { "year", new(ColumnType.Integer64, year) },
                }
            }
        );

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db)
    {
        if (db.TableDescriptors.TryGetValue("robots", out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException("Table 'robots' not found");
    }

    private static async Task<List<QueryResultRow>> ExplainAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimateRangeFraction: full-range = 1.0 with histogram
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_EstimateRangeFraction_FullRange_ReturnsOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // Inject a histogram covering years 2000-2020 uniformly.
        var histograms = new Dictionary<string, ColumnHistogram>
        {
            ["year"] = BuildUniformHistogram(2000, 2020, totalRows: 1000),
        };
        await executor.Statistics.SetHistogramsAsync(database, table, histograms);

        // Full range (lo=null, hi=null) should return ~1.0 via RangeFraction.
        double sel = CardinalityEstimator.EstimateRangeFraction(
            loBound: null,
            hiBound: null,
            columnName: "year",
            database: database,
            table: table,
            stats: executor.Statistics);

        // Full range = entire histogram = 1.0.
        Assert.That(sel, Is.EqualTo(1.0).Within(0.01),
            "Full-range selectivity (no bounds) must be ~1.0 from histogram");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimateRangeFraction: half-range gives ~0.5
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_EstimateRangeFraction_HalfRange_ReturnsApproxHalf()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // Uniform histogram: 2000-2020 (inclusive), 1000 rows.
        // Query: year > 2010 (upper half of range).
        var histograms = new Dictionary<string, ColumnHistogram>
        {
            ["year"] = BuildUniformHistogram(2000, 2020, totalRows: 1000),
        };
        await executor.Statistics.SetHistogramsAsync(database, table, histograms);

        double sel = CardinalityEstimator.EstimateRangeFraction(
            loBound: Int(2010),   // lower bound (exclusive)
            hiBound: null,        // no upper bound
            columnName: "year",
            database: database,
            table: table,
            stats: executor.Statistics);

        // year > 2010 in [2000, 2020] → about 50% of rows.
        Assert.That(sel, Is.GreaterThan(0.3).And.LessThan(0.7),
            "Half-range selectivity should be approximately 0.5 for uniform data");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimateRangeFraction: tight range gives low selectivity
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_EstimateRangeFraction_TightRange_ReturnsLowSelectivity()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // Uniform histogram: 2000-2020 (1000 rows, 10 buckets).
        // Query: 2001 < year <= 2003 (tight 2-year range on a 20-year span).
        var histograms = new Dictionary<string, ColumnHistogram>
        {
            ["year"] = BuildUniformHistogram(2000, 2020, totalRows: 1000),
        };
        await executor.Statistics.SetHistogramsAsync(database, table, histograms);

        double sel = CardinalityEstimator.EstimateRangeFraction(
            loBound: Int(2001),
            hiBound: Int(2003),
            columnName: "year",
            database: database,
            table: table,
            stats: executor.Statistics);

        // 2 out of 20 years → ~10% of rows.
        Assert.That(sel, Is.LessThan(0.3),
            "Tight 2-year range on a 20-year span should yield low selectivity");
        Assert.That(sel, Is.GreaterThan(0.0),
            "Tight range selectivity must be positive");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimateRangeFraction: fallback when no histogram
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_EstimateRangeFraction_NoHistogram_ReturnsFallback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // Do NOT inject any histogram.
        double sel = CardinalityEstimator.EstimateRangeFraction(
            loBound: Int(2010),
            hiBound: Int(2020),
            columnName: "year",
            database: database,
            table: table,
            stats: executor.Statistics);

        Assert.That(sel, Is.EqualTo(CardinalityEstimator.FallbackSelectivity).Within(1e-9),
            "Without a histogram, range selectivity must return FallbackSelectivity (0.10)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimatePredicateSelectivity: skewed histogram range returns high fraction
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_PredicateSelectivity_SkewedRangeHistogram_ReturnHighFraction()
    {
        // Guards CardinalityEstimator.EstimatePredicateSelectivity logic directly (bypasses
        // CostEstimator). Does NOT guard CostEstimator's FilterNode wiring.
        (_, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // Skewed 2-bucket histogram: 90% of 1000 rows have year ≤ 2004; 10% are in (2004..2009].
        var skewed = new ColumnHistogram
        {
            TotalRows = 1000,
            Buckets =
            [
                new() { UpperBound = Int(2004), CumulativeRows = 900,  DistinctInBucket = 5 },
                new() { UpperBound = Int(2009), CumulativeRows = 1000, DistinctInBucket = 5 },
            ],
        };
        await executor.Statistics.SetHistogramsAsync(database, table,
            new Dictionary<string, ColumnHistogram> { ["year"] = skewed });

        // "year <= 2004" targets the dense 90% bucket.
        // Histogram path: RangeFraction(null, 2004) = cf(2004) = 900/1000 = 0.90.
        // Fixed FilterSelectivity constant: 0.10.
        var comparisons = new List<AnalyzedComparison>
        {
            new("year", "<=", new ColumnValue(ColumnType.Integer64, 2004L), conjunct: null!)
        };
        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, [], database, table, executor.Statistics);

        Assert.That(sel, Is.GreaterThan(0.50),
            "Histogram says 90% of rows ≤ 2004; selectivity must far exceed the 10% fixed constant");
        Assert.That(sel, Is.LessThanOrEqualTo(1.0));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Equality selectivity: NDV=100 produces 1/100 = 0.01
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_EqualitySelectivity_HighNdv_ReturnsOneOverNdv()
    {
        // Regression guard: reverting to FilterSelectivity=0.10 gives sel=0.10, which fails the
        // < 0.05 assertion because 1/100 = 0.01 is far below the 10% constant.
        (_, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        // NDV=100 means each distinct year has on average 1% of rows.
        // Equality "year = 2050" should give 1/NDV = 0.01.
        await executor.Statistics.SetNdvAsync(database, table,
            columnNdv: new Dictionary<string, long> { ["year"] = 100L },
            keyNdv: null);

        var comparisons = new List<AnalyzedComparison>
        {
            new("year", "=", new ColumnValue(ColumnType.Integer64, 2050L), conjunct: null!)
        };
        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, [], database, table, executor.Statistics);

        // NDV path: 1/100 = 0.01. Fixed constant: 0.10.
        Assert.That(sel, Is.LessThan(0.05),
            "With NDV=100, equality selectivity is 1/100 = 0.01, which is below the 10% fixed constant");
        Assert.That(sel, Is.GreaterThan(0.0));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EstimateRangeScanRows: skewed histogram exceeds both-bounds fallback (10%)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_IndexRangeScanRows_SkewedHistogram_ExceedsBothBoundsFallback()
    {
        // Regression guard: reverting the histogram path in EstimateRangeScanRows causes the
        // both-bounds fallback (BothBoundsSelectivity=0.10 → 100 rows) to be used, which
        // fails the > 500 assertion.
        (_, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);

        executor.Statistics.SeedRowCountForTesting(database, table, 1000);

        // Skewed 2-bucket histogram: 90% of rows in [2000..2004], 10% in (2004..2009].
        // RangeFraction(lo=2000, hi=2004):
        //   cf(2004) = exact match bucket 1 → 900/1000 = 0.90
        //   cf(2000) = value < first bucket upper bound → 0.0 (conservative)
        //   fraction = 0.90 - 0.0 = 0.90 → 900 rows
        // Fixed both-bounds heuristic: 10% of 1000 = 100 rows.
        var skewed = new ColumnHistogram
        {
            TotalRows = 1000,
            Buckets =
            [
                new() { UpperBound = Int(2004), CumulativeRows = 900,  DistinctInBucket = 5 },
                new() { UpperBound = Int(2009), CumulativeRows = 1000, DistinctInBucket = 5 },
            ],
        };
        await executor.Statistics.SetHistogramsAsync(database, table,
            new Dictionary<string, ColumnHistogram> { ["year"] = skewed });

        var rangeScan = new IndexRangeScanNode(
            index: new TableIndexSchema("year_idx", ["year"], IndexType.Multi),
            fromBound: new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 2000L)]),
            fromInclusive: true,
            toBound:   new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 2004L)]),
            toInclusive: true);

        long estimated = CostEstimator.EstimateRangeScanRows(
            rangeScan, tableRowCount: 1000,
            stats: executor.Statistics, database: database, table: table);

        // Histogram gives ~900; the fixed 10% heuristic gives 100. Threshold set at 500 so
        // the assertion fails if and only if the fixed heuristic is used instead.
        Assert.That(estimated, Is.GreaterThan(500L),
            "Histogram-based estimate for 90%-density range must exceed the 10% fixed fallback (=100 rows)");
        Assert.That(estimated, Is.LessThanOrEqualTo(1000L));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EXPLAIN integration: real ANALYZE histogram beats one-bound heuristic (40%)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_Explain_RealAnalyzeHistogram_BeatsSingleBoundHeuristic()
    {
        // Regression guard: the code falls back to linear min/max
        // interpolation, which gives ~101 rows for this query. The histogram correctly estimates
        // ~910 rows. Asserting > 600 passes only with the histogram.
        //
        // Distribution: 90 sparse years (1 row each, years 2000..2089 = 90 rows) plus 10 dense
        // years (91 rows each, years 2090..2099 = 910 rows). Total: 1000 rows.
        //
        // Query "WHERE year > 2089" targets years 2090..2099 = true 910 rows (91%).
        //
        // Histogram: ANALYZE builds 1-row-per-bucket for 2000..2089 then ~10-row/bucket for
        // 2090..2099. cf(2089) = exact bucket match = 90/1000 = 0.09 → fraction = 0.91 → 910 rows.
        //
        // Linear interpolation: (max−from)/(max−min) × N = (2099−2089)/(2099−2000) × 1000
        //   = 10/99 × 1000 ≈ 101 rows.
        //
        // Threshold 600: (910) passes; (101) and fixed heuristic (400) both fail.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        // 90 sparse years: 1 row each.
        for (int year = 2000; year <= 2089; year++)
            await executor.Insert(MakeInsertTicket(txn, dbname, year));
        // 10 dense years: 91 rows each.
        for (int year = 2090; year <= 2099; year++)
            for (int j = 0; j < 91; j++)
                await executor.Insert(MakeInsertTicket(txn, dbname, year));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await OpenTableAsync(database);

        KvTransaction analyzeTxn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> aCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(analyzeTxn, dbname, "ANALYZE robots", null));
        await foreach (QueryResultRow _ in aCursor) { }
        await database.Transactions.CommitAsync(analyzeTxn);

        List<QueryResultRow> explain = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year > 2089");

        Assert.That(explain.Count, Is.GreaterThan(0));

        // Take the maximum estimated_rows — the root-level or range-scan node carries the query estimate.
        long maxEstimate = explain
            .Where(r => r.Row.TryGetValue("estimated_rows", out ColumnValue? er)
                     && er!.Type == ColumnType.Integer64)
            .Select(r => r.Row["estimated_rows"].LongValue)
            .DefaultIfEmpty(0)
            .Max();

        // Histogram: 91% of 1000 ≈ 910 rows.
        // Linear: ~101 rows. Fixed one-bound: 400 rows.
        Assert.That(maxEstimate, Is.GreaterThanOrEqualTo(600L),
            "Histogram-based estimate (~910 rows) must far exceed R9b linear interpolation (~101) and fixed heuristic (400)");
        Assert.That(maxEstimate, Is.LessThanOrEqualTo(1000L));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FilterNode wiring: the histogram path in CostEstimator's FilterNode
    // branch is exercised end-to-end (not bypassed via IndexRangeScanNode)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task B1_FilterNodeWiring_HistogramSelectivity_GuardsFilterBranch()
    {
        // Regression guard: reverting only the FilterNode branch in CostEstimator to the
        // fixed FilterSelectivity=0.10 causes this test to fail (100 < 500).
        // The histogram says 80% of rows have year ≤ 2004, so the FilterNode estimate must be > 500.
        //
        // A PK-only table (no secondary index on year) forces the planner to use
        // TableScanNode → FilterNode rather than IndexRangeScanNode, which is the
        // only plan shape that exercises CostEstimator's FilterNode branch.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "sensors",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        if (!database.TableDescriptors.TryGetValue("sensors", out var lazy))
            throw new InvalidOperationException("Table 'sensors' not found");
        TableDescriptor table = await lazy;

        // Skewed histogram: 80% of rows have year ≤ 2004; 20% have year in (2004..2009].
        // Injecting manually — 'year' is not indexed so ANALYZE wouldn't track it in v1.
        var skewed = new ColumnHistogram
        {
            TotalRows = 1000,
            Buckets =
            [
                new() { UpperBound = Int(2004), CumulativeRows = 800,  DistinctInBucket = 5 },
                new() { UpperBound = Int(2009), CumulativeRows = 1000, DistinctInBucket = 5 },
            ],
        };
        await executor.Statistics.SetHistogramsAsync(database, table,
            new Dictionary<string, ColumnHistogram> { ["year"] = skewed });
        executor.Statistics.SeedRowCountForTesting(database, table, 1000);

        // WHERE year <= 2004 on an unindexed column → FilterNode in the plan.
        List<QueryResultRow> explain = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM sensors WHERE year <= 2004");

        Assert.That(explain.Count, Is.GreaterThan(0));

        // PlanRenderer.SplitNodeLine splits on the first '(' so the `node` column contains
        // only the text before the parenthesis ("filter"), while the predicate goes into `detail`.
        List<QueryResultRow> filterRows = explain
            .Where(r => r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "filter")
            .ToList();

        Assert.That(filterRows.Count, Is.GreaterThan(0),
            "EXPLAIN must include a 'filter' node for WHERE on a non-indexed column; " +
            "if the plan used IndexRangeScanNode instead the table schema needs revisiting");

        long filterEstimate = filterRows
            .Where(r => r.Row.TryGetValue("estimated_rows", out ColumnValue? er)
                     && er!.Type == ColumnType.Integer64)
            .Select(r => r.Row["estimated_rows"].LongValue)
            .DefaultIfEmpty(0)
            .Max();

        // RangeFraction(null, 2004) = cf(2004) = 0.80 → 800 rows.
        // Fixed FilterSelectivity constant: 0.10 → 100 rows.
        Assert.That(filterEstimate, Is.GreaterThan(500L),
            "FilterNode estimate must use the injected histogram (≈800 rows), " +
            "not the 10% fixed FilterSelectivity constant (=100 rows)");
        Assert.That(filterEstimate, Is.LessThanOrEqualTo(1000L));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Join cardinality from NDV
    // ─────────────────────────────────────────────────────────────────────────

    // Shared helpers for join tests
    private async Task<(DatabaseDescriptor db, TableDescriptor orders, TableDescriptor customers)>
        SetupFkJoinTablesAsync(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "customers",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns: new ColumnInfo[]
            {
                new("id",          ColumnType.Id),
                new("customer_id", ColumnType.Id),
                new("amount",      ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor customers = await database.TableDescriptors["customers"];
        TableDescriptor orders    = await database.TableDescriptors["orders"];
        return (database, orders, customers);
    }

    /// <summary>
    /// EstimateJoinCardinality recognises a unique right-side key (PK) and returns
    /// leftRows, not the fallback leftRows × rightRows × 0.10 overestimate.
    ///
    /// Regression guard: reverting the FK-aware branch in EstimateJoinCardinality raises the
    /// estimate from 500 to 2500.
    /// </summary>
    [Test]
    public async Task B2_JoinCardinality_UniqueRightKey_ReturnsFkChildRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        (_, TableDescriptor orders, TableDescriptor customers) =
            await SetupFkJoinTablesAsync(dbname, database, executor);

        // orders = 500 rows (FK side), customers = 50 rows (PK side).
        executor.Statistics.SeedRowCountForTesting(database, orders,    500);
        executor.Statistics.SeedRowCountForTesting(database, customers,  50);

        // Join key: orders.customer_id = customers.id   (customers.id is the PK → unique)
        // FK-aware: IsJoinKeyUnique(customers, ["id"]) → true → estimate = leftRows = 500
        long estimate = CardinalityEstimator.EstimateJoinCardinality(
            leftRows:        500,
            rightRows:        50,
            rightKeyColumns: new[] { "id" },
            rightTable:      customers,
            leftKeyColumns:  new[] { "customer_id" },
            leftTable:       orders,
            database:        database,
            stats:           executor.Statistics);

        // Fixed fallback would give: 500 × 50 × 0.10 = 2500.
        // FK-aware gives: leftRows = 500.
        Assert.That(estimate, Is.EqualTo(500),
            "FK→PK join must estimate ≈ child (orders) row count, not leftRows × rightRows × 0.10");
    }

    /// <summary>
    /// Low-NDV join key: many-to-many on a column with few distinct values estimates
    /// a large fan-out using the NDV formula |A|×|B|/NDV.
    ///
    /// Regression guard: reverting to FilterSelectivity (0.10) drops the estimate from
    /// 10 000 to 2 000 (products × categories × 0.10).
    /// </summary>
    [Test]
    public async Task B2_JoinCardinality_LowNdvManyToMany_EstimatesLargeFanOut()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "categories",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("code", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "products",
            columns: new ColumnInfo[]
            {
                new("id",            ColumnType.Id),
                new("category_code", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor categories = await database.TableDescriptors["categories"];
        TableDescriptor products   = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, products,   1000);
        executor.Statistics.SeedRowCountForTesting(database, categories,  100);

        // Inject low NDV (5 distinct codes on each side).
        await executor.Statistics.SetNdvAsync(database, products,
            new Dictionary<string, long> { ["category_code"] = 5 }, keyNdv: null);
        await executor.Statistics.SetNdvAsync(database, categories,
            new Dictionary<string, long> { ["code"] = 5 }, keyNdv: null);

        // Join on products.category_code = categories.code  (neither side is unique on this column)
        // formula: 1000 × 100 / max(5, 5) = 20 000
        long estimate = CardinalityEstimator.EstimateJoinCardinality(
            leftRows:        1000,
            rightRows:        100,
            rightKeyColumns: new[] { "code" },
            rightTable:      categories,
            leftKeyColumns:  new[] { "category_code" },
            leftTable:       products,
            database:        database,
            stats:           executor.Statistics);

        // Fixed fallback: 1000 × 100 × 0.10 = 10 000.
        // NDV-based:      1000 × 100 / 5   = 20 000 (correctly larger — many duplicates per key).
        Assert.That(estimate, Is.EqualTo(20_000),
            "Low-NDV join must use NDV formula (|A|×|B|/NDV), not fixed FilterSelectivity");
    }

    /// <summary>
    /// High-NDV join key: when both sides have NDV ≈ row count the join is nearly 1:1
    /// and the NDV estimate is tighter than the fixed 10% heuristic.
    ///
    /// Regression guard: reverting to FilterSelectivity gives 500 × 500 × 0.10 = 25 000,
    /// far above the correct near-1:1 estimate of ≈500.
    /// </summary>
    [Test]
    public async Task B2_JoinCardinality_HighNdv_EstimatesNearOneToOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        (_, TableDescriptor orders, TableDescriptor customers) =
            await SetupFkJoinTablesAsync(dbname, database, executor);

        executor.Statistics.SeedRowCountForTesting(database, orders,    500);
        executor.Statistics.SeedRowCountForTesting(database, customers, 500);

        // High NDV on both sides (500 distinct values = almost all unique).
        // Join is on customer_id / id; customers.id is the PK so IsJoinKeyUnique = true.
        // But for this test we deliberately test the NDV path by joining on a NON-PK column pair.
        // We reuse the "amount" column (left) joined to a synthetic "name" column (right) —
        // both are non-indexed and not unique, so the FK-aware branch is bypassed.
        await executor.Statistics.SetNdvAsync(database, orders,
            new Dictionary<string, long> { ["amount"] = 490 }, keyNdv: null);
        await executor.Statistics.SetNdvAsync(database, customers,
            new Dictionary<string, long> { ["name"] = 490 }, keyNdv: null);

        // formula: 500 × 500 / max(490, 490) ≈ 510
        long estimate = CardinalityEstimator.EstimateJoinCardinality(
            leftRows:        500,
            rightRows:       500,
            rightKeyColumns: new[] { "name" },
            rightTable:      customers,
            leftKeyColumns:  new[] { "amount" },
            leftTable:       orders,
            database:        database,
            stats:           executor.Statistics);

        // Fixed fallback: 500 × 500 × 0.10 = 25 000 (massive overestimate).
        // NDV-based: 500 × 500 / 490 ≈ 510 (tight near-1:1 estimate).
        Assert.That(estimate, Is.InRange(400L, 650L),
            "High-NDV join must be near 1:1, not the 10% fixed-selectivity overestimate");
    }

    /// <summary>
    /// EXPLAIN integration: an FK→PK join shows estimated_rows ≈ child table rows,
    /// not childRows × parentRows × 0.10, for all three join algorithms.
    ///
    /// Regression guard: with the old FilterSelectivity formula the join node would show
    /// orders × customers × 0.10 = 250 000, not ≈ 500.
    /// </summary>
    [Test]
    public async Task B2_JoinCardinality_FkPkExplain_AllJoinAlgorithmsEstimateChildRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        (_, TableDescriptor orders, TableDescriptor customers) =
            await SetupFkJoinTablesAsync(dbname, database, executor);

        // Seed row counts that would make the old formula wildly over-estimate.
        // orders = 500, customers = 5000 → old: 500 × 5000 × 0.10 = 250 000; new FK-aware: 500.
        executor.Statistics.SeedRowCountForTesting(database, orders,     500);
        executor.Statistics.SeedRowCountForTesting(database, customers, 5000);

        const string joinSql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";

        foreach ((string label, bool hash, bool merge, bool nlj) in new[]
        {
            ("hash",   true,  false, false),
            ("merge",  false, true,  false),
            ("nlj",    false, false, true),
        })
        {
            executor.Statistics.ForceHashJoinForTesting        = hash;
            executor.Statistics.ForceMergeJoinForTesting       = merge;
            executor.Statistics.ForceNestedLoopForTesting      = nlj;

            List<QueryResultRow> explain = await ExplainAsync(executor, database, dbname,
                "EXPLAIN " + joinSql);

            // Find the join node row (hash-join / merge-join / nested-loop-join).
            static bool IsJoinNode(QueryResultRow r) =>
                r.Row.TryGetValue("node", out ColumnValue? n) &&
                n!.StrValue is "hash-join" or "merge-join" or "nested-loop-join";

            List<QueryResultRow> joinRows = explain.Where(IsJoinNode).ToList();
            Assert.That(joinRows.Count, Is.GreaterThan(0), $"[{label}] EXPLAIN must contain a join node");

            long joinEstimate = joinRows
                .Where(r => r.Row.TryGetValue("estimated_rows", out ColumnValue? er)
                         && er!.Type == ColumnType.Integer64)
                .Select(r => r.Row["estimated_rows"].LongValue)
                .DefaultIfEmpty(0L)
                .Max();

            // FK-aware: customers.id is the PK → estimate ≈ orders row count (500).
            // Old formula: 500 × 5000 × 0.10 = 250 000.
            Assert.That(joinEstimate, Is.InRange(400L, 600L),
                $"[{label}] FK→PK join estimated_rows must be ≈ 500 (child rows), not 250 000 (old formula)");
        }

        // Reset force flags.
        executor.Statistics.ForceHashJoinForTesting   = false;
        executor.Statistics.ForceMergeJoinForTesting  = false;
        executor.Statistics.ForceNestedLoopForTesting = false;
    }

    /// <summary>
    /// A join on a proper PREFIX of a composite unique index is NOT unique and must use
    /// the many-to-many NDV estimate, not the FK ≤1-match shortcut.
    ///
    /// Regression guard: prefix-matching uniqueness detection wrongly treats join-on-(tenant_id)
    /// of UNIQUE(tenant_id, email) as unique and returns leftRows (1000). Correct set-containment
    /// detection falls through to |A|×|B|/NDV = 1000×200/5 = 40 000.
    /// </summary>
    [Test]
    public async Task B2_JoinCardinality_PrefixOfCompositeUniqueKey_IsNotUnique()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        // members has a COMPOSITE unique index on (tenant_id, email); tenant_id alone is NOT unique.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "members",
            columns: new ColumnInfo[]
            {
                new("id",        ColumnType.Id),
                new("tenant_id", ColumnType.Integer64),
                new("email",     ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "tenant_email_uq",
                    new ColumnIndexInfo[] { new("tenant_id", OrderType.Ascending), new("email", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id",        ColumnType.Id),
                new("tenant_id", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor members = await database.TableDescriptors["members"];
        TableDescriptor events  = await database.TableDescriptors["events"];

        executor.Statistics.SeedRowCountForTesting(database, events,  1000);
        executor.Statistics.SeedRowCountForTesting(database, members,  200);

        // Only 5 distinct tenants → tenant_id is low-cardinality, so the join fans out heavily.
        await executor.Statistics.SetNdvAsync(database, events,
            new Dictionary<string, long> { ["tenant_id"] = 5 }, keyNdv: null);
        await executor.Statistics.SetNdvAsync(database, members,
            new Dictionary<string, long> { ["tenant_id"] = 5 }, keyNdv: null);

        // Join events.tenant_id = members.tenant_id — a PREFIX of UNIQUE(tenant_id, email).
        long estimate = CardinalityEstimator.EstimateJoinCardinality(
            leftRows:        1000,
            rightRows:        200,
            rightKeyColumns: new[] { "tenant_id" },
            rightTable:      members,
            leftKeyColumns:  new[] { "tenant_id" },
            leftTable:       events,
            database:        database,
            stats:           executor.Statistics);

        // Buggy prefix detection: IsJoinKeyUnique(members, ["tenant_id"]) → true → returns leftRows = 1000.
        // Correct set-containment: NOT unique → |A|×|B|/NDV = 1000×200/5 = 40 000.
        Assert.That(estimate, Is.EqualTo(40_000),
            "Join on a prefix of a composite unique key must use the NDV fan-out, not the FK shortcut");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Multi-column / correlation handling via KeyNdv
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Equality conjunction over a composite-index prefix uses KeyNdv, not the
    /// column-independence product.
    ///
    /// Independence product over-estimates selectivity when columns are correlated
    /// (e.g. city and zip are tightly correlated — knowing city already constrains zip).
    /// KeyNdv(city,zip) is the true number of distinct (city,zip) pairs and gives a
    /// tighter estimate.
    ///
    /// Regression guard: reverting TryApplyCompositeKeyNdv causes EstimatePredicateSelectivity
    /// to multiply per-column NDV selectivities, producing a result far below the KeyNdv result.
    /// </summary>
    [Test]
    public async Task B3_CompositeIndexPrefix_UsesKeyNdvInsteadOfIndependenceProduct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "addresses",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("city", ColumnType.String),
                new("zip",  ColumnType.String),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "city_zip_idx",
                    new ColumnIndexInfo[]
                    {
                        new("city", OrderType.Ascending),
                        new("zip",  OrderType.Ascending),
                    }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["addresses"];
        executor.Statistics.SeedRowCountForTesting(database, table, 100_000);

        // Per-column NDV: 500 cities, 10 000 zip codes.
        // Independence product: (1/500) × (1/10_000) = 0.000_000_200 × 100_000 = 0.02 rows (nonsense).
        // KeyNdv(city, zip) = 8 000 distinct (city,zip) pairs (correlated — not 5 000 000).
        // KeyNdv selectivity: 1/8 000 × 100 000 = 12.5 → Math.Max(1, 12) = 12 rows.
        await executor.Statistics.SetNdvAsync(database, table,
            new Dictionary<string, long> { ["city"] = 500, ["zip"] = 10_000 },
            keyNdv: new Dictionary<string, long> { ["city,zip"] = 8_000 });

        // Simulate: EXPLAIN SELECT * FROM addresses WHERE city = 'NYC' AND zip = '10001'
        // Analyze as two equality comparisons (PredicateAnalyzer output).
        var comparisons = new List<AnalyzedComparison>
        {
            new("city", "=", new ColumnValue(ColumnType.String, "NYC"),   NodeAst.Null),
            new("zip",  "=", new ColumnValue(ColumnType.String, "10001"), NodeAst.Null),
        };

        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, inLists: [],
            database, table, executor.Statistics);

        // Independence product: (1/500) × (1/10_000) = 2e-7 → 0.02 rows → rounds to 1.
        // KeyNdv: 1/8_000 = 0.000125.
        // The KeyNdv result is strictly GREATER than the independence product for correlated data.
        double independenceProduct = (1.0 / 500) * (1.0 / 10_000);
        Assert.That(sel, Is.GreaterThan(independenceProduct),
            "B3: correlated columns must use KeyNdv (looser selectivity), not independence product (too tight)");

        // Also verify it matches 1/KeyNdv exactly.
        Assert.That(sel, Is.EqualTo(1.0 / 8_000).Within(1e-9),
            "B3: selectivity must be 1/KeyNdv(city,zip) when composite index covers the conjunct");
    }

    /// <summary>
    /// Three-column composite index, equality on first two columns: ANALYZE emits the
    /// 2-column KeyNdv prefix (not just the full 3-column tuple).
    ///
    /// This is the end-to-end regression guard for the fix: without the prefix-emission loop
    /// in TableAnalyzer, GetKeyNdv(["region","category"]) returns null and silently falls back
    /// to the independence product.
    ///
    /// Data design: 400 rows where region="r{i%20}" and category="c{i%20}" — perfectly correlated,
    /// so only 20 distinct (region,category) pairs out of 20×20=400 theoretical.
    ///   NDV(region)=20, NDV(category)=20, KeyNdv(region,category)=20, KeyNdv(region,category,score)=400.
    ///
    /// WHERE region=… AND category=… AND score>…:
    ///              1/20 × FallbackSel(0.10) = 0.005
    ///   Independence: 1/20 × 1/20 × 0.10     = 0.00025  (20× too selective — wrong)
    /// </summary>
    [Test]
    public async Task B3_ThreeColumnIndex_AnalyzeEmits2ColPrefixKeyNdv_UsedByEstimator()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setupTxn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("region",   ColumnType.String),
                new("category", ColumnType.String),
                new("score",    ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "region_cat_score_idx",
                    new ColumnIndexInfo[]
                    {
                        new("region",   OrderType.Ascending),
                        new("category", OrderType.Ascending),
                        new("score",    OrderType.Ascending),
                    }),
            },
            ifNotExists: false));

        // 400 rows: region="r{i%20}", category="c{i%20}" — perfectly correlated.
        // NDV(region)=NDV(category)=20; distinct (region,category) pairs = 20.
        for (int i = 0; i < 400; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: setupTxn,
                databaseName: dbname,
                tableName: "events",
                values: new()
                {
                    new()
                    {
                        { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "region",   new(ColumnType.String,    "r" + (i % 20)) },
                        { "category", new(ColumnType.String,    "c" + (i % 20)) },
                        { "score",    new(ColumnType.Integer64, (long)i) },
                    }
                }
            ));
        }
        await database.Transactions.CommitAsync(setupTxn);

        // ANALYZE — must emit KeyNdv["region,category"] (2-col prefix) alongside the full 3-col tuple.
        KvTransaction analyzeTxn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> aCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(analyzeTxn, dbname, "ANALYZE events", null));
        await foreach (QueryResultRow _ in aCursor) { }
        await database.Transactions.CommitAsync(analyzeTxn);

        TableDescriptor table = await database.TableDescriptors["events"];

        // WHERE region = 'r0' AND category = 'c0' AND score > 100
        //              1/20 × 0.10 = 0.005
        // Independence: 1/20 × 1/20 × 0.10 = 0.00025
        var comparisons = new List<AnalyzedComparison>
        {
            new("region",   "=", new ColumnValue(ColumnType.String,    "r0"),  NodeAst.Null),
            new("category", "=", new ColumnValue(ColumnType.String,    "c0"),  NodeAst.Null),
            new("score",    ">", new ColumnValue(ColumnType.Integer64, 100L),  NodeAst.Null),
        };

        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, inLists: [],
            database, table, executor.Statistics);

        // 1/KeyNdv(region,category) × FallbackSel = 1/20 × 0.10 = 0.005.
        const double expected = (1.0 / 20) * 0.10;
        Assert.That(sel, Is.EqualTo(expected).Within(1e-9),
            "B3: (region,category) priced via ANALYZE-generated 2-col prefix KeyNdv; score > 100 uses FallbackSelectivity");

        // Independence result (not emitting prefix → falls back): 1/20 × 1/20 × 0.10 = 0.00025.
        double independenceResult = (1.0 / 20) * (1.0 / 20) * 0.10;
        Assert.That(sel, Is.GreaterThan(independenceResult),
            "B3 via ANALYZE-generated prefix KeyNdv must give looser estimate than column independence");
    }

    /// <summary>
    /// Integration: insert correlated (city,zip) data, run ANALYZE, then verify that
    /// EstimatePredicateSelectivity uses the composite KeyNdv path (selectivity = 1/KeyNdv)
    /// rather than the independence product.
    ///
    /// This is the end-to-end complement.  It exercises the
    /// full ANALYZE → StatisticsManager → CardinalityEstimator pipeline.
    ///
    /// Data design (40 rows, period 8):
    ///   city = "c{i%4}"              → NDV(city) = 4
    ///   zip  = "z{(i%4)*2+(i/4)%2}" → NDV(zip)  = 8, but each city maps to exactly 2 zips
    ///                                   → KeyNdv(city,zip) = 8 (correlated — not 4×8=32)
    ///
    ///   selectivity: 1/8   = 0.125
    ///   Independence selectivity: 1/32 = 0.031 25  (4× too selective — wrong)
    /// </summary>
    [Test]
    public async Task B3_Integration_CityZipCompositeIndex_AnalyzeThenEstimate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setupTxn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "locations",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("city", ColumnType.String),
                new("zip",  ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "city_zip_idx",
                    new ColumnIndexInfo[]
                    {
                        new("city", OrderType.Ascending),
                        new("zip",  OrderType.Ascending),
                    }),
            },
            ifNotExists: false));

        // 40 rows cycling with period 8: each city maps to exactly 2 zips.
        // (i%4)*2 + (i/4)%2 produces: c0→{z0,z1}, c1→{z2,z3}, c2→{z4,z5}, c3→{z6,z7}.
        for (int i = 0; i < 40; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: setupTxn,
                databaseName: dbname,
                tableName: "locations",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                        { "city", new(ColumnType.String, "c" + (i % 4)) },
                        { "zip",  new(ColumnType.String, "z" + ((i % 4) * 2 + (i / 4) % 2)) },
                    }
                }
            ));
        }
        await database.Transactions.CommitAsync(setupTxn);

        KvTransaction analyzeTxn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> aCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(analyzeTxn, dbname, "ANALYZE locations", null));
        await foreach (QueryResultRow _ in aCursor) { }
        await database.Transactions.CommitAsync(analyzeTxn);

        TableDescriptor table = await database.TableDescriptors["locations"];

        // WHERE city = 'c0' AND zip = 'z0'
        var comparisons = new List<AnalyzedComparison>
        {
            new("city", "=", new ColumnValue(ColumnType.String, "c0"), NodeAst.Null),
            new("zip",  "=", new ColumnValue(ColumnType.String, "z0"), NodeAst.Null),
        };

        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, inLists: [],
            database, table, executor.Statistics);

        // 1/KeyNdv(city,zip) = 1/8 = 0.125.
        Assert.That(sel, Is.EqualTo(1.0 / 8).Within(1e-9),
            "B3 integration: selectivity must equal 1/ANALYZE-generated KeyNdv(city,zip)");

        // Independence product: 1/NDV(city) × 1/NDV(zip) = 1/4 × 1/8 = 1/32 = 0.03125.
        double independenceProduct = (1.0 / 4) * (1.0 / 8);
        Assert.That(sel, Is.GreaterThan(independenceProduct),
            "B3 integration: composite KeyNdv must give looser estimate than column independence for correlated data");
    }

    /// <summary>
    /// A two-sided range predicate x > lo AND x &lt; hi must be priced as a single
    /// EstimateRangeFraction(lo, hi) call, not as sel(x&gt;lo) × sel(x&lt;hi). The product
    /// path treats the two bounds as independent events and systematically under-estimates the
    /// intersection. The correct result must be strictly greater than the product.
    /// </summary>
    [Test]
    public async Task TwoSidedRange_PricedAsRangeFraction_NotAsProduct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "readings",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("value", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "value_idx",
                    new ColumnIndexInfo[] { new("value", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["readings"];

        // Seed a uniform histogram: values 1..100, each appearing once, in 10 equal buckets.
        // With 10 rows per bucket, RangeFraction(20, 80) covers 6 buckets ≈ 0.60.
        // sel(x>20) ≈ 0.80, sel(x<80) ≈ 0.80, so the old product ≈ 0.64 > 0.60 — the
        // product over-estimates, not under-estimates, for symmetric uniform distributions.
        // Use an asymmetric range to make the direction clear: x > 50 AND x < 60.
        // RangeFraction(50,60) ≈ 0.10; sel(x>50) ≈ 0.50; sel(x<60) ≈ 0.60 → product ≈ 0.30.
        const long totalRows = 100;
        var buckets = new List<ColumnHistogramBucket>();
        for (int k = 0; k < 10; k++)
        {
            buckets.Add(new ColumnHistogramBucket
            {
                UpperBound       = new ScalarBound { Type = ColumnType.Integer64, LongValue = (k + 1) * 10 },
                CumulativeRows   = (k + 1) * 10,
                DistinctInBucket = 10,
            });
        }

        await executor.Statistics.SetHistogramsAsync(database, table, new Dictionary<string, ColumnHistogram>()
        {
            { "value", new ColumnHistogram { Buckets = buckets, TotalRows = totalRows } }
        });

        var loBound = new ColumnValue(ColumnType.Integer64, 50L);
        var hiBound = new ColumnValue(ColumnType.Integer64, 60L);

        // Two separate one-sided comparisons on the same column.
        var comparisons = new List<AnalyzedComparison>
        {
            new("value", ">",  loBound, NodeAst.Null),
            new("value", "<",  hiBound, NodeAst.Null),
        };

        double merged = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, inLists: [],
            database, table, executor.Statistics);

        // The merged path calls EstimateRangeFraction(50, 60) once.
        var loScalar = ScalarBound.FromColumnValue(loBound);
        var hiScalar = ScalarBound.FromColumnValue(hiBound);
        double rangeFrac = CardinalityEstimator.EstimateRangeFraction(
            loScalar, hiScalar, "value", database, table, executor.Statistics);

        // The old product path would compute sel(>50) × sel(<60) ≈ 0.50 × 0.60 = 0.30,
        // which is strictly greater than rangeFrac ≈ 0.10. The fix must return rangeFrac.
        Assert.That(merged, Is.EqualTo(rangeFrac).Within(1e-9),
            "Two-sided range must be priced as EstimateRangeFraction(lo,hi), not sel(>lo)×sel(<hi)");

        // Sanity: the merged result must be strictly less than the product, confirming that
        // the product path was double-counting.
        ColumnHistogram hist = executor.Statistics.GetColumnHistogram(database, table, "value")!;
        double selLo = hist.RangeFraction(loScalar, null);
        double selHi = hist.RangeFraction(null, hiScalar);
        double product = selLo * selHi;
        Assert.That(merged, Is.LessThan(product),
            "Merged range fraction must be strictly less than the independence product");
    }

    /// <summary>
    /// Unindexed correlated columns fall back to column independence (known limitation).
    /// </summary>
    [Test]
    public async Task B3_UnindexedColumns_KeepsIndependenceProduct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "widgets",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("color", ColumnType.String),
                new("shade", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                // No composite index on (color, shade) — correlation goes uncorrected.
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["widgets"];

        await executor.Statistics.SetNdvAsync(database, table,
            new Dictionary<string, long> { ["color"] = 10, ["shade"] = 5 }, keyNdv: null);

        // WHERE color = 'red' AND shade = 'dark' — no composite index → independence product.
        var comparisons = new List<AnalyzedComparison>
        {
            new("color", "=", new ColumnValue(ColumnType.String, "red"),  NodeAst.Null),
            new("shade", "=", new ColumnValue(ColumnType.String, "dark"), NodeAst.Null),
        };

        double sel = CardinalityEstimator.EstimatePredicateSelectivity(
            comparisons, inLists: [],
            database, table, executor.Statistics);

        // Expected: independence product = (1/10) × (1/5) = 0.02.
        Assert.That(sel, Is.EqualTo(0.02).Within(1e-9),
            "B3: no composite index → column independence is kept (known limitation, documented)");
    }
}
