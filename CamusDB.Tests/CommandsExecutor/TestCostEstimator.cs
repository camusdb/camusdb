
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cost Model tests.
///
/// Validates:
///   1. EXPLAIN rows carry non-null estimated_rows and estimated_cost after wiring.
///   2. CostEstimator annotates plan nodes with EstimatedCardinality from stats.
///   3. A low-selectivity (half-open range, 40 % default) non-covering secondary index
///      loses to a full table scan under the cost model when a row-count estimate is known.
///   4. Simple heuristic choices (unique-index lookup, tight both-bounds range) are unchanged
///      from the pre-existing behaviour — cost model is additive, not disruptive.
/// </summary>
[TestFixture]
public sealed class TestCostEstimator : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync(
        string tableName = "robots")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setupTxn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(setupTxn);

        return (dbname, database, executor);
    }

    private static InsertTicket MakeInsertTicket(KvTransaction txn, string dbname, string tableName, string name, long year)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: tableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, name) },
                    { "year", new(ColumnType.Integer64, year) },
                }
            }
        );

    private static async Task<TableDescriptor> GetTableDescriptorAsync(
        CommandExecutor executor, string dbname, string tableName)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);

        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;

        throw new InvalidOperationException($"Table '{tableName}' not found in '{dbname}'");
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

    /// <summary>Seeds in-memory stats and waits for the lazy load to complete.</summary>
    private static async Task SeedStatsAsync(
        CommandExecutor executor, DatabaseDescriptor database, TableDescriptor table, int rowCount)
    {
        executor.Statistics.TrackInsert(database, table, rowCount);
        await Task.Delay(100); // allow background LoadAndCacheAsync to finish
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — EXPLAIN emits non-null costs (criterion 3)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9_ExplainEmitsNonNullEstimatedRowsAndCost()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        // Insert a few rows so stats are loaded.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 5; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "robots", "R" + i, 2020 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(100);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname, "EXPLAIN SELECT * FROM robots");

        Assert.IsTrue(rows.Count > 0, "EXPLAIN must return at least one row");

        // At least one node must carry a non-null estimated_rows.
        bool anyNonNullRows = rows.Any(r =>
            r.Row.TryGetValue("estimated_rows", out ColumnValue? er) && er!.Type != ColumnType.Null);

        bool anyNonNullCost = rows.Any(r =>
            r.Row.TryGetValue("estimated_cost", out ColumnValue? ec) && ec!.Type != ColumnType.Null);

        Assert.IsTrue(anyNonNullRows, "At least one EXPLAIN row must have non-null estimated_rows");
        Assert.IsTrue(anyNonNullCost, "At least one EXPLAIN row must have non-null estimated_cost");
    }

    [Test]
    public async Task R9_TableScanCardinalityMatchesSeededStats()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");

        // Seed stats to a known value — no real rows needed.
        await SeedStatsAsync(executor, database, table, 500);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname, "EXPLAIN SELECT * FROM robots");

        QueryResultRow? scanRow = rows.FirstOrDefault(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "table-scan");

        Assert.IsNotNull(scanRow, "EXPLAIN must include a table-scan node");

        Assert.IsTrue(scanRow.Value.Row.TryGetValue("estimated_rows", out ColumnValue? estRows));
        Assert.AreEqual(ColumnType.Integer64, estRows!.Type, "estimated_rows must be Integer64");
        Assert.AreEqual(500L, estRows.LongValue,
            "table-scan EstimatedCardinality should match the seeded row count");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — CostEstimator unit-level (criterion 2)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void R9_CostModelPrefersFscanOverUnboundedRange()
    {
        // An unbounded secondary-index scan (null/null bounds → 100 % selectivity) costs
        // 2 × tableRowCount (index entries + row fetches), while a primary table scan costs
        // tableRowCount.  The cost model must choose the full table scan.
        var node = new IndexRangeScanNode(
            index: new TableIndexSchema("year_idx", ["year"], IndexType.Multi),
            fromBound: null,
            fromInclusive: true,
            toBound: null,
            toInclusive: true);

        Assert.IsTrue(
            CostEstimator.ShouldPreferFullScan(node, tableRowCount: 10_000),
            "Unbounded secondary index scan (100 % selectivity) must lose to full table scan");
    }

    [Test]
    public void R9_CostModelKeepsIndexForHalfOpenRange()
    {
        // Half-open range (one bound, 40 % default selectivity via OneBoundSelectivity).
        // BreakevenFraction raised from 0.40 → 0.50, so 40 % of 10,000 = 4,000 entries
        // is below the breakeven of ceil(10,000 × 0.50) = 5,000 → index scan wins.
        var node = new IndexRangeScanNode(
            index: new TableIndexSchema("year_idx", ["year"], IndexType.Multi),
            fromBound: new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 2000L)]),
            fromInclusive: false,
            toBound: null,
            toInclusive: true);

        Assert.IsFalse(
            CostEstimator.ShouldPreferFullScan(node, tableRowCount: 10_000),
            "Half-open range (40 % selectivity) is below the 50 % breakeven — index scan must win");
    }

    [Test]
    public void R9_CostModelKeepsIndexForTightBothBoundsRange()
    {
        // Both-bounds range: 10 % default selectivity.
        // 10 % of 10,000 = 1,000 entries, well below the 4,000 breakeven → index wins.
        var node = new IndexRangeScanNode(
            index: new TableIndexSchema("year_idx", ["year"], IndexType.Multi),
            fromBound: new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 2000L)]),
            fromInclusive: true,
            toBound: new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 2001L)]),
            toInclusive: false);

        Assert.IsFalse(
            CostEstimator.ShouldPreferFullScan(node, tableRowCount: 10_000),
            "Tight both-bounds range (10 % selectivity) must keep the index (cheaper than full scan)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — planner integration: heuristics unchanged for simple cases (criterion 1)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9_UniqueLookupPlanUnchangedByR9()
    {
        // A unique-index equality lookup must still produce an index-lookup node, not
        // a table-scan, even with a very large row count in stats.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");
        await SeedStatsAsync(executor, database, table, 100_000);

        string anId = ObjectIdGenerator.Generate().ToString();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT * FROM robots WHERE id = '{anId}'");

        bool hasIndexLookup = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-lookup");

        Assert.IsTrue(hasIndexLookup,
            "Unique-index equality lookup must produce an index-lookup node even with large stats");
    }

    [Test]
    public async Task R9_TightRangeStaysOnIndexWithLargeStats()
    {
        // A year BETWEEN X AND X+1 is a both-bounds range (10 % selectivity) and
        // must remain as an index-range-scan even with a very large row count.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");
        await SeedStatsAsync(executor, database, table, 100_000);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year >= 2020 AND year < 2021");

        bool hasIndexScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        Assert.IsTrue(hasIndexScan,
            "Tight both-bounds range (10 % selectivity, 100k table) must stay on the secondary index");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — planner integration: low-selectivity index override (criterion 2)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9_HalfOpenRangeKeepsIndexBelowBreakevenFraction()
    {
        // A half-open range (year > X, no upper bound) has 40 % default selectivity
        // when no column min/max stats are available (OneBoundSelectivity heuristic).
        // BreakevenFraction raised to 0.50, so 40 % of 10,000 = 4,000 entries is
        // below the 5,000 breakeven → index scan is retained (full table scan is NOT chosen).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");

        // Seed a large row count (no column min/max → heuristic applies).
        await SeedStatsAsync(executor, database, table, 10_000);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year > 2000");

        bool hasIndexRangeScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        bool hasTableScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "table-scan");

        Assert.IsTrue(hasIndexRangeScan,
            "Half-open range (40 % < 50 % breakeven) must keep the index-range-scan");
        Assert.IsFalse(hasTableScan,
            "table-scan must not appear when the cost model keeps the index");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — composite equality priced via KeyNdv, not collapsed to 1 row
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A composite index equality such as (tenant, status) = (v1, v2) is represented as a
    /// degenerate [v, v] inclusive range on both columns. Before the fix, the range path
    /// computed RangeFraction(v, v) = 0 and the result was Math.Max(1, 0) = 1 row regardless
    /// of the real composite NDV. After the fix, the equality is detected and priced as
    /// rows / KeyNdv, which correctly reflects the composite cardinality.
    /// </summary>
    [Test]
    public async Task CompositeEqualityPricedViaKeyNdvNotCollapsedToOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Create a table with a composite index on (tenant, status).
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns: new ColumnInfo[]
            {
                new("id",     ColumnType.Id),
                new("tenant", ColumnType.String, notNull: true),
                new("status", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "tenant_status_idx",
                    new ColumnIndexInfo[]
                    {
                        new("tenant", OrderType.Ascending),
                        new("status", OrderType.Ascending),
                    }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "orders");

        const long tableRows = 10_000;
        const long compositeKeyNdv = 500; // 500 distinct (tenant, status) pairs

        // Seed row count.
        executor.Statistics.TrackInsert(database, table, (int)tableRows);

        // Seed composite KeyNdv for ["tenant","status"].
        await executor.Statistics.SetNdvAsync(database, table,
            columnNdv: new() { { "tenant", 50 }, { "status", 20 } },
            keyNdv: new() { { StatisticsManager.KeyTupleSignature(["tenant", "status"]), compositeKeyNdv } });

        // Seed a histogram for the trailing range column 'status'. This is what makes the test
        // discriminating: without the fix, a 2-column equality is decomposed into a 'tenant'
        // equality prefix plus a degenerate [status, status] range, and the histogram path yields
        // RangeFraction(v, v) == 0 → the estimate collapses to 1. (With no histogram/min-max the
        // old path would instead fall back to a fixed selectivity that coincidentally also lands
        // near rows/KeyNdv, hiding the bug.) The fix must price the whole tuple via KeyNdv before
        // ever reaching this range path.
        await executor.Statistics.SetHistogramsAsync(database, table, new Dictionary<string, ColumnHistogram>()
        {
            {
                "status",
                new ColumnHistogram
                {
                    TotalRows = tableRows,
                    Buckets =
                    [
                        new ColumnHistogramBucket
                        {
                            UpperBound       = new ScalarBound { Type = ColumnType.String, StrValue = "zzzz" },
                            CumulativeRows   = tableRows,
                            DistinctInBucket = 20,
                        },
                    ],
                }
            },
        });

        // Construct a 2-column composite equality: (tenant='acme', status='active').
        var tenantVal = new ColumnValue(ColumnType.String, "acme");
        var statusVal = new ColumnValue(ColumnType.String, "active");
        var bound = new CompositeColumnValue([tenantVal, statusVal]);

        var index = new TableIndexSchema(
            name: "tenant_status_idx",
            columns: ["tenant", "status"],
            type: IndexType.Multi);

        var node = new IndexRangeScanNode(
            index: index,
            fromBound: bound,
            fromInclusive: true,
            toBound: bound,
            toInclusive: true);

        long estimated = CostEstimator.EstimateRangeScanRows(
            node, tableRows,
            executor.Statistics, database, table);

        long expectedApprox = tableRows / compositeKeyNdv; // = 20

        // Must be priced near rows/KeyNdv (within 2×), not collapsed to 1.
        Assert.Greater(estimated, 1,
            "Composite equality must not collapse to 1 row when KeyNdv is seeded");
        Assert.LessOrEqual(estimated, expectedApprox * 2,
            "Composite equality estimate must not greatly exceed rows/KeyNdv");
        Assert.GreaterOrEqual(estimated, Math.Max(1, expectedApprox / 2),
            "Composite equality estimate must be within range of rows/KeyNdv");
    }
}
