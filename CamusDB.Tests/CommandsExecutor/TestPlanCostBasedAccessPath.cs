
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cost-based access-path selection.
///
/// Validates that when per-table statistics are available the query planner enumerates all
/// viable index steps, costs each against the full-scan baseline, and picks the cheapest —
/// instead of picking the highest rule-scored candidate. Covers:
///   1. The cheapest index is chosen even when a different index has a higher heuristic score.
///   2. A low-selectivity range falls back to a full table scan via cost comparison.
///   3. A highly selective range keeps the index via cost comparison.
///   4. Without stats the rule-based (score-based) path is used unchanged.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestPlanCostBasedAccessPath : BaseTest
{
    /// <summary>
    /// This fixture exists to test the cost-based access-path branch, so every engine it builds has that
    /// branch enabled — it is off by default.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { CostBasedAccessPathEnabled = true };

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>
    /// Creates a test table "things" with two secondary indexes: category_idx on the
    /// low-cardinality column (5 distinct values) and priority_idx on the high-cardinality
    /// column (50 distinct values, sequential).
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "things",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("category", ColumnType.Integer64),
                new("priority", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",          new ColumnIndexInfo[] { new("id",       OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "category_idx", new ColumnIndexInfo[] { new("category", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "priority_idx", new ColumnIndexInfo[] { new("priority", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static InsertTicket MakeInsertTicket(KvTransaction txn, string dbname, long category, long priority)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: "things",
            values: new()
            {
                new()
                {
                    { "id",       new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "category", new(ColumnType.Integer64, category) },
                    { "priority", new(ColumnType.Integer64, priority) },
                }
            }
        );

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db)
    {
        if (db.TableDescriptors.TryGetValue("things", out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new System.InvalidOperationException("Table 'things' not found");
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

    /// <summary>
    /// Injects a known row count and histograms for both columns, giving the cost model
    /// precise selectivity estimates without running ANALYZE on real data.
    ///
    /// category histogram (values 0-4, uniform, 10 rows per value, 50 total):
    ///   Equality predicate "category = 2" → range [2, 3) → selectivity ≈ 20% → ~10 rows
    ///
    /// priority histogram (values 0-49, uniform, 1 row per value, 5 buckets of 10 values):
    ///   Range predicate "priority > 45" → range (45, ∞) → selectivity ≈ 8% → ~4 rows
    /// </summary>
    private static async Task SeedStatsAsync(
        CommandExecutor executor, DatabaseDescriptor database, TableDescriptor table)
    {
        executor.Statistics.TrackInsert(database, table, 50);
        await Task.Delay(150); // let TrackInsert flush so SetHistogramsAsync sees the row count

        // category: 5 equally-sized buckets, one value per bucket, 10 rows each.
        var catHist = new ColumnHistogram
        {
            TotalRows = 50,
            Buckets =
            [
                new() { UpperBound = Int(0), CumulativeRows = 10, DistinctInBucket = 1 },
                new() { UpperBound = Int(1), CumulativeRows = 20, DistinctInBucket = 1 },
                new() { UpperBound = Int(2), CumulativeRows = 30, DistinctInBucket = 1 },
                new() { UpperBound = Int(3), CumulativeRows = 40, DistinctInBucket = 1 },
                new() { UpperBound = Int(4), CumulativeRows = 50, DistinctInBucket = 1 },
            ],
        };

        // priority: 5 buckets, 10 values each, 10 rows each.
        var priHist = new ColumnHistogram
        {
            TotalRows = 50,
            Buckets =
            [
                new() { UpperBound = Int(9),  CumulativeRows = 10, DistinctInBucket = 10 },
                new() { UpperBound = Int(19), CumulativeRows = 20, DistinctInBucket = 10 },
                new() { UpperBound = Int(29), CumulativeRows = 30, DistinctInBucket = 10 },
                new() { UpperBound = Int(39), CumulativeRows = 40, DistinctInBucket = 10 },
                new() { UpperBound = Int(49), CumulativeRows = 50, DistinctInBucket = 10 },
            ],
        };

        await executor.Statistics.SetHistogramsAsync(database, table,
            new Dictionary<string, ColumnHistogram>
            {
                ["category"] = catHist,
                ["priority"]  = priHist,
            });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core test: cheapest index wins even when it has a lower rule-score
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task CheaperIndex_ByHistogramSelectivity_WinsOverHigherScoredIndex()
    {
        // Two indexes:
        //   category_idx: equality predicate "category = 2" → rule-score 5010 (equality prefix)
        //                 histogram selectivity ≈ 20% → 10 estimated rows → cost = 20
        //   priority_idx: range predicate "priority > 45" → rule-score 5001 (one-bound range)
        //                 histogram selectivity ≈ 8%  → 4  estimated rows → cost = 8
        //   full scan:    50 rows → cost = 50
        //
        // Rule-based (flag off): category_idx wins (score 5010 > 5001).
        // Cost-based (flag on):  priority_idx wins (cost 8 < 20 < 50).
        //
        // The test verifies that with stats available the planner picks priority_idx, not
        // category_idx, proving the cost-based enumeration flipped the decision from score to cost.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);
        await SeedStatsAsync(executor, database, table);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM things WHERE category = 2 AND priority > 45");

        // Exactly one index-range-scan node must be present.
        List<QueryResultRow> rangeScanRows = rows.Where(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan").ToList();

        Assert.AreEqual(1, rangeScanRows.Count, "There must be exactly one index-range-scan node");

        // The chosen index must be priority_idx (cheaper by cost), not category_idx (higher score).
        QueryResultRow chosenRow = rangeScanRows[0];
        Assert.IsTrue(chosenRow.Row.TryGetValue("detail", out ColumnValue? detail),
            "index-range-scan must have a detail column");
        Assert.IsTrue(detail!.StrValue!.Contains("priority_idx", System.StringComparison.Ordinal),
            $"Cost-based selection must pick priority_idx (cheaper) but detail was: {detail.StrValue}");
        Assert.IsFalse(detail!.StrValue!.Contains("category_idx", System.StringComparison.Ordinal),
            "category_idx must NOT be chosen when priority_idx has lower cost");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Low-selectivity falls to full scan via cost comparison
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task LowSelectivity_Range_FallsToFullScan_ViaCostComparison()
    {
        // "category >= 0" has a half-open range on category_idx.
        // With the injected histogram (category values 0-4, uniform):
        //   RangeFraction(lo=0, hi=null) = 1.0 - CumulativeFraction(0) = 1.0 - 0.2 = 0.8
        //   Estimated rows = 50 × 0.8 = 40   →  index scan cost = 2 × 40 = 80
        //   Full scan cost = 50
        // Cost comparison: full scan (50) < index scan (80) → full scan chosen.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);
        await SeedStatsAsync(executor, database, table);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM things WHERE category >= 0");

        bool hasTableScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "table-scan");
        bool hasIndexScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        Assert.IsTrue(hasTableScan,
            "Low-selectivity range (≈80% of 50 rows) must fall back to full table scan via cost comparison");
        Assert.IsFalse(hasIndexScan,
            "index-range-scan must not appear when the full scan is cheaper");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // High-selectivity range keeps the index via cost comparison
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task HighSelectivity_Range_KeepsIndex_ViaCostComparison()
    {
        // "priority > 45" is a highly selective half-open range on priority_idx.
        // With the injected histogram:
        //   RangeFraction(lo=45, hi=null) ≈ 0.08  →  4 rows  →  cost = 8
        //   Full scan cost = 50
        // Cost comparison: index scan (8) < full scan (50) → index scan kept.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);
        await SeedStatsAsync(executor, database, table);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM things WHERE priority > 45");

        bool hasIndexScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        Assert.IsTrue(hasIndexScan,
            "High-selectivity range (≈8% of 50 rows) must keep the index-range-scan");

        // Verify it's priority_idx specifically.
        QueryResultRow? scanRow = rows.FirstOrDefault(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        Assert.IsTrue(scanRow!.Value.Row.TryGetValue("detail", out ColumnValue? detail));
        Assert.IsTrue(detail!.StrValue!.Contains("priority_idx", System.StringComparison.Ordinal),
            $"Chosen index must be priority_idx, detail was: {detail.StrValue}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Without stats the rule-based (score-based) path is used
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task NoStats_RuleBasedFallback_PicksHigherScoredIndex()
    {
        // Without seeded stats GetRowCountEstimate returns null → cost-based path skipped entirely.
        // The rule-based TrySelectScan runs: category_idx has score 5010 (equality prefix),
        // priority_idx has score 5001 (one-bound range) → category_idx wins by score.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        // No SeedStatsAsync → GetRowCountEstimate returns null → cost-based path skipped.

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM things WHERE category = 2 AND priority > 45");

        QueryResultRow? scanRow = rows.FirstOrDefault(r =>
            r.Row.TryGetValue("node", out ColumnValue? n)
            && (n!.StrValue == "index-range-scan" || n.StrValue == "table-scan"));

        Assert.IsNotNull(scanRow, "EXPLAIN must return a scan node");

        // Rule-based: category_idx (score 5010) beats priority_idx (score 5001).
        if (scanRow!.Value.Row.TryGetValue("node", out ColumnValue? nodeType)
            && nodeType!.StrValue == "index-range-scan")
        {
            scanRow.Value.Row.TryGetValue("detail", out ColumnValue? detail);
            Assert.IsTrue(detail!.StrValue!.Contains("category_idx", System.StringComparison.Ordinal),
                $"Without stats the rule-based path must pick category_idx (score 5010 > 5001), detail was: {detail.StrValue}");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cost-based selection coexists with downstream sort-elision
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task OrderBy_WithStats_CostBasedPicksOrderSatisfyingIndex_ElidesSort()
    {
        // Cost-based selection runs for ORDER BY queries. Correctness is preserved because
        // sort-elision is applied downstream from the chosen scan node's own ordering:
        // when the cost model picks an index that already delivers rows in ORDER BY order, the
        // sort is elided; when it doesn't, the sort is correctly retained.
        //
        // Scenario: "WHERE priority >= 45 ORDER BY priority" — priority_idx satisfies both the
        // selective range (~4 of 50 rows ≈ 8%, far below breakeven) and the ORDER BY, so the
        // cost model picks priority_idx and the downstream sort is elided.
        //
        // Note: a sort node renders as "sort" / "sort(...)" (PlanRenderer.RenderSort), so the
        // node column is "sort" after SplitNodeLine — NOT "sort-by".

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();
        TableDescriptor table = await OpenTableAsync(database);
        await SeedStatsAsync(executor, database, table);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM things WHERE priority >= 45 ORDER BY priority");

        bool hasSort = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "sort");
        bool hasIndexScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n) && n!.StrValue == "index-range-scan");

        Assert.IsTrue(hasIndexScan,
            "Cost-based selection must pick the selective priority_idx range scan (it satisfies both the predicate and the order)");
        Assert.IsFalse(hasSort,
            "The cost-chosen priority_idx already delivers rows in ORDER BY order, so the sort must be elided");
    }
}
