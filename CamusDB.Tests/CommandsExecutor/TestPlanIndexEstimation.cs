
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for index bound selection and range-scan row estimation.
///
/// Range bounds are order-independent: conflicting comparisons on the same column
/// produce the tightest bound regardless of conjunct walk order.
///
/// Composite-index range selectivity uses the actual range column (not the equality
/// prefix column) combined with the prefix NDV fraction.
///
/// String/Id equality represented as a degenerate [v, v] inclusive range is
/// estimated via 1/NDV, not via RangeFraction(v, v) which returns ~0.
/// </summary>
[TestFixture]
public sealed class TestPlanIndexEstimation : BaseTest
{
    // ─── Helpers ────────────────────────────────────────────────────────────────

    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>
    /// Builds a minimal PredicateAnalysis from a hand-constructed list of comparisons.
    /// The Conjunct field on each comparison is unused by IndexScanSelector, so null! is safe.
    /// </summary>
    private static PredicateAnalysis MakeAnalysis(params AnalyzedComparison[] comparisons)
        => new(comparisons, [], [], []);

    private static AnalyzedComparison Cmp(string col, string op, long val)
        => new(col, op, new ColumnValue(ColumnType.Integer64, val), null!);

    private static AnalyzedComparison CmpStr(string col, string op, string val)
        => new(col, op, new ColumnValue(ColumnType.String, val), null!);

    /// <summary>
    /// Creates a one-column (int64) index table. The [SetUp]-seeded flag state is not relevant
    /// here since we call IndexScanSelector / CostEstimator directly.
    /// </summary>
    private async Task<(DatabaseDescriptor database, TableDescriptor table, CommandExecutor executor)>
        CreateSingleColumnTableAsync(string colName, ColumnType colType, string idxName, IndexType idxType = IndexType.Multi)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns:
            [
                new("id",   ColumnType.Id),
                new(colName, colType),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",    [new("id",    OrderType.Ascending)]),
                new(idxType == IndexType.Unique ? ConstraintType.IndexUnique : ConstraintType.IndexMulti,
                    idxName, [new(colName, OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["items"];
        return (database, table, executor);
    }

    private async Task<(DatabaseDescriptor database, TableDescriptor table, CommandExecutor executor)>
        CreateCompositeIndexTableAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns:
            [
                new("id",         ColumnType.Id),
                new("tenant_id",  ColumnType.Integer64, notNull: true),
                new("created_at", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",         [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "tenant_ts_idx", [new("tenant_id", OrderType.Ascending), new("created_at", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["events"];
        return (database, table, executor);
    }

    // ─── Range-bound tightening tests ───────────────────────────────────────────

    [Test]
    public async Task ConflictingLowerBounds_TighterWins_AscendingOrder()
    {
        // "x > 20 AND x > 45": walked as [>20, >45] — second overwrote first before fix.
        // After fix: fromBound must be 45 (the tighter / higher lower bound).
        (_, TableDescriptor table, _) = await CreateSingleColumnTableAsync("x", ColumnType.Integer64, "x_idx");

        QueryPlanStep? step = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">", 20), Cmp("x", ">", 45)));

        Assert.IsNotNull(step, "Expected a range scan step");
        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, step!.Value.Type);
        Assert.IsNotNull(step.Value.FromBound, "FromBound must be set");
        Assert.AreEqual(45L, step.Value.FromBound!.Values[0].LongValue,
            "Tighter lower bound (45) must win over looser (20)");
    }

    [Test]
    public async Task ConflictingLowerBounds_TighterWins_DescendingOrder()
    {
        // "x > 45 AND x > 20": walked as [>45, >20] — second overwrote first before fix.
        // After fix: fromBound must still be 45.
        (_, TableDescriptor table, _) = await CreateSingleColumnTableAsync("x", ColumnType.Integer64, "x_idx");

        QueryPlanStep? step = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">", 45), Cmp("x", ">", 20)));

        Assert.IsNotNull(step, "Expected a range scan step");
        Assert.AreEqual(45L, step!.Value.FromBound!.Values[0].LongValue,
            "Tighter lower bound (45) must win regardless of conjunct order");
    }

    [Test]
    public async Task BothOrderings_ProduceIdenticalBounds()
    {
        (_, TableDescriptor table, _) = await CreateSingleColumnTableAsync("x", ColumnType.Integer64, "x_idx");

        QueryPlanStep? asc = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">", 20), Cmp("x", ">", 45)));
        QueryPlanStep? desc = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">", 45), Cmp("x", ">", 20)));

        Assert.IsNotNull(asc);
        Assert.IsNotNull(desc);
        Assert.AreEqual(asc!.Value.FromBound!.Values[0].LongValue,
                        desc!.Value.FromBound!.Values[0].LongValue,
            "Both orderings must produce the same from-bound");
        Assert.AreEqual(asc.Value.FromInclusive, desc.Value.FromInclusive,
            "Both orderings must produce the same from-inclusive flag");
    }

    [Test]
    public async Task ConflictingUpperBounds_TighterWins()
    {
        // "x < 100 AND x < 50": tighter upper is 50.
        (_, TableDescriptor table, _) = await CreateSingleColumnTableAsync("x", ColumnType.Integer64, "x_idx");

        QueryPlanStep? stepAsc = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", "<", 100), Cmp("x", "<", 50)));
        QueryPlanStep? stepDesc = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", "<", 50), Cmp("x", "<", 100)));

        Assert.IsNotNull(stepAsc);
        Assert.IsNotNull(stepDesc);
        Assert.AreEqual(50L, stepAsc!.Value.ToBound!.Values[0].LongValue,
            "Tighter upper bound (50) must win over looser (100)");
        Assert.AreEqual(50L, stepDesc!.Value.ToBound!.Values[0].LongValue,
            "Both orderings must produce toBound = 50");
    }

    [Test]
    public async Task ExclusiveBeatsInclusiveForSameValue_LowerBound()
    {
        // "x > 45" (exclusive) is tighter than "x >= 45" (inclusive) for the same value.
        (_, TableDescriptor table, _) = await CreateSingleColumnTableAsync("x", ColumnType.Integer64, "x_idx");

        QueryPlanStep? step1 = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">=", 45), Cmp("x", ">", 45)));
        QueryPlanStep? step2 = IndexScanSelector.TrySelectScan(table,
            MakeAnalysis(Cmp("x", ">", 45), Cmp("x", ">=", 45)));

        Assert.IsNotNull(step1);
        Assert.IsNotNull(step2);
        Assert.IsFalse(step1!.Value.FromInclusive, "exclusive > must beat inclusive >= for the same value (order 1)");
        Assert.IsFalse(step2!.Value.FromInclusive, "exclusive > must beat inclusive >= for the same value (order 2)");
    }

    // ─── Composite-index range selectivity tests ────────────────────────────────

    [Test]
    public async Task CompositeIndex_RangeSelectivityUsesRangeColumn()
    {
        // Index (tenant_id, created_at), query "WHERE tenant_id = 5 AND created_at > 1000".
        // The step emits FromBound = [5, 1000], ToBound = [6] (prefix upper).
        //
        // Before fix: EstimateRangeScanRows reads column[0]=tenant_id from Values[0]=5 and 6,
        //   getting min/max selectivity of (6-5)/(max-min) ≈ 1/(max-1) — ignores created_at.
        // After fix: equality prefix (tenant_id, NDV=10) × range on created_at > 1000 via min/max:
        //   prefix sel = 1/10, range sel ≈ 50% → 1000 * 0.10 * 0.50 = 50 rows.
        //
        // Discriminating: without the fix the estimate is ~1/(max-1)*1000 ≈ 10 rows (not 50).

        (DatabaseDescriptor database, TableDescriptor table, CommandExecutor executor) =
            await CreateCompositeIndexTableAsync();

        executor.Statistics.SeedRowCountForTesting(database, table, 1_000);
        await executor.Statistics.SetNdvAsync(database, table,
            columnNdv: new Dictionary<string, long> { ["tenant_id"] = 10, ["created_at"] = 500 },
            keyNdv: null);

        // Seed min/max for created_at: min=0, max=2000 → > 1000 covers 50%.
        executor.Statistics.SeedColumnStats(database, table, 1_000,
            columnStats: new Dictionary<string, ColumnMinMax>
            {
                ["created_at"] = new ColumnMinMax
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 0    },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 2000 },
                },
                ["tenant_id"] = new ColumnMinMax
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 1   },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 100 },
                },
            },
            indexCounts: []);

        TableIndexSchema idx = table.Indexes["tenant_ts_idx"];

        // FromBound = [5, 1000] (exclusive on created_at side), ToBound = [6] (prefix upper).
        IndexRangeScanNode node = new(
            idx,
            fromBound:    new CompositeColumnValue([new(ColumnType.Integer64, 5L), new(ColumnType.Integer64, 1000L)]),
            fromInclusive: false,
            toBound:      new CompositeColumnValue([new(ColumnType.Integer64, 6L)]),
            toInclusive:  false);

        long rows = CostEstimator.EstimateRangeScanRows(node, 1_000, Options, executor.Statistics, database, table);

        // Prefix sel = 1/10 = 10%; range sel = (2000-1000)/(2000-0) = 50%.
        // Expected ≈ 1000 * 0.10 * 0.50 = 50 rows.
        // Without the fix: 1000 * (6-5)/(100-1) ≈ 10 rows.
        Assert.Greater(rows, 20L, "composite-index estimate must include both prefix and range selectivity");
        Assert.Less(rows, 100L,   "composite-index estimate must not be close to full-scan cardinality");
    }

    [Test]
    public async Task CompositeIndex_UpperOpenRange_SymmetricEstimate()
    {
        // Regression for the upper-open composite case: "tenant_id = 5 AND created_at < 1000".
        // IndexScanSelector emits FromBound=null, ToBound=[5, 1000] (toLen=2).
        // Before fix: rangeColIdx = max(0, fromLen-1) = max(0, -1) = 0 → used tenant_id, not created_at.
        // After fix:  rangeColIdx = max(0, max(fromLen, toLen)-1) = max(0, 1) = 1 → uses created_at.
        //
        // Symmetric predicates (> 1000 and < 1000 each cover 50% of created_at [0..2000])
        // must produce the same estimate as the lower-open case.
        //
        // The tenant_id min/max range is deliberately WIDE (0..10000): under the bug the estimate
        // collapses to cf(tenant_id <= 5) over [0..10000] ≈ 1 row, which diverges sharply (≈50:1)
        // from the correct ~50 — so reverting the fix to fromLen-1 fails this test. A narrow tenant
        // range would let the wrong-column estimate land coincidentally close to the right answer.
        (DatabaseDescriptor database, TableDescriptor table, CommandExecutor executor) =
            await CreateCompositeIndexTableAsync();

        executor.Statistics.SeedRowCountForTesting(database, table, 1_000);
        await executor.Statistics.SetNdvAsync(database, table,
            columnNdv: new Dictionary<string, long> { ["tenant_id"] = 10, ["created_at"] = 500 },
            keyNdv: null);

        executor.Statistics.SeedColumnStats(database, table, 1_000,
            columnStats: new Dictionary<string, ColumnMinMax>
            {
                ["created_at"] = new ColumnMinMax
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 0    },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 2000 },
                },
                ["tenant_id"] = new ColumnMinMax
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 0     },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 10000 },
                },
            },
            indexCounts: []);

        TableIndexSchema idx = table.Indexes["tenant_ts_idx"];

        // Upper-open: ToBound = [5, 1000], FromBound = null (fromLen=0, toLen=2).
        IndexRangeScanNode upperOpen = new(
            idx,
            fromBound:     null,
            fromInclusive: true,
            toBound:       new CompositeColumnValue([new(ColumnType.Integer64, 5L), new(ColumnType.Integer64, 1000L)]),
            toInclusive:   false);

        // Lower-open (the working case from the existing test):
        // FromBound = [5, 1000], ToBound = [6] (prefix upper, fromLen=2, toLen=1).
        IndexRangeScanNode lowerOpen = new(
            idx,
            fromBound:     new CompositeColumnValue([new(ColumnType.Integer64, 5L), new(ColumnType.Integer64, 1000L)]),
            fromInclusive: false,
            toBound:       new CompositeColumnValue([new(ColumnType.Integer64, 6L)]),
            toInclusive:   false);

        long upperOpenRows = CostEstimator.EstimateRangeScanRows(upperOpen, 1_000, Options, executor.Statistics, database, table);
        long lowerOpenRows = CostEstimator.EstimateRangeScanRows(lowerOpen, 1_000, Options, executor.Statistics, database, table);

        // Both should be ≈ 1000 * 1/10 * 50% = 50 rows.
        Assert.Greater(upperOpenRows, 20L,
            "Upper-open composite range must use range column (created_at), not prefix column (tenant_id)");
        Assert.Less(upperOpenRows, 100L,
            "Upper-open composite range estimate must not be close to full-scan cardinality");

        // Symmetry: upper-open and lower-open estimates must be close (both cover ~50% of created_at).
        double ratio = (double)Math.Max(upperOpenRows, lowerOpenRows) / Math.Min(upperOpenRows, lowerOpenRows);
        Assert.Less(ratio, 3.0,
            $"Upper-open ({upperOpenRows}) and lower-open ({lowerOpenRows}) estimates must be in the same ballpark");
    }

    // ─── Degenerate-range NDV estimation tests ──────────────────────────────────

    [Test]
    public async Task StringEqualityDegenerate_UsesNdv()
    {
        // String equality "category = 'electronics'" on a non-unique index is represented as
        // an inclusive [v, v] range. RangeFraction(v, v) ≈ 0 → ~1 row without the NDV fix.
        // After fix: estimate = tableRows / NDV = 1000 / 10 = 100 rows.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "products",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",          [new("id",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "category_idx", [new("category", OrderType.Ascending)]),
            ],
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, table, 1_000);
        await executor.Statistics.SetNdvAsync(database, table,
            columnNdv: new Dictionary<string, long> { ["category"] = 10 },
            keyNdv: null);

        TableIndexSchema idx = table.Indexes["category_idx"];

        // Degenerate [v, v] inclusive range — used for String equality.
        IndexRangeScanNode node = new(
            idx,
            fromBound:     new CompositeColumnValue([new(ColumnType.String, "electronics")]),
            fromInclusive: true,
            toBound:       new CompositeColumnValue([new(ColumnType.String, "electronics")]),
            toInclusive:   true);

        long rows = CostEstimator.EstimateRangeScanRows(node, 1_000, Options, executor.Statistics, database, table);

        // Should be 1000/10 = 100, not ~1 (the degenerate range estimate).
        Assert.GreaterOrEqual(rows, 80L,
            "String equality [v,v] must be estimated via 1/NDV, not via degenerate range fraction");
        Assert.LessOrEqual(rows, 150L,
            "String equality [v,v] estimate must be ≈ tableRows/NDV");
    }

    [Test]
    public async Task DegenerateRange_WithoutNdv_FallsBackToHeuristic()
    {
        // When no NDV is available for the String column, the estimate must fall back to the
        // fixed heuristic (BothBoundsSelectivity = 10%) rather than returning ~1.
        // This guards the "stats absent → graceful fallback" invariant.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "products2",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",          [new("id",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "category_idx", [new("category", OrderType.Ascending)]),
            ],
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await database.TableDescriptors["products2"];
        executor.Statistics.SeedRowCountForTesting(database, table, 1_000);
        // Deliberately do NOT seed NDV.

        TableIndexSchema idx = table.Indexes["category_idx"];

        IndexRangeScanNode node = new(
            idx,
            fromBound:     new CompositeColumnValue([new(ColumnType.String, "electronics")]),
            fromInclusive: true,
            toBound:       new CompositeColumnValue([new(ColumnType.String, "electronics")]),
            toInclusive:   true);

        long rows = CostEstimator.EstimateRangeScanRows(node, 1_000, Options, executor.Statistics, database, table);

        // Without NDV, falls through to BothBoundsSelectivity = 10% → 100 rows.
        // The important thing is it does NOT return ~1 (the broken degenerate result).
        Assert.Greater(rows, 1L, "Without NDV the estimate must not collapse to ~1 row");
    }
}
