
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.SQLParser;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for predicate absorption against composite index-scan bounds.
/// The critical property: a composite bound's exclusivity applies only to its terminal
/// component — a scan bounded above by &lt; (5,10) still contains rows with a = 5
/// (e.g. (5,9)), so per-column bounds on non-terminal components are inclusive.
/// </summary>
[TestFixture]
public sealed class TestIndexScanBoundAnalysis
{
    private static readonly TableIndexSchema abIndex = new("ab_idx", new[] { "a", "b" }, IndexType.Multi);

    private static readonly TableDescriptor table = CreateTable();

    private static TableDescriptor CreateTable()
    {
        TableSchema schema = new()
        {
            Id = "t1",
            Name = "t1",
            Columns =
            [
                new TableColumnSchema("c0", "a", ColumnType.Integer64, false, null),
                new TableColumnSchema("c1", "b", ColumnType.Integer64, false, null),
            ],
            Version = 0
        };

        TableDescriptor descriptor = new(schema.Id!, schema.Name!, schema, store: null!);
        descriptor.Indexes["ab_idx"] = abIndex;
        return descriptor;
    }

    /// <summary>
    /// Range scan over (a,b): [(5,3) exclusive, (5,10) exclusive] — the shape produced by
    /// WHERE a = 5 AND b &gt; 3 AND b &lt; 10.
    /// </summary>
    private static QueryPlanStep EqualityPrefixRangeStep() =>
        new(
            QueryPlanStepType.RangeScanFromIndex,
            abIndex,
            fromBound: new CompositeColumnValue(new[]
            {
                new ColumnValue(ColumnType.Integer64, 5L),
                new ColumnValue(ColumnType.Integer64, 3L),
            }),
            fromInclusive: false,
            toBound: new CompositeColumnValue(new[]
            {
                new ColumnValue(ColumnType.Integer64, 5L),
                new ColumnValue(ColumnType.Integer64, 10L),
            }),
            toInclusive: false);

    private static AnalyzedComparison Comparison(string column, string op, long constant) =>
        new(
            column,
            op,
            new ColumnValue(ColumnType.Integer64, constant),
            new NodeAst(NodeType.Integer, null, null, null, null, null, null, null, constant.ToString()));

    [Test]
    public void StrictLessOnNonTerminalPrefixColumnIsNotAbsorbed()
    {
        // Rows with a = 5 are inside the scan, so "a < 5" can still reject rows and must be
        // kept as a residual filter — absorbing it would return rows for the unsatisfiable
        // conjunction a = 5 AND a < 5.
        QueryPlanStep step = EqualityPrefixRangeStep();

        Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", "<", 5), step, table));
    }

    [Test]
    public void StrictGreaterOnNonTerminalPrefixColumnIsNotAbsorbed()
    {
        // Mirror of the upper side: FromBound (5,3) exclusive still contains rows with a = 5
        // (e.g. (5,4)), so "a > 5" is not implied by the scan.
        QueryPlanStep step = EqualityPrefixRangeStep();

        Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", ">", 5), step, table));
    }

    [Test]
    public void NonStrictBoundsOnNonTerminalPrefixColumnAreAbsorbed()
    {
        // Every row in the scan has a = 5, so a <= 5, a >= 5, and a = 5 are all implied.
        QueryPlanStep step = EqualityPrefixRangeStep();

        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", "<=", 5), step, table));
        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", ">=", 5), step, table));
        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", "=", 5), step, table));
    }

    [Test]
    public void TerminalComponentKeepsCompositeExclusivity()
    {
        // b is the terminal component of both bounds, so the composite exclusivity applies:
        // the scan covers b in [4, 9], which implies b < 10 and b > 3 but not b < 9 or b > 4.
        QueryPlanStep step = EqualityPrefixRangeStep();

        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("b", "<", 10), step, table));
        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("b", ">", 3), step, table));
        Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("b", "<", 9), step, table));
        Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("b", ">", 4), step, table));
    }

    [Test]
    public void MixedTypeEqualityIsNotAbsorbedAndDoesNotThrow()
    {
        // Point bounds on an Id-typed column with a String constant that failed Id coercion
        // (e.g. id = '<24-hex>' AND id = 'not-an-id'): ColumnValue.CompareTo throws on a type
        // mismatch, so absorption must decline instead of erroring the query — the residual
        // filter then correctly yields zero rows.
        QueryPlanStep step = new(
            QueryPlanStepType.QueryFromIndex,
            abIndex,
            new ColumnValue(ColumnType.Id, "0123456789abcdef01234567"));

        AnalyzedComparison stringComparison = new(
            "a",
            "=",
            new ColumnValue(ColumnType.String, "not-an-id"),
            new NodeAst(NodeType.String, null, null, null, null, null, null, null, "not-an-id"));

        Assert.DoesNotThrow(() =>
            Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(stringComparison, step, table)));
    }

    [Test]
    public void NullColumnValuesCompareEqual()
    {
        // NULL == NULL must be 0: a comparator where a < b AND b < a simultaneously is
        // inconsistent, and List.Sort can throw on it when a sorted column holds several NULLs.
        ColumnValue null1 = new(ColumnType.Null, 0);
        ColumnValue null2 = new(ColumnType.Null, 0);
        ColumnValue value = new(ColumnType.Integer64, 5L);

        Assert.AreEqual(0, null1.CompareTo(null2));
        Assert.AreEqual(0, null2.CompareTo(null1));
        Assert.Less(null1.CompareTo(value), 0, "NULL sorts before non-NULL");
        Assert.Greater(value.CompareTo(null1), 0, "non-NULL sorts after NULL");
    }

    [Test]
    public void SingleColumnRangeKeepsExclusivity()
    {
        // Single-component bound: the column is terminal, so < (5) exclusive means max = 4 and
        // "a < 5" is genuinely absorbed.
        QueryPlanStep step = new(
            QueryPlanStepType.RangeScanFromIndex,
            abIndex,
            fromBound: null,
            fromInclusive: false,
            toBound: new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 5L) }),
            toInclusive: false);

        Assert.IsTrue(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", "<", 5), step, table));
        Assert.IsFalse(IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(Comparison("a", "<", 4), step, table));
    }
}
