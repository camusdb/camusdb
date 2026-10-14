/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Unit;

/// <summary>The rules that turn EXPLAIN (ANALYZE) rows and result rows into one measurement.</summary>
[TestFixture]
[Category("PlanQuality")]
public sealed class TestPlanMeasures
{
    private static readonly CorpusQuery Query = new("q", ["tag"], "SELECT 1", "test:1");

    private static QueryResultRow Row(params (string Column, ColumnValue Value)[] cells) =>
        new(default, cells.ToDictionary(c => c.Column, c => c.Value, StringComparer.OrdinalIgnoreCase));

    private static ColumnValue L(long value) => new(ColumnType.Integer64, value);

    private static ColumnValue S(string value) => new(ColumnType.String, value);

    private static QueryResultRow Node(string node, string detail, long? estimated, long? actual, long read, long lookups, long scans, double? timeMs = null) =>
        Row(("stage", S("analyze")), ("node", S(node)), ("detail", S(detail)),
            ("estimated_rows", estimated is { } e ? L(e) : ColumnValue.Null),
            ("estimated_cost", ColumnValue.Null),
            ("actual_rows", actual is { } a ? L(a) : ColumnValue.Null),
            ("rows_read", L(read)),
            ("actual_time_ms", timeMs is { } t ? new ColumnValue(ColumnType.Float64, t) : ColumnValue.Null),
            ("kv_lookups", L(lookups)),
            ("kv_scan_entries", L(scans)));

    private static QueryResultRow PlanInfo(string detail) =>
        Row(("stage", S("analyze")), ("node", S("plan-info")), ("detail", S(detail)),
            ("estimated_rows", ColumnValue.Null), ("actual_rows", ColumnValue.Null), ("rows_read", ColumnValue.Null),
            ("kv_lookups", ColumnValue.Null), ("kv_scan_entries", ColumnValue.Null));

    [TestCase(10, 10, 1.0)]
    [TestCase(100, 10, 10.0)]
    [TestCase(10, 100, 10.0)]
    [TestCase(5, 0, 5.0)]
    [TestCase(0, 5, 5.0)]
    [TestCase(0, 0, 1.0)]
    public void QError_IsSymmetricAndFloorsAtOneRow(long estimated, long actual, double expected)
    {
        Assert.AreEqual(expected, PlanMeasures.QError(estimated, actual), 1e-9);
    }

    [Test]
    public void Build_SumsWorkOverPlanNodesOnly()
    {
        List<QueryResultRow> analyze =
        [
            Node("hash-join", "on=a.x=y, build=b", 10, 8, 20, 0, 20, timeMs: 3.5),
            Node("table-scan", "table=a", 100, 100, 100, 0, 100),
            PlanInfo("shape=abc, schema-deps=[1@1], planning-ms=0.42, join-order=cost-based, join-steps-costed=2"),
        ];

        QueryMeasurement m = PlanMeasures.Build(Query, analyze, [], statementRowsRead: 120);

        Assert.AreEqual(240, m.Work, "rows_read + kv_lookups + kv_scan_entries of the two plan nodes");
        Assert.AreEqual(120, m.StatementRowsRead);
        Assert.AreEqual("cost-based", m.JoinOrder);
        Assert.AreEqual(2, m.JoinStepsCosted);
        Assert.AreEqual(0.42, m.PlanningMs, 1e-9);
        Assert.AreEqual(3.5, m.ExecutionMs, 1e-9);
        Assert.AreEqual(2, m.Nodes.Count, "the plan-info row is not a plan node");
    }

    [Test]
    public void Build_UsesTheJoinOrderEstimateOnAJoinItCosted()
    {
        // The annotation estimates an index-nested-loop join as 1 row per outer row; the search
        // estimated 50, and 40 rows came out.
        List<QueryResultRow> analyze =
        [
            Node("index-nested-loop-join", "on=a.x = b.id, index=~pk, left=a.x, right=id, join-order-est-rows=50", 1, 40, 40, 40, 0),
            Node("table-scan", "table=a", 40, 40, 40, 0, 40),
        ];

        QueryMeasurement m = PlanMeasures.Build(Query, analyze, [], 80);

        Assert.AreEqual(50, m.Nodes[0].EstimatedRows);
        Assert.AreEqual(1.25, m.QErrorMax, 1e-9);
    }

    [Test]
    public void Build_WithoutPlanInfo_HasNoJoinOrder()
    {
        QueryMeasurement m = PlanMeasures.Build(Query, [Node("table-scan", "table=a", 5, 5, 5, 0, 5)], [], 5);

        Assert.IsNull(m.JoinOrder);
        Assert.AreEqual(0, m.JoinStepsCosted);
        Assert.AreEqual(1.0, m.QErrorMax);
    }

    [Test]
    public void Fingerprint_DropsTheJoinOrderEstimateAndKeepsTheRest()
    {
        List<PlanNodeMeasure> nodes =
        [
            new("hash-join", "on=a.x=y, build=b, join-order-est-rows=12", 12, 9, 0, 0, 0),
            new("nested-loop-join", "join-order-est-rows=3", 3, 3, 0, 0, 0),
            new("index-range-scan", "index=price_idx, from>=950, to<=1000", 4, 4, 4, 0, 4),
            new("project", "", 4, 4, 0, 0, 0),
        ];

        Assert.That(PlanMeasures.Fingerprint(nodes), Is.EqualTo(new[]
        {
            "hash-join(on=a.x=y, build=b)",
            "nested-loop-join",
            "index-range-scan(index=price_idx, from>=950, to<=1000)",
            "project",
        }));
    }

    [Test]
    public void ResultHash_DoesNotDependOnRowOrColumnOrder()
    {
        QueryResultRow a1 = Row(("x", L(1)), ("y", S("a")));
        QueryResultRow a2 = Row(("x", L(2)), ("y", S("b")));
        QueryResultRow b1 = Row(("y", S("a")), ("x", L(1)));
        QueryResultRow b2 = Row(("y", S("b")), ("x", L(2)));

        Assert.AreEqual(PlanMeasures.ResultHash([a1, a2]), PlanMeasures.ResultHash([b2, b1]));
        Assert.AreNotEqual(PlanMeasures.ResultHash([a1, a2]).Hash, PlanMeasures.ResultHash([a1]).Hash);
    }

    [Test]
    public void ResultHash_SeparatesTypesAndNulls()
    {
        string asNumber = PlanMeasures.ResultHash([Row(("x", L(1)))]).Hash;
        string asText = PlanMeasures.ResultHash([Row(("x", S("1")))]).Hash;
        string asNull = PlanMeasures.ResultHash([Row(("x", ColumnValue.Null))]).Hash;
        string asNullText = PlanMeasures.ResultHash([Row(("x", S("NULL")))]).Hash;

        Assert.AreNotEqual(asNumber, asText);
        Assert.AreNotEqual(asNull, asNullText);
    }

    [Test]
    public void ResultHash_RoundsFloatsToSixDecimals()
    {
        // The same average summed in another order can differ in the last bits.
        string a = PlanMeasures.ResultHash([Row(("avg", new ColumnValue(ColumnType.Float64, 0.1 + 0.2)))]).Hash;
        string b = PlanMeasures.ResultHash([Row(("avg", new ColumnValue(ColumnType.Float64, 0.3)))]).Hash;

        Assert.AreEqual(a, b);
    }
}
