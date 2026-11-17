/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The plan of the query of one derived table (an inlined CTE, a view body, a derived table in FROM), as
/// EXPLAIN built it, so that EXPLAIN shows the operators of that query under the line of the derived table.
///
/// <para>With EXPLAIN (ANALYZE) the plan has counters, and the derived-table executor runs this plan in
/// place of planning the query again (<see cref="DerivedTableExecutor"/>). So the counters under the line
/// are the counters of the rows that ran. A plan runs one time (<see cref="TryStartRun"/>); a second read
/// of the same derived table in the statement plans its query again, as it does without EXPLAIN, and its
/// counters are not shown.</para>
///
/// <para>A CTE that the statement computes once has one entry for all its readers, and only the first
/// reader in the plan shows the plan (<see cref="RenderedAt"/>): the query runs one time, for whichever
/// reader reads it first.</para>
/// </summary>
internal sealed class ExplainedDerivedTable
{
    private int started;

    public ExplainedDerivedTable(QueryPlan plan, BoundDerivedTableSource renderedAt, bool analyze)
    {
        Plan = plan;
        RenderedAt = renderedAt;
        Analyze = analyze;
    }

    /// <summary>The plan of the derived table's query.</summary>
    public QueryPlan Plan { get; }

    /// <summary>The reader of the derived table whose plan line shows <see cref="Plan"/> under it.</summary>
    public BoundDerivedTableSource RenderedAt { get; }

    /// <summary>True when the plan collects counters, for EXPLAIN (ANALYZE).</summary>
    public bool Analyze { get; }

    /// <summary>
    /// The rows the query gave and the time of its run, for the derived-table line. Filled while the plan
    /// runs, and only with <see cref="Analyze"/>.
    /// </summary>
    public PlanNodeStats ScanStats { get; } = new();

    /// <summary>True for the first caller only: the caller then runs <see cref="Plan"/>.</summary>
    public bool TryStartRun() => Interlocked.Exchange(ref started, 1) == 0;
}

/// <summary>
/// The derived-table plans of one EXPLAIN statement (<see cref="ExplainedDerivedTable"/>), by reader and, for
/// a CTE computed once, by the CTE. The outer plan and every plan built under it share one instance
/// (<see cref="QueryPlan.ExplainedDerivedTables"/>), so the executor of any of those plans finds the entry of
/// a derived table it reads.
/// </summary>
internal sealed class ExplainedDerivedTables
{
    private readonly Dictionary<BoundDerivedTableSource, ExplainedDerivedTable> bySource = new(ReferenceEqualityComparer.Instance);

    private readonly Dictionary<NodeAst, ExplainedDerivedTable> byComputedOnce = new(ReferenceEqualityComparer.Instance);

    /// <summary>The entry of <paramref name="source"/>, or null when EXPLAIN built no plan for it.</summary>
    public ExplainedDerivedTable? For(BoundDerivedTableSource source) =>
        bySource.TryGetValue(source, out ExplainedDerivedTable? explained) ? explained : null;

    /// <summary>The entry of the CTE that <paramref name="marker"/> computes once, or null when it has none yet.</summary>
    public ExplainedDerivedTable? ForComputedOnce(NodeAst marker) =>
        byComputedOnce.TryGetValue(marker, out ExplainedDerivedTable? explained) ? explained : null;

    /// <summary>Records <paramref name="explained"/> for <paramref name="source"/>, and for its CTE when it is computed once.</summary>
    public void Add(BoundDerivedTableSource source, ExplainedDerivedTable explained)
    {
        bySource[source] = explained;

        if (source.Source.ComputedOnce is { } marker)
            byComputedOnce.TryAdd(marker, explained);
    }
}
