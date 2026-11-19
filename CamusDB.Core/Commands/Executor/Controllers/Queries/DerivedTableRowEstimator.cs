/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The row estimate of each derived table that a query reads in <c>FROM</c>: the estimate of the root of
/// the plan of the derived table's query. Without it each such table counts as
/// <see cref="CostEstimator.DefaultTableRowCount"/> rows, so a CTE that keeps 800 rows and one that keeps
/// 60,000 look the same to the join-order search, to the build-side choice and to every estimate above them.
///
/// <para><b>How.</b> Before the join planner plans a query, <see cref="EstimateSources"/> plans the query of
/// each of its derived tables with planners that have no plan cache and no trace, and keeps the root estimate
/// (<see cref="RowsOf"/>). That plan of an inner query runs the same step for the derived tables it reads, so a
/// chain of CTEs gets its estimates from the bottom up. The plans are only read for their estimates; the
/// derived-table executor plans each query again when it runs, with the plan-time leaf lookup that a plan
/// here leaves out (it reads rows).</para>
///
/// <para><b>Which tables.</b> A table with a query of its own. A function in <c>FROM</c> keeps its exact count
/// (<see cref="TableFunctionEvaluation.TryCountRows"/>). A <c>LATERAL</c> body runs for each left row, so the
/// nested-loop join prices it (see <see cref="CostEstimator"/>). A set operation, a work table and the reader
/// of a recursive CTE have no single plan, so they keep the default.</para>
///
/// <para><b>Where the estimate is kept.</b> In a table keyed by the identity of the bound source. The
/// bound-query cache shares a bound source between statements, so the value is the one of the last plan;
/// every plan that reads the source writes it again first, with the current statistics. The source is a
/// record that plans use as a dictionary key, so the estimate is not a field of the record: a field that
/// changes would change its equality. Two plans at the same time can write two values, and each one is a
/// valid estimate.</para>
///
/// <para><b>Cost.</b> One extra plan of each derived query each time the query that reads it is planned,
/// also on a plan-cache hit, because the annotation of a cached plan reads the estimate too. The executor
/// plans a CTE chain one level at a time, and each level plans the levels under it again, so a chain of
/// <c>n</c> levels costs about <c>n²/2</c> extra plans.</para>
/// </summary>
internal sealed class DerivedTableRowEstimator
{
    private static readonly ConditionalWeakTable<BoundDerivedTableSource, StrongBox<long>> Estimates = new();

    private readonly QueryPlanner singleSourcePlanner;

    private readonly JoinQueryPlanner multiSourcePlanner;

    public DerivedTableRowEstimator(StatisticsManager? stats, CamusDBOptions options)
    {
        singleSourcePlanner = new QueryPlanner(stats, null, options);
        multiSourcePlanner = new JoinQueryPlanner(stats, null, options, traceEstimates: false, rowEstimator: this);
    }

    /// <summary>
    /// The rows of <paramref name="source"/>: the exact count of a function in <c>FROM</c>, else the estimate
    /// that the last plan of its query wrote (see <see cref="EstimateSources"/>), else null.
    /// </summary>
    public static long? RowsOf(BoundDerivedTableSource source)
    {
        if (TableFunctionEvaluation.TryCountRows(source) is { } exact)
            return exact;

        return Estimates.TryGetValue(source, out StrongBox<long>? box) ? Volatile.Read(ref box.Value) : null;
    }

    /// <summary>
    /// The rule that made <see cref="RowsOf"/> for <paramref name="source"/>, for the estimate trace: the exact
    /// count of a function, the estimate of a plan, or the fixed default when neither exists (also for a null
    /// source, an alias that names no derived table).
    /// </summary>
    public static EstimateRule RuleOf(BoundDerivedTableSource? source)
    {
        if (source is null)
            return EstimateRule.DerivedDefault;

        if (TableFunctionEvaluation.TryCountRows(source) is not null)
            return EstimateRule.FunctionRows;

        return Estimates.TryGetValue(source, out _) ? EstimateRule.DerivedPlan : EstimateRule.DerivedDefault;
    }

    /// <summary>
    /// Plans the query of each derived table of <paramref name="bound"/> that has one, and keeps the estimate of
    /// the plan root for <see cref="RowsOf"/>. <paramref name="outer"/> is the ticket of the query that reads them;
    /// each inner query gets the ticket the derived-table executor gives it when it runs.
    /// </summary>
    public void EstimateSources(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket outer)
    {
        foreach (BoundDerivedTableSource source in bound.DerivedSources)
        {
            if (!HasQueryToPlan(source))
                continue;

            long rows = PlanRootRows(database, source, outer);
            StrongBox<long> box = Estimates.GetValue(source, static _ => new StrongBox<long>());
            Volatile.Write(ref box.Value, rows);
        }
    }

    /// <summary>True when <paramref name="source"/> runs a query of its own that one plan describes.</summary>
    private static bool HasQueryToPlan(BoundDerivedTableSource source) =>
        source.Lateral is null
        && source.SetOperation is null
        && source.Source.Function is null
        && source.Source.WorkTable is null
        && (source.InnerBound.Sources.Count > 0 || source.InnerBound.DerivedSources.Count > 0)
        && BoundSetOperation.PassedThrough(source.InnerBound) is not { Source.WorkTable: not null };

    private long PlanRootRows(DatabaseDescriptor database, BoundDerivedTableSource source, QueryTicket outer)
    {
        BoundSelectQuery inner = source.InnerBound;

        // The ticket that DerivedTableExecutor gives the inner query when it runs.
        ExecuteSQLTicket executeTicket = new(
            txnState: outer.TxnState,
            database: outer.DatabaseName,
            sql: "",
            parameters: outer.Parameters,
            cancellationToken: outer.CancellationToken,
            probe: outer.Probe);

        QueryTicket innerTicket = QueryTicketAdapter.ToQueryTicket(
            inner, executeTicket, source.InnerSubqueries.ExistsRegistry, source.InnerSubqueries.SemiJoinSpecs);

        QueryPlan plan = inner.IsMultiSource
            ? multiSourcePlanner.GetPlan(database, inner, innerTicket)
            : singleSourcePlanner.GetPlan(database, inner.PrimaryTable, innerTicket);

        return plan.Root.EstimatedCardinality ?? CostEstimator.DefaultTableRowCount;
    }
}
