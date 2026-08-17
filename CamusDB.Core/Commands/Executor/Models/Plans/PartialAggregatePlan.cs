
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// One AVG that was decomposed for partial execution: fragments ship
/// <see cref="SumName"/>/<see cref="CountName"/> partial columns, the merge re-sums both, and
/// the finalizer emits <see cref="OutputName"/> = Float64(sum / count) (NULL when count is 0 —
/// the aggregator's own empty-input semantics) and drops the internal pair. The division is a
/// typed post-merge step, never a SQL expression: the engine's <c>/</c> integer-divides
/// Integer64 operands, which would corrupt integer-column averages.
/// </summary>
public sealed record PartialAvgFinalizer(string OutputName, string SumName, string CountName);

/// <summary>
/// Everything the executor needs to run an aggregate as per-span partials below a gather:
/// what each fragment computes over its span (<see cref="ShipProjections"/>, grouped by
/// <see cref="ShipGroupBy"/>), how the coordinator merges the partial rows by re-aggregation
/// (<see cref="MergeProjections"/> / <see cref="MergeGroupBy"/> — COUNT merges by SUM over its
/// alias, SUM/MIN/MAX by themselves, group identifiers pass through), and which AVG pairs to
/// finalize afterwards. Built only by the planner, only for shapes whose semantics survive the
/// split: simple aliased decomposable aggregates, simple-identifier group keys all present in
/// the projection, no HAVING (its hidden-aggregate expansion needs the original rows).
/// Both halves run through the engine's own <c>QueryAggregator</c>, which is what keeps
/// NULL/empty semantics identical to sequential execution.
/// </summary>
public sealed class PartialAggregatePlan
{
    /// <summary>Projections each fragment evaluates over its span's filter-survivors.</summary>
    public required IReadOnlyList<NodeAst> ShipProjections { get; init; }

    /// <summary>Fragment-side GROUP BY (simple identifiers); null for global aggregates.</summary>
    public IReadOnlyList<NodeAst>? ShipGroupBy { get; init; }

    /// <summary>Coordinator-side re-aggregation projections over the partial rows.</summary>
    public required IReadOnlyList<NodeAst> MergeProjections { get; init; }

    /// <summary>Coordinator-side GROUP BY over the partial rows; null for global aggregates.</summary>
    public IReadOnlyList<NodeAst>? MergeGroupBy { get; init; }

    /// <summary>AVG pairs to divide and project after the merge; empty when no AVG.</summary>
    public IReadOnlyList<PartialAvgFinalizer> AvgFinalizers { get; init; } = [];
}
