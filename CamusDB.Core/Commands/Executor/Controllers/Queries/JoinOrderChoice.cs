/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// How a join tree got its <c>ON</c> predicates. The plan cache records it with the alias order,
/// because the order alone cannot reproduce a tree whose conjuncts were redistributed.
/// </summary>
internal enum JoinBuildMode
{
    /// <summary>
    /// Each join keeps one whole <c>ON</c> predicate of the query, and the <c>WHERE</c> conjuncts
    /// that name two or more tables stay after the last join. The heuristic order and the
    /// declared tree are built this way. A cache hit replays the order through
    /// <see cref="JoinOrderOptimizer.ReorderByAliases"/>.
    /// </summary>
    WholePredicate,

    /// <summary>
    /// The tree was built through <see cref="JoinGraph.BuildTree"/>: each conjunct sits at the
    /// join where its last table enters, and the eligible <c>WHERE</c> conjuncts moved into the
    /// joins. A cache hit rebuilds the tree through <see cref="JoinGraph"/> from the current query,
    /// also when the cached order equals the declared order. The heuristic's cross-product repair
    /// builds trees this way; the builder selects their operators by its own estimates.
    /// </summary>
    Conjunct,

    /// <summary>
    /// Built like <see cref="Conjunct"/>, from an order the cost-based enumerator chose. The
    /// builder also takes the estimates of each join from the enumerator
    /// (<see cref="JoinEnumerator.EstimateSteps"/>), so it builds the operators the order was
    /// costed with. A cache hit rebuilds the tree and recomputes those estimates.
    /// </summary>
    CostBased,
}

/// <summary>The join operator that one step of a left-deep plan uses; see <see cref="CostEstimator.SelectJoinStepOperator"/>.</summary>
internal enum JoinStepOperator
{
    /// <summary>The right input is read again for each left row: no conjunct is an equi-key.</summary>
    NestedLoop,

    /// <summary>A hash table on the smaller input, probed by the other.</summary>
    Hash,

    /// <summary>One index probe into the right table for each left row.</summary>
    IndexLookup,
}

/// <summary>
/// The enumerator's estimates for one join of the tree it chose: the rows of the left input, the
/// rows of the right input after its pushed filter, the cost of reading the right input (a full
/// scan), and the operator those numbers select. The builder uses them in place of its own
/// estimates, which see a join input only as a fixed default and a filtered right input only by
/// its output rows.
/// </summary>
internal readonly record struct JoinStepEstimate(
    double LeftRows,
    double RightRows,
    double RightScanCost,
    JoinStepOperator Operator);

/// <summary>
/// The join tree that the planner builds, how it was built, the filter that runs after the
/// last join, and, for a cost-based tree, the enumerator's estimates of each join (keyed by the
/// <see cref="JoinSource"/> instance, not by value). The filter belongs to the tree: a tree built
/// through <see cref="JoinGraph"/> already evaluates the <c>WHERE</c> conjuncts it took, so the
/// filter must not contain them again.
/// </summary>
internal sealed record JoinOrderChoice(
    QuerySource Source,
    JoinBuildMode Mode,
    NodeAst? PostJoinFilter,
    IReadOnlyDictionary<JoinSource, JoinStepEstimate>? StepEstimates = null)
{
    public static JoinOrderChoice WholePredicate(QuerySource source, JoinPredicatePushdown.Result pushdown) =>
        new(source, JoinBuildMode.WholePredicate, pushdown.PostJoinFilter);

    public static JoinOrderChoice Conjunct(QuerySource source, JoinGraph graph) =>
        new(source, JoinBuildMode.Conjunct, graph.RemainingPostJoinFilter);

    public static JoinOrderChoice CostBased(
        QuerySource source,
        JoinGraph graph,
        IReadOnlyDictionary<JoinSource, JoinStepEstimate> steps) =>
        new(source, JoinBuildMode.CostBased, graph.RemainingPostJoinFilter, steps);
}
