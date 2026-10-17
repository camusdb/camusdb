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
/// The shape of the index lookups one join step would make into its right table, the inputs of
/// <see cref="CostEstimator.IndexLookupJoinStepCost"/> beyond the left rows: whether the index is
/// unique (a point read for each key) or not (a bounded scan for each key), the right rows one
/// probe returns on average, and the distinct keys of the left input. The join keeps the rows of
/// each probed key in a memo, so a left input that repeats a key probes the store once for it;
/// <see cref="LeftKeyDistinct"/> bounds the probes, and it is unbounded when the distinct count of
/// the left column is unknown, so every left row then counts as a probe.
/// <see cref="CostEstimator.EstimateIndexLookupProfile"/> builds it from the statistics, and the
/// enumerator and the builder both price a step through the same instance.
/// </summary>
internal readonly record struct IndexLookupProfile(bool UniqueIndex, double MatchesPerProbe, double LeftKeyDistinct);

/// <summary>
/// The enumerator's estimates for one join of the tree it chose: the rows of the left input, the
/// rows of the right input after its pushed filter, the cost of reading the right input (a full
/// scan), the shape of the index lookups the step could make (null when no conjunct of the step
/// can drive one), the operator those numbers select, and the rows the join produces (the subset
/// estimate of every table up to and including this join's right input). The builder uses the
/// input estimates in place of its own, which see a join input only as a fixed default and a
/// filtered right input only by its output rows. The output estimate decides nothing; it is kept
/// on the built node so EXPLAIN (ANALYZE) can compare the search's model with the rows the join
/// produced.
/// </summary>
internal readonly record struct JoinStepEstimate(
    double LeftRows,
    double RightRows,
    double RightScanCost,
    IndexLookupProfile? IndexLookup,
    JoinStepOperator Operator,
    double OutputRows);

/// <summary>
/// The join tree that the planner builds, how it was built, the filter that runs after the
/// last join, and, for a cost-based tree, the enumerator's estimates of each join (keyed by the
/// <see cref="JoinSource"/> instance, not by value). The filter belongs to the tree: a tree built
/// through <see cref="JoinGraph"/> already evaluates the <c>WHERE</c> conjuncts it took, so the
/// filter must not contain them again.
///
/// <para><see cref="CostedJoinSteps"/> is the number of join steps that the dynamic program of
/// <see cref="JoinEnumerator"/> costed to make this choice, and 0 when the program did not run. A
/// choice from the heuristic after the program found no order keeps the count of the program, so
/// the count shows the search work also when the cost-based order was not used. The count decides
/// nothing; EXPLAIN (ANALYZE) prints it, because it repeats between runs and planning time does
/// not.</para>
/// </summary>
internal sealed record JoinOrderChoice(
    QuerySource Source,
    JoinBuildMode Mode,
    NodeAst? PostJoinFilter,
    IReadOnlyDictionary<JoinSource, JoinStepEstimate>? StepEstimates = null,
    long CostedJoinSteps = 0,
    bool Greedy = false)
{
    public static JoinOrderChoice WholePredicate(QuerySource source, JoinPredicatePushdown.Result pushdown) =>
        new(source, JoinBuildMode.WholePredicate, pushdown.PostJoinFilter);

    public static JoinOrderChoice Conjunct(QuerySource source, JoinGraph graph) =>
        new(source, JoinBuildMode.Conjunct, graph.RemainingPostJoinFilter);

    /// <param name="greedy">
    /// True when the order is the greedy one of <see cref="JoinEnumerator"/>, taken because the
    /// search passed its step budget; the tree is built and costed like a searched one.
    /// </param>
    public static JoinOrderChoice CostBased(
        QuerySource source,
        JoinGraph graph,
        IReadOnlyDictionary<JoinSource, JoinStepEstimate> steps,
        long costedJoinSteps,
        bool greedy = false) =>
        new(source, JoinBuildMode.CostBased, graph.RemainingPostJoinFilter, steps, costedJoinSteps, greedy);
}

/// <summary>
/// How the planner chose the join order of one plan, for EXPLAIN (ANALYZE) and the plan-quality
/// harness. <see cref="Path"/> is <c>cost-based</c> when the dynamic program of
/// <see cref="JoinEnumerator"/> chose the order; <c>greedy</c> when the program passed its step
/// budget and the greedy order of the enumerator was costed instead; <c>heuristic</c> when
/// neither ran, so that <see cref="JoinOrderOptimizer"/> chose the order or the declared tree
/// stayed (the cost-based search is off, statistics are not loaded, the join has more than
/// <see cref="JoinEnumerator.MaxTablesForEnumeration"/> tables, or no order avoids a cross
/// product); and <c>cached</c> when a plan-cache hit replayed an earlier order.
/// <see cref="CostedJoinSteps"/> is <see cref="JoinOrderChoice.CostedJoinSteps"/>; a cache hit
/// costs no step.
/// </summary>
internal sealed record JoinOrderSearchSummary(string Path, long CostedJoinSteps);
