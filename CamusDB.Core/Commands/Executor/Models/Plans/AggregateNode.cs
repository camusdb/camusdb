
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Computes aggregate projections over its input rows.</summary>
public sealed class AggregateNode : PhysicalPlanNode
{
    private static readonly HashSet<string> DecomposableAggFunctions =
        new(StringComparer.OrdinalIgnoreCase) { "count", "sum", "min", "max" };

    /// <summary>GROUP BY expressions captured at plan time; null for global (ungrouped) aggregates.</summary>
    public IReadOnlyList<NodeAst>? GroupByExpressions { get; init; }

    /// <summary>Projection expressions that are aggregate calls (e.g. COUNT(*), SUM(x)), captured at plan time.</summary>
    public IReadOnlyList<NodeAst>? AggregateProjections { get; init; }

    /// <summary>
    /// True when every aggregate call in this node uses a function that can be split into
    /// per-partition local computation plus a coordinator merge: COUNT, SUM, MIN, MAX.
    /// AVG is non-decomposable without SUM/COUNT splitting (deferred).
    /// Nodes with no explicit aggregate projections (e.g. pure GROUP BY) are treated as decomposable.
    /// </summary>
    public bool IsDecomposableAggregate
    {
        get
        {
            if (AggregateProjections is null or { Count: 0 })
                return true;

            foreach (NodeAst proj in AggregateProjections)
            {
                // Unwrap alias: SELECT count(*) AS c → target is the count(*) call.
                NodeAst target = proj.nodeType == NodeType.ExprAlias ? proj.leftAst! : proj;

                // Compound aggregates (e.g. SUM(x)+1, COALESCE(SUM(x),0)) are not decomposable
                // because the outer expression must be evaluated after the merge, not per-partition.
                if (QueryExpressionClassifier.IsCompoundAggregateProjection(target))
                    return false;

                if (target.nodeType != NodeType.ExprFuncCall)
                    continue;

                string? funcName = target.leftAst?.yytext;
                if (!DecomposableAggFunctions.Contains(funcName ?? ""))
                    return false;
            }

            return true;
        }
    }

    public override bool CanDecomposeToLocalPlusMerge => IsDecomposableAggregate;

    /// <summary>
    /// True when the planner has proved that the chosen index scan delivers rows with equal
    /// group keys adjacently, enabling O(1)-memory streaming aggregation. When false, the
    /// default hash-dictionary path is used.
    /// </summary>
    public bool IsStreamingGroupBy { get; init; }

    public AggregateNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
