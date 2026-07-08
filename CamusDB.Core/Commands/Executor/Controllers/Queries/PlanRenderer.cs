
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Renders a physical plan tree as a deterministic, indented, multi-line string.
/// Canonical node names are stable across EXPLAIN output (R1 keystone).
/// </summary>
public static class PlanRenderer
{
    private const string Indent = "  ";

    /// <summary>
    /// Renders the plan tree rooted at <paramref name="plan"/>.Root using full ticket context.
    /// </summary>
    /// <param name="plan">The query plan to render.</param>
    /// <param name="includeRequiredColumns">Append <c>cols=[...]</c> from column pushdown.</param>
    /// <param name="includeDistributedProperties">
    /// Append distributed-plan metadata: <c>order=[...]</c> when <see cref="PhysicalPlanNode.OutputOrdering"/>
    /// is set, and <c>decomposable=true/false</c> from <see cref="PhysicalPlanNode.CanDecomposeToLocalPlusMerge"/>.
    /// </param>
    /// <param name="includeShapeMetadata">
    /// Prepend R10 plan-cache metadata: <c>shape=&lt;id&gt;</c> and <c>schema-deps=[table@v, ...]</c>
    /// when <see cref="QueryPlan.QueryShapeId"/> is set.
    /// </param>
    public static string Render(
        QueryPlan plan,
        bool includeRequiredColumns = false,
        bool includeDistributedProperties = false,
        bool includeShapeMetadata = false)
    {
        var sb = new StringBuilder();

        if (includeShapeMetadata && plan.QueryShapeId is not null)
        {
            sb.Append("-- shape=").Append(plan.QueryShapeId);
            if (plan.SchemaDeps is { Count: > 0 })
            {
                string deps = string.Join(", ", plan.SchemaDeps.Select(d => $"{d.TableId}@{d.SchemaVersion}"));
                sb.Append(" schema-deps=[").Append(deps).Append(']');
            }
            sb.Append('\n');
        }

        RenderNode(plan.Root, plan, sb, 0, includeRequiredColumns, includeDistributedProperties);
        return sb.ToString().TrimEnd('\n');
    }

    /// <summary>
    /// Walks the plan tree depth-first (parent before children), yielding (name, detail) per node.
    /// Used by both <see cref="Render"/> and the EXPLAIN result-set executor.
    /// </summary>
    public static IEnumerable<(string Name, string Detail)> WalkNodes(PhysicalPlanNode root, QueryPlan plan)
    {
        return WalkNodesInner(root, plan);
    }

    private static IEnumerable<(string Name, string Detail)> WalkNodesInner(PhysicalPlanNode node, QueryPlan plan)
    {
        yield return SplitNodeLine(GetRenderLine(node, plan));
        if (node.Input is not null)
            foreach (var item in WalkNodesInner(node.Input, plan))
                yield return item;
    }

    private static (string Name, string Detail) SplitNodeLine(string line)
    {
        int paren = line.IndexOf('(');
        if (paren < 0) return (line, "");
        return (line[..paren], line[(paren + 1)..^1]);
    }

    private static string GetRenderLine(PhysicalPlanNode node, QueryPlan plan) =>
        node switch
        {
            TableScanNode n => RenderTableScan(n, plan),
            IndexLookupNode n => RenderIndexLookup(n, plan),
            IndexRangeScanNode n => RenderIndexRangeScan(n, plan),
            IndexInListScanNode n => RenderIndexInListScan(n),
            FilterNode n => RenderFilter(n),
            HavingFilterNode n => RenderHavingFilter(n),
            AggregateNode n => RenderAggregate(n),
            SortNode n => RenderSort(n),
            LimitNode n => RenderLimit(n),
            ProjectNode => "project",
            DistinctNode n => n.IsStreaming ? "distinct(streaming: true)" : "distinct(hash)",
            NestedLoopJoinNode n => RenderNestedLoopJoin(n),
            IndexNestedLoopJoinNode n => RenderIndexNestedLoopJoin(n),
            HashJoinNode n => RenderHashJoin(n),
            MergeJoinNode n => RenderMergeJoin(n),
            DerivedTableScanNode n => RenderDerivedTableScan(n),
            SemiJoinNode n => RenderSemiJoin(n),
            _ => node.GetType().Name,
        };

    private static void RenderNode(
        PhysicalPlanNode node,
        QueryPlan plan,
        StringBuilder sb,
        int depth,
        bool includeRequiredColumns,
        bool includeDistributedProperties)
    {
        string prefix = string.Concat(Enumerable.Repeat(Indent, depth));

        string line = GetRenderLine(node, plan);

        if (includeRequiredColumns && node.RequiredColumns is { Count: > 0 })
        {
            string cols = string.Join(",", node.RequiredColumns.OrderBy(c => c, StringComparer.Ordinal));
            line += $" cols=[{cols}]";
        }

        if (includeDistributedProperties)
        {
            if (node.OutputOrdering is { Count: > 0 })
            {
                string order = string.Join(", ", node.OutputOrdering.Select(
                    o => $"{o.ColumnName} {(o.Type == OrderType.Ascending ? "ASC" : "DESC")}"));
                line += $" order=[{order}]";
            }
            line += $" decomposable={node.CanDecomposeToLocalPlusMerge.ToString().ToLowerInvariant()}";
            if (node.Distribution is { } dist)
                line += $" dist={dist}";
        }

        sb.Append(prefix + line).Append('\n');

        if (node.Input is not null)
            RenderNode(node.Input, plan, sb, depth + 1, includeRequiredColumns, includeDistributedProperties);
    }

    // ── leaf / scan nodes ──────────────────────────────────────────────────

    private static string RenderTableScan(TableScanNode node, QueryPlan plan)
    {
        // Join scans: BoundSource carries the table reference.
        // Single-table scans: fall back to plan.Table.Name.
        string tableName = node.BoundSource?.Table.Name ?? plan.Table.Name;

        return node.Source switch
        {
            TableScanSource.ForcedIndex => $"table-scan(table={tableName}, forced-index={node.Index!.Name})",
            _ => $"table-scan(table={tableName})",
        };
    }

    private static string RenderIndexLookup(IndexLookupNode node, QueryPlan plan)
    {
        string key = RenderCompositeKey(node.LookupKey);
        string indexOnly = plan.IndexOnly ? ", index-only=true" : "";
        return $"index-lookup(index={node.Index.Name}, key={key}{indexOnly})";
    }

    private static string RenderIndexInListScan(IndexInListScanNode node) =>
        $"index-in-list(index={node.Index.Name}, values={node.Values.Count})";

    private static string RenderIndexRangeScan(IndexRangeScanNode node, QueryPlan plan)
    {
        var parts = new List<string> { $"index={node.Index.Name}" };

        if (node.FromBound is not null)
        {
            string from = RenderCompositeKey(node.FromBound);
            string op = node.FromInclusive ? ">=" : ">";
            parts.Add($"from{op}{from}");
        }

        if (node.ToBound is not null)
        {
            string to = RenderCompositeKey(node.ToBound);
            string op = node.ToInclusive ? "<=" : "<";
            parts.Add($"to{op}{to}");
        }

        if (plan.IndexOnly)
            parts.Add("index-only=true");

        return $"index-range-scan({string.Join(", ", parts)})";
    }

    // ── filter nodes ───────────────────────────────────────────────────────

    private static string RenderFilter(FilterNode node) =>
        $"filter({RenderExpr(node.Predicate)})";

    private static string RenderHavingFilter(HavingFilterNode node) =>
        $"having-filter({RenderExpr(node.Predicate)})";

    // ── pipeline nodes: read from node properties (self-describing) ───────────

    private static string RenderAggregate(AggregateNode node)
    {
        var parts = new List<string>();

        if (node.GroupByExpressions is { Count: > 0 })
            parts.Add($"group=[{string.Join(", ", node.GroupByExpressions.Select(RenderExpr))}]");

        if (node.AggregateProjections is { Count: > 0 })
            parts.Add($"aggs=[{string.Join(", ", node.AggregateProjections.Select(RenderAggregateCall))}]");

        return parts.Count > 0 ? $"aggregate({string.Join(", ", parts)})" : "aggregate";
    }

    private static string RenderSort(SortNode node)
    {
        if (node.OrderBy is not { Count: > 0 })
            return "sort";

        string cols = string.Join(", ", node.OrderBy.Select(
            o => $"{o.ColumnName} {(o.Type == OrderType.Ascending ? "ASC" : "DESC")}"));
        return $"sort({cols})";
    }

    private static string RenderLimit(LimitNode node)
    {
        if (node.LimitValue is null)
            return "limit";

        string detail = node.LimitValue.Value.ToString(CultureInfo.InvariantCulture);

        if (node.OffsetValue is not null)
            detail += $" offset {node.OffsetValue.Value.ToString(CultureInfo.InvariantCulture)}";

        return $"limit({detail})";
    }

    // ── join nodes ─────────────────────────────────────────────────────────

    private static string RenderNestedLoopJoin(NestedLoopJoinNode node)
    {
        string detail = $"on={RenderExpr(node.OnPredicate)}, right={node.RightSource.Alias}";
        if (node.RightExecutionFilter is not null)
            detail += $", right-filter={RenderExpr(node.RightExecutionFilter)}";
        return $"nested-loop-join({detail})";
    }

    private static string RenderIndexNestedLoopJoin(IndexNestedLoopJoinNode node)
    {
        string detail = $"on={RenderExpr(node.OnPredicate)}, index={node.Index.Name}, left={node.LeftLookupColumn}, right={node.RightIndexColumn}";
        if (node.RightExecutionFilter is not null)
            detail += $", right-filter={RenderExpr(node.RightExecutionFilter)}";
        return $"index-nested-loop-join({detail})";
    }

    private static string RenderHashJoin(HashJoinNode node)
    {
        string keys = string.Join(", ", node.ProbeKeyColumns.Zip(node.BuildKeyColumns,
            (p, b) => $"{p}={b}"));
        string build = node.BuildSide == HashJoinBuildSide.Left
            ? ResolveNodeAlias(node.Input) ?? node.BuildSource.Alias
            : node.BuildSource.Alias;
        string detail = $"on={keys}, build={build}";
        if (node.BuildExecutionFilter is not null)
            detail += $", build-filter={RenderExpr(node.BuildExecutionFilter)}";
        return $"hash-join({detail})";
    }

    private static string? ResolveNodeAlias(PhysicalPlanNode? node) => node switch
    {
        TableScanNode ts       => ts.BoundSource?.Alias,
        DerivedTableScanNode d => d.BoundSource.Alias,
        _                      => null,
    };

    private static string RenderMergeJoin(MergeJoinNode node)
    {
        string keys = string.Join(", ", node.LeftKeyColumns.Zip(node.RightKeyColumns,
            (l, r) => $"{l}={r}"));
        string detail = $"on={keys}";
        if (node.RightExecutionFilter is not null)
            detail += $", right-filter={RenderExpr(node.RightExecutionFilter)}";
        return $"merge-join({detail})";
    }

    private static string RenderDerivedTableScan(DerivedTableScanNode node) =>
        $"derived-table-scan(alias={node.BoundSource.Alias})";

    private static string RenderSemiJoin(SemiJoinNode node)
    {
        string kind = node.Mode switch
        {
            SemiJoinMode.Semi => "semi-join",
            SemiJoinMode.Anti => "anti-join",
            SemiJoinMode.NullAwareAnti => "null-aware-anti-join",
            _ => "semi-join",
        };
        string detail = $"outer={node.OuterColumn}, inner={node.InnerTable.Name}.{node.InnerColumn}";
        if (node.InnerIndex is not null)
            detail += $", index={node.InnerIndex.Name}";
        return $"{kind}({detail})";
    }

    // ── expression renderer ────────────────────────────────────────────────

    /// <summary>Renders a NodeAst expression as a compact, deterministic string.</summary>
    public static string RenderExpr(NodeAst expr)
    {
        return expr.nodeType switch
        {
            NodeType.Identifier => expr.yytext ?? "?",
            NodeType.Integer => expr.yytext ?? "?",
            NodeType.Float => expr.yytext ?? "?",
            NodeType.Bool => expr.yytext ?? "?",
            NodeType.Null => "NULL",
            NodeType.String => $"'{StripOuterQuotes(expr.yytext ?? "")}'",
            NodeType.ObjectIdLiteral => expr.yytext ?? "?",
            NodeType.Placeholder => expr.yytext ?? "?",
            NodeType.ExprAllFields => "*",

            NodeType.ExprAlias =>
                $"{RenderExpr(expr.leftAst!)} AS {expr.yytext ?? RenderExpr(expr.rightAst!)}",

            NodeType.ExprEquals => $"{RenderExpr(expr.leftAst!)} = {RenderExpr(expr.rightAst!)}",
            NodeType.ExprNotEquals => $"{RenderExpr(expr.leftAst!)} <> {RenderExpr(expr.rightAst!)}",
            NodeType.ExprLessThan => $"{RenderExpr(expr.leftAst!)} < {RenderExpr(expr.rightAst!)}",
            NodeType.ExprGreaterThan => $"{RenderExpr(expr.leftAst!)} > {RenderExpr(expr.rightAst!)}",
            NodeType.ExprLessEqualsThan => $"{RenderExpr(expr.leftAst!)} <= {RenderExpr(expr.rightAst!)}",
            NodeType.ExprGreaterEqualsThan => $"{RenderExpr(expr.leftAst!)} >= {RenderExpr(expr.rightAst!)}",

            NodeType.ExprBetween =>
                $"{RenderExpr(expr.leftAst!)} BETWEEN {RenderExpr(expr.extendedOne!)} AND {RenderExpr(expr.extendedTwo!)}",

            NodeType.ExprAnd => $"{RenderExpr(expr.leftAst!)} AND {RenderExpr(expr.rightAst!)}",
            NodeType.ExprOr => $"{RenderExpr(expr.leftAst!)} OR {RenderExpr(expr.rightAst!)}",

            NodeType.ExprLike => $"{RenderExpr(expr.leftAst!)} LIKE {RenderExpr(expr.rightAst!)}",
            NodeType.ExprILike => $"{RenderExpr(expr.leftAst!)} ILIKE {RenderExpr(expr.rightAst!)}",

            NodeType.ExprNot => $"NOT {RenderExpr(expr.leftAst!)}",
            NodeType.ExprIsNull => $"{RenderExpr(expr.leftAst!)} IS NULL",
            NodeType.ExprIsNotNull => $"{RenderExpr(expr.leftAst!)} IS NOT NULL",

            NodeType.ExprAdd => $"{RenderExpr(expr.leftAst!)} + {RenderExpr(expr.rightAst!)}",
            NodeType.ExprSub => $"{RenderExpr(expr.leftAst!)} - {RenderExpr(expr.rightAst!)}",
            NodeType.ExprMult => $"{RenderExpr(expr.leftAst!)} * {RenderExpr(expr.rightAst!)}",
            NodeType.ExprDiv  => $"{RenderExpr(expr.leftAst!)} / {RenderExpr(expr.rightAst!)}",

            NodeType.ExprFuncCall =>
                $"{expr.leftAst?.yytext ?? "?"}({(expr.rightAst is null || expr.rightAst.nodeType == NodeType.ExprAllFields ? "*" : RenderExpr(expr.rightAst))})",

            NodeType.ExprCast => $"CAST({RenderExpr(expr.leftAst!)} AS {expr.rightAst?.yytext ?? "?"})",

            NodeType.ExprInSubquery => $"{RenderExpr(expr.leftAst!)} IN (SELECT ...)",
            NodeType.ExprNotInSubquery => $"{RenderExpr(expr.leftAst!)} NOT IN (SELECT ...)",
            NodeType.ExprInMembership => $"{RenderExpr(expr.leftAst!)} IN (...)",
            NodeType.ExprNotInMembership => $"{RenderExpr(expr.leftAst!)} NOT IN (...)",
            NodeType.ExprScalarSubquery => "(SELECT ...)",
            NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated => "EXISTS (SELECT ...)",

            _ => $"[{expr.nodeType}]",
        };
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private static string RenderCompositeKey(CompositeColumnValue key)
    {
        if (key.Values.Length == 1)
            return RenderColumnValue(key.Values[0]);

        return "(" + string.Join(", ", key.Values.Select(RenderColumnValue)) + ")";
    }

    private static string RenderColumnValue(ColumnValue v) =>
        v.Type switch
        {
            CamusDB.Core.Catalogs.Models.ColumnType.String => $"'{v.StrValue}'",
            CamusDB.Core.Catalogs.Models.ColumnType.Id => v.StrValue ?? "?",
            CamusDB.Core.Catalogs.Models.ColumnType.Integer64 => v.LongValue.ToString(CultureInfo.InvariantCulture),
            CamusDB.Core.Catalogs.Models.ColumnType.Float64 => v.FloatValue.ToString(CultureInfo.InvariantCulture),
            CamusDB.Core.Catalogs.Models.ColumnType.Bool => v.BoolValue.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
            CamusDB.Core.Catalogs.Models.ColumnType.Null => "NULL",
            _ => "?",
        };

    private static string RenderAggregateCall(NodeAst proj)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(proj);
        return RenderExpr(target);
    }

    private static string StripOuterQuotes(string raw)
    {
        if (raw.Length >= 2 && raw[0] == raw[^1] && (raw[0] == '"' || raw[0] == '\''))
            return raw[1..^1];
        return raw.Trim('"');
    }
}
