/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Turns a SELECT whose select list or ORDER BY holds a window call into an ordinary query over a
/// derived table that carries the windows. It runs at the end of query creation, so every consumer
/// that builds a query there — a top-level SELECT, a subquery, a view body, a set-operation operand,
/// CTAS and INSERT … SELECT — gets windows the same way.
///
/// <para><b>The shape.</b> <c>SELECT items FROM src WHERE w GROUP BY g HAVING h ORDER BY o LIMIT l</c>
/// becomes <c>SELECT items' FROM (SELECT e0 AS ~win_in0, … [, *] FROM src WHERE w GROUP BY g HAVING h)
/// AS *window* ORDER BY o' LIMIT l</c>, and the derived table carries the window calls
/// (<see cref="DerivedTableSource.Windows"/>). The inner query computes every window input and every
/// window-free part of the outer items as a hidden column. The window operator runs over its rows
/// and adds one hidden result column per call. The outer query reads both kinds of column by name.</para>
///
/// <para><b>Why the logical order holds.</b> SQL runs windows after WHERE, GROUP BY and HAVING and
/// before the select list, DISTINCT, ORDER BY and LIMIT. The inner query does the first part
/// completely, aggregates included, so a window over <c>sum(x)</c> reads a finished value. The outer
/// query does the rest. Nothing in the planner or the aggregator needs to know about windows.</para>
///
/// <para><b>Names.</b> Every outer item is aliased to the output name it had, so the column names of
/// the result do not change. A <c>*</c> stays in the inner query; the outer <c>*</c> expands to the
/// derived columns that are not <see cref="DerivedColumnSchema.Hidden"/>, which is the same set.</para>
///
/// <para><b>Validation.</b> A window call where SQL does not allow one (WHERE, GROUP BY, HAVING, a join
/// ON, inside another window call) and a call this engine does not run are refused here, with
/// <see cref="CamusDBErrorCodes.InvalidInput"/>, before any table is opened. The walks stop at a nested
/// SELECT, because a subquery is created, and so checked, on its own.</para>
///
/// <para>The rewrite is a pure function of the query, like the rest of query creation, so the
/// bound-query cache may reuse its result across executions.</para>
/// </summary>
internal static class WindowQueryRewriter
{
    /// <summary>Prefix of a hidden window input column of the inner query.</summary>
    public const string InputPrefix = "~win_in";

    /// <summary>Prefix of a hidden window result column. Every hidden window column starts with it.</summary>
    public const string ResultPrefix = "~win";

    /// <summary>
    /// True when <paramref name="columnName"/> (bare, or qualified with a <c>.</c>) is a hidden window
    /// column. User columns cannot start with <c>~</c> (a column name holds letters, digits and
    /// underscores only), so this never matches a real column.
    /// </summary>
    public static bool IsHiddenColumn(string columnName)
    {
        int dot = columnName.LastIndexOf('.');
        ReadOnlySpan<char> bare = dot >= 0 ? columnName.AsSpan(dot + 1) : columnName.AsSpan();
        return bare.StartsWith(ResultPrefix, StringComparison.Ordinal);
    }

    /// <summary>True when <paramref name="ast"/> holds a window call outside any nested SELECT.</summary>
    public static bool ContainsWindow(NodeAst? ast)
    {
        if (ast is null)
            return false;

        Stack<NodeAst> pending = new();
        pending.Push(ast);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.ExprWindowCall)
                return true;

            // A subquery is created, and so checked, on its own.
            if (node.nodeType == NodeType.Select)
                continue;

            Push(pending, node.leftAst);
            Push(pending, node.rightAst);
            Push(pending, node.extendedOne);
            Push(pending, node.extendedTwo);
            Push(pending, node.extendedThree);
            Push(pending, node.extendedFour);
            Push(pending, node.extendedFive);
            Push(pending, node.extendedSix);
            Push(pending, node.extendedSeven);
        }

        return false;
    }

    /// <summary>
    /// Returns <paramref name="query"/> unchanged when it holds no window call, and its windowed
    /// rewrite otherwise. Throws <see cref="CamusDBErrorCodes.InvalidInput"/> for a window call in a
    /// clause that does not allow one, even when the select list has none.
    /// </summary>
    public static SelectQuery Rewrite(SelectQuery query)
    {
        RefuseIn(query.Where?.Expression, "WHERE");
        RefuseIn(query.Having?.Expression, "HAVING");

        if (query.GroupBy is { } groupBy)
        {
            foreach (NodeAst expression in groupBy)
                RefuseIn(expression, "GROUP BY");
        }

        RefuseInJoinConditions(query.Source);

        bool windowed = false;

        foreach (ProjectionItem item in query.Projections)
            windowed |= ContainsWindow(item.Expression);

        if (query.OrderBy is { } orderBy)
        {
            foreach (OrderByItem item in orderBy)
                windowed |= ContainsWindow(item.Expression);
        }

        if (!windowed)
            return query;

        return new Builder(query).Build();
    }

    private static void RefuseIn(NodeAst? ast, string clause)
    {
        if (ContainsWindow(ast))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Window functions are not allowed in {clause}");
    }

    private static void RefuseInJoinConditions(QuerySource source)
    {
        Stack<QuerySource> pending = new();
        pending.Push(source);

        while (pending.Count > 0)
        {
            if (pending.Pop() is not JoinSource join)
                continue;

            RefuseIn(join.OnPredicate, "a JOIN condition");
            pending.Push(join.Left);
            pending.Push(join.Right);
        }
    }

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst Alias(NodeAst expression, string name) =>
        new(NodeType.ExprAlias, expression, Identifier(name), null, null, null, null, null, null);

    /// <summary>
    /// A literal or a placeholder. Only these stay in the outer query and in a window call's constant
    /// arguments: anything else may read a column or be an aggregate, so it must be computed by the
    /// inner query. <c>count(*)</c> reads no column and is still an aggregate, which is why "reads no
    /// column" is not the test.
    /// </summary>
    private static bool IsConstant(NodeAst node) => node.nodeType is NodeType.Integer
        or NodeType.Float
        or NodeType.String
        or NodeType.Bool
        or NodeType.Null
        or NodeType.Placeholder
        or NodeType.BytesLiteral;

    /// <summary>
    /// The state of one rewrite: the hidden inputs and the window calls found so far, each kept once.
    /// </summary>
    private sealed class Builder
    {
        private readonly SelectQuery query;
        private readonly List<NodeAst> inputs = [];
        private readonly List<string> inputNames = [];
        private readonly List<NodeAst> windowCalls = [];
        private readonly List<WindowFunctionCall> windows = [];

        public Builder(SelectQuery query) => this.query = query;

        public SelectQuery Build()
        {
            IReadOnlyList<ProjectionItem> projections = query.Projections;
            List<ProjectionItem> outerProjections = new(projections.Count);
            NodeAst?[] liftedItems = new NodeAst?[projections.Count];
            bool hasStar = false;

            for (int i = 0; i < projections.Count; i++)
            {
                ProjectionItem item = projections[i];
                NodeAst target = QueryExpressionClassifier.UnwrapAlias(item.Expression);

                if (target.nodeType == NodeType.ExprAllFields)
                {
                    hasStar = true;
                    outerProjections.Add(item);
                    continue;
                }

                string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(item.Expression, i);
                NodeAst lifted = Lift(target);
                liftedItems[i] = lifted;
                outerProjections.Add(new ProjectionItem(Alias(lifted, name), item.OutputName ?? name));
            }

            List<OrderByItem>? outerOrderBy = null;

            if (query.OrderBy is { Count: > 0 } orderBy)
            {
                // SELECT DISTINCT * sorts before it projects, so it never checks that an ORDER BY key
                // is in its select list. A window key cannot be: refuse it as PostgreSQL does.
                if (query.IsDistinct && hasStar)
                {
                    foreach (OrderByItem item in orderBy)
                    {
                        if (ContainsWindow(item.Expression))
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                "For SELECT DISTINCT, an ORDER BY window function must appear in the select list");
                    }
                }

                outerOrderBy = new(orderBy.Count);

                foreach (OrderByItem item in orderBy)
                    outerOrderBy.Add(new OrderByItem(LiftOrderExpression(item.Expression, liftedItems), item.Direction));
            }

            List<ProjectionItem> innerProjections = new(inputs.Count + 1);

            if (hasStar)
                innerProjections.Add(new ProjectionItem(NodeAst.ExprAllFields, null));

            // A query whose windows read nothing (SELECT row_number() OVER () FROM t) still needs one
            // inner column, because a select list cannot be empty.
            if (inputs.Count == 0 && !hasStar)
                Register(NodeAst.FromLong(1));

            for (int i = 0; i < inputs.Count; i++)
                innerProjections.Add(new ProjectionItem(Alias(inputs[i], inputNames[i]), inputNames[i]));

            SelectQuery inner = new(
                Source: query.Source,
                Projections: innerProjections,
                Where: query.Where,
                GroupBy: query.GroupBy,
                Having: query.Having is { } having ? new BoundPredicate(ResolveHavingAliases(having.Expression)) : null,
                OrderBy: null,
                Limit: null,
                Offset: null,
                IsDistinct: false,
                CacheHint: null);

            DerivedTableSource derived = new(inner, DerivedTableSource.WindowDerivedAlias)
            {
                Windows = windows,
            };

            return new SelectQuery(
                Source: derived,
                Projections: outerProjections,
                Where: null,
                GroupBy: null,
                Having: null,
                OrderBy: outerOrderBy,
                Limit: query.Limit,
                Offset: query.Offset,
                IsDistinct: query.IsDistinct,
                CacheHint: query.CacheHint);
        }

        /// <summary>
        /// An ORDER BY item of the outer query. A name that is an output alias stays a name, because the
        /// outer query keeps every alias. An item equal to a select-list expression (an ordinal resolves
        /// to one) reuses that item's lifted form, so a DISTINCT query still finds it in its select list.
        /// Anything else is lifted on its own.
        /// </summary>
        private NodeAst LiftOrderExpression(NodeAst expression, NodeAst?[] liftedItems)
        {
            IReadOnlyList<ProjectionItem> projections = query.Projections;

            if (expression.nodeType == NodeType.Identifier)
            {
                foreach (ProjectionItem item in projections)
                {
                    if (item.Expression.nodeType == NodeType.ExprAlias
                        && string.Equals(item.OutputName, expression.yytext, StringComparison.OrdinalIgnoreCase))
                        return expression;
                }
            }

            for (int i = 0; i < projections.Count; i++)
            {
                if (liftedItems[i] is { } lifted
                    && QueryAstComparer.AreEquivalent(QueryExpressionClassifier.UnwrapAlias(projections[i].Expression), expression))
                    return lifted;
            }

            return Lift(expression);
        }

        /// <summary>
        /// HAVING may name a select-list alias. The inner query no longer has the outer aliases, so a
        /// name that is not a GROUP BY column and is the output name of a window-free select item is
        /// replaced with that item's expression — the same precedence the HAVING evaluator applies.
        /// </summary>
        private NodeAst ResolveHavingAliases(NodeAst having)
        {
            if (having.nodeType == NodeType.Identifier && having.yytext is { } name)
            {
                if (query.GroupBy is { } groupBy)
                {
                    foreach (NodeAst key in groupBy)
                    {
                        if (key.nodeType == NodeType.Identifier && string.Equals(key.yytext, name, StringComparison.Ordinal))
                            return having;
                    }
                }

                for (int i = 0; i < query.Projections.Count; i++)
                {
                    NodeAst item = query.Projections[i].Expression;

                    if (item.nodeType == NodeType.ExprAlias
                        && string.Equals(QueryProjectionResolver.GetOutputNameFromProjectionExpression(item, i), name, StringComparison.Ordinal)
                        && !ContainsWindow(item))
                        return QueryExpressionClassifier.UnwrapAlias(item);
                }

                return having;
            }

            if (having.nodeType == NodeType.Select)
                return having;

            NodeAst? left = having.leftAst is { } l ? ResolveHavingAliases(l) : null;
            NodeAst? right = having.rightAst is { } r ? ResolveHavingAliases(r) : null;
            NodeAst? one = having.extendedOne is { } e1 ? ResolveHavingAliases(e1) : null;
            NodeAst? two = having.extendedTwo is { } e2 ? ResolveHavingAliases(e2) : null;

            if (ReferenceEquals(left, having.leftAst)
                && ReferenceEquals(right, having.rightAst)
                && ReferenceEquals(one, having.extendedOne)
                && ReferenceEquals(two, having.extendedTwo))
                return having;

            // A function name is the call's left child, an identifier that must never be replaced.
            if (having.nodeType == NodeType.ExprFuncCall)
                left = having.leftAst;

            return new NodeAst(having.nodeType, left, right, one, two, having.extendedThree, having.extendedFour,
                having.extendedFive, having.yytext, having.extendedSix, having.extendedSeven);
        }

        /// <summary>
        /// The outer form of a value expression: a window call becomes a reference to its result
        /// column, a constant stays, any other window-free expression becomes a reference to a hidden
        /// inner column, and an expression that holds a window call keeps its shape with each part
        /// lifted. Recursion descends only into expressions that hold a window call, so its depth is
        /// bounded by the expression depth the parser admits.
        /// </summary>
        private NodeAst Lift(NodeAst node)
        {
            if (node.nodeType == NodeType.ExprWindowCall)
                return Identifier(RegisterWindow(node));

            if (!ContainsWindow(node))
                return IsConstant(node) ? node : Identifier(Register(node));

            return node.nodeType switch
            {
                // The name is not a value; only the arguments are lifted.
                NodeType.ExprFuncCall => Copy(node, node.leftAst, LiftPart(node.rightAst)),
                // The target type is not a value.
                NodeType.ExprCast => Copy(node, LiftPart(node.leftAst), node.rightAst),
                _ => Copy(node, LiftPart(node.leftAst), LiftPart(node.rightAst)),
            };
        }

        /// <summary>
        /// Lifts a child slot. A list or a CASE branch is structure, not a value: it is never replaced
        /// as a whole, only its members are.
        /// </summary>
        private NodeAst? LiftPart(NodeAst? node)
        {
            if (node is null)
                return null;

            if (node.nodeType is NodeType.ExprArgumentList or NodeType.ExprList or NodeType.ExprCaseWhenList or NodeType.ExprCaseWhen)
                return Copy(node, LiftPart(node.leftAst), LiftPart(node.rightAst));

            return Lift(node);
        }

        private NodeAst Copy(NodeAst node, NodeAst? left, NodeAst? right) =>
            new(node.nodeType, left, right, LiftPart(node.extendedOne), LiftPart(node.extendedTwo),
                LiftPart(node.extendedThree), LiftPart(node.extendedFour), LiftPart(node.extendedFive),
                node.yytext, LiftPart(node.extendedSix), LiftPart(node.extendedSeven));

        /// <summary>
        /// Returns the hidden inner column that computes <paramref name="expression"/>. A deterministic
        /// expression is added once and shared by every item that writes it. An expression with a
        /// volatile call (<c>gen_uuid_v4()</c>, <c>random()</c>) gives a new value at every evaluation,
        /// so each occurrence gets its own column, as each occurrence in a plain select list is its own
        /// evaluation.
        /// </summary>
        private string Register(NodeAst expression)
        {
            if (!ScalarFunctionEvaluator.ContainsVolatileFunction(expression))
            {
                for (int i = 0; i < inputs.Count; i++)
                {
                    if (QueryAstComparer.AreEquivalent(inputs[i], expression))
                        return inputNames[i];
                }
            }

            string name = InputPrefix + inputs.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
            inputs.Add(expression);
            inputNames.Add(name);
            return name;
        }

        /// <summary>
        /// Returns the result column of <paramref name="call"/>, describing the call once. Two calls
        /// share a result only when they are deterministic: a call that reads a volatile function
        /// (<c>sum(random()) OVER ()</c>) is a new computation each time it is written.
        /// </summary>
        private string RegisterWindow(NodeAst call)
        {
            if (!ScalarFunctionEvaluator.ContainsVolatileFunction(call))
            {
                for (int i = 0; i < windowCalls.Count; i++)
                {
                    if (QueryAstComparer.AreEquivalent(windowCalls[i], call))
                        return windows[i].ResultColumn;
                }
            }

            NodeAst function = call.leftAst!;
            NodeAst spec = call.rightAst!;

            if (ContainsWindow(function.rightAst) || ContainsWindow(spec))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Window function calls cannot be nested: a window call's arguments, PARTITION BY and ORDER BY "
                    + "cannot hold another window call");

            List<NodeAst> arguments = [];
            CollectList(function.rightAst, NodeType.ExprArgumentList, arguments);

            string functionName = function.leftAst?.yytext ?? "";
            WindowFunctionKind kind = ResolveKind(functionName, arguments);

            List<NodeAst> callArguments = new(arguments.Count);

            for (int i = 0; i < arguments.Count; i++)
            {
                NodeAst argument = arguments[i];

                if (argument.nodeType == NodeType.ExprAllFields || IsConstant(argument))
                    callArguments.Add(argument);
                else
                    callArguments.Add(Identifier(Register(argument)));
            }

            List<NodeAst> partitionExpressions = [];
            CollectList(spec.leftAst, NodeType.ExprList, partitionExpressions);

            List<string> partitionColumns = new(partitionExpressions.Count);

            foreach (NodeAst expression in partitionExpressions)
                partitionColumns.Add(Register(expression));

            List<NodeAst> orderItems = [];
            CollectList(spec.rightAst, NodeType.IdentifierList, orderItems);

            List<QueryOrderBy> orderColumns = new(orderItems.Count);

            foreach (NodeAst item in orderItems)
            {
                (NodeAst expression, OrderType direction) = item.nodeType switch
                {
                    NodeType.SortAsc => (item.leftAst!, OrderType.Ascending),
                    NodeType.SortDesc => (item.leftAst!, OrderType.Descending),
                    _ => (item, OrderType.Ascending),
                };

                orderColumns.Add(new QueryOrderBy(Register(expression), direction));
            }

            WindowFrameSpec frame = spec.extendedOne is { } frameAst ? ToFrame(frameAst) : WindowFrameSpec.Default;

            string resultColumn = ResultPrefix + windows.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);

            windowCalls.Add(call);
            windows.Add(new WindowFunctionCall(resultColumn, kind, callArguments, partitionColumns, orderColumns, frame, call));

            return resultColumn;
        }

        private static void CollectList(NodeAst? node, NodeType listType, List<NodeAst> items)
        {
            if (node is null)
                return;

            ExpressionChains.Flatten(node, listType, items);
        }

        private static WindowFrameSpec ToFrame(NodeAst frame) =>
            new(
                string.Equals(frame.yytext, WindowAst.Range, StringComparison.Ordinal),
                ToBound(frame.leftAst!),
                frame.rightAst is { } end ? ToBound(end) : new WindowBound(WindowBoundKind.CurrentRow, null));

        private static WindowBound ToBound(NodeAst bound) => bound.yytext switch
        {
            WindowAst.UnboundedPreceding => new WindowBound(WindowBoundKind.UnboundedPreceding, null),
            WindowAst.Preceding => new WindowBound(WindowBoundKind.Preceding, bound.leftAst),
            WindowAst.CurrentRow => new WindowBound(WindowBoundKind.CurrentRow, null),
            WindowAst.Following => new WindowBound(WindowBoundKind.Following, bound.leftAst),
            _ => new WindowBound(WindowBoundKind.UnboundedFollowing, null),
        };

        /// <summary>
        /// Maps a function name to the window function it calls and checks the argument count. A name
        /// outside the list — an unknown function, a scalar function, an extension aggregate — is
        /// refused, because the window operator has no state for it.
        /// </summary>
        private static WindowFunctionKind ResolveKind(string name, List<NodeAst> arguments)
        {
            (WindowFunctionKind Kind, int Min, int Max)? entry = name.ToLowerInvariant() switch
            {
                "row_number" => (WindowFunctionKind.RowNumber, 0, 0),
                "rank" => (WindowFunctionKind.Rank, 0, 0),
                "dense_rank" => (WindowFunctionKind.DenseRank, 0, 0),
                "percent_rank" => (WindowFunctionKind.PercentRank, 0, 0),
                "cume_dist" => (WindowFunctionKind.CumeDist, 0, 0),
                "ntile" => (WindowFunctionKind.Ntile, 1, 1),
                "lag" => (WindowFunctionKind.Lag, 1, 3),
                "lead" => (WindowFunctionKind.Lead, 1, 3),
                "first_value" => (WindowFunctionKind.FirstValue, 1, 1),
                "last_value" => (WindowFunctionKind.LastValue, 1, 1),
                "nth_value" => (WindowFunctionKind.NthValue, 2, 2),
                "count" => (WindowFunctionKind.Count, 1, 1),
                "sum" => (WindowFunctionKind.Sum, 1, 1),
                "avg" => (WindowFunctionKind.Avg, 1, 1),
                "min" => (WindowFunctionKind.Min, 1, 1),
                "max" => (WindowFunctionKind.Max, 1, 1),
                _ => null,
            };

            if (entry is not { } found)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"'{name}' is not a window function. A window call (OVER) takes row_number, rank, dense_rank, "
                    + "percent_rank, cume_dist, ntile, lag, lead, first_value, last_value, nth_value, count, sum, avg, min or max");

            if (arguments.Count < found.Min || arguments.Count > found.Max)
            {
                string expected = found.Min == found.Max
                    ? found.Min.ToString(System.Globalization.CultureInfo.InvariantCulture)
                    : $"{found.Min} to {found.Max}";

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Window function {name.ToLowerInvariant()} takes {expected} argument(s), got {arguments.Count}");
            }

            for (int i = 0; i < arguments.Count; i++)
            {
                if (arguments[i].nodeType == NodeType.ExprAllFields && found.Kind != WindowFunctionKind.Count)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Window function {name.ToLowerInvariant()} does not take *");
            }

            return found.Kind;
        }
    }
}
