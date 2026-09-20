
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class SubqueryValueListAst
{
    /// <summary>
    /// Builds an <c>ExprInMembership</c> node by asynchronously enumerating the
    /// <see cref="SpillableValueList"/> in the materialization. The enumeration reads from disk
    /// when the value list has spilled, keeping peak in-memory usage bounded during collection.
    /// The caller is responsible for disposing the materialization after this method returns.
    ///
    /// <para>The list drops the NULL values the subquery returned, so the node carries that fact in
    /// <c>extendedOne</c> and the emptiness of the subquery in <c>extendedTwo</c>, exactly as the
    /// NOT IN node does. Without them <c>5 IN (SELECT …)</c> over <c>{1, NULL}</c> would be FALSE
    /// instead of UNKNOWN, and an all-NULL result would look like an empty one.</para>
    /// </summary>
    public static async Task<NodeAst> BuildInMembershipAsync(
        NodeAst lhs,
        InSubqueryMaterialization materialization,
        CancellationToken ct = default) =>
        new(
            NodeType.ExprInMembership,
            lhs,
            await BuildAsync(materialization.Values, ct).ConfigureAwait(false),
            extendedOne: BoolAst(materialization.ContainsNull),
            extendedTwo: BoolAst(materialization.IsEmpty),
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);

    /// <summary>
    /// Builds an <c>ExprNotInMembership</c> node by asynchronously enumerating the
    /// <see cref="SpillableValueList"/> in the materialization. Three-valued semantics are
    /// preserved via the <c>extendedOne</c>/<c>extendedTwo</c> bool AST nodes.
    /// The caller is responsible for disposing the materialization after this method returns.
    /// </summary>
    public static async Task<NodeAst> BuildNotInMembershipAsync(
        NodeAst lhs,
        InSubqueryMaterialization materialization,
        CancellationToken ct = default) =>
        new(
            NodeType.ExprNotInMembership,
            lhs,
            await BuildAsync(materialization.Values, ct).ConfigureAwait(false),
            extendedOne: BoolAst(materialization.ContainsNull),
            extendedTwo: BoolAst(materialization.IsEmpty),
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);

    /// <summary>
    /// Builds an <c>ARRAY[…]</c> literal node holding everything the subquery returned, for the
    /// ordered quantified comparisons (<c>x &lt; ANY (SELECT …)</c> and the rest). Those fold over an
    /// array value, not over an <c>IN</c> list, so the materialized rows become an array the ordinary
    /// expression evaluator can build.
    ///
    /// <para>The materialization drops the NULL rows and records them in
    /// <c>ContainsNull</c>, so one NULL element is appended when it saw any. One is enough: the fold
    /// only asks whether a comparison was UNKNOWN, never how many were. An empty subquery gives an
    /// empty <c>ARRAY[]</c>, which the fold reads as FALSE for <c>ANY</c> and TRUE for
    /// <c>ALL</c>.</para>
    ///
    /// <para>The caller disposes the materialization after this method returns.</para>
    /// </summary>
    public static async Task<NodeAst> BuildArrayLiteralAsync(
        InSubqueryMaterialization materialization,
        CancellationToken ct = default)
    {
        List<NodeAst> items = [];

        await foreach (ColumnValue value in materialization.Values.EnumerateAsync(ct).ConfigureAwait(false))
        {
            if (value.Type == ColumnType.Null)
                continue;

            items.Add(ColumnValueAstBuilder.FromColumnValue(value));
        }

        if (materialization.ContainsNull)
            items.Add(NodeAst.Null);

        return new NodeAst(
            NodeType.ArrayLiteral,
            ExpressionChains.Combine(NodeType.ExprList, items),
            null, null, null, null, null, null, null);
    }

    /// <summary>
    /// Builds an <c>ExprList</c> tree from a literal <c>IReadOnlyList&lt;ColumnValue&gt;</c>.
    /// Used for grammar-parsed literal IN lists (e.g. <c>WHERE x IN (1, 2, 3)</c>).
    /// A long list is built balanced (see <see cref="ExpressionChains"/>): a left-deep list is one
    /// node deeper per value, and a recursive walker over it overflows the stack.
    /// </summary>
    public static NodeAst? Build(IReadOnlyList<ColumnValue> values)
    {
        if (values.Count == 0)
            return null;

        List<NodeAst> items = new(values.Count);

        for (int i = 0; i < values.Count; i++)
            items.Add(ColumnValueAstBuilder.FromColumnValue(values[i]));

        return ExpressionChains.Combine(NodeType.ExprList, items);
    }

    /// <summary>
    /// Builds an <c>ExprList</c> tree by asynchronously enumerating a
    /// <see cref="SpillableValueList"/>. When the list has spilled to disk, values are read
    /// from the spill file rather than from an in-memory <c>List&lt;ColumnValue&gt;</c>,
    /// bounding collection-time memory. Null values are skipped (nulls are tracked separately
    /// in <c>InSubqueryMaterialization.ContainsNull</c>).
    ///
    /// <para>The result is balanced for a long list. A subquery can return any number of values, and
    /// this tree is built at execution time, after the parser's own rebalancing and depth check, so
    /// a left-deep list here would be one node deeper per value returned and would overflow the stack
    /// of the first recursive walker to visit it.</para>
    /// </summary>
    private static async Task<NodeAst?> BuildAsync(SpillableValueList list, CancellationToken ct = default)
    {
        List<NodeAst> items = [];

        await foreach (ColumnValue value in list.EnumerateAsync(ct).ConfigureAwait(false))
        {
            if (value.Type == ColumnType.Null)
                continue;

            items.Add(ColumnValueAstBuilder.FromColumnValue(value));
        }

        return ExpressionChains.Combine(NodeType.ExprList, items);
    }

    /// <summary>
    /// Evaluates an <c>ExprInMembership</c> or <c>ExprNotInMembership</c> node with SQL
    /// three-valued logic, and returns TRUE, FALSE, or a NULL value for UNKNOWN.
    ///
    /// <para><c>x IN (a, b, c)</c> is defined as <c>x = a OR x = b OR x = c</c>, and <c>x NOT IN
    /// (…)</c> is <c>NOT (x IN (…))</c>. It follows that:</para>
    /// <list type="bullet">
    ///   <item>An empty list gives FALSE for IN and TRUE for NOT IN, whatever <c>x</c> is. Only a
    ///   subquery list can be empty; the grammar requires at least one literal item.</item>
    ///   <item>Otherwise a NULL <c>x</c> gives UNKNOWN, because every <c>x = item</c> is UNKNOWN.</item>
    ///   <item>A match gives TRUE for IN and FALSE for NOT IN, even when the list also holds a NULL.</item>
    ///   <item>No match plus a NULL item gives UNKNOWN for both, because that item's equality is
    ///   UNKNOWN.</item>
    /// </list>
    ///
    /// <para>UNKNOWN must stay a NULL value and never collapse to FALSE here. A WHERE filter treats
    /// both alike, but a NOT above this node does not: NOT FALSE is TRUE, and NOT UNKNOWN is
    /// UNKNOWN. A CHECK constraint also passes on UNKNOWN and fails on FALSE.</para>
    ///
    /// <para>A subquery list has its NULL items removed, so its node records them in
    /// <c>extendedOne</c> and its emptiness in <c>extendedTwo</c>. A literal list keeps its NULL
    /// items and has neither flag. <see cref="PreparedInSet.Evaluate"/> must return the same result.</para>
    /// </summary>
    public static ColumnValue EvaluateMembership(ColumnValue lhs, NodeAst expr, Dictionary<string, ColumnValue>? parameters = null)
    {
        bool negated = expr.nodeType == NodeType.ExprNotInMembership;

        if (IsEmptyList(expr))
            return ColumnValue.FromBool(negated);

        if (lhs.Type == ColumnType.Null)
            return ColumnValue.Null;

        bool sawNull = ReadBool(expr.extendedOne);

        foreach (ColumnValue candidate in Enumerate(expr.rightAst, parameters))
        {
            if (candidate.Type == ColumnType.Null)
            {
                sawNull = true;
                continue;
            }

            if (MixedNumericComparison.EqualsForMembership(lhs, candidate))
                return ColumnValue.FromBool(!negated);
        }

        return sawNull ? ColumnValue.Null : ColumnValue.FromBool(negated);
    }

    /// <summary>
    /// True when a membership node's list is known to hold a NULL item: a subquery list whose
    /// NULL items were removed at materialization. A literal list keeps its NULL items in the
    /// tree, so this returns false for it; callers that read the literal items see those NULLs
    /// themselves.
    /// </summary>
    public static bool ListContainsRemovedNull(NodeAst expr) => ReadBool(expr.extendedOne);

    /// <summary>
    /// True when a membership node's list is empty. A materialized subquery with no rows sets
    /// <c>extendedTwo</c>. A node with no list and no removed NULL is empty too; a list of only
    /// NULL items is not, because <c>extendedOne</c> records them.
    /// </summary>
    private static bool IsEmptyList(NodeAst expr) =>
        ReadBool(expr.extendedTwo) || (expr.rightAst is null && !ReadBool(expr.extendedOne));

    private static bool ReadBool(NodeAst? ast)
    {
        if (ast is null || ast.nodeType != NodeType.Bool || ast.yytext is null)
            return false;

        return bool.Parse(ast.yytext);
    }

    private static NodeAst BoolAst(bool value) => value ? NodeAst.True : NodeAst.False;

    /// <summary>
    /// Yields the values of an <c>ExprList</c> tree in source order. Iterative, so neither the depth
    /// of a list nor its shape can overflow the stack; the previous nested-iterator form also cost
    /// time proportional to depth for every value it yielded.
    /// </summary>
    private static IEnumerable<ColumnValue> Enumerate(NodeAst? ast, Dictionary<string, ColumnValue>? parameters = null)
    {
        if (ast is null)
            yield break;

        Stack<NodeAst> pending = new();
        pending.Push(ast);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.ExprList)
            {
                // Right first, so the left side pops first and source order is preserved.
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);

                if (node.leftAst is not null)
                    pending.Push(node.leftAst);

                continue;
            }

            yield return SQLExecutorBaseCreator.EvalExpr(node, new Dictionary<string, ColumnValue>(), parameters);
        }
    }
}
