/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The three-valued fold behind an ordered quantified comparison — <c>x &lt; ANY (a)</c>,
/// <c>x &gt;= ALL (a)</c>, <c>x = ALL (a)</c>, <c>x &lt;&gt; ANY (a)</c> and the rest. The membership
/// forms never reach here: the parser turns them into <c>IN</c> / <c>NOT IN</c> /
/// <c>array_contains</c>, which carry their own equal rule.
///
/// <para>Every caller folds through this one method, so the WHERE evaluator and a CHECK constraint
/// cannot disagree on a NULL. The result is TRUE, FALSE, or a NULL value for UNKNOWN; UNKNOWN must
/// stay a NULL value and must never collapse to FALSE, because a <c>NOT</c> above the node and a
/// CHECK constraint both read the difference.</para>
///
/// <para>The rules, in the order the method applies them:</para>
/// <list type="number">
///   <item>A NULL array gives UNKNOWN. There is no set to fold over, and the set is not known to be
///     empty either.</item>
///   <item>An empty array gives FALSE for <c>ANY</c> and TRUE for <c>ALL</c>, whatever <c>x</c> is,
///     including a NULL <c>x</c>. <c>ANY</c> asserts that one element satisfies the comparison and
///     no element does; <c>ALL</c> asserts that none violates it and none can. This test comes
///     before the NULL-<c>x</c> test for that reason, exactly as <c>array_contains</c> orders
///     them.</item>
///   <item>A NULL <c>x</c> over a non-empty array gives UNKNOWN: every element comparison is
///     UNKNOWN.</item>
///   <item>Otherwise the fold is short-circuiting. <c>ANY</c> gives TRUE at the first TRUE
///     comparison; <c>ALL</c> gives FALSE at the first FALSE one. A decided result wins over a
///     NULL element that was already seen, because one true witness settles <c>ANY</c> and one
///     counter-example settles <c>ALL</c>.</item>
///   <item>With no decisive element, the answer is UNKNOWN when any element comparison was UNKNOWN,
///     and otherwise FALSE for <c>ANY</c> and TRUE for <c>ALL</c>.</item>
/// </list>
///
/// <para>The right operand takes one of two forms. A written array gives an array value, and a
/// subquery gives an <see cref="NodeType.ExprValueSet"/> node that a rewrite step built from the
/// rows. Both fold through the same private method, so the two forms cannot drift apart. The set
/// may hold values of more than one type, which an array cannot: a subquery can return an Integer64
/// in one row and a Float64 in the next, and each pair is compared on its own.</para>
///
/// <para>One element pair is compared by <see cref="SQLExecutorBaseCreator.EvalComparison"/>, the
/// same method the scalar operator uses. So a mixed numeric pair widens rather than failing, and an
/// incomparable pair raises the type error the scalar form raises — the quantifier changes how many
/// comparisons run, never what one comparison means.</para>
/// </summary>
internal static class QuantifiedComparisonEvaluator
{
    /// <summary>
    /// The evaluated values of one <see cref="NodeType.ExprValueSet"/> node. The node holds literals
    /// only, so its values are the same for every row, and the fold would otherwise re-evaluate every
    /// element for every row it is asked about. The table is keyed on the identity of the node and
    /// holds it weakly, so the values go away with the rewritten tree that owns them.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeAst, ColumnValue[]> ValueSetCache = new();

    /// <summary>
    /// True when the right operand of <paramref name="expr"/> is a set of values a rewrite step
    /// already read from a subquery. A caller must ask this before it evaluates the right operand:
    /// the set is not an expression and has no value of its own, so
    /// <see cref="EvaluateOverValueSet"/> takes its place.
    /// </summary>
    public static bool HasValueSet(NodeAst expr) => expr.rightAst?.nodeType == NodeType.ExprValueSet;

    /// <summary>
    /// Folds the values of the set against <paramref name="left"/>, by the rules
    /// <see cref="Evaluate"/> applies to an array. The set replaces the array for a subquery, and the
    /// two differ in one way only: a set may hold values of more than one type, because a subquery
    /// can return them and each pair is compared on its own.
    /// </summary>
    public static ColumnValue EvaluateOverValueSet(NodeAst expr, ColumnValue left) =>
        Fold(expr, left, ValuesOf(expr.rightAst!));

    /// <summary>
    /// Returns the evaluated values of a value-set node, and keeps them for the next row. A node
    /// that crossed the wire to a peer arrives with no values kept, so the first fold there builds
    /// them from the literals in the node.
    /// </summary>
    private static ColumnValue[] ValuesOf(NodeAst valueSet)
    {
        if (ValueSetCache.TryGetValue(valueSet, out ColumnValue[]? cached))
            return cached;

        List<ColumnValue> values = [];

        foreach (ColumnValue value in SubqueryValueListAst.Enumerate(valueSet.leftAst))
            values.Add(value);

        ColumnValue[] evaluated = values.ToArray();

        // A benign race stores whichever equal array wins last.
        ValueSetCache.AddOrUpdate(valueSet, evaluated);
        return evaluated;
    }

    /// <summary>
    /// Folds <paramref name="right"/> against <paramref name="left"/> with the operator and
    /// quantifier that <paramref name="expr"/> carries. The caller evaluates both operands, so the
    /// WHERE path and the CHECK path can each use their own rule for reading a column.
    /// </summary>
    public static ColumnValue Evaluate(NodeAst expr, ColumnValue left, ColumnValue right)
    {
        if (right.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (right.Type != ColumnType.Array)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The right operand of {QuantifiedComparison.OperatorText(QuantifiedComparison.OperatorOf(expr))} " +
                $"{QuantifiedComparison.QuantifierOf(expr)} must be an array or a subquery but is {right.Type}");

        return Fold(expr, left, right.ArrayValues!);
    }

    /// <summary>
    /// The fold itself, over elements that come either from an array value or from a value set. Both
    /// forms apply the same rules, so one method serves them and they cannot drift apart.
    /// </summary>
    private static ColumnValue Fold(NodeAst expr, ColumnValue left, IReadOnlyList<ColumnValue> elements)
    {
        NodeType op = QuantifiedComparison.OperatorOf(expr);
        bool isAll = QuantifiedComparison.IsAll(expr);

        if (elements.Count == 0)
            return ColumnValue.FromBool(isAll);

        if (left.Type == ColumnType.Null)
            return ColumnValue.Null;

        bool sawNull = false;

        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue comparison = SQLExecutorBaseCreator.EvalComparison(op, left, elements[i]);

            if (comparison.Type != ColumnType.Bool)
            {
                sawNull = true;
                continue;
            }

            if (comparison.BoolValue != isAll)
                return ColumnValue.FromBool(!isAll);
        }

        return sawNull ? ColumnValue.Null : ColumnValue.FromBool(isAll);
    }
}
