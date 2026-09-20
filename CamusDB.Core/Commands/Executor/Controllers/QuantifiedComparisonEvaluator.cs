/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
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
/// <para>One element pair is compared by <see cref="SQLExecutorBaseCreator.EvalComparison"/>, the
/// same method the scalar operator uses. So a mixed numeric pair widens rather than failing, and an
/// incomparable pair raises the type error the scalar form raises — the quantifier changes how many
/// comparisons run, never what one comparison means.</para>
/// </summary>
internal static class QuantifiedComparisonEvaluator
{
    /// <summary>
    /// Folds <paramref name="right"/> against <paramref name="left"/> with the operator and
    /// quantifier that <paramref name="expr"/> carries. The caller evaluates both operands, so the
    /// WHERE path and the CHECK path can each use their own rule for reading a column.
    /// </summary>
    public static ColumnValue Evaluate(NodeAst expr, ColumnValue left, ColumnValue right)
    {
        NodeType op = QuantifiedComparison.OperatorOf(expr);
        bool isAll = QuantifiedComparison.IsAll(expr);

        if (right.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (right.Type != ColumnType.Array)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The right operand of {QuantifiedComparison.OperatorText(op)} " +
                $"{QuantifiedComparison.QuantifierOf(expr)} must be an array or a subquery but is {right.Type}");

        IReadOnlyList<ColumnValue> elements = right.ArrayValues!;

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
