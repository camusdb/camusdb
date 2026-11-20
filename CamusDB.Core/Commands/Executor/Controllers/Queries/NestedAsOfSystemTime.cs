
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Refuses <c>AS OF SYSTEM TIME</c> anywhere but on the outer query of a statement.
/// </summary>
/// <remarks>
/// <para>The grammar accepts the clause on every <c>SELECT</c>, but only the statement root reads it:
/// the clause on the outer query moves the whole statement, subqueries included, to one snapshot. A
/// clause on a CTE body, a derived table, or a scalar, <c>IN</c>, <c>EXISTS</c> or <c>LATERAL</c>
/// subquery was parsed and never read, so the query returned current data with no error, and an
/// invalid value was not even checked. It is refused with <c>CADB0409</c>, the code an operand of a
/// set operation already gets.</para>
///
/// <para>An operand of <c>UNION</c>, <c>INTERSECT</c> or <c>EXCEPT</c> is skipped here: the set-operation
/// builder refuses it with a message that names the operator. A view or materialized-view body is
/// refused at <c>CREATE</c> by <c>ViewDefinitionBuilder</c>.</para>
/// </remarks>
internal static class NestedAsOfSystemTime
{
    /// <summary>Refuses the clause on any query below <paramref name="root"/>.</summary>
    internal static void RequireOnlyOnTheOuterQuery(NodeAst root)
    {
        Stack<(NodeAst Node, bool Skip)> pending = new();
        pending.Push((root, true));

        while (pending.Count > 0)
        {
            (NodeAst node, bool skip) = pending.Pop();

            if (!skip && node.nodeType == NodeType.Select && node.extendedSeven is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidAsOfSystemTime,
                    "AS OF SYSTEM TIME is supported only on the outer query; every subquery reads in the snapshot of the statement");

            // The operands of a set operation are refused by the set-operation builder, with the operator's name.
            bool operands = node.nodeType == NodeType.SetOperationReference;

            Push(pending, node.leftAst, operands);
            Push(pending, node.rightAst, operands);
            Push(pending, node.extendedOne, false);
            Push(pending, node.extendedTwo, false);
            Push(pending, node.extendedThree, false);
            Push(pending, node.extendedFour, false);
            Push(pending, node.extendedFive, false);
            Push(pending, node.extendedSix, false);
            Push(pending, node.extendedSeven, false);
        }
    }

    private static void Push(Stack<(NodeAst, bool)> pending, NodeAst? child, bool skip)
    {
        if (child is not null)
            pending.Push((child, skip));
    }
}
