
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Refuses <c>AS OF SYSTEM TIME</c> on any query that does not read it: everywhere but on the outer
/// query of a SELECT, and in every subquery of a write.
/// </summary>
/// <remarks>
/// <para>The grammar accepts the clause on every <c>SELECT</c>, but only the statement root reads it:
/// the clause on the outer query moves the whole statement, subqueries included, to one snapshot. A
/// clause on a CTE body, a derived table, or a scalar, <c>IN</c>, <c>EXISTS</c> or <c>LATERAL</c>
/// subquery was parsed and never read, so the query returned current data with no error, and an
/// invalid value was not even checked. It is refused with <c>CADB0409</c>, the code an operand of a
/// set operation already gets.</para>
///
/// <para>A write reads its subqueries in the snapshot of the statement, so the clause on a subquery
/// of <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c> (WHERE, SET, VALUES, FROM, USING, ON CONFLICT,
/// RETURNING) is refused the same way. The source query of <c>INSERT … SELECT</c> and of
/// <c>CREATE TABLE … AS SELECT</c> is the outer query of its read: its own clause copies rows from the
/// past through a snapshot of its own, and only the queries below it are refused.</para>
///
/// <para>An operand of <c>UNION</c>, <c>INTERSECT</c> or <c>EXCEPT</c> is skipped here: the set-operation
/// builder refuses it with a message that names the operator. A view or materialized-view body is
/// refused at <c>CREATE</c> by <c>ViewDefinitionBuilder</c>.</para>
///
/// <para>The walk uses an explicit stack, because a statement can hold thousands of VALUES rows and
/// the row list is left-deep.</para>
/// </remarks>
internal static class NestedAsOfSystemTime
{
    /// <summary>Refuses the clause on any query below <paramref name="root"/>.</summary>
    internal static void RequireOnlyOnTheOuterQuery(NodeAst root) =>
        Refuse(root, root, "AS OF SYSTEM TIME is supported only on the outer query; every subquery reads in the snapshot of the statement");

    /// <summary>
    /// Refuses the clause on any query of an <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c> (joined
    /// forms included), except on the outer source query of <c>INSERT … SELECT</c>. Any other
    /// statement is left alone. Called on the statement after a write through a view is rewritten onto
    /// its table, and before any subquery runs.
    /// </summary>
    internal static void RequireNoneInWrite(NodeAst statement)
    {
        switch (statement.nodeType)
        {
            case NodeType.InsertSelect:
                Refuse(statement, statement.extendedOne,
                    "AS OF SYSTEM TIME is supported in INSERT ... SELECT only on the outer source query; " +
                    "every other query of the statement reads in the snapshot of the statement");
                return;

            case NodeType.Insert:
            case NodeType.Update:
            case NodeType.Delete:
                Refuse(statement, null,
                    $"AS OF SYSTEM TIME is not supported in a subquery of {StatementName(statement.nodeType)}; " +
                    "every subquery of a write reads in the snapshot of the statement");
                return;
        }
    }

    private static string StatementName(NodeType nodeType) => nodeType switch
    {
        NodeType.Update => "UPDATE",
        NodeType.Delete => "DELETE",
        _ => "INSERT",
    };

    /// <summary>
    /// Throws <paramref name="message"/> for the first <c>SELECT</c> below <paramref name="root"/> that
    /// carries the clause. <paramref name="outerQuery"/>, when not null, is the one query whose own
    /// clause is read, so it is not refused; the queries below it are.
    /// </summary>
    private static void Refuse(NodeAst root, NodeAst? outerQuery, string message)
    {
        Stack<(NodeAst Node, bool Skip)> pending = new();
        pending.Push((root, ReferenceEquals(root, outerQuery)));

        while (pending.Count > 0)
        {
            (NodeAst node, bool skip) = pending.Pop();

            if (!skip && node.nodeType == NodeType.Select && node.extendedSeven is not null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidAsOfSystemTime, message);

            // The operands of a set operation are refused by the set-operation builder, with the operator's name.
            bool operands = node.nodeType == NodeType.SetOperationReference;

            Push(pending, node.leftAst, operands, outerQuery);
            Push(pending, node.rightAst, operands, outerQuery);
            Push(pending, node.extendedOne, false, outerQuery);
            Push(pending, node.extendedTwo, false, outerQuery);
            Push(pending, node.extendedThree, false, outerQuery);
            Push(pending, node.extendedFour, false, outerQuery);
            Push(pending, node.extendedFive, false, outerQuery);
            Push(pending, node.extendedSix, false, outerQuery);
            Push(pending, node.extendedSeven, false, outerQuery);
        }
    }

    private static void Push(Stack<(NodeAst, bool)> pending, NodeAst? child, bool skip, NodeAst? outerQuery)
    {
        if (child is not null)
            pending.Push((child, skip || ReferenceEquals(child, outerQuery)));
    }
}
