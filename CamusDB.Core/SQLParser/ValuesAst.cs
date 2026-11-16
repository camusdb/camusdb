/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds and reads the <c>VALUES</c> list in <c>FROM</c>: <c>(VALUES (1, 'a'), (2, 'b')) AS v (id, k)</c>.
///
/// <para><b>Shape.</b> The parser writes the list as a derived table over a <c>UNION ALL</c> of FROM-less
/// <c>SELECT</c>s, one for each row: <c>(SELECT 1 AS column1, 'a' AS column2 UNION ALL SELECT 2, 'b') AS v (id, k)</c>.
/// A list of one row is the one <c>SELECT</c>. So every relation slot, the join planner, the column list,
/// <c>LATERAL</c> and the result types work with no code of their own: a column type follows the
/// <c>UNION</c> rule, as in PostgreSQL. A long chain is rebalanced after the parse
/// (<see cref="SetOperationAst.Normalize"/>), so its depth is <c>log2</c> of the row count.</para>
///
/// <para><b>Mark.</b> The derived table node carries the word <c>values</c> in its <c>yytext</c>, next to the
/// <c>LATERAL</c> mark (<see cref="LateralAst"/>). The view renderer reads it to write the <c>VALUES</c> form
/// back, and query creation reads it to name the source in <c>EXPLAIN</c>. Every code path that builds a
/// derived table node again must carry that <c>yytext</c>.</para>
///
/// <para><b>Rules checked here, as in PostgreSQL.</b> Every row has the same number of items. An item cannot
/// be <c>DEFAULT</c>, an aggregate call or a window call: in a FROM-less <c>SELECT</c> an aggregate would
/// return one row of its own, and <c>DEFAULT</c> has no column to take a default from. The aggregate check
/// knows the built-in names only, because a parse is shared by every database and an extension aggregate
/// belongs to one database.</para>
/// </summary>
public static class ValuesAst
{
    private const string Word = "values";

    private const string LateralWord = "lateral values";

    /// <summary>
    /// Builds the derived table for <c>(VALUES rows) AS alias [(columns)]</c>. <paramref name="rows"/> is the
    /// row list as the <c>INSERT</c> grammar builds it: one row, or a left-deep
    /// <see cref="NodeType.InsertBatchList"/> of rows, each row one item or a left-deep
    /// <see cref="NodeType.ExprList"/> of items.
    /// </summary>
    public static NodeAst Relation(NodeAst rows, NodeAst alias, NodeAst? columns)
    {
        List<NodeAst> rowList = Flatten(rows, NodeType.InsertBatchList);

        NodeAst? query = null;
        int width = -1;

        for (int r = 0; r < rowList.Count; r++)
        {
            List<NodeAst> items = Flatten(rowList[r], NodeType.ExprList);

            if (width < 0)
                width = items.Count;
            else if (items.Count != width)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "VALUES lists must all be the same length");

            NodeAst? fields = null;
            for (int i = 0; i < items.Count; i++)
            {
                CheckItem(items[i]);

                // The first row names the columns, as the left operand of a UNION does.
                NodeAst field = r == 0
                    ? new NodeAst(NodeType.ExprAlias, items[i], Identifier(ColumnName(i)), null, null, null, null, null, null)
                    : items[i];

                fields = fields is null
                    ? field
                    : new NodeAst(NodeType.IdentifierList, fields, field, null, null, null, null, null, null);
            }

            NodeAst row = new(NodeType.Select, fields, null, null, null, null, null, null, null);

            query = query is null ? row : SetOperationAst.Combine(SetOperationAst.Union, "ALL", query, row);
        }

        query = SetOperationAst.Finish(query!, null, null, null);

        return new NodeAst(NodeType.DerivedTableReference, query, alias, null, null, columns, null, null, Word);
    }

    /// <summary>The name of the column at zero-based <paramref name="index"/> when no column list names it.</summary>
    public static string ColumnName(int index) => "column" + (index + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>True when <paramref name="node"/> is a derived table that <see cref="Relation"/> built.</summary>
    public static bool IsValues(NodeAst node) =>
        node.nodeType == NodeType.DerivedTableReference
        && (string.Equals(node.yytext, Word, StringComparison.Ordinal)
            || string.Equals(node.yytext, LateralWord, StringComparison.Ordinal));

    /// <summary>The <c>yytext</c> of a derived table node with the <c>LATERAL</c> mark added to <paramref name="yytext"/>.</summary>
    internal static string? WithLateral(string? yytext, string lateralWord) =>
        string.Equals(yytext, Word, StringComparison.Ordinal) ? LateralWord : lateralWord;

    /// <summary>True when <paramref name="yytext"/> is the mark of a <c>LATERAL</c> <c>VALUES</c> list.</summary>
    internal static bool IsLateralMark(string? yytext) => string.Equals(yytext, LateralWord, StringComparison.Ordinal);

    /// <summary>
    /// The rows of the <c>VALUES</c> list in <paramref name="derived"/>, in source order, each one the list of its
    /// items with the column names removed. The view renderer writes them back as <c>VALUES</c>. The chain may
    /// be rebalanced and the stored body binder may alias every item, so this reads both forms.
    /// </summary>
    public static List<List<NodeAst>> Rows(NodeAst derived)
    {
        List<List<NodeAst>> rows = [];
        Stack<NodeAst> pending = new();
        pending.Push(derived.leftAst!);

        while (pending.TryPop(out NodeAst? query))
        {
            if (SetOperationAst.IsSetOperationSelect(query))
            {
                pending.Push(query.rightAst!.rightAst!);
                pending.Push(query.rightAst.leftAst!);
                continue;
            }

            List<NodeAst> items = Flatten(query.leftAst!, NodeType.IdentifierList);
            for (int i = 0; i < items.Count; i++)
            {
                if (items[i].nodeType == NodeType.ExprAlias)
                    items[i] = items[i].leftAst!;
            }

            rows.Add(items);
        }

        return rows;
    }

    /// <summary>The number of rows of the <c>VALUES</c> list in <paramref name="derived"/>.</summary>
    public static int RowCount(NodeAst derived)
    {
        int count = 0;
        Stack<NodeAst> pending = new();
        pending.Push(derived.leftAst!);

        while (pending.TryPop(out NodeAst? query))
        {
            if (SetOperationAst.IsSetOperationSelect(query))
            {
                pending.Push(query.rightAst!.rightAst!);
                pending.Push(query.rightAst.leftAst!);
                continue;
            }

            count++;
        }

        return count;
    }

    /// <summary>Refuses an item that a <c>VALUES</c> list in <c>FROM</c> cannot hold. A subquery is not entered.</summary>
    private static void CheckItem(NodeAst item)
    {
        Stack<NodeAst> pending = new();
        pending.Push(item);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.ExprDefault:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DEFAULT is not allowed in a VALUES list in FROM");

                case NodeType.ExprWindowCall:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Window functions are not allowed in VALUES");

                case NodeType.ExprFuncCall when node.leftAst?.yytext is { } name && AggregateFunctionCatalog.IsReservedName(name):
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Aggregate functions are not allowed in VALUES");

                case NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery or NodeType.ExprInSubquery
                    or NodeType.ExprNotInSubquery or NodeType.Select:
                    continue;
            }

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null)
                    pending.Push(child);
            }
        }
    }

    /// <summary>The elements of the left-deep list <paramref name="list"/> of node type <paramref name="listType"/>, in order.</summary>
    private static List<NodeAst> Flatten(NodeAst list, NodeType listType)
    {
        List<NodeAst> items = [];
        Stack<NodeAst> pending = new();
        pending.Push(list);

        while (pending.TryPop(out NodeAst? node))
        {
            if (node.nodeType == listType)
            {
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);
                if (node.leftAst is not null)
                    pending.Push(node.leftAst);
                continue;
            }

            items.Add(node);
        }

        return items;
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);
}
