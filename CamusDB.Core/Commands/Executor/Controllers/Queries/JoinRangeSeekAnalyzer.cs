/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Finds a <see cref="JoinRangeSeek"/> for a join that has no equality key: <c>ON</c> conjuncts that
/// bound one column of the right table from below and from above with expressions of the left side,
/// where an ascending index of the right table starts with that column.
///
/// <para>Without it such a join runs as a plain nested loop, which tests every right row against the
/// predicate for every left row: a "numbers table" join (<c>JOIN numbers n ON n.n BETWEEN s.lo AND
/// s.hi</c>) of 5,000 left rows and 4,096 numbers tests 20 million pairs to return 64,000.</para>
///
/// <para>The accepted shapes are narrow on purpose, so that a bound can never read a right column and
/// can always be evaluated against the left row alone: the key is an identifier qualified with the
/// right alias; a bound is built from literals, parameters, identifiers qualified with another alias
/// of the query, arithmetic, <c>CAST</c>, <c>CASE</c> and calls of functions that are not volatile. A
/// subquery, a window call or an unqualified name is refused. Returning null is always safe: the join
/// keeps the plain nested loop, which is the correctness baseline.</para>
///
/// <para>Both a lower and an upper bound are required. A seek with one open side can read most of
/// the right table for each left row, and a read from storage per left row costs more than the replay
/// of the right side from memory that the plain nested loop does (see
/// <see cref="NestedLoopInnerSpool"/>).</para>
/// </summary>
internal static class JoinRangeSeekAnalyzer
{
    public static JoinRangeSeek? TryPlan(BoundJoinRightSource right, NodeAst? onPredicate, BoundSelectQuery bound)
    {
        if (right.Table is null || onPredicate is null)
            return null;

        TableDescriptor table = right.Table.Table;
        string rightAlias = right.Alias;
        HashSet<string> leftAliases = JoinSideExpressions.LeftAliases(bound, rightAlias);

        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, conjuncts);

        Dictionary<string, List<JoinRangeSeekBound>> byColumn = new(StringComparer.OrdinalIgnoreCase);

        foreach (NodeAst conjunct in conjuncts)
            CollectBounds(conjunct, right, rightAlias, leftAliases, byColumn);

        if (byColumn.Count == 0)
            return null;

        TableIndexSchema? best = null;
        List<JoinRangeSeekBound>? bestBounds = null;

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            if (index.Columns.Length == 0 || !IndexScanSelector.IsAscending(index, 0))
                continue;

            if (!byColumn.TryGetValue(index.Columns[0], out List<JoinRangeSeekBound>? bounds))
                continue;

            if (!HasBothSides(bounds))
                continue;

            // A unique index has no entry for a row with a NULL in any key column, so the columns after
            // the bounded one must be NOT NULL for the seek to see every row that can match.
            if (!IndexScanSelector.UniqueIndexHoldsEveryQualifyingRow(table, index, byColumn: null, constrainedPrefixLength: 1))
                continue;

            if (best is not null && index.Columns.Length >= best.Columns.Length)
                continue;

            best = index;
            bestBounds = bounds;
        }

        if (best is null)
            return null;

        return new JoinRangeSeek(best, best.Columns[0], ColumnTypeOf(table, best.Columns[0]), bestBounds!);
    }

    private static void CollectBounds(
        NodeAst conjunct,
        BoundJoinRightSource right,
        string rightAlias,
        HashSet<string> leftAliases,
        Dictionary<string, List<JoinRangeSeekBound>> byColumn)
    {
        if (conjunct.nodeType == NodeType.ExprBetween)
        {
            // x BETWEEN low AND high: the operand is leftAst, the bounds are extendedOne and extendedTwo.
            if (TryRightKey(conjunct.leftAst, right, rightAlias, out string column)
                && conjunct.extendedOne is { } low && JoinSideExpressions.IsLeftOnly(low, leftAliases, allowParameters: true)
                && conjunct.extendedTwo is { } high && JoinSideExpressions.IsLeftOnly(high, leftAliases, allowParameters: true))
            {
                Add(byColumn, column, new JoinRangeSeekBound(">=", low));
                Add(byColumn, column, new JoinRangeSeekBound("<=", high));
            }

            return;
        }

        string? op = conjunct.nodeType switch
        {
            NodeType.ExprLessThan => "<",
            NodeType.ExprLessEqualsThan => "<=",
            NodeType.ExprGreaterThan => ">",
            NodeType.ExprGreaterEqualsThan => ">=",
            _ => null,
        };

        if (op is null || conjunct.leftAst is null || conjunct.rightAst is null)
            return;

        if (TryRightKey(conjunct.leftAst, right, rightAlias, out string keyLeft) && JoinSideExpressions.IsLeftOnly(conjunct.rightAst, leftAliases, allowParameters: true))
        {
            Add(byColumn, keyLeft, new JoinRangeSeekBound(op, conjunct.rightAst));
            return;
        }

        if (TryRightKey(conjunct.rightAst, right, rightAlias, out string keyRight) && JoinSideExpressions.IsLeftOnly(conjunct.leftAst, leftAliases, allowParameters: true))
            Add(byColumn, keyRight, new JoinRangeSeekBound(Flip(op), conjunct.leftAst));
    }

    private static string Flip(string op) => op switch
    {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        _ => "<=",
    };

    private static void Add(Dictionary<string, List<JoinRangeSeekBound>> byColumn, string column, JoinRangeSeekBound seekBound)
    {
        if (!byColumn.TryGetValue(column, out List<JoinRangeSeekBound>? list))
            byColumn[column] = list = [];

        list.Add(seekBound);
    }

    private static bool HasBothSides(List<JoinRangeSeekBound> bounds)
    {
        bool lower = false, upper = false;

        foreach (JoinRangeSeekBound seekBound in bounds)
        {
            if (seekBound.IsLower)
                lower = true;
            else
                upper = true;
        }

        return lower && upper;
    }

    /// <summary>True when <paramref name="node"/> is a column of the right table, named with the right alias.</summary>
    private static bool TryRightKey(NodeAst? node, BoundJoinRightSource right, string rightAlias, out string column)
    {
        column = "";

        if (node is not { nodeType: NodeType.Identifier, yytext: { } text })
            return false;

        if (!JoinSideExpressions.TrySplitQualified(text, out string alias, out column))
            return false;

        return string.Equals(alias, rightAlias, StringComparison.OrdinalIgnoreCase) && right.HasColumn(column);
    }

    private static ColumnType ColumnTypeOf(TableDescriptor table, string columnName)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        return column?.Type ?? ColumnType.Null;
    }
}
