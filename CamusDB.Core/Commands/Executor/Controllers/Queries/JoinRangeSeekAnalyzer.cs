/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
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
        HashSet<string> leftAliases = LeftAliases(bound, rightAlias);

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

    private static HashSet<string> LeftAliases(BoundSelectQuery bound, string rightAlias)
    {
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        foreach (BoundTableSource source in bound.Sources)
            aliases.Add(source.Alias);

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
            aliases.Add(source.Alias);

        aliases.Remove(rightAlias);
        return aliases;
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
                && conjunct.extendedOne is { } low && IsLeftOnly(low, leftAliases)
                && conjunct.extendedTwo is { } high && IsLeftOnly(high, leftAliases))
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

        if (TryRightKey(conjunct.leftAst, right, rightAlias, out string keyLeft) && IsLeftOnly(conjunct.rightAst, leftAliases))
        {
            Add(byColumn, keyLeft, new JoinRangeSeekBound(op, conjunct.rightAst));
            return;
        }

        if (TryRightKey(conjunct.rightAst, right, rightAlias, out string keyRight) && IsLeftOnly(conjunct.leftAst, leftAliases))
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

        if (!TrySplitQualified(text, out string alias, out column))
            return false;

        return string.Equals(alias, rightAlias, StringComparison.OrdinalIgnoreCase) && right.HasColumn(column);
    }

    /// <summary>
    /// True when <paramref name="expression"/> reads nothing but literals, parameters and columns of the
    /// left side, and calls no volatile function. The walk is iterative, because an expression can be
    /// as deep as the statement allows.
    /// </summary>
    private static bool IsLeftOnly(NodeAst expression, HashSet<string> leftAliases)
    {
        if (ScalarFunctionEvaluator.ContainsVolatileFunction(expression))
            return false;

        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    if (node.yytext is null
                        || !TrySplitQualified(node.yytext, out string alias, out _)
                        || !leftAliases.Contains(alias))
                        return false;
                    continue;

                case NodeType.Integer:
                case NodeType.Float:
                case NodeType.String:
                case NodeType.Bool:
                case NodeType.Null:
                case NodeType.Placeholder:
                    continue;

                case NodeType.ExprFuncCall:
                    // leftAst is the function name, not a column.
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprCast:
                    // rightAst is the target type, not a column.
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;

                case NodeType.ExprAdd:
                case NodeType.ExprSub:
                case NodeType.ExprMult:
                case NodeType.ExprDiv:
                case NodeType.ExprArgumentList:
                case NodeType.ExprList:
                case NodeType.ExprCase:
                case NodeType.ExprCaseWhen:
                case NodeType.ExprCaseWhenList:
                case NodeType.ExprEquals:
                case NodeType.ExprNotEquals:
                case NodeType.ExprLessThan:
                case NodeType.ExprLessEqualsThan:
                case NodeType.ExprGreaterThan:
                case NodeType.ExprGreaterEqualsThan:
                case NodeType.ExprAnd:
                case NodeType.ExprOr:
                case NodeType.ExprNot:
                case NodeType.ExprIsNull:
                case NodeType.ExprIsNotNull:
                    PushChildren(node, pending);
                    continue;

                default:
                    return false;
            }
        }

        return true;
    }

    private static void PushChildren(NodeAst node, Stack<NodeAst> pending)
    {
        foreach (NodeAst? child in new[] { node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo,
                     node.extendedThree, node.extendedFour, node.extendedFive, node.extendedSix, node.extendedSeven })
        {
            if (child is not null)
                pending.Push(child);
        }
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex <= 0 || dotIndex >= identifier.Length - 1)
        {
            alias = "";
            columnName = identifier;
            return false;
        }

        alias = identifier[..dotIndex];
        columnName = identifier[(dotIndex + 1)..];
        return true;
    }

    private static ColumnType ColumnTypeOf(TableDescriptor table, string columnName)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        return column?.Type ?? ColumnType.Null;
    }
}
