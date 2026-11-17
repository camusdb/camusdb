/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Decides whether an expression of a join <c>ON</c> can be evaluated against the row of one side
/// alone. A join operator that computes such an expression once for each row of a side (a range seek
/// bound, an expression hash key) must never read a column of the other side, a value that changes
/// between calls, or a name it cannot resolve in the row.
///
/// <para>The accepted shapes are narrow on purpose: literals, identifiers qualified with an alias of
/// the side, arithmetic, <c>CAST</c>, <c>CASE</c>, comparisons and logical operators inside a
/// <c>CASE</c>, and calls of functions that are not volatile. A subquery, a window call or an
/// unqualified name is refused. A refusal is always safe: the caller keeps the plain plan.</para>
///
/// <para>The binder qualifies each bare name of a join predicate that one source has
/// (<see cref="JoinPredicateQualifier"/>), so a column of a bound statement arrives here with its
/// alias, however the statement wrote it.</para>
/// </summary>
internal static class JoinSideExpressions
{
    /// <summary>Every alias of the query other than <paramref name="rightAlias"/>.</summary>
    public static HashSet<string> LeftAliases(BoundSelectQuery bound, string rightAlias)
    {
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        foreach (BoundTableSource source in bound.Sources)
            aliases.Add(source.Alias);

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
            aliases.Add(source.Alias);

        aliases.Remove(rightAlias);
        return aliases;
    }

    /// <summary>
    /// True when <paramref name="expression"/> reads nothing but literals, columns of the left side
    /// and, with <paramref name="allowParameters"/>, statement parameters, and calls no volatile
    /// function. A caller that stores the expression in a cached plan and evaluates it with no
    /// parameters passes false. The walk is iterative, because an expression can be as deep as the
    /// statement allows.
    /// </summary>
    public static bool IsLeftOnly(NodeAst expression, HashSet<string> leftAliases, bool allowParameters) =>
        IsSafe(expression, allowParameters, identifier =>
            TrySplitQualified(identifier, out string alias, out _) && leftAliases.Contains(alias));

    /// <summary>
    /// True when <paramref name="expression"/> reads nothing but literals and columns of
    /// <paramref name="right"/>, each qualified with its alias, has no parameter and calls no volatile
    /// function. <paramref name="rowKeys"/> maps the text of each identifier (<c>p.u</c>) to the key
    /// under which a right row holds the cell (<c>u</c>, or the row key of a derived column), so the
    /// expression can be evaluated against an unqualified right row.
    /// </summary>
    public static bool IsRightOnly(NodeAst expression, BoundJoinRightSource right, out Dictionary<string, string> rowKeys)
    {
        Dictionary<string, string> keys = new(StringComparer.Ordinal);
        rowKeys = keys;

        return IsSafe(expression, allowParameters: false, identifier =>
        {
            if (!TrySplitQualified(identifier, out string alias, out string column)
                || !string.Equals(alias, right.Alias, StringComparison.OrdinalIgnoreCase)
                || !right.HasColumn(column))
                return false;

            string rowKey = column;
            if (right.Derived is { } derived && !derived.TryGetRowKey(column, out rowKey))
                return false;

            keys[identifier] = rowKey;
            return true;
        });
    }

    /// <summary>True when <paramref name="expression"/> reads at least one column.</summary>
    public static bool ReadsAColumn(NodeAst expression)
    {
        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.Identifier)
                return true;

            // The name of a called function and the target type of a CAST are identifiers too.
            if (node.nodeType == NodeType.ExprFuncCall)
            {
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);
                continue;
            }

            if (node.nodeType == NodeType.ExprCast)
            {
                if (node.leftAst is not null)
                    pending.Push(node.leftAst);
                continue;
            }

            PushChildren(node, pending);
        }

        return false;
    }

    private static bool IsSafe(NodeAst expression, bool allowParameters, Func<string, bool> acceptIdentifier)
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
                    if (node.yytext is null || !acceptIdentifier(node.yytext))
                        return false;
                    continue;

                case NodeType.Placeholder:
                    if (!allowParameters)
                        return false;
                    continue;

                case NodeType.Integer:
                case NodeType.Float:
                case NodeType.String:
                case NodeType.Bool:
                case NodeType.Null:
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

    /// <summary>Splits <c>alias.column</c>; false for a name with no qualifier.</summary>
    public static bool TrySplitQualified(string identifier, out string alias, out string columnName)
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

    private static void PushChildren(NodeAst node, Stack<NodeAst> pending)
    {
        foreach (NodeAst? child in new[] { node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo,
                     node.extendedThree, node.extendedFour, node.extendedFive, node.extendedSix, node.extendedSeven })
        {
            if (child is not null)
                pending.Push(child);
        }
    }
}
