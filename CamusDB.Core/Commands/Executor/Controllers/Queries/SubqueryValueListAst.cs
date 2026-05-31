
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class SubqueryValueListAst
{
    public static NodeAst BuildInMembership(NodeAst lhs, InSubqueryMaterialization materialization) =>
        new(
            NodeType.ExprInMembership,
            lhs,
            Build(materialization.Values),
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);

    public static NodeAst BuildNotInMembership(NodeAst lhs, InSubqueryMaterialization materialization) =>
        new(
            NodeType.ExprNotInMembership,
            lhs,
            Build(materialization.Values),
            extendedOne: BoolAst(materialization.ContainsNull),
            extendedTwo: BoolAst(materialization.IsEmpty),
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);

    public static NodeAst? Build(IReadOnlyList<ColumnValue> values)
    {
        if (values.Count == 0)
            return null;

        NodeAst current = ColumnValueAstBuilder.FromColumnValue(values[0]);

        for (int i = 1; i < values.Count; i++)
        {
            current = new NodeAst(
                NodeType.ExprList,
                current,
                ColumnValueAstBuilder.FromColumnValue(values[i]),
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: null);
        }

        return current;
    }

    public static bool ContainsValue(ColumnValue lhs, NodeAst? valueListAst)
    {
        if (valueListAst is null || lhs.Type == ColumnType.Null)
            return false;

        foreach (ColumnValue candidate in Enumerate(valueListAst))
        {
            if (candidate.Type == ColumnType.Null)
                continue;

            if (lhs.CompareTo(candidate) == 0)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Evaluates SQL <c>NOT IN</c> with three-valued semantics. Returns null for unknown.
    /// </summary>
    public static bool? EvaluateNotInMembership(ColumnValue lhs, NodeAst expr)
    {
        bool containsNull = ReadBool(expr.extendedOne);
        bool isEmpty = ReadBool(expr.extendedTwo);

        if (isEmpty)
            return true;

        foreach (ColumnValue candidate in Enumerate(expr.rightAst))
        {
            if (candidate.Type == ColumnType.Null)
                continue;

            if (lhs.Type != ColumnType.Null && lhs.CompareTo(candidate) == 0)
                return false;
        }

        if (containsNull)
            return null;

        return true;
    }

    private static bool ReadBool(NodeAst? ast)
    {
        if (ast is null || ast.nodeType != NodeType.Bool || ast.yytext is null)
            return false;

        return bool.Parse(ast.yytext);
    }

    private static NodeAst BoolAst(bool value) =>
        new(
            NodeType.Bool,
            leftAst: null,
            rightAst: null,
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: value ? "true" : "false");

    private static IEnumerable<ColumnValue> Enumerate(NodeAst? ast)
    {
        if (ast is null)
            yield break;

        if (ast.nodeType == NodeType.ExprList)
        {
            if (ast.leftAst is not null)
            {
                foreach (ColumnValue value in Enumerate(ast.leftAst))
                    yield return value;
            }

            if (ast.rightAst is not null)
            {
                foreach (ColumnValue value in Enumerate(ast.rightAst))
                    yield return value;
            }

            yield break;
        }

        yield return SQLExecutorBaseCreator.EvalExpr(ast, new(), parameters: null);
    }
}
