
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Converts a parsed single-table <see cref="NodeType.Select"/> AST into a logical <see cref="SelectQuery"/>.
/// </summary>
internal sealed class SelectQueryCreator
{
    public SelectQuery CreateSelectQuery(NodeAst ast)
    {
        if (ast.nodeType != NodeType.Select)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Expected SELECT statement");

        if (ast.leftAst is null || ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid SELECT statement");

        QuerySource source = CreateQuerySource(ast.rightAst);
        IReadOnlyList<ProjectionItem> projections = CreateProjections(ast.leftAst);

        BoundPredicate? where = ast.extendedOne is not null
            ? new BoundPredicate(ast.extendedOne)
            : null;

        IReadOnlyList<OrderByItem>? orderBy = CreateOrderBy(ast.extendedTwo);
        IReadOnlyList<NodeAst>? groupBy = CreateGroupBy(ast.extendedFive);

        return new SelectQuery(
            Source: source,
            Projections: projections,
            Where: where,
            GroupBy: groupBy,
            OrderBy: orderBy,
            Limit: ast.extendedThree,
            Offset: ast.extendedFour);
    }

    private static QuerySource CreateQuerySource(NodeAst fromAst)
    {
        return fromAst.nodeType switch
        {
            NodeType.TableReference => CreateTableSource(fromAst),
            NodeType.Join => new JoinSource(
                CreateQuerySource(fromAst.leftAst!),
                CreateTableSource(fromAst.rightAst!),
                JoinKind.Inner,
                fromAst.extendedOne!),
            NodeType.Identifier => new TableSource(fromAst.yytext!),
            NodeType.IdentifierWithOpts => new TableSource(
                fromAst.leftAst!.yytext!,
                Alias: null,
                ForcedIndexName: GetForcedIndex(fromAst)),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid FROM clause"),
        };
    }

    private static TableSource CreateTableSource(NodeAst tableAst)
    {
        if (tableAst.nodeType == NodeType.TableReference)
        {
            string tableName = tableAst.leftAst!.yytext!;
            string? alias = tableAst.rightAst?.yytext;
            string? forcedIndex = tableAst.extendedOne is not null
                ? GetForcedIndex(tableAst.extendedOne)
                : null;

            return new TableSource(tableName, alias, forcedIndex);
        }

        return tableAst.nodeType switch
        {
            NodeType.Identifier => new TableSource(tableAst.yytext!),
            NodeType.IdentifierWithOpts => new TableSource(
                tableAst.leftAst!.yytext!,
                Alias: null,
                ForcedIndexName: GetForcedIndex(tableAst)),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid table reference"),
        };
    }

    private static string? GetForcedIndex(NodeAst rightAst)
    {
        if (rightAst.nodeType != NodeType.IdentifierWithOpts)
            return null;

        if (!rightAst.rightAst!.yytext!.Equals("FORCE_INDEX", StringComparison.InvariantCultureIgnoreCase))
            return null;

        string index = rightAst.extendedOne!.yytext!;
        return index.Equals("pk", StringComparison.InvariantCultureIgnoreCase)
            ? CamusDBConfig.PrimaryKeyInternalName
            : index;
    }

    private static IReadOnlyList<ProjectionItem> CreateProjections(NodeAst projectionAst)
    {
        List<ProjectionItem> projections = new();
        CollectProjectionItems(projectionAst, projections);
        return projections;
    }

    private static void CollectProjectionItems(NodeAst ast, List<ProjectionItem> projections)
    {
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                CollectProjectionItems(ast.leftAst, projections);

            if (ast.rightAst is not null)
                CollectProjectionItems(ast.rightAst, projections);

            return;
        }

        projections.Add(new ProjectionItem(ast, TryGetProjectionOutputName(ast)));
    }

    private static string? TryGetProjectionOutputName(NodeAst ast)
    {
        return ast.nodeType switch
        {
            NodeType.ExprAlias => ast.rightAst?.yytext,
            NodeType.Identifier => ast.yytext,
            _ => null,
        };
    }

    private static IReadOnlyList<NodeAst>? CreateGroupBy(NodeAst? groupByAst)
    {
        if (groupByAst is null)
            return null;

        if (groupByAst.nodeType != NodeType.GroupBy || groupByAst.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid GROUP BY clause");

        List<NodeAst> expressions = new();
        CollectGroupByExpressions(groupByAst.leftAst, expressions);

        if (expressions.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "GROUP BY requires at least one expression");

        return expressions;
    }

    private static void CollectGroupByExpressions(NodeAst ast, List<NodeAst> expressions)
    {
        if (ast.nodeType == NodeType.ExprList)
        {
            if (ast.leftAst is not null)
                CollectGroupByExpressions(ast.leftAst, expressions);

            if (ast.rightAst is not null)
                CollectGroupByExpressions(ast.rightAst, expressions);

            return;
        }

        expressions.Add(ast);
    }

    private static IReadOnlyList<OrderByItem>? CreateOrderBy(NodeAst? orderByAst)
    {
        if (orderByAst is null)
            return null;

        List<OrderByItem> orderItems = new();
        CollectOrderByItems(orderByAst, orderItems);
        return orderItems;
    }

    private static void CollectOrderByItems(NodeAst orderByAst, List<OrderByItem> orderItems)
    {
        switch (orderByAst.nodeType)
        {
            case NodeType.Identifier:
                orderItems.Add(new OrderByItem(orderByAst, OrderType.Ascending));
                return;

            case NodeType.SortAsc:
                orderItems.Add(new OrderByItem(orderByAst.leftAst!, OrderType.Ascending));
                return;

            case NodeType.SortDesc:
                orderItems.Add(new OrderByItem(orderByAst.leftAst!, OrderType.Descending));
                return;

            case NodeType.IdentifierList:
                if (orderByAst.leftAst is not null)
                    CollectOrderByItems(orderByAst.leftAst, orderItems);

                if (orderByAst.rightAst is not null)
                    CollectOrderByItems(orderByAst.rightAst, orderItems);

                return;

            default:
                orderItems.Add(new OrderByItem(orderByAst, OrderType.Ascending));
                return;
        }
    }
}
