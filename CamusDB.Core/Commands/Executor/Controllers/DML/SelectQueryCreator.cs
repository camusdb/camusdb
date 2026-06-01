
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
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

        (QuerySource source, BoundPredicate? where) = CreateFromClause(ast.rightAst, ast.extendedOne);
        IReadOnlyList<ProjectionItem> projections = CreateProjections(ast.leftAst);

        IReadOnlyList<OrderByItem>? orderBy = CreateOrderBy(ast.extendedTwo, projections);
        IReadOnlyList<NodeAst>? groupBy = CreateGroupBy(ast.extendedFive, projections);
        BoundPredicate? having = CreateHaving(ast.extendedSix);

        return new SelectQuery(
            Source: source,
            Projections: projections,
            Where: where,
            GroupBy: groupBy,
            Having: having,
            OrderBy: orderBy,
            Limit: ast.extendedThree,
            Offset: ast.extendedFour,
            IsDistinct: ast.yytext is not null);
    }

    private (QuerySource Source, BoundPredicate? Where) CreateFromClause(NodeAst fromAst, NodeAst? whereAst)
    {
        BoundPredicate? where = whereAst is not null ? new BoundPredicate(whereAst) : null;

        if (fromAst.nodeType == NodeType.CommaJoin)
            return CommaJoinNormalizer.Normalize(fromAst, where, CreateTableReferenceSource);

        return (CreateQuerySource(fromAst), where);
    }

    private static QuerySource CreateTableReferenceSource(NodeAst tableAst) =>
        tableAst.nodeType switch
        {
            NodeType.TableReference => CreateTableSource(tableAst),
            NodeType.DerivedTableReference => CreateDerivedTableSource(tableAst),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid comma join table reference"),
        };

    private static QuerySource CreateQuerySource(NodeAst fromAst)
    {
        return fromAst.nodeType switch
        {
            NodeType.TableReference => CreateTableSource(fromAst),
            NodeType.DerivedTableReference => CreateDerivedTableSource(fromAst),
            NodeType.Join => new JoinSource(
                CreateQuerySource(fromAst.leftAst!),
                CreateQuerySource(fromAst.rightAst!),
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

    private static DerivedTableSource CreateDerivedTableSource(NodeAst derivedAst)
    {
        if (derivedAst.leftAst is null || derivedAst.rightAst?.yytext is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Derived table requires a subquery and alias");
        }

        SelectQuery innerQuery = new SelectQueryCreator().CreateSelectQuery(derivedAst.leftAst);
        return new DerivedTableSource(innerQuery, derivedAst.rightAst.yytext);
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

    private static IReadOnlyList<NodeAst>? CreateGroupBy(
        NodeAst? groupByAst,
        IReadOnlyList<ProjectionItem> projections)
    {
        if (groupByAst is null)
            return null;

        if (groupByAst.nodeType != NodeType.GroupBy || groupByAst.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid GROUP BY clause");

        List<NodeAst> expressions = new();
        CollectGroupByExpressions(groupByAst.leftAst, expressions, projections);

        if (expressions.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "GROUP BY requires at least one expression");

        return expressions;
    }

    private static void CollectGroupByExpressions(
        NodeAst ast,
        List<NodeAst> expressions,
        IReadOnlyList<ProjectionItem> projections)
    {
        if (ast.nodeType == NodeType.ExprList)
        {
            if (ast.leftAst is not null)
                CollectGroupByExpressions(ast.leftAst, expressions, projections);

            if (ast.rightAst is not null)
                CollectGroupByExpressions(ast.rightAst, expressions, projections);

            return;
        }

        NodeAst expression = ResolveSelectOrdinal(ast, projections, "GROUP BY");

        if (QueryExpressionClassifier.IsAggregateProjection(expression))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "GROUP BY ordinal cannot reference an aggregate projection");
        }

        expressions.Add(expression);
    }

    private static BoundPredicate? CreateHaving(NodeAst? havingAst)
    {
        if (havingAst is null)
            return null;

        if (havingAst.nodeType != NodeType.Having || havingAst.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid HAVING clause");

        return new BoundPredicate(havingAst.leftAst);
    }

    private static IReadOnlyList<OrderByItem>? CreateOrderBy(
        NodeAst? orderByAst,
        IReadOnlyList<ProjectionItem> projections)
    {
        if (orderByAst is null)
            return null;

        List<OrderByItem> orderItems = new();
        CollectOrderByItems(orderByAst, orderItems, projections);
        return orderItems;
    }

    private static void CollectOrderByItems(
        NodeAst orderByAst,
        List<OrderByItem> orderItems,
        IReadOnlyList<ProjectionItem> projections)
    {
        switch (orderByAst.nodeType)
        {
            case NodeType.Identifier:
            case NodeType.Integer:
                orderItems.Add(new OrderByItem(
                    ResolveSelectOrdinal(orderByAst, projections, "ORDER BY"),
                    OrderType.Ascending));
                return;

            case NodeType.SortAsc:
                orderItems.Add(new OrderByItem(
                    ResolveSelectOrdinal(orderByAst.leftAst!, projections, "ORDER BY"),
                    OrderType.Ascending));
                return;

            case NodeType.SortDesc:
                orderItems.Add(new OrderByItem(
                    ResolveSelectOrdinal(orderByAst.leftAst!, projections, "ORDER BY"),
                    OrderType.Descending));
                return;

            case NodeType.IdentifierList:
                if (orderByAst.leftAst is not null)
                    CollectOrderByItems(orderByAst.leftAst, orderItems, projections);

                if (orderByAst.rightAst is not null)
                    CollectOrderByItems(orderByAst.rightAst, orderItems, projections);

                return;

            default:
                orderItems.Add(new OrderByItem(
                    ResolveSelectOrdinal(orderByAst, projections, "ORDER BY"),
                    OrderType.Ascending));
                return;
        }
    }

    private static NodeAst ResolveSelectOrdinal(
        NodeAst expression,
        IReadOnlyList<ProjectionItem> projections,
        string clauseName)
    {
        if (expression.nodeType != NodeType.Integer)
            return expression;

        if (!int.TryParse(expression.yytext, out int ordinal) || ordinal < 1 || ordinal > projections.Count)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"{clauseName} position {expression.yytext} is out of range");
        }

        NodeAst projection = QueryExpressionClassifier.UnwrapAlias(projections[ordinal - 1].Expression);

        if (projection.nodeType == NodeType.ExprAllFields)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"{clauseName} position {ordinal} cannot reference *");
        }

        return projection;
    }
}
