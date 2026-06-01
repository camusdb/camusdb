
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Computes the minimal set of base-table columns referenced by a query (QP6.1).
/// </summary>
internal static class RequiredColumnAnalyzer
{
    public static IReadOnlySet<string>? ComputeSingleTable(QueryTicket ticket)
    {
        if (RequiresAllColumns(ticket.Projection))
            return null;

        HashSet<string> required = new(StringComparer.Ordinal);
        CollectFromProjections(ticket.Projection, ticket.RowNameResolver, required, ticket);
        CollectFromExpression(ticket.Where, ticket.RowNameResolver, required, ticket);
        CollectFromOrderBy(ticket.OrderBy, required);
        CollectFromExpressionList(ticket.GroupBy, ticket.RowNameResolver, required, ticket);
        CollectFromHaving(ticket, required);

        return required;
    }

    public static Dictionary<string, IReadOnlySet<string>> ComputeJoinPlan(
        BoundSelectQuery bound,
        QueryTicket ticket,
        NodeAst? postJoinFilter,
        IReadOnlyDictionary<string, NodeAst?> scanFiltersByAlias)
    {
        if (RequiresAllColumns(ticket.Projection))
        {
            return BuildAllColumnsByAlias(bound);
        }

        Dictionary<string, HashSet<string>> requiredByAlias = new(StringComparer.Ordinal);

        foreach (BoundTableSource source in bound.Sources)
            requiredByAlias[source.Alias] = new HashSet<string>(StringComparer.Ordinal);

        CollectJoinSources(bound.Query.Source, bound, ticket, postJoinFilter, scanFiltersByAlias, requiredByAlias);

        Dictionary<string, IReadOnlySet<string>> output = new(StringComparer.Ordinal);

        foreach (KeyValuePair<string, HashSet<string>> entry in requiredByAlias)
            output[entry.Key] = entry.Value;

        return output;
    }

    private static void CollectJoinSources(
        QuerySource source,
        BoundSelectQuery bound,
        QueryTicket ticket,
        NodeAst? postJoinFilter,
        IReadOnlyDictionary<string, NodeAst?> scanFiltersByAlias,
        Dictionary<string, HashSet<string>> requiredByAlias)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                BoundTableSource boundSource = BoundSourceCatalog.FindTableSource(bound, tableSource);
                HashSet<string> required = requiredByAlias[boundSource.Alias];
                CollectFromProjectionsForAlias(ticket.Projection, bound.RowNames, boundSource.Alias, required, ticket);
                CollectFromExpressionForAlias(ticket.Where, bound.RowNames, boundSource.Alias, required, ticket);
                CollectFromExpressionForAlias(postJoinFilter, bound.RowNames, boundSource.Alias, required, ticket);

                if (scanFiltersByAlias.TryGetValue(boundSource.Alias, out NodeAst? scanFilter))
                    CollectFromExpressionForAlias(scanFilter, bound.RowNames, boundSource.Alias, required, ticket);

                CollectFromOrderByForAlias(ticket.OrderBy, bound.RowNames, boundSource.Alias, required);
                CollectFromExpressionListForAlias(ticket.GroupBy, bound.RowNames, boundSource.Alias, required, ticket);
                CollectFromHavingForAlias(ticket, bound.RowNames, boundSource.Alias, required);
                return;
            }

            case DerivedTableSource:
                return;

            case JoinSource joinSource:
                CollectJoinSources(joinSource.Left, bound, ticket, postJoinFilter, scanFiltersByAlias, requiredByAlias);
                CollectJoinSources(joinSource.Right, bound, ticket, postJoinFilter, scanFiltersByAlias, requiredByAlias);
                CollectFromExpressionForJoin(joinSource.OnPredicate, bound.RowNames, joinSource, requiredByAlias);
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static Dictionary<string, IReadOnlySet<string>> BuildAllColumnsByAlias(BoundSelectQuery bound)
    {
        Dictionary<string, IReadOnlySet<string>> output = new(StringComparer.Ordinal);

        foreach (BoundTableSource source in bound.Sources)
            output[source.Alias] = null!;

        return output;
    }

    private static bool RequiresAllColumns(List<NodeAst>? projection) =>
        projection is null || projection is [{ nodeType: NodeType.ExprAllFields }];

    private static void CollectFromProjections(
        List<NodeAst>? projection,
        QueryRowNameResolver? rowNames,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (projection is null)
            return;

        foreach (NodeAst expression in projection)
            CollectFromExpression(expression, rowNames, required, ticket);
    }

    private static void CollectFromProjectionsForAlias(
        List<NodeAst>? projection,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (projection is null)
            return;

        foreach (NodeAst expression in projection)
            CollectFromExpressionForAlias(expression, rowNames, alias, required, ticket);
    }

    private static void CollectFromOrderBy(List<QueryOrderBy>? orderBy, HashSet<string> required)
    {
        if (orderBy is null)
            return;

        foreach (QueryOrderBy clause in orderBy)
            required.Add(clause.ColumnName);
    }

    private static void CollectFromOrderByForAlias(
        List<QueryOrderBy>? orderBy,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required)
    {
        if (orderBy is null)
            return;

        foreach (QueryOrderBy clause in orderBy)
        {
            if (TryResolveColumnForAlias(clause.ColumnName, rowNames, alias, out string columnName))
                required.Add(columnName);
        }
    }

    private static void CollectFromExpressionList(
        IReadOnlyList<NodeAst>? expressions,
        QueryRowNameResolver? rowNames,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (expressions is null)
            return;

        foreach (NodeAst expression in expressions)
            CollectFromExpression(expression, rowNames, required, ticket);
    }

    private static void CollectFromExpressionListForAlias(
        IReadOnlyList<NodeAst>? expressions,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (expressions is null)
            return;

        foreach (NodeAst expression in expressions)
            CollectFromExpressionForAlias(expression, rowNames, alias, required, ticket);
    }

    private static void CollectFromExpression(
        NodeAst? expression,
        QueryRowNameResolver? rowNames,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (expression is null)
            return;

        HashSet<string> identifiers = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(expression, identifiers);

        foreach (string identifier in identifiers)
            AddResolvedColumn(identifier, rowNames, required);

        CollectCorrelatedExistsOuterColumns(expression, ticket, rowNames, required, alias: null);
    }

    private static void CollectFromExpressionForAlias(
        NodeAst? expression,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required,
        QueryTicket ticket)
    {
        if (expression is null)
            return;

        HashSet<string> identifiers = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(expression, identifiers);

        foreach (string identifier in identifiers)
        {
            if (TryResolveColumnForAlias(identifier, rowNames, alias, out string columnName))
                required.Add(columnName);
        }

        CollectCorrelatedExistsOuterColumns(expression, ticket, rowNames, required, alias);
    }

    private static void CollectCorrelatedExistsOuterColumns(
        NodeAst expression,
        QueryTicket ticket,
        QueryRowNameResolver? rowNames,
        HashSet<string> required,
        string? alias)
    {
        if (expression.nodeType == NodeType.ExprExistsCorrelated
            && ticket.ExistsSubqueries?.TryGet(expression, out PreparedExistsSubquery prepared) == true)
        {
            if (alias is null)
                CollectOuterCorrelationColumns(prepared, rowNames, required);
            else
                CollectOuterCorrelationColumnsForAlias(prepared, rowNames!, alias, required);
        }

        if (expression.leftAst is not null)
            CollectCorrelatedExistsOuterColumns(expression.leftAst, ticket, rowNames, required, alias);

        if (expression.rightAst is not null)
            CollectCorrelatedExistsOuterColumns(expression.rightAst, ticket, rowNames, required, alias);
    }

    private static void CollectOuterCorrelationColumns(
        PreparedExistsSubquery prepared,
        QueryRowNameResolver? rowNames,
        HashSet<string> required)
    {
        if (prepared.InnerWhere is null)
            return;

        HashSet<string> innerAliases = ExistsSubqueryAnalyzer.CollectSourceAliases(prepared.InnerBound.Query.Source);
        HashSet<string> references = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(prepared.InnerWhere, references);

        foreach (string reference in references)
        {
            if (!ExistsSubqueryAnalyzer.ReferencesOuterScope(reference, innerAliases))
                continue;

            AddResolvedColumn(reference, rowNames, required);
        }
    }

    private static void CollectOuterCorrelationColumnsForAlias(
        PreparedExistsSubquery prepared,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required)
    {
        if (prepared.InnerWhere is null)
            return;

        HashSet<string> innerAliases = ExistsSubqueryAnalyzer.CollectSourceAliases(prepared.InnerBound.Query.Source);
        HashSet<string> references = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(prepared.InnerWhere, references);

        foreach (string reference in references)
        {
            if (!ExistsSubqueryAnalyzer.ReferencesOuterScope(reference, innerAliases))
                continue;

            if (TryResolveColumnForAlias(reference, rowNames, alias, out string columnName))
                required.Add(columnName);
        }
    }

    private static void CollectFromExpressionForJoin(
        NodeAst onPredicate,
        QueryRowNameResolver rowNames,
        JoinSource joinSource,
        Dictionary<string, HashSet<string>> requiredByAlias)
    {
        HashSet<string> identifiers = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(onPredicate, identifiers);

        HashSet<string> leftAliases = ExistsSubqueryAnalyzer.CollectSourceAliases(joinSource.Left);
        HashSet<string> rightAliases = ExistsSubqueryAnalyzer.CollectSourceAliases(joinSource.Right);

        foreach (string identifier in identifiers)
        {
            if (TrySplitQualified(identifier, out string alias, out _))
            {
                if (leftAliases.Contains(alias) || rightAliases.Contains(alias))
                {
                    if (TryResolveColumnForAlias(identifier, rowNames, alias, out string columnName))
                        TryAddRequiredColumnForJoinAlias(requiredByAlias, alias, columnName);
                }

                continue;
            }

            foreach (string tableAlias in leftAliases.Concat(rightAliases))
            {
                if (TryResolveColumnForAlias(identifier, rowNames, tableAlias, out string columnName))
                    TryAddRequiredColumnForJoinAlias(requiredByAlias, tableAlias, columnName);
            }
        }
    }

    private static void TryAddRequiredColumnForJoinAlias(
        Dictionary<string, HashSet<string>> requiredByAlias,
        string alias,
        string columnName)
    {
        if (requiredByAlias.TryGetValue(alias, out HashSet<string>? required))
            required.Add(columnName);
    }

    private static void CollectFromHaving(QueryTicket ticket, HashSet<string> required)
    {
        if (ticket.Having is null)
            return;

        CollectFromPostAggregateExpression(
            ticket.Having,
            ticket,
            ticket.RowNameResolver,
            required,
            insideAggregate: false);
    }

    private static void CollectFromHavingForAlias(
        QueryTicket ticket,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required)
    {
        if (ticket.Having is null)
            return;

        CollectFromPostAggregateExpressionForAlias(
            ticket.Having,
            ticket,
            rowNames,
            alias,
            required,
            insideAggregate: false);
    }

    private static void CollectFromPostAggregateExpression(
        NodeAst expression,
        QueryTicket ticket,
        QueryRowNameResolver? rowNames,
        HashSet<string> required,
        bool insideAggregate)
    {
        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                if (insideAggregate)
                {
                    AddResolvedColumn(expression.yytext!, rowNames, required);
                    return;
                }

                if (TryResolveHavingToExpression(expression, ticket, out NodeAst resolved))
                {
                    if (resolved.nodeType == NodeType.Identifier)
                    {
                        AddResolvedColumn(resolved.yytext!, rowNames, required);
                        return;
                    }

                    CollectFromPostAggregateExpression(
                        resolved,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate: false);
                }

                return;

            case NodeType.ExprAlias:
                CollectFromPostAggregateExpression(
                    expression.leftAst!,
                    ticket,
                    rowNames,
                    required,
                    insideAggregate);
                return;

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                {
                    if (expression.rightAst is not null)
                    {
                        CollectFromPostAggregateExpression(
                            expression.rightAst,
                            ticket,
                            rowNames,
                            required,
                            insideAggregate: true);
                    }

                    return;
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprCast:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprArgumentList:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprOr:
            case NodeType.ExprAnd:
            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprBetween:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                if (expression.extendedOne is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.extendedOne,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                if (expression.extendedTwo is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.extendedTwo,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpression(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
            case NodeType.ExprAllFields:
                return;
        }
    }

    private static void CollectFromPostAggregateExpressionForAlias(
        NodeAst expression,
        QueryTicket ticket,
        QueryRowNameResolver rowNames,
        string alias,
        HashSet<string> required,
        bool insideAggregate)
    {
        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                if (insideAggregate)
                {
                    if (TryResolveColumnForAlias(expression.yytext!, rowNames, alias, out string columnName))
                        required.Add(columnName);

                    return;
                }

                if (TryResolveHavingToExpression(expression, ticket, out NodeAst resolved))
                {
                    if (resolved.nodeType == NodeType.Identifier)
                    {
                        if (TryResolveColumnForAlias(resolved.yytext!, rowNames, alias, out string columnName))
                            required.Add(columnName);

                        return;
                    }

                    CollectFromPostAggregateExpressionForAlias(
                        resolved,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate: false);
                }

                return;

            case NodeType.ExprAlias:
                CollectFromPostAggregateExpressionForAlias(
                    expression.leftAst!,
                    ticket,
                    rowNames,
                    alias,
                    required,
                    insideAggregate);
                return;

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                {
                    if (expression.rightAst is not null)
                    {
                        CollectFromPostAggregateExpressionForAlias(
                            expression.rightAst,
                            ticket,
                            rowNames,
                            alias,
                            required,
                            insideAggregate: true);
                    }

                    return;
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprCast:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprArgumentList:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprOr:
            case NodeType.ExprAnd:
            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                if (expression.rightAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.rightAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprBetween:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                if (expression.extendedOne is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.extendedOne,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                if (expression.extendedTwo is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.extendedTwo,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                if (expression.leftAst is not null)
                {
                    CollectFromPostAggregateExpressionForAlias(
                        expression.leftAst,
                        ticket,
                        rowNames,
                        alias,
                        required,
                        insideAggregate);
                }

                return;

            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
            case NodeType.ExprAllFields:
                return;
        }
    }

    private static bool TryResolveHavingToExpression(NodeAst expression, QueryTicket ticket, out NodeAst resolved)
    {
        resolved = expression;

        if (ticket.Projection is null)
            return false;

        List<ProjectionItem> projectionItems = BuildProjectionItems(ticket);

        if (!QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
            expression,
            projectionItems,
            ticket.GroupBy,
            out string columnName))
        {
            return false;
        }

        resolved = ResolveHavingExpressionAst(ticket, columnName);
        return true;
    }

    private static List<ProjectionItem> BuildProjectionItems(QueryTicket ticket)
    {
        List<ProjectionItem> items = new(ticket.Projection!.Count);

        for (int i = 0; i < ticket.Projection.Count; i++)
        {
            NodeAst expression = ticket.Projection[i];
            string? outputName = expression.nodeType switch
            {
                NodeType.ExprAlias => expression.rightAst?.yytext,
                NodeType.Identifier => expression.yytext,
                _ => null,
            };

            items.Add(new ProjectionItem(expression, outputName));
        }

        return items;
    }

    private static NodeAst ResolveHavingExpressionAst(QueryTicket ticket, string columnName)
    {
        if (ticket.GroupBy is { Count: > 0 })
        {
            for (int i = 0; i < ticket.GroupBy.Count; i++)
            {
                if (!string.Equals(
                    QueryProjectionResolver.GetGroupByOutputName(ticket.GroupBy[i], i),
                    columnName,
                    StringComparison.Ordinal))
                {
                    continue;
                }

                return ticket.GroupBy[i];
            }
        }

        if (ticket.Projection is not null)
        {
            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                if (!string.Equals(
                    QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[i], i),
                    columnName,
                    StringComparison.Ordinal))
                {
                    continue;
                }

                return QueryExpressionClassifier.UnwrapAlias(ticket.Projection[i]);
            }
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Could not resolve HAVING expression for output name '{columnName}'");
    }

    private static void AddResolvedColumn(
        string identifier,
        QueryRowNameResolver? rowNames,
        HashSet<string> required)
    {
        if (rowNames is not null)
        {
            required.Add(GetBareColumnName(rowNames.ResolveRowLookupKey(identifier)));
            return;
        }

        required.Add(GetBareColumnName(identifier));
    }

    private static bool TryResolveColumnForAlias(
        string identifier,
        QueryRowNameResolver rowNames,
        string alias,
        out string columnName)
    {
        if (TrySplitQualified(identifier, out string identifierAlias, out string bareColumn))
        {
            if (!string.Equals(identifierAlias, alias, StringComparison.Ordinal))
            {
                columnName = "";
                return false;
            }

            columnName = bareColumn;
            return true;
        }

        try
        {
            string lookupKey = rowNames.ResolveRowLookupKey(identifier);

            if (!TrySplitQualified(lookupKey, out string resolvedAlias, out columnName))
            {
                columnName = lookupKey;
                return true;
            }

            if (!string.Equals(resolvedAlias, alias, StringComparison.Ordinal))
                return false;

            return true;
        }
        catch (CamusDBException)
        {
            columnName = "";
            return false;
        }
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.LastIndexOf('.');

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

    private static string GetBareColumnName(string identifier)
    {
        int dotIndex = identifier.LastIndexOf('.');
        return dotIndex >= 0 ? identifier[(dotIndex + 1)..] : identifier;
    }
}
