
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Temporary bridge from logical <see cref="SelectQuery"/> back to legacy <see cref="QueryTicket"/>.
/// </summary>
internal static class QueryTicketAdapter
{
    public static QueryTicket ToQueryTicket(SelectQuery query, ExecuteSQLTicket ticket) =>
        ToQueryTicketInternal(query, ticket, rowNameResolver: null);

    public static QueryTicket ToQueryTicket(BoundSelectQuery bound, ExecuteSQLTicket ticket) =>
        ToQueryTicket(bound, ticket, existsSubqueries: null);

    public static QueryTicket ToQueryTicket(
        BoundSelectQuery bound,
        ExecuteSQLTicket ticket,
        ExistsSubqueryRegistry? existsSubqueries) =>
        ToQueryTicketInternal(bound.Query, ticket, bound.RowNames, existsSubqueries);

    private static QueryTicket ToQueryTicketInternal(
        SelectQuery query,
        ExecuteSQLTicket ticket,
        QueryRowNameResolver? rowNameResolver,
        ExistsSubqueryRegistry? existsSubqueries = null)
    {
        TableSource tableSource = GetPrimaryTableSource(query.Source);
        NodeAst? where = query.Where?.Expression;
        PredicateAnalysis analyzedWhere = PredicateAnalyzer.Analyze(where, ticket.Parameters);

        return new QueryTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: tableSource.TableName,
            index: tableSource.ForcedIndexName,
            projection: query.Projections.Select(item => item.Expression).ToList(),
            filters: null,
            where: where,
            orderBy: ToQueryOrderBy(query.OrderBy, query.Projections, query.GroupBy, query.GroupBy is { Count: > 0 }),
            limit: query.Limit,
            offset: query.Offset,
            parameters: ticket.Parameters,
            groupBy: query.GroupBy,
            having: query.Having?.Expression,
            rowNameResolver: rowNameResolver,
            analyzedWhere: analyzedWhere,
            existsSubqueries: existsSubqueries,
            isDistinct: query.IsDistinct);
    }

    /// <summary>
    /// Resolves legacy ticket table metadata for single-table scans and multi-source queries.
    /// Derived sources use the first inner base table when available, otherwise the derived alias.
    /// </summary>
    private static TableSource GetPrimaryTableSource(QuerySource source)
    {
        switch (source)
        {
            case TableSource tableSource:
                return tableSource;

            case DerivedTableSource derivedSource:
                return GetPrimaryTableSourceFromDerived(derivedSource);

            case JoinSource joinSource:
                return GetPrimaryTableSource(joinSource.Left);

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static TableSource GetPrimaryTableSourceFromDerived(DerivedTableSource derived)
    {
        TableSource? innerTable = TryGetFirstTableSource(derived.Query.Source);

        if (innerTable is not null)
            return innerTable;

        return new TableSource(derived.Alias, Alias: derived.Alias);
    }

    private static TableSource? TryGetFirstTableSource(QuerySource source)
    {
        switch (source)
        {
            case TableSource tableSource:
                return tableSource;

            case DerivedTableSource derivedSource:
                return TryGetFirstTableSource(derivedSource.Query.Source);

            case JoinSource joinSource:
                return TryGetFirstTableSource(joinSource.Left) ?? TryGetFirstTableSource(joinSource.Right);

            default:
                return null;
        }
    }

    private static List<QueryOrderBy>? ToQueryOrderBy(
        IReadOnlyList<OrderByItem>? orderBy,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        bool sortAfterProjection)
    {
        if (orderBy is null || orderBy.Count == 0)
            return null;

        List<QueryOrderBy> clauses = new(orderBy.Count);

        foreach (OrderByItem item in orderBy)
        {
            clauses.Add(new QueryOrderBy(
                ResolveOrderColumnName(item.Expression, projections, groupBy, sortAfterProjection),
                item.Direction));
        }

        return clauses;
    }

    private static string ResolveOrderColumnName(
        NodeAst expression,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        bool sortAfterProjection)
    {
        if (sortAfterProjection
            && QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
                expression,
                projections,
                groupBy,
                out string columnName))
        {
            return columnName;
        }

        if (expression.nodeType == NodeType.Identifier)
            return expression.yytext ?? "";

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }
}
