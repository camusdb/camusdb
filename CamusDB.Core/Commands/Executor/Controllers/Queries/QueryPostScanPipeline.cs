
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Applies aggregate, sort, project, and limit operators after row production (scan or join).
/// Ordering matches <see cref="QueryPlanner"/>.
/// </summary>
internal static class QueryPostScanPipeline
{
    public static IAsyncEnumerable<QueryResultRow> Apply(
        DatabaseDescriptor database,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> cursor,
        QueryFilterer queryFilterer,
        QuerySorter querySorter,
        QueryAggregator queryAggregator,
        QueryProjector queryProjector,
        QueryLimiter queryLimiter)
    {
        bool hasGroupBy = ticket.GroupBy is { Count: > 0 };
        bool havingApplied = ticket.Having is null;

        if (hasGroupBy)
        {
            cursor = queryAggregator.AggregateResultset(ticket, cursor);
            cursor = queryFilterer.FilterHavingResultset(database, ticket, cursor);
            havingApplied = true;

            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                cursor = querySorter.SortResultset(ticket, cursor);

            if (ticket.Projection is not null && ticket.Projection.Count > 0 && !IsFullProjection(ticket.Projection))
                cursor = queryProjector.ProjectResultset(ticket, cursor);

            if (ticket.Limit is not null || ticket.Offset is not null)
                cursor = queryLimiter.LimitResultset(ticket, cursor);

            return cursor;
        }

        if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
            cursor = querySorter.SortResultset(ticket, cursor);

        if (ticket.Limit is not null || ticket.Offset is not null)
            cursor = queryLimiter.LimitResultset(ticket, cursor);

        if (ticket.Projection is not null && ticket.Projection.Count > 0)
        {
            if (HasAggregation(ticket.Projection, ticket))
            {
                cursor = queryAggregator.AggregateResultset(ticket, cursor);
                cursor = queryFilterer.FilterHavingResultset(database, ticket, cursor);
                havingApplied = true;
            }

            if (!IsFullProjection(ticket.Projection))
                cursor = queryProjector.ProjectResultset(ticket, cursor);
        }

        if (!havingApplied)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "HAVING requires GROUP BY or an aggregate projection");
        }

        return cursor;
    }

    internal static bool IsFullProjection(List<NodeAst> projection) =>
        projection is [{ nodeType: NodeType.ExprAllFields }];

    internal static bool HasAggregation(List<NodeAst> projection, QueryTicket ticket)
    {
        foreach (NodeAst nodeAst in projection)
        {
            if (!QueryExpressionClassifier.IsAggregateProjection(nodeAst))
                continue;

            if (projection.Count > 1 && ticket.GroupBy is not { Count: > 0 })
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Aggregations cannot be accompanied by other projections or expressions.");
            }

            return true;
        }

        return false;
    }
}
