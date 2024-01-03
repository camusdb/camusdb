
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
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryAggregator
{
    internal IAsyncEnumerable<QueryResultRow> AggregateResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be aggregated");

        QueryAggregationType aggregationType = GetAggregationType(ticket.Projection);

        return aggregationType switch
        {
            QueryAggregationType.Count => AggregateCount(dataCursor),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This aggregation type is not supported"),
        };
    }

    private async IAsyncEnumerable<QueryResultRow> AggregateCount(IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)        
            count++;

        yield return new QueryResultRow(
            new BTreeTuple(new(), new()), 
            new() { { "0", new ColumnValue(ColumnType.Integer64, count) } 
        });
    }

    private static QueryAggregationType GetAggregationType(List<NodeAst> projection)
    {
        foreach (NodeAst nodeAst in projection)
        {
            if (nodeAst.nodeType == NodeType.ExprFuncCall)
            {
                switch (nodeAst.leftAst!.yytext!.ToLowerInvariant())
                {
                    case "count":
                        return QueryAggregationType.Count;
                    case "max":
                        return QueryAggregationType.Max;
                    case "min":
                        return QueryAggregationType.Min;
                }
            }
        }

        return QueryAggregationType.None;
    }
}