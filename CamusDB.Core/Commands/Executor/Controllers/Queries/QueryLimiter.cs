
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryLimiter
{
    // @todo rewrite this method to support any level of sorting
    internal IAsyncEnumerable<QueryResultRow> LimitResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {        
        if (ticket.Limit is not null && ticket.Offset is null)
            return LimitResultsetWithoutOffset(ticket, dataCursor);

        if (ticket.Limit is not null && ticket.Offset is not null)
            return LimitResultsetWithOffset(ticket, dataCursor);

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal limit context");
    }

    private static async IAsyncEnumerable<QueryResultRow> LimitResultsetWithoutOffset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Limit is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal limit context");

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

        int count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (count >= limit.LongValue)
                yield break;

            yield return resultRow;

            count++;
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> LimitResultsetWithOffset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Limit is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal limit context");

        if (ticket.Offset is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal offset context");

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

        ColumnValue offset = SqlExecutor.EvalExpr(ticket.Offset, new(), ticket.Parameters);
        if (offset.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Offset is not Integer64");

        int count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (count >= (limit.LongValue + offset.LongValue))
                yield break;

            if (count >= offset.LongValue)
                yield return resultRow;

            count++;
        }
    }
}