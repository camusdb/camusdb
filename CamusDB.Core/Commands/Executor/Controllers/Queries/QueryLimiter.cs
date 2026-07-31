
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
    /// <summary>
    /// Applies LIMIT and/or OFFSET to the cursor. Either clause may be present alone:
    /// a missing OFFSET skips nothing and a missing LIMIT streams every remaining row
    /// (offset-only queries like <c>SELECT … OFFSET 20</c> are valid SQL).
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> LimitResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Limit is null && ticket.Offset is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal limit context");

        return LimitOffsetResultset(ticket, dataCursor);
    }

    private static async IAsyncEnumerable<QueryResultRow> LimitOffsetResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        long? limit = null;
        long offset = 0;

        if (ticket.Limit is not null)
        {
            ColumnValue limitValue = SqlExecutor.EvalExpr(ticket.Limit, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (limitValue.Type != ColumnType.Integer64)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

            limit = limitValue.LongValue;
        }

        if (ticket.Offset is not null)
        {
            ColumnValue offsetValue = SqlExecutor.EvalExpr(ticket.Offset, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (offsetValue.Type != ColumnType.Integer64)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Offset is not Integer64");

            offset = offsetValue.LongValue;
        }

        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (limit is not null && count >= offset + limit.Value)
                yield break;

            if (count >= offset)
                yield return resultRow;

            count++;
        }
    }
}