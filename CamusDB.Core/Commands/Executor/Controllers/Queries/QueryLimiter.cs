
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
    /// The error for a LIMIT or OFFSET parameter that is not an integer. The grammar takes only an
    /// integer literal or a placeholder here, so the value is what the client bound, and this is client
    /// input, not an engine fault. A float is refused, not rounded: GoogleSQL requires an integer here,
    /// and the row count must not depend on a conversion the statement did not ask for.
    /// </summary>
    internal static CamusDBException NotAnInteger(string clause, ColumnValue value) =>
        new(CamusDBErrorCodes.InvalidInput, $"{clause} must be an integer, got {value.Type}");

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
                throw NotAnInteger("LIMIT", limitValue);

            limit = limitValue.LongValue;
        }

        if (ticket.Offset is not null)
        {
            ColumnValue offsetValue = SqlExecutor.EvalExpr(ticket.Offset, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (offsetValue.Type != ColumnType.Integer64)
                throw NotAnInteger("OFFSET", offsetValue);

            offset = offsetValue.LongValue;
        }

        // The input is lazy, so every row pulled from it is work done upstream — for a join, finding
        // one more match can mean reading much of its right side. The loop therefore stops as soon as
        // it has yielded the last row it needs, and LIMIT 0 pulls nothing, instead of pulling one row
        // past the limit only to discard it.
        if (limit is not null && limit.Value <= 0)
            yield break;

        // The position after the last row to return. It saturates: a LIMIT near long.MaxValue plus an
        // OFFSET must not wrap to a negative end, which would stop the query after its first row.
        long end = limit is null || (offset > 0 && limit.Value > long.MaxValue - offset)
            ? long.MaxValue
            : offset + limit.Value;

        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (count >= offset)
                yield return resultRow;

            count++;

            if (count >= end)
                yield break;
        }
    }
}