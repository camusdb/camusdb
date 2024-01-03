
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryAggregator
{
    internal async IAsyncEnumerable<QueryResultRow> AggregateResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be aggregated");

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            yield return resultRow;
        }
    }
}