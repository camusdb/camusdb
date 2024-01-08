
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// This controller allows querying the information_schema. The tables of the information_schema
/// are simulated from the internal structures.
/// </summary>
internal sealed class SchemaQuerier
{
    public IAsyncEnumerable<QueryResultRow> Query(QueryTicket ticket)
    {
        string viewName = ticket.TableName.ToLowerInvariant();

        switch (viewName)
        {
            case "tables":
                return QueryTablesSchema();

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInformationSchema, "Invalid information_schema table");
        }
    }

    private IAsyncEnumerable<QueryResultRow> QueryTablesSchema()
    {
        throw new NotImplementedException();
    }
}