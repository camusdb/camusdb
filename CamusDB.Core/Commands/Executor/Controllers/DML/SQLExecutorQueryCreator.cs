
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorQueryCreator : SQLExecutorBaseCreator
{
    private readonly SelectQueryCreator selectQueryCreator = new();

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(ast);
        return QueryTicketAdapter.ToQueryTicket(selectQuery, ticket);
    }
}
