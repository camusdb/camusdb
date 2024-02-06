
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.BufferPool.Models;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class DeleteFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public DeleteTicket Ticket { get; }

    public QueryExecutor QueryExecutor { get; }
    
    public List<QueryResultRow> RowsToDelete { get; set; } = new();

    public int DeletedRows { get; set; }    

    public DeleteFluxState(DatabaseDescriptor database, TableDescriptor table, DeleteTicket ticket, QueryExecutor queryExecutor)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
    }
}
