
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class DeleteFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public DeleteTicket Ticket { get; }

    public QueryExecutor QueryExecutor { get; }

    /// <summary>
    /// Matched rows buffered before deletion (Halloween problem). Uses <see cref="SpillableRowList"/>
    /// so that very large matched sets spill to disk rather than exhausting the process heap.
    /// Owned by <c>RowDeleter.DeleteInternal</c>, which disposes it after the mutation phase.
    /// </summary>
    public SpillableRowList RowsToDelete { get; set; } = new();

    public int DeletedRows { get; set; }

    public DeleteFluxState(DatabaseDescriptor database, TableDescriptor table, DeleteTicket ticket, QueryExecutor queryExecutor)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
    }
}
