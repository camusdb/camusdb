
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class UpdateFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public UpdateTicket Ticket { get; }

    public UpdateFluxIndexState Indexes { get; }

    public QueryExecutor QueryExecutor { get; }

    public List<BufferPageOperation> ModifiedPages { get; } = new();

    public List<IDisposable> Locks { get; } = new();

    public List<QueryResultRow>? RowsToUpdate { get; set; }

    public int ModifiedRows { get; set; }

    public UpdateFluxState(
        DatabaseDescriptor database,
        TableDescriptor table,
        UpdateTicket ticket,
        QueryExecutor queryExecutor,
        UpdateFluxIndexState indexes
    )
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
        Indexes = indexes;
    }
}
