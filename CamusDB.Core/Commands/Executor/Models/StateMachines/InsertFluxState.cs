
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.BufferPool.Models;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class InsertFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public InsertTicket Ticket { get; }

    public InsertFluxIndexState Indexes { get; }

    public List<BufferPageOperation> ModifiedPages { get; } = new();

    public List<IDisposable> Locks { get; } = new();

    public int InsertedRows { get; set; }

    public InsertFluxState(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket, InsertFluxIndexState indexes)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        Indexes = indexes;
    }
}
