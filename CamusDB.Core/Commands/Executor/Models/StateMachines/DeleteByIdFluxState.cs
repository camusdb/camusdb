
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

public sealed class DeleteByIdFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public DeleteByIdTicket Ticket { get; }    

    public DeleteByIdFluxIndexState Indexes { get; }

    public List<BufferPageOperation> ModifiedPages { get; } = new();

    public List<IDisposable> Locks { get; } = new();

    public Dictionary<string, ColumnValue> ColumnValues { get; set; } = new();

    public BTreeTuple? RowTuple { get; set; } = new(new(), new());

    public List<BufferPage>? Pages { get; internal set; }

    public DeleteByIdFluxState(DatabaseDescriptor database, TableDescriptor table, DeleteByIdTicket ticket, DeleteByIdFluxIndexState indexes)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        Indexes = indexes;
    }
}
