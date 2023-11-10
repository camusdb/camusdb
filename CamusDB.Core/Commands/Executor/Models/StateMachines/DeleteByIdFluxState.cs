
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class DeleteByIdFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public DeleteByIdTicket Ticket { get; }    

    public DeleteByIdFluxIndexState Indexes { get; }

    public List<InsertModifiedPage> ModifiedPages { get; } = new();

    public List<IDisposable> Locks { get; } = new();

    public uint Sequence { get; set; }

    public Dictionary<string, ColumnValue> ColumnValues { get; set; } = new();

    public BTreeTuple? RowTuple { get; set; } = new(-1, -1);

    public DeleteByIdFluxState(DatabaseDescriptor database, TableDescriptor table, DeleteByIdTicket ticket, DeleteByIdFluxIndexState indexes)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        Indexes = indexes;
    }
}
