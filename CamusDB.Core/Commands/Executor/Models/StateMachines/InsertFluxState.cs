
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class InsertFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public InsertTicket Ticket { get; }

    public uint Sequence { get; set; }

    public BTreeTuple RowTuple { get; set; } = new(-1, -1);

    public InsertFluxIndexState Indexes { get; }

    public InsertFluxState(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket, InsertFluxIndexState indexes)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        Indexes = indexes;
    }
}
