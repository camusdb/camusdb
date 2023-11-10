
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveMultiKeysIndexTicket
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public InsertTicket Ticket { get; }

    public BTreeTuple RowTuple { get; }

    public List<IDisposable> Locks { get; }

    public List<InsertModifiedPage> ModifiedPages { get; }

    public SaveMultiKeysIndexTicket(
        DatabaseDescriptor database,
        TableDescriptor table,
        InsertTicket ticket,
        BTreeTuple rowTuple,
        List<IDisposable> locks,
        List<InsertModifiedPage> modifiedPages
    )
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        RowTuple = rowTuple;
        Locks = locks;
        ModifiedPages = modifiedPages;
    }
}