
/*
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class DropIndexFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public AlterIndexTicket Ticket { get; }

    public AlterIndexFluxIndexState Indexes { get; }

    public QueryExecutor QueryExecutor { get; }

    public int ModifiedRows { get; set; }

    public DropIndexFluxState(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        QueryExecutor queryExecutor,
        AlterIndexFluxIndexState indexes
    )
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
        Indexes = indexes;
    }
}
