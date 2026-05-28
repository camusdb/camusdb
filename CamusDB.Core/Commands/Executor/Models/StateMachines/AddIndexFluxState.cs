
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class AddIndexFluxState
{
    public CatalogsManager Catalogs { get; }

    public KvTransaction Tx { get; }

    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public AlterIndexTicket Ticket { get; }

    public QueryExecutor QueryExecutor { get; }

    public List<QueryResultRow>? RowsToFeed { get; set; }

    public int ModifiedRows { get; set; }

    public AddIndexFluxState(
        CatalogsManager catalogs,
        KvTransaction tx,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        QueryExecutor queryExecutor)
    {
        Catalogs = catalogs;
        Tx = tx;
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
    }
}
