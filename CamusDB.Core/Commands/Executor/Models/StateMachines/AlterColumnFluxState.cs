
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class AlterColumnFluxState
{
    public CatalogsManager Catalogs { get; }

    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public AlterColumnTicket Ticket { get; }

    public QueryExecutor QueryExecutor { get; }

    public List<BufferPageOperation> ModifiedPages { get; } = new();    

    public IAsyncEnumerable<QueryResultRow>? DataCursor { get; set; }

    public int ModifiedRows { get; set; }

    public AlterColumnFluxState(
        CatalogsManager catalogs,
        DatabaseDescriptor database, 
        TableDescriptor table, 
        AlterColumnTicket ticket, 
        QueryExecutor queryExecutor, 
        AlterColumnFluxIndexState indexes
    )
    {
        Catalogs = catalogs;
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;        
    }
}

