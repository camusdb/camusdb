
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class AddIndexFluxState
{
    public CatalogsManager Catalogs { get; }

    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public AlterIndexTicket Ticket { get; }

    public AlterIndexFluxIndexState Indexes { get; }

    public QueryExecutor QueryExecutor { get; }

    public ObjectIdValue IndexOffset { get; set; }

    public BPTree<CompositeColumnValue, ColumnValue, BTreeTuple>? Btree { get; set; }

    public List<QueryResultRow>? RowsToFeed { get; set; }

    public List<IDisposable> Locks { get; } = new();

    public List<BufferPageOperation> ModifiedPages { get; } = new();        

    public int ModifiedRows { get; set; }

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)>? IndexDeltas { get; set; }    

    public AddIndexFluxState(
        CatalogsManager catalogs,
        DatabaseDescriptor database, 
        TableDescriptor table, 
        AlterIndexTicket ticket, 
        QueryExecutor queryExecutor, 
        AlterIndexFluxIndexState indexes)
    {
        Catalogs = catalogs;
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
        Indexes = indexes;
    }
}
