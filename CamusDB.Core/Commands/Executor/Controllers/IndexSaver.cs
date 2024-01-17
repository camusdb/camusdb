
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Indexes;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.BufferPool.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexSaver
{
    private readonly IndexUniqueSaver indexUniqueSaver;    

    private readonly IndexUniqueOffsetSaver indexUniqueOffsetSaver;

    public IndexSaver()
    {
        indexUniqueSaver = new(this);        
        indexUniqueOffsetSaver = new(this);
    }

    public async Task<BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>> Save(SaveOffsetIndexTicket ticket)
    {
        return await indexUniqueOffsetSaver.Save(ticket).ConfigureAwait(false);
    }

    public async Task<BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>> Save(SaveIndexTicket ticket)
    {
        return await indexUniqueSaver.Save(ticket).ConfigureAwait(false);
    }  

    public async Task Persist(
        BufferPoolManager tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> deltas)
    {
        await indexUniqueOffsetSaver.Persist(
            tablespace,
            index,
            modifiedPages,
            deltas
        ).ConfigureAwait(false);
    }

    public async Task Persist(
        BufferPoolManager tablespace,
        BTree<CompositeColumnValue, BTreeTuple> index,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas)
    {
        await indexUniqueSaver.Persist(
            tablespace,
            index,
            modifiedPages,
            deltas
        ).ConfigureAwait(false);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.Remove(ticket).ConfigureAwait(false);        
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        await indexUniqueOffsetSaver.Remove(ticket).ConfigureAwait(false);
    }
}
