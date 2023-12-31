
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

    private readonly IndexMultiSaver indexMultiSaver;

    private readonly IndexUniqueOffsetSaver indexUniqueOffsetSaver;

    public IndexSaver()
    {
        indexUniqueSaver = new(this);
        indexMultiSaver = new(this);
        indexUniqueOffsetSaver = new(this);
    }

    public async Task<BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>> Save(SaveUniqueOffsetIndexTicket ticket)
    {
        return await indexUniqueOffsetSaver.Save(ticket);
    }

    public async Task<BTreeMutationDeltas<ColumnValue, BTreeTuple?>> Save(SaveUniqueIndexTicket ticket)
    {
        return await indexUniqueSaver.Save(ticket);
    }

    public async Task Save(SaveMultiKeyIndexTicket ticket)
    {        
        await indexMultiSaver.Save(ticket);
    }

    public void Persist(
        BufferPoolHandler tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> deltas)
    {
        indexUniqueOffsetSaver.Persist(
            tablespace,
            index,
            modifiedPages,
            deltas
        );
    }

    public void Persist(
        BufferPoolHandler tablespace,
        BTree<ColumnValue, BTreeTuple?> index,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<ColumnValue, BTreeTuple?> deltas)
    {
        indexUniqueSaver.Persist(
            tablespace,
            index,
            modifiedPages,
            deltas
        );
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.Remove(ticket);        
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        await indexUniqueOffsetSaver.Remove(ticket);
    }

    public async Task Remove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        await indexMultiSaver.Remove(tablespace, index, key);
    }
}
