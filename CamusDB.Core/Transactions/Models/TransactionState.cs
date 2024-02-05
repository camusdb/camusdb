
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.Transactions.Models;

public sealed class TransactionState
{
	public HLCTimestamp TxnId { get; }

    public List<BufferPageOperation> ModifiedPages { get; } = new();

    public List<IDisposable> Locks { get; } = new();

    public List<(BTree<ObjectIdValue, ObjectIdValue>, BTreeTuple)> MainTableDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> UniqueIndexDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> MultiIndexDeltas { get; } = new();

    public TransactionState(HLCTimestamp txnId)
	{
        TxnId = txnId;
    }

    public async Task TryAdquireLocks(TableDescriptor table)
	{
        Locks.Add(await table.Rows.WriterLockAsync());

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            TableIndexSchema indexSchema = index.Value;

            if (indexSchema.Type == IndexType.Unique || indexSchema.Type == IndexType.Multi)
                Locks.Add(await indexSchema.BTree.WriterLockAsync());
        }            
    }

    public void ReleaseLocks()
    {
        foreach (IDisposable disposable in Locks)
            disposable.Dispose();
    }
}
