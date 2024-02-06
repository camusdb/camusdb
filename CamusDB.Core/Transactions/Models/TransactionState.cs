
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

    public Dictionary<TableDescriptor, List<IDisposable>> Locks { get; } = new();

    public List<(BTree<ObjectIdValue, ObjectIdValue>, BTreeTuple)> MainTableDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> UniqueIndexDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> MultiIndexDeltas { get; } = new();

    public TransactionState(HLCTimestamp txnId)
	{
        TxnId = txnId;
    }

    public async Task TryAdquireLocks(TableDescriptor table)
	{
        if (Locks.ContainsKey(table))
            return;

        List<IDisposable> locks = new()
        {
            await table.Rows.WriterLockAsync()
        };

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            TableIndexSchema indexSchema = index.Value;

            if (indexSchema.Type == IndexType.Unique || indexSchema.Type == IndexType.Multi)
                locks.Add(await indexSchema.BTree.WriterLockAsync());
        }

        Locks.Add(table, locks);
    }

    public void ReleaseLocks()
    {
        foreach (KeyValuePair<TableDescriptor, List<IDisposable>> keyValue in Locks)
        {
            foreach (IDisposable disposable in keyValue.Value)
                disposable.Dispose();
        }
    }
}
