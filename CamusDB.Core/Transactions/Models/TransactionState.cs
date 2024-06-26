﻿
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

public sealed class TransactionState : IDisposable
{
    public TransactionStatus Status { get; set; }

    public HLCTimestamp TxnId { get; }

    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public List<BufferPageOperation> ModifiedPages { get; } = new();

    public Dictionary<TableDescriptor, List<IDisposable>> ReadLocks { get; } = new();

    public Dictionary<TableDescriptor, List<IDisposable>> WriteLocks { get; } = new();

    public List<(BTree<ObjectIdValue, ObjectIdValue>, BTreeTuple)> MainTableDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> UniqueIndexDeltas { get; } = new();

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> MultiIndexDeltas { get; } = new();

    public TransactionState(HLCTimestamp txnId)
    {
        TxnId = txnId;
    }

    /// <summary>
    /// Try adquire the write locks if they haven't been adquired before
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    public async Task TryAdquireWriteLocks(TableDescriptor table)
    {
        if (WriteLocks.ContainsKey(table))
            return;

        try
        {            
            await Semaphore.WaitAsync();

            if (WriteLocks.ContainsKey(table))
                return;

            if (ReadLocks.TryGetValue(table, out List<IDisposable>? readLocks)) // upgrade locks
            {
                foreach (IDisposable disposable in readLocks)
                    disposable.Dispose();

                ReadLocks.Remove(table);
            }

            List<IDisposable> locks = new()
            {
                await table.Rows.WriterLockAsync()
            };

            foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
            {
                TableIndexSchema indexSchema = index.Value;

                if (indexSchema.Type is IndexType.Unique or IndexType.Multi)
                    locks.Add(await indexSchema.BTree.WriterLockAsync());
            }

            WriteLocks.Add(table, locks);
        }
        finally
        {
            Semaphore.Release();
        }
    }

    /// <summary>
    /// Try adquire the table rows lock
    /// </summary>
    /// <param name="table"></param>    
    /// <returns></returns>
    public async Task TryAdquireTableRowsReadLock(TableDescriptor table)
    {
        if (WriteLocks.ContainsKey(table) || ReadLocks.ContainsKey(table))
            return;

        try
        {            
            await Semaphore.WaitAsync();

            if (WriteLocks.ContainsKey(table) || ReadLocks.ContainsKey(table))
                return;

            ReadLocks.Add(table, new()
            {
                await table.Rows.ReaderLockAsync()
            });
        }
        finally
        {
            Semaphore.Release();
        }
    }

    public async Task TryAdquireTableIndexReadLock(TableDescriptor table, BTree<CompositeColumnValue, BTreeTuple> index)
    {
        if (WriteLocks.ContainsKey(table) || ReadLocks.ContainsKey(table))
            return;

        try
        {
            await Semaphore.WaitAsync();

            if (WriteLocks.ContainsKey(table) || ReadLocks.ContainsKey(table))
                return;

            ReadLocks.Add(table, new()
            {
                await index.ReaderLockAsync()
            });
        }
        finally
        {
            Semaphore.Release();
        }
    }

    /// <summary>
    /// Release all adquired locks
    /// </summary>
    public void ReleaseLocks()
    {
        foreach (KeyValuePair<TableDescriptor, List<IDisposable>> keyValue in ReadLocks)
        {
            foreach (IDisposable disposable in keyValue.Value)
                disposable.Dispose();
        }

        foreach (KeyValuePair<TableDescriptor, List<IDisposable>> keyValue in WriteLocks)
        {
            foreach (IDisposable disposable in keyValue.Value)
                disposable.Dispose();
        }
    }

    public void Dispose()
    {
        Semaphore?.Dispose();
    }    
}
