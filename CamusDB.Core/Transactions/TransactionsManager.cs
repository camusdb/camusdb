﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.BufferPool;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.Transactions;

/// <summary>
/// Transaction Manager
///
/// Responsabilities:
/// 
/// Transaction Processing: Oversee and manage the execution of database transactions, ensuring
/// they are processed efficiently and effectively. This involves handling multiple concurrent
/// transactions, maintaining data integrity, and ensuring that all the transaction
/// properties (ACID – Atomicity, Consistency, Isolation, Durability) are upheld.
///
/// Concurrency Control: Implement and manage concurrency control mechanisms to prevent data
/// conflicts during simultaneous transaction processing. This includes handling lock management,
/// detecting and resolving deadlocks, and ensuring isolation levels are maintained as per the requirements.
/// </summary>
public sealed class TransactionsManager
{
    private readonly HybridLogicalClock hybridLogicalClock;

    private readonly IndexSaver indexSaver = new();

    private readonly ConcurrentDictionary<HLCTimestamp, TransactionState> transactions = new();

    public TransactionsManager(HybridLogicalClock hybridLogicalClock)
    {
        this.hybridLogicalClock = hybridLogicalClock;
    }

    public TransactionState GetState(HLCTimestamp txnId)
    {
        if (transactions.TryGetValue(txnId, out TransactionState? txState))
            return txState;

        throw new Exception("Transaction hasn't been started");
    }

    public async Task<TransactionState> Start()
    {
        HLCTimestamp txnId = await hybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);
        TransactionState txState = new(txnId);
        transactions.TryAdd(txnId, txState);
        return txState;
    }

    public async Task Commit(DatabaseDescriptor database, TableDescriptor table, TransactionState txnState)
    {
        // Persist all the changes to the table and indexes
        await PersistTableAndIndexChanges(database, table, txnState).ConfigureAwait(false);

        // Apply all the changes to the modified pages in an atomic operation
        database.BufferPool.ApplyPageOperations(txnState.ModifiedPages);

        // Release all the locks acquired by the transaction
        txnState.ReleaseLocks();

        Console.WriteLine("Commited tx {0}", txnState.TxnId);
    }

    public void Rollback(TransactionState txnState)
    {
        // Release all the locks acquired by the transaction
        txnState.ReleaseLocks();
    }

    private async Task PersistTableAndIndexChanges(DatabaseDescriptor database, TableDescriptor table, TransactionState txnState)
    {
        BufferPoolManager tablespace = database.BufferPool;

        foreach (BTreeTuple tuple in txnState.MainTableDeltas)
        {
            SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
               tablespace: tablespace,
               index: table.Rows,
               txnId: txnState.TxnId,
               commitState: BTreeCommitState.Committed,
               key: tuple.SlotOne,
               value: tuple.SlotTwo,
               modifiedPages: txnState.ModifiedPages
           );

            // Main table index stores rowid pointing to page offeset
            await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);
        }

        if (txnState.UniqueIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue uniqueKeyValue, BTreeTuple tuple) uniqueIndex in txnState.UniqueIndexDeltas)
            {
                SaveIndexTicket saveUniqueIndexTicket = new(
                    tablespace: tablespace,
                    index: uniqueIndex.index,
                    txnId: txnState.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: uniqueIndex.uniqueKeyValue,
                    value: uniqueIndex.tuple,
                    modifiedPages: txnState.ModifiedPages
                );

                await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);
            }
        }

        if (txnState.MultiIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue multiKeyValue, BTreeTuple tuple) multIndex in txnState.MultiIndexDeltas)
            {
                SaveIndexTicket saveMultiIndexTicket = new(
                    tablespace: tablespace,
                    index: multIndex.index,
                    txnId: txnState.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: multIndex.multiKeyValue,
                    value: multIndex.tuple,
                    modifiedPages: txnState.ModifiedPages
                );

                await indexSaver.Save(saveMultiIndexTicket).ConfigureAwait(false);
            }
        }
    }    
}

