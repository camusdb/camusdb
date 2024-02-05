
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.Time;

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

    private readonly ConcurrentDictionary<HLCTimestamp, TransactionState> transactions = new();

    public TransactionsManager(HybridLogicalClock hybridLogicalClock)
    {
        this.hybridLogicalClock = hybridLogicalClock;
    }

    public void Commit(TransactionState txState)
    {
        throw new NotImplementedException();
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
}

