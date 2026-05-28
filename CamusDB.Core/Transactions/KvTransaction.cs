
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Transactions;

/// <summary>Lifecycle state of a <see cref="KvTransaction"/>.</summary>
public enum KvTransactionStatus
{
    Active,
    Committed,
    RolledBack
}

/// <summary>
/// Holds the runtime state of a single CamusDB transaction backed by Kahuna.
///
/// Each instance owns a Kahuna <see cref="HLCTimestamp"/> obtained from
/// <c>LocateAndStartTransaction</c> and accumulates the sets of acquired locks and
/// modified keys that must be forwarded to <c>LocateAndCommitTransaction</c> /
/// <c>LocateAndRollbackTransaction</c> at the end of the transaction.
///
/// This object is NOT thread-safe — one transaction per logical thread/task.
/// </summary>
public sealed class KvTransaction
{
    /// <summary>
    /// The Kahuna transaction timestamp. Passed as the <c>transactionId</c> argument
    /// to every transactional KV operation.
    /// </summary>
    public HLCTimestamp TransactionId { get; }

    /// <summary>
    /// The unique identifier that routes commit/rollback to the correct Kahuna
    /// transaction coordinator. Must match the value from <c>KeyValueTransactionOptions.UniqueId</c>
    /// used to start the transaction.
    /// </summary>
    public string UniqueId { get; }

    /// <summary>Current lifecycle state. Set by <see cref="KvTransactionsManager"/>.</summary>
    public KvTransactionStatus Status { get; internal set; } = KvTransactionStatus.Active;

    private HashSet<(string key, KeyValueDurability durability)>? acquiredLocks;
    private HashSet<(string key, KeyValueDurability durability)>? modifiedKeys;

    public KvTransaction(HLCTimestamp transactionId, string uniqueId)
    {
        TransactionId = transactionId;
        UniqueId = uniqueId;
    }

    /// <summary>
    /// Records that an exclusive lock was acquired on <paramref name="key"/>.
    /// Idempotent — re-adding the same key is a no-op.
    /// </summary>
    public void TrackLock(string key, KeyValueDurability durability)
    {
        acquiredLocks ??= [];
        acquiredLocks.Add((key, durability));
    }

    /// <summary>
    /// Records that <paramref name="key"/> was written or deleted within this transaction.
    /// </summary>
    public void TrackModified(string key, KeyValueDurability durability)
    {
        modifiedKeys ??= [];
        modifiedKeys.Add((key, durability));
    }

    /// <summary>Returns the acquired-locks list in the shape Kahuna's commit/rollback expects.</summary>
    public List<KeyValueTransactionModifiedKey> GetAcquiredLocks()
    {
        if (acquiredLocks is null)
            return [];

        List<KeyValueTransactionModifiedKey> result = new(acquiredLocks.Count);
        foreach ((string key, KeyValueDurability durability) in acquiredLocks)
            result.Add(new KeyValueTransactionModifiedKey { Key = key, Durability = durability });
        return result;
    }

    /// <summary>Returns the modified-keys list in the shape Kahuna's commit/rollback expects.</summary>
    public List<KeyValueTransactionModifiedKey> GetModifiedKeys()
    {
        if (modifiedKeys is null)
            return [];

        List<KeyValueTransactionModifiedKey> result = new(modifiedKeys.Count);
        foreach ((string key, KeyValueDurability durability) in modifiedKeys)
            result.Add(new KeyValueTransactionModifiedKey { Key = key, Durability = durability });
        return result;
    }
}
