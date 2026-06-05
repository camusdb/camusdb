
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

    private readonly Lock trackSync = new();
    private HashSet<(string key, KeyValueDurability durability)>? acquiredLocks;
    private HashSet<(string prefix, KeyValueDurability durability)>? acquiredPrefixLocks;
    private HashSet<(string key, KeyValueDurability durability)>? modifiedKeys;
    private Dictionary<string, SchemaVersionPin>? schemaPins;

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
        lock (trackSync)
        {
            acquiredLocks ??= [];
            acquiredLocks.Add((key, durability));
        }
    }

    /// <summary>
    /// Records that an exclusive <b>prefix</b> (range) lock was acquired for this transaction.
    /// Unlike per-key write intents, a read-only prefix lock is not cleared by the commit/rollback
    /// 2PC (which only finalizes <em>modified</em> keys), so <see cref="KvTransactionsManager"/>
    /// releases these explicitly on commit and rollback. Idempotent.
    /// </summary>
    public void TrackPrefixLock(string prefix, KeyValueDurability durability)
    {
        lock (trackSync)
        {
            acquiredPrefixLocks ??= [];
            acquiredPrefixLocks.Add((prefix, durability));
        }
    }

    /// <summary>Snapshot of the prefix (range) locks acquired by this transaction.</summary>
    public IReadOnlyList<(string prefix, KeyValueDurability durability)> GetAcquiredPrefixLocks()
    {
        lock (trackSync)
        {
            if (acquiredPrefixLocks is null || acquiredPrefixLocks.Count == 0)
                return [];

            return [.. acquiredPrefixLocks];
        }
    }

    /// <summary>
    /// Records that <paramref name="key"/> was written or deleted within this transaction.
    /// </summary>
    public void TrackModified(string key, KeyValueDurability durability)
    {
        lock (trackSync)
        {
            modifiedKeys ??= [];
            modifiedKeys.Add((key, durability));
        }
    }

    public void PinSchemaVersion(
        string resource,
        long schemaVersion,
        Func<long> currentVersion,
        Func<bool>? isStillValid = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        ArgumentNullException.ThrowIfNull(currentVersion);

        schemaPins ??= new(StringComparer.Ordinal);

        if (schemaPins.TryGetValue(resource, out SchemaVersionPin pin))
        {
            if (pin.SchemaVersion != schemaVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Transaction {UniqueId} already pinned schema resource '{resource}' at version {pin.SchemaVersion}, not {schemaVersion}"
                );

            return;
        }

        schemaPins[resource] = new(schemaVersion, currentVersion, isStillValid);
    }

    public void ValidateSchemaPins()
    {
        if (schemaPins is null)
            return;

        foreach ((string resource, SchemaVersionPin pin) in schemaPins)
        {
            if (pin.IsStillValid is not null && !pin.IsStillValid())
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Transaction {UniqueId} pinned schema resource '{resource}', but it is no longer present"
                );
            }

            long current = pin.CurrentVersion();
            if (current == pin.SchemaVersion)
                continue;

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Transaction {UniqueId} pinned schema resource '{resource}' at version {pin.SchemaVersion}, but current version is {current}"
            );
        }
    }

    public long? GetPinnedSchemaVersion(string resource)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);

        return schemaPins is not null && schemaPins.TryGetValue(resource, out SchemaVersionPin pin)
            ? pin.SchemaVersion
            : null;
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

    private readonly record struct SchemaVersionPin(
        long SchemaVersion,
        Func<long> CurrentVersion,
        Func<bool>? IsStillValid
    );
}
