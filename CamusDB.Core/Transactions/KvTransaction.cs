
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
/// The bounds of a Kahuna exclusive key-range lock held by a transaction, retained so the lock can
/// be released over the identical interval on commit/rollback. A whole-bucket lock is
/// <c>(prefix, null, true, null, true)</c>.
/// </summary>
public readonly record struct RangeLockBounds(
    string Prefix, string? StartKey, bool StartInclusive, string? EndKey, bool EndInclusive,
    KeyValueDurability Durability);

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
    private HashSet<RangeLockBounds>? acquiredRangeLocks;
    private HashSet<(string key, KeyValueDurability durability)>? modifiedKeys;
    private Dictionary<string, SchemaVersionPin>? schemaPins;

    /// <summary>
    /// When true this is a synthetic read-only transaction backed by <see cref="HLCTimestamp.Zero"/>.
    /// Kahuna uses <c>HLCTimestamp.Zero</c> as the non-transactional snapshot signal (read-committed
    /// semantics per key, no MVCC context). No <c>StartTransaction</c> / <c>CommitTransaction</c>
    /// round-trips are issued; commit and rollback are no-ops.
    /// </summary>
    public bool IsReadOnly { get; }

    /// <summary>
    /// Isolation level requested when this transaction was begun.
    /// Default is <see cref="CamusIsolationLevel.ReadCommitted"/> — behaviour is identical to the
    /// long-standing code path. <see cref="CamusIsolationLevel.Serializable"/> is carried as metadata
    /// only and does not yet change locking or snapshot-read behaviour.
    /// </summary>
    public CamusIsolationLevel IsolationLevel { get; }

    /// <summary>
    /// Mode (read-write or read-only) requested when this transaction was begun.
    /// Default is <see cref="CamusTransactionMode.ReadWrite"/>. Carried as metadata only and does
    /// not yet change read behaviour.
    /// </summary>
    public CamusTransactionMode TransactionMode { get; }

    /// <summary>
    /// Server-minted HLC timestamp that defines the MVCC snapshot for a
    /// <see cref="CamusIsolationLevel.Serializable"/> + <see cref="CamusTransactionMode.ReadOnly"/>
    /// transaction. Every read in the transaction uses this single <c>T</c> so the whole transaction
    /// observes one consistent point in the version history.
    ///
    /// <para><see cref="HLCTimestamp.Zero"/> on all other transaction types — the default
    /// (Read Committed) path is unchanged.</para>
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; }

    public KvTransaction(
        HLCTimestamp transactionId,
        string uniqueId,
        bool isReadOnly = false,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        CamusTransactionMode transactionMode = CamusTransactionMode.ReadWrite,
        HLCTimestamp readTimestamp = default)
    {
        TransactionId = transactionId;
        UniqueId = uniqueId;
        IsReadOnly = isReadOnly;
        IsolationLevel = isolationLevel;
        TransactionMode = transactionMode;
        ReadTimestamp = readTimestamp;
    }

    /// <summary>
    /// Creates a read-only snapshot transaction. Uses <see cref="HLCTimestamp.Zero"/> so Kahuna
    /// performs non-transactional reads of the latest committed value — no Kahuna round-trips.
    /// </summary>
    public static KvTransaction CreateReadOnly() =>
        new(HLCTimestamp.Zero, string.Empty, isReadOnly: true,
            transactionMode: CamusTransactionMode.ReadOnly);

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
    /// Records that a Kahuna key-range lock was acquired for this transaction (key-range routed
    /// spaces). Like prefix locks, range locks are read-only and not finalized by the 2PC, so
    /// <see cref="KvTransactionsManager"/> releases them explicitly over the same bounds. Idempotent.
    /// </summary>
    public void TrackRangeLock(
        string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        KeyValueDurability durability)
    {
        lock (trackSync)
        {
            acquiredRangeLocks ??= [];
            acquiredRangeLocks.Add(new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, durability));
        }
    }

    /// <summary>Snapshot of the key-range locks acquired by this transaction.</summary>
    public IReadOnlyList<RangeLockBounds> GetAcquiredRangeLocks()
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null || acquiredRangeLocks.Count == 0)
                return [];

            return [.. acquiredRangeLocks];
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
