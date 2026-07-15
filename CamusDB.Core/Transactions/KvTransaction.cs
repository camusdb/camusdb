
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Kahuna.Server.KeyValues;
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
/// The bounds of a Kahuna key-range lock held by a transaction, retained so the lock can be
/// released over the identical interval on commit/rollback and renewed by the heartbeat.
/// A whole-bucket lock is <c>(prefix, null, true, null, true)</c>.
/// </summary>
public readonly record struct RangeLockBounds(
    string Prefix, string? StartKey, bool StartInclusive, string? EndKey, bool EndInclusive,
    KeyValueDurability Durability, RangeLockMode Mode = RangeLockMode.Shared);

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
    /// transaction coordinator. Supplied as <c>KeyValueTransactionOptions.CoordinatorKey</c>
    /// when the transaction is started; it pins the coordinator session to one partition leader.
    /// </summary>
    public string UniqueId { get; }

    /// <summary>
    /// The coordinator key that pins this transaction's server-side session to a partition leader.
    /// Passed on every registered operation so the confirmed effect folds into the coordinator-owned
    /// working set. Identical to <see cref="UniqueId"/> — the value CamusDB supplies as
    /// <c>KeyValueTransactionOptions.CoordinatorKey</c> at start.
    /// </summary>
    public string CoordinatorKey => UniqueId;

    /// <summary>
    /// The routing identity handed to <c>LocateAndCommitTransaction</c> / <c>LocateAndRollbackTransaction</c>.
    /// The Kahuna coordinator finalizes from its own working set, so the handle (transaction id +
    /// coordinator key) is the only thing the client supplies at finalize — no client-built key list.
    /// <see cref="TransactionHandle.RecordAnchorKey"/> is left null: it matters only for durable-decision
    /// recovery, which the best-effort commit path does not use.
    /// </summary>
    public TransactionHandle Handle => new(TransactionId, UniqueId);

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
    /// Isolation level for this transaction. Defaults to <see cref="CamusIsolationLevel.ReadCommitted"/>.
    /// May be upgraded via <c>SET TRANSACTION ISOLATION LEVEL</c> before any locks are acquired —
    /// the SQL handler calls <see cref="ApplyIsolationLevel"/> which guards against mid-flight changes.
    /// </summary>
    public CamusIsolationLevel IsolationLevel { get; private set; }

    /// <summary>
    /// Mode (read-write or read-only) for this transaction. Defaults to <see cref="CamusTransactionMode.ReadWrite"/>.
    /// May be changed via <c>SET TRANSACTION … READ ONLY</c> before any locks are acquired.
    /// </summary>
    public CamusTransactionMode TransactionMode { get; private set; }

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

    /// <summary>
    /// Concurrency strategy the Kahuna coordinator was started with for this transaction.
    /// <see cref="KeyValueTransactionLocking.Pessimistic"/> (the default) acquires locks before each
    /// write and relies on them alone; <see cref="KeyValueTransactionLocking.Optimistic"/> defers
    /// conflict detection to a read-set validation at commit, which requires reads to be folded into
    /// the coordinator working set (see <see cref="FoldReads"/>).
    /// </summary>
    public KeyValueTransactionLocking Locking { get; }

    /// <summary>
    /// Whether reads issued by this transaction are tracked by the coordinator for a commit-time
    /// read-set conflict check. <see cref="ReadValidation.None"/> (the default) skips tracking;
    /// <see cref="ReadValidation.TrackAndValidate"/> requests the check even for a pessimistic
    /// transaction. It cannot be combined with a fixed <see cref="ReadTimestamp"/> (a historical
    /// snapshot has no live read dependency), so <see cref="BeginAsync"/> never sets both.
    /// </summary>
    public ReadValidation ReadValidation { get; }

    /// <summary>
    /// Whether each read this transaction performs should be registered with the coordinator so its
    /// existence/base-revision observation folds into the server-owned working set and is validated
    /// at commit. True only when the transaction actually wants read-set validation — optimistic
    /// locking (which validates its read set by definition) or an explicit
    /// <see cref="ReadValidation.TrackAndValidate"/> request — and only for a real registered
    /// transaction reading the latest version (a <see cref="HLCTimestamp.Zero"/> identity has no
    /// coordinator session to fold into, and a fixed <see cref="ReadTimestamp"/> snapshot carries no
    /// live read dependency).
    ///
    /// <para><b>Not yet consumed by the read path.</b> This is the gate the scan/point-read helpers
    /// will consult to decide whether to pass a coordinator key + operation id. Read folding is
    /// blocked until Kahuna's streaming scan and batch-read entry points accept those registration
    /// parameters (the point-read API already does; the dominant scan/batch paths do not). Until
    /// then reads never fold, so optimistic transactions detect write-write conflicts (writes fold)
    /// but not read-write / write-skew conflicts. Wiring this on without full scan coverage would be
    /// misleading, since a scanned-then-modified row would go undetected.</para>
    /// </summary>
    public bool FoldReads =>
        TransactionId != HLCTimestamp.Zero &&
        ReadTimestamp.IsNull() &&
        (Locking == KeyValueTransactionLocking.Optimistic ||
         ReadValidation == ReadValidation.TrackAndValidate);

    /// <summary>
    /// Monotonic elapsed-time counter started when this transaction was created. Used to enforce
    /// <see cref="CamusDBConfig.MaxSerializableTransactionLifetimeMs"/> — immune to NTP wall-clock
    /// jumps, consistent with the monotonic lock-wait deadline used elsewhere in the store.
    ///
    /// <para><c>null</c> for zero-snapshot (read-only) transactions, which have no lifetime limit.</para>
    /// </summary>
    private readonly Stopwatch? lifetimeWatch;

    // Mutation budget, guarded by trackSync.
    private int mutationCount;
    private readonly int mutationLimit; // 0 or negative = unlimited (DDL, read-only, disabled)

    /// <summary>
    /// Returns <c>true</c> if <paramref name="maxLifetimeMs"/> is positive and the transaction
    /// has been open longer than that duration. Always <c>false</c> for zero-snapshot (RO) transactions.
    /// </summary>
    public bool IsExpired(int maxLifetimeMs) =>
        maxLifetimeMs > 0 &&
        lifetimeWatch is not null &&
        lifetimeWatch.ElapsedMilliseconds > maxLifetimeMs;

    /// <summary>
    /// Reserves <paramref name="count"/> mutations against this transaction's budget before the
    /// corresponding writes are issued. Throws <see cref="CamusDBErrorCodes.TransactionMutationLimitExceeded"/>
    /// if the budget would be exceeded; otherwise adds to the running total monotonically.
    /// No-op when the limit is disabled (<c>&lt;= 0</c>) or <paramref name="count"/> is zero.
    /// </summary>
    public void ReserveMutations(int count)
    {
        if (mutationLimit <= 0 || count == 0) return;
        lock (trackSync)
        {
            if ((long)mutationCount + count > mutationLimit)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMutationLimitExceeded,
                    $"Transaction {UniqueId} would exceed the maximum of {mutationLimit} mutations " +
                    $"(already {mutationCount}, requested {count}); split the work into smaller transactions");
            mutationCount += count;
        }
    }

    /// <summary>Running total of mutations reserved so far in this transaction.</summary>
    public int MutationCount { get { lock (trackSync) return mutationCount; } }

    public KvTransaction(
        HLCTimestamp transactionId,
        string uniqueId,
        bool isReadOnly = false,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        CamusTransactionMode transactionMode = CamusTransactionMode.ReadWrite,
        HLCTimestamp readTimestamp = default,
        int mutationLimit = 0,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic,
        ReadValidation readValidation = ReadValidation.None)
    {
        TransactionId = transactionId;
        UniqueId = uniqueId;
        IsReadOnly = isReadOnly;
        IsolationLevel = isolationLevel;
        TransactionMode = transactionMode;
        ReadTimestamp = readTimestamp;
        this.mutationLimit = mutationLimit;
        Locking = locking;
        ReadValidation = readValidation;
        if (transactionId != HLCTimestamp.Zero)
            lifetimeWatch = Stopwatch.StartNew();
    }

    /// <summary>
    /// Creates a read-only snapshot transaction. Uses <see cref="HLCTimestamp.Zero"/> so Kahuna
    /// performs non-transactional reads of the latest committed value — no Kahuna round-trips.
    /// </summary>
    public static KvTransaction CreateReadOnly() =>
        new(HLCTimestamp.Zero, string.Empty, isReadOnly: true,
            transactionMode: CamusTransactionMode.ReadOnly);

    /// <summary>
    /// Creates a cheap serializable read-only snapshot transaction pinned to
    /// <paramref name="snapshotT"/>. The transaction identity is still
    /// <see cref="HLCTimestamp.Zero"/> (no Kahuna transaction object, no begin/commit round-trip),
    /// but every read is issued with <c>readTimestamp = snapshotT</c> so all reads in the query
    /// see one consistent point in the version history — exactly as a full Serializable+ReadOnly
    /// transaction would, but without the session-open cost.
    /// </summary>
    public static KvTransaction CreateSnapshotReadOnly(HLCTimestamp snapshotT) =>
        new(HLCTimestamp.Zero, string.Empty, isReadOnly: true,
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly,
            readTimestamp: snapshotT);

    /// <summary>
    /// Whether any statement (SELECT, INSERT, UPDATE, DELETE, SHOW, EXPLAIN …) has executed
    /// in this transaction. Set to <c>true</c> by <see cref="MarkStatementExecuted"/> before
    /// the first non-<c>SET TRANSACTION</c> statement runs. Used by <see cref="ApplyIsolationLevel"/>
    /// to reject a level change after the transaction has already read or written data.
    /// </summary>
    private bool statementExecuted;

    /// <summary>
    /// Marks the transaction as having executed at least one statement. Must be called by the
    /// executor for every statement type <b>except</b> <c>SET TRANSACTION</c> — the standard SQL
    /// rule is that <c>SET TRANSACTION</c> must precede all other statements in the transaction.
    /// </summary>
    public void MarkStatementExecuted()
    {
        // No lock needed: a bool write is atomic on all .NET-supported architectures, and the
        // flag only transitions false→true (never back), so a torn read is harmless.
        statementExecuted = true;
    }

    /// <summary>
    /// Applies a new isolation level and/or transaction mode to this transaction. Called by the
    /// <c>SET TRANSACTION ISOLATION LEVEL</c> SQL handler.
    ///
    /// <para>Throws <see cref="CamusDBErrorCodes.InvalidInput"/> if any prior statement has
    /// executed in this transaction. Standard SQL requires <c>SET TRANSACTION</c> to be the first
    /// statement. A ReadCommitted read acquires no locks but still "touches" the transaction —
    /// upgrading to Serializable after that read would silently omit the shared lock the new level
    /// requires for the data already read.</para>
    /// </summary>
    public void ApplyIsolationLevel(CamusIsolationLevel level, CamusTransactionMode mode)
    {
        lock (trackSync)
        {
            if (statementExecuted)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION must be the first statement of a transaction — " +
                    $"transaction {UniqueId} has already executed a statement");

            IsolationLevel  = level;
            TransactionMode = mode;
        }
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
    /// Records that a Kahuna key-range lock was acquired for this transaction (key-range routed
    /// spaces). Like prefix locks, range locks are read-only and not finalized by the 2PC, so
    /// <see cref="KvTransactionsManager"/> releases them explicitly over the same bounds. Idempotent.
    /// </summary>
    public void TrackRangeLock(
        string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        KeyValueDurability durability, RangeLockMode mode = RangeLockMode.Shared)
    {
        lock (trackSync)
        {
            acquiredRangeLocks ??= [];
            acquiredRangeLocks.Add(new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, durability, mode));
        }
    }

    /// <summary>
    /// Returns <c>true</c> if the transaction holds a whole-bucket range lock (start and end
    /// both <c>null</c>) on <paramref name="prefix"/>, regardless of Shared/Exclusive mode.
    /// A whole-bucket lock already covers every point within the bucket, so callers can skip
    /// per-point lock acquisition.
    /// </summary>
    public bool HasWholeBucketLock(string prefix)
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null)
                return false;
            foreach (RangeLockBounds b in acquiredRangeLocks)
            {
                if (b.Prefix == prefix && b.StartKey is null && b.EndKey is null)
                    return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Counts singleton point locks (<c>[key,key]</c> entries) held for <paramref name="prefix"/>.
    /// Used to decide when to escalate to a whole-bucket lock.
    /// </summary>
    public int CountPointLocksForBucket(string prefix)
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null)
                return 0;
            int count = 0;
            foreach (RangeLockBounds b in acquiredRangeLocks)
            {
                if (b.Prefix == prefix && b.StartKey is not null && b.StartKey == b.EndKey)
                    count++;
            }
            return count;
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

    /// <summary>
    /// Returns the modified-keys set as raw (key, durability) pairs. Used by the cache
    /// invalidation path, which needs the keys in tuple form to call
    /// <see cref="IQueryResultCache.InvalidateByModifiedKeys"/> without an extra allocation.
    /// </summary>
    public List<(string key, KeyValueDurability durability)> GetModifiedKeyPairs()
    {
        if (modifiedKeys is null)
            return [];

        return [.. modifiedKeys];
    }

    private readonly record struct SchemaVersionPin(
        long SchemaVersion,
        Func<long> CurrentVersion,
        Func<bool>? IsStillValid
    );
}
