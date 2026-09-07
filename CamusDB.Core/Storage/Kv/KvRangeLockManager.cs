/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Owns every Kahuna lock one table's access paths take: the range (prefix) locks that make a scan
/// serializable, the singleton point locks that implement strict two-phase locking for a read, and
/// the exclusive key locks a write needs to drive 2PC.
///
/// <para><b>Why range locks and point locks are the same mechanism.</b> A Kahuna range lock over
/// <c>[key, key]</c> is a point lock, so read protection and phantom protection are expressed once.
/// Two transactions holding <c>Shared</c> locks over overlapping ranges coexist; Kahuna's write-path
/// fence is what rejects a phantom mutation into a range someone holds.</para>
///
/// <para><b>Deadlock avoidance is by timestamp order, not by detection.</b> When an
/// <c>AlreadyLocked</c> denial names its holder, the requester compares transaction start
/// timestamps: an older requester waits for a younger holder, a younger requester aborts at once and
/// is replayed from BeginAsync. Waits therefore only ever point older-to-younger, so no wait cycle
/// can form and two transactions contending in reverse order can never both abort.</para>
///
/// <para><b>Optimistic transactions still lock on the read side.</b> The write path skips the
/// explicit exclusive lock under optimistic locking, but a Serializable read takes its shared
/// predicate lock regardless of <see cref="KvTransaction.Locking"/>. Serializable + Optimistic is
/// therefore a hybrid — optimistic writes with commit-time validation, plus retained predicate locks
/// that keep the read phantom-free. Weakening it to fully lock-free would silently break isolation.</para>
/// </summary>
internal sealed class KvRangeLockManager
{
    private readonly IKahuna kahuna;
    private readonly ILogger logger;
    private readonly KvKeyBuilder keys;
    private readonly KvConflictMessageBuilder messages;
    private readonly KahunaRetryPolicy retry;

    // Index KvIds registered as key-range routed by TableOpener.
    // Written once during table open (inside AsyncLazy, single-threaded), then only read.
    private readonly HashSet<string> rangedIndexIds = [];

    /// <summary>Configuration snapshot; swapped atomically by <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    // Safety-net expiry for an exclusive range (prefix) lock. The lock is released explicitly when
    // the owning transaction commits or rolls back; this expiry only bounds a leak if the client
    // dies mid-transaction. A range scan that needs serializable isolation should comfortably
    // finish inside this window.
    private int RangeLockExpiresMs => options.RangeLockExpiresMs;

    internal KvRangeLockManager(
        IKahuna kahuna,
        ILogger logger,
        KvKeyBuilder keys,
        KvConflictMessageBuilder messages,
        KahunaRetryPolicy retry,
        CamusDBOptions options)
    {
        this.kahuna = kahuna;
        this.logger = logger;
        this.keys = keys;
        this.messages = messages;
        this.retry = retry;
        this.options = options;
    }

    /// <summary>Swaps in a newly published configuration snapshot. See <see cref="KvTableStore.ApplyOptions"/>.</summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Marks <paramref name="indexId"/> as key-range routed on this node. Called by
    /// <c>TableOpener</c> after successfully registering the index space. Once marked,
    /// <see cref="AcquireIndexRangeLockAsync"/> uses a Kahuna range lock instead of a prefix lock.
    /// </summary>
    internal void MarkIndexAsRanged(string indexId) => rangedIndexIds.Add(indexId);

    /// <summary>
    /// True when this transaction is the one shape that needs phantom protection as part of strict
    /// two-phase locking. Read paths use it to decide whether a shared predicate lock is taken at
    /// all; it is deliberately independent of <see cref="KvTransaction.Locking"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsSerializableReadWrite(KvTransaction tx)
        => tx is { IsolationLevel: CamusIsolationLevel.Serializable, TransactionMode: CamusTransactionMode.ReadWrite };

    /// <summary>
    /// Returns <c>true</c> if <paramref name="tx"/> holds a singleton <b>Shared</b> point lock
    /// <c>[key, key]</c> for the given <paramref name="bucketPrefix"/>. Used by write paths
    /// to detect the read-then-write pattern and trigger the S→X upgrade.
    ///
    /// <para>The mode is part of the test on purpose: a key this transaction has already promoted
    /// to Exclusive (an earlier write in the same transaction) needs no second upgrade, and asking
    /// for one only spends a round trip re-asserting a lock it holds.</para>
    /// </summary>
    internal static bool HasSharedPointLock(KvTransaction tx, string bucketPrefix, string key)
        => tx.HasPointLock(bucketPrefix, key, RangeLockMode.Shared);

    // -----------------------------------------------------------------------
    // Range (prefix) locking — opt-in serializable scans
    // -----------------------------------------------------------------------

    /// <summary>
    /// Acquires a <b>shared</b> lock over the table's entire row range for <paramref name="tx"/>,
    /// giving a serializable, phantom-free view: a concurrent transaction cannot insert, update,
    /// or delete any row in this table until <paramref name="tx"/> commits or rolls back (the
    /// write-path fence blocks mutations into the locked range), while other read scans over the
    /// same range coexist. Call this <b>before</b> a cursor scan (<see cref="KvTableStore.ScanRows"/>)
    /// that must not observe concurrent mutations across page boundaries — the default scan path only
    /// guarantees a snapshot, not serializability. The lock is tracked on the transaction and
    /// released automatically on commit/rollback.
    ///
    /// In key-range-sharding mode the lock is always acquired (existing behaviour). In hash mode
    /// (single partition per table) the lock fires only for Serializable read-write transactions
    /// — the isolation level that requires phantom protection as part of strict two-phase locking.
    /// Read Committed and Serializable read-only transactions are exempt.
    /// </summary>
    internal Task AcquireRowRangeLockAsync(KvTransaction tx, bool exclusive = false, CancellationToken cancellationToken = default)
    {
        if (!options.KeyRangeShardingEnabled && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;

        return AcquireRangeLockAsync(
            tx,
            keys.RowBucketPrefix,
            null,
            true,
            null,
            true,
            cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared
        );
    }

    /// <summary>
    /// Takes an <b>unconditional exclusive</b> lock over this table's entire row key range for
    /// <paramref name="tx"/>, in every routing mode and at every isolation level.
    /// </summary>
    /// <remarks>
    /// <para>This is the write fence a contents swap needs, and it is deliberately not
    /// <see cref="AcquireRowRangeLockAsync"/>. That method is a scan helper: in hash routing it takes
    /// no lock at all unless the transaction is Serializable read-write, because an ordinary scan has
    /// no phantom to protect against. A truncate does — it is about to make every row in the range
    /// unreachable, so a writer that staged a row into the old key-space and commits afterwards would
    /// have its write acknowledged into storage nothing reads. Kahuna's durable range-lock write fence
    /// aborts exactly those writers, optimistic and pessimistic alike, and makes later ones wait.</para>
    ///
    /// <para>The lock is registered on the transaction, renewed by the coordinator for the session's
    /// lifetime, and released when the transaction finalizes. Hold it across the schema-log commit and
    /// the checkpoint: the cut timestamp is only a valid linearization point while it is held.</para>
    /// </remarks>
    internal Task AcquireExclusiveRowSpaceFenceAsync(KvTransaction tx, CancellationToken cancellationToken = default)
        => AcquireRangeLockAsync(
            tx,
            keys.RowBucketPrefix,
            null,
            true,
            null,
            true,
            cancellationToken,
            RangeLockMode.Exclusive
        );

    /// <summary>
    /// Locks an index's entire key range for <paramref name="tx"/> (serializable index
    /// scan / phantom protection over the index bucket). Used for full-index scans where no
    /// tighter bound is known. For bounded range scans use
    /// <see cref="AcquireBoundedIndexRangeLockAsync"/>.
    ///
    /// Pass <paramref name="exclusive"/> = true for UPDATE/DELETE locate scans: the Exclusive
    /// mode blocks concurrent Serializable+RW readers from acquiring a Shared lock on the same
    /// range, preventing them from observing a partial mutation during the write phase.
    ///
    /// Fires when key-range sharding is enabled and the index is ranged, or unconditionally for
    /// Serializable read-write transactions (which need phantom protection regardless of sharding
    /// mode — in hash mode the lock covers the single partition that owns the index bucket).
    /// </summary>
    internal Task AcquireIndexRangeLockAsync(KvTransaction tx, string indexId, bool exclusive = false, CancellationToken cancellationToken = default)
    {
        if ((!options.KeyRangeShardingEnabled || !rangedIndexIds.Contains(indexId)) && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;

        return AcquireRangeLockAsync(
            tx,
            keys.BuildIndexBucketPrefix(indexId),
            null,
            true,
            null,
            true,
            cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared
        );
    }

    /// <summary>
    /// Like <see cref="AcquireIndexRangeLockAsync"/> but locks only the sub-range
    /// <c>[fromBound, toBound]</c> instead of the whole bucket. This delivers the per-range
    /// concurrency win promised by key-range sharding: two transactions scanning disjoint
    /// portions of the same index don't conflict. The bounds are encoded with
    /// <see cref="KeyEncoder"/> to match the stored key format. For non-unique indexes
    /// <see cref="KvStoreConstants.IndexKeySentinel"/> (U+FFFF) is appended so all rowId suffixes of
    /// the last value are included — see the constant's declaration for the invariant this relies on.
    ///
    /// Pass <paramref name="exclusive"/> = true for UPDATE/DELETE locate scans (same rationale
    /// as <see cref="AcquireIndexRangeLockAsync"/>).
    ///
    /// Fires under the same conditions as <see cref="AcquireIndexRangeLockAsync"/>.
    /// </summary>
    internal Task AcquireBoundedIndexRangeLockAsync(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue? fromBound, bool fromInclusive,
        CompositeColumnValue? toBound,   bool toInclusive,
        bool unique,
        bool exclusive = false,
        CancellationToken cancellationToken = default,
        int keyColumnCount = 0)
    {
        if ((!options.KeyRangeShardingEnabled || !rangedIndexIds.Contains(indexId)) && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;

        string bucketPrefix = keys.BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";

        OrderType[]? directions = keys.DirectionsOf(indexId);
        string? fromEncoded = fromBound is not null ? KeyEncoder.Encode(fromBound, directions) : null;
        string? toEncoded   = toBound   is not null ? KeyEncoder.Encode(toBound, directions)   : null;

        // Non-unique stored key = {encodedValue}{rowId24}. The IndexKeySentinel (U+FFFF) sorts after
        // every rowId suffix, so it is appended to push a bound past all of a value's row entries.
        //
        // Start bound: for an exclusive lower bound (fromInclusive=false, i.e. ">") the lock must
        // begin strictly after value V's rows. Without the sentinel, {encode(V)} exclusive still
        // covers {encode(V)+rowId} (those sort after {encode(V)}), wrongly locking value V and
        // overlapping a disjoint "<= V" lock. Appending the sentinel makes the bound clear V's rows.
        // For an inclusive lower bound (">=") value V is wanted, so no sentinel.
        //
        // A prefix bound (fewer components than the index has key columns) behaves like the
        // non-unique case even on a unique index: real keys carry the remaining columns' encodings
        // after the bound, so the sentinel is what makes the lock span the prefix's rows. Without
        // this a prefix scan on a unique index would lock a bare point while reading a whole range,
        // leaving the phantom inserts it is supposed to fence unfenced. keyColumnCount == 0 means
        // the caller did not say, so the bound is assumed full-width (previous behaviour).
        bool fromIsPrefixBound = keyColumnCount > 0 && fromBound is not null && fromBound.Values.Length < keyColumnCount;
        bool toIsPrefixBound   = keyColumnCount > 0 && toBound   is not null && toBound.Values.Length   < keyColumnCount;

        string? startKey = fromEncoded is not null
            ? ((unique && !fromIsPrefixBound) || fromInclusive ? keyPrefix + fromEncoded : keyPrefix + fromEncoded + KvStoreConstants.IndexKeySentinel)
            : null;

        // End bound: the sentinel is appended ONLY when toInclusive=true, to cover all rowId suffixes
        // of the last value. When toInclusive=false ("<") it must be omitted: appending it would
        // extend the exclusive bound from {encode(V)} to {encode(V)+U+FFFF}, which includes
        // {encode(V)} itself and any {encode(V)+rowId} entries, causing a false range-overlap with
        // the next disjoint lock whose fromBound is exactly {encode(V)}.
        string? endKey   = toEncoded is not null
            ? ((unique && !toIsPrefixBound) || !toInclusive ? keyPrefix + toEncoded : keyPrefix + toEncoded + KvStoreConstants.IndexKeySentinel)
            : null;

        return AcquireRangeLockAsync(
            tx,
            bucketPrefix,
            startKey,
            fromInclusive,
            endKey,
            toInclusive,
            cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared
        );
    }

    /// <summary>
    /// Core range-lock implementation used by both the whole-bucket and bounded variants.
    /// Acquires a range lock of the given <paramref name="mode"/> (default <c>Shared</c>),
    /// retrying on <c>MustRetry</c>; tracks the acquired lock on the transaction for release
    /// at commit/rollback.
    ///
    /// <para><b>Shared</b> — the correct mode for read scans (SELECT). Two transactions scanning
    /// overlapping ranges coexist (S∩S), while Kahuna's write-path fence rejects phantom mutations
    /// from other transactions into the locked range.</para>
    ///
    /// <para><b>Exclusive</b> — used when a Serializable+RW txn promotes a shared singleton
    /// lock it already holds on a key it is about to write. Kahuna promotes the existing Shared
    /// entry to Exclusive in-place if no other transaction holds a conflicting overlapping lock;
    /// if one does, <c>AlreadyLocked</c> is returned and the caller receives a
    /// <see cref="CamusDBErrorCodes.TransactionConflict"/> exception.</para>
    /// </summary>
    private async Task AcquireRangeLockAsync(
        KvTransaction tx,
        string bucketPrefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        CancellationToken cancellationToken,
        RangeLockMode mode = RangeLockMode.Shared)
    {
        ArgumentNullException.ThrowIfNull(tx);

        // Start the deferred Kahuna session (if not yet started) before any lock attempt.
        // For zero-snapshot and eager-start transactions this is a no-op.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        // A range lock needs a real transaction identity to own and later release it. The
        // Zero-snapshot read-only fast path (point reads, and all reads in single-partition mode)
        // has no such identity, so it cannot take a range lock — skip. A promoted read-only scan
        // (real id, key-range mode) falls through and takes a shared lock like any transaction.
        if (tx.TransactionId == HLCTimestamp.Zero)
            return;

        // Enforce the serializable transaction lifetime deadline. Only Serializable+RW
        // transactions acquire range locks whose expiry would silently break isolation;
        // ReadCommitted transactions in key-range mode also take scan locks but have no
        // serializability to protect, so they are exempt (consistent with the CommitAsync gate).
        if (IsSerializableReadWrite(tx) && tx.IsExpired(options.MaxSerializableTransactionLifetimeMs))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionLifetimeExceeded,
                $"Serializable transaction {tx.UniqueId} exceeded the maximum lifetime " +
                $"({options.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");

        long deadline = retry.LockWaitDeadlineTicks();
        int retries = 0;

        // Register the range-lock acquisition with the coordinator so it folds into the session working
        // set. This is what lets the coordinator renew the lock's TTL for the life of the transaction
        // (retiring the client-side heartbeat) and release it on finalize. A range lock always has a real
        // transaction identity (checked above), so the coordinator key is always present here.
        //
        // The operation id is bound to this exact range-lock declaration. It is RETAINED across transient
        // MustRetry/WaitingForReplication retries, so if a completion ack was lost the coordinator replays
        // the effect under the same id instead of stranding the original registration and double-folding
        // the lock. A confirmed AlreadyLocked denial is a definite negative observation, so a wait-then-
        // retry after one starts a fresh attempt id (a genuinely new acquire attempt).
        TransactionOperationId rangeLockOperationId = TransactionOperationId.NewRandom();

        while (true)
        {
            (KeyValueResponseType type, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
                tx.TransactionId, bucketPrefix,
                startKey, startInclusive, endKey, endInclusive,
                RangeLockExpiresMs, KeyValueDurability.Persistent, mode, cancellationToken,
                tx.CoordinatorKey, rangeLockOperationId
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Locked)
            {
                tx.TrackRangeLock(
                    bucketPrefix,
                    startKey,
                    startInclusive,
                    endKey,
                    endInclusive,
                    KeyValueDurability.Persistent,
                    mode
                );

                if (options.LockTracingEnabled && logger.IsEnabled(LogLevel.Debug))
                {
                    string modeStr = mode.ToString();
                    Log.LogRangeLockAcquired(logger, modeStr, bucketPrefix,
                        startKey ?? "-∞", startInclusive, endKey ?? "+∞", endInclusive, tx.UniqueId);
                }
                return;
            }

            // Transient (routing flip / replication-not-ready): bounded-wait, then surface MustRetry.
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionMustRetry,
                        messages.LockWaitDeadlineMessage(tx, $"{mode} range-lock acquisition on {messages.DescribeBucket(bucketPrefix)} over " +
                                                    $"[{KvConflictMessageBuilder.Printable(startKey ?? "-∞")}, {KvConflictMessageBuilder.Printable(endKey ?? "+∞")}]"));
                ServerDiagnostics.AddKvRetryWait("rangelock_transient");
                await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries++), cancellationToken).ConfigureAwait(false);
                continue;
            }

            // AlreadyLocked is a serialization conflict with a foreign holder: for Shared, another txn
            // holds an overlapping Exclusive lock; for Exclusive, another txn holds an overlapping
            // Shared or Exclusive lock (including the S→X upgrade case).
            //
            // Resolve it by deadlock-avoidance ordering on the holder's start timestamp (its
            // transaction id, returned alongside the denial). An OLDER requester (smaller HLC) WAITS
            // for the younger holder to release; a YOUNGER requester ABORTS immediately and is replayed
            // from BeginAsync. Waits therefore only ever point older→younger, so no wait cycle can form
            // and two transactions contending in reverse order can never both abort — the older wins
            // deterministically. With no holder reported (Zero), no ordering is possible, so fall back
            // to immediate abort.
            if (type == KeyValueResponseType.AlreadyLocked)
            {
                bool requesterIsOlder = holder != HLCTimestamp.Zero && tx.TransactionId.CompareTo(holder) < 0;
                if (requesterIsOlder && Stopwatch.GetTimestamp() < deadline)
                {
                    ServerDiagnostics.AddKvRetryWait("rangelock_holder_wait");
                    await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries++), cancellationToken).ConfigureAwait(false);
                    // A confirmed denial ends this observation; the wait-then-retry is a fresh acquire
                    // attempt, so mint a new id rather than replay the denied one.
                    rangeLockOperationId = TransactionOperationId.NewRandom();
                    continue; // wait for the younger holder to finish
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"Range '{bucketPrefix}' [{startKey ?? "-∞"},{endKey ?? "+∞"}] is locked by another transaction (conflict during {mode} acquire)");
            }

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Failed to acquire {mode} range lock on '{bucketPrefix}': {type}");
        }
    }

    /// <summary>
    /// Acquires a shared (S) range lock whose start and end are both the same <paramref name="key"/>,
    /// effectively locking exactly one point. Used by read paths in Serializable read-write
    /// transactions to implement strict 2PL: the lock is held until commit/rollback, preventing
    /// a concurrent writer from committing a mutation to that key while this transaction is live.
    ///
    /// Two transactions acquiring shared point locks on the same key coexist (S∩S compatible).
    /// Conflict is detected at the writer's prepare/commit time via Kahuna's range-lock fence.
    ///
    /// <para><b>Escalation:</b> once the per-bucket point-lock count reaches
    /// <see cref="CamusDBOptions.LockEscalationThreshold"/>, a single whole-bucket Shared lock
    /// (<c>[null,null)</c>) is acquired instead. All subsequent reads on the same bucket skip the
    /// per-point RPC — the whole-bucket lock already covers them. Old per-point entries are kept in
    /// tracking and released at commit.</para>
    ///
    /// <para><b>Already covered:</b> a transaction that reads the same key more than once (a very
    /// common shape — read a row, re-read it later in the same statement or transaction) needs no
    /// second acquire. The first one is held to commit under strict two-phase locking, so every
    /// later read is already protected and re-asserting it only spends a round trip. An Exclusive
    /// singleton counts as covering too, since it is strictly stronger than the Shared lock wanted
    /// here.</para>
    /// </summary>
    internal Task AcquireSharedPointLockAsync(
        KvTransaction tx,
        string bucketPrefix,
        string key,
        CancellationToken cancellationToken)
    {
        // Whole-bucket lock already covers this point — no new RPC needed.
        if (tx.HasWholeBucketLock(bucketPrefix))
            return Task.CompletedTask;

        // Already holding this exact point (Shared, or Exclusive after a write) — likewise covered.
        if (tx.HasPointLock(bucketPrefix, key))
            return Task.CompletedTask;

        // Past the threshold: promote to a single whole-bucket Shared lock.
        // Old per-point entries remain tracked and are released at commit/rollback.
        if (tx.CountPointLocksForBucket(bucketPrefix) >= options.LockEscalationThreshold)
            return AcquireRangeLockAsync(
                tx,
                bucketPrefix,
                null,
                true,
                null,
                true,
                cancellationToken
            );

        return AcquireRangeLockAsync(
            tx,
            bucketPrefix,
            key,
            true,
            key,
            true,
            cancellationToken
        );
    }

    /// <summary>
    /// Upgrades a shared singleton range lock on <paramref name="key"/> to exclusive in-place.
    /// Called by write paths in Serializable read-write transactions when the same transaction
    /// already holds a shared point lock on the key it is about to write.
    ///
    /// After the upgrade no other transaction can acquire a new shared or exclusive range lock on
    /// this key until the upgrading transaction commits or rolls back. Kahuna validates the upgrade
    /// by checking that no other transaction holds a conflicting overlapping lock; if one does,
    /// <c>AlreadyLocked</c> is returned and a <see cref="CamusDBErrorCodes.TransactionConflict"/>
    /// exception is raised.
    /// </summary>
    internal Task UpgradeToExclusivePointLockAsync(
        KvTransaction tx,
        string bucketPrefix,
        string key,
        CancellationToken cancellationToken)
        => AcquireRangeLockAsync(tx, bucketPrefix, key, true, key, true, cancellationToken, RangeLockMode.Exclusive);

    /// <summary>
    /// Takes the per-key exclusive write lock that drives the 2PC commit path for a single-key
    /// mutation. This is a different Kahuna mechanism from the range lock above and each is
    /// load-bearing for a different invariant: the write intent this sets is what the commit
    /// finalizes, while an exclusive range lock is what excludes concurrent readers and phantoms.
    ///
    /// <para>Optimistic transactions take no explicit exclusive write lock — the write's implicit
    /// point lock plus commit-time validation replace it. That skip applies to the write lock only:
    /// under Serializable the read/scan predicate locks still apply (see the class summary).</para>
    /// </summary>
    internal async Task AcquireExclusiveKeyLockAsync(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        // Start the deferred Kahuna session before the optimistic/pessimistic branch so the
        // session is open for the write that follows even when optimistic skips the lock.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (tx.Locking == KeyValueTransactionLocking.Optimistic)
            return;

        (KeyValueResponseType lockType, _, _, _) = await retry.RetryOnMustRetryRegistered(tx, "exclusive lock acquisition", key,
            (coordinatorKey, operationId) => kahuna.LocateAndTryAcquireExclusiveLock(tx.TransactionId, key, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (lockType == KeyValueResponseType.AlreadyLocked)
            throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, $"Key {key} is locked by another transaction");

        // A range that lost quorum or changed leadership (e.g. under a partition) aborts the lock
        // acquisition; that is transient and retryable from BeginAsync, not corruption.
        if (lockType == KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Lock acquisition on {key} was aborted by Kahuna — retry the operation from BeginAsync");

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {lockType}");

        if (options.LockTracingEnabled)
            Log.LogPointLockAcquired(logger, key, tx.UniqueId);
    }
}
