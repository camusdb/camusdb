
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Per-table data access layer built on top of <see cref="IKahuna"/>.
///
/// Key layout (all keys share the leading <c>{dbId}:{tableId}</c> segment so databases are
/// isolated in the shared keyspace and Kommander routes the whole table to one partition):
///
///   Primary rows:      {dbId}:{tableId}:r/{rowIdHex24}                         → serialized row bytes
///   Unique index:      {dbId}:{tableId}:i:{indexId}/{encodedKey}               → rowIdHex24 (UTF-8)
///   Non-unique index:  {dbId}:{tableId}:i:{indexId}/{encodedKey}{rowIdHex24}   → rowIdHex24 (UTF-8)
///     (rowId appended without separator; it is always exactly 24 lowercase hex chars)
///
/// Routing constraint:
///   LocateAndScanRange routes via SimpleHash(prefix) while individual TrySet/Delete
///   route via InversePrefixedStaticHash(key, '/') = SimpleHash(key[..lastSlash]).
///   For rows: bucket prefix "{dbId}:{tableId}:r" → SimpleHash("{dbId}:{tableId}:r") matches writes.
///   For indexes: bucket prefix "{dbId}:{tableId}:i:{indexId}" → SimpleHash("{dbId}:{tableId}:i:{indexId}")
///   matches writes whose key is "{dbId}:{tableId}:i:{indexId}/{...}" (last slash before the suffix).
///   Note: non-unique keys are "{dbId}:{tableId}:i:{indexId}/{encodedKey}{rowId}" with no extra slash,
///   so the routing invariant holds for both unique and non-unique on a single partition.
///   With multiple partitions (Phase 6) this requires review.
///
/// All write methods take a <see cref="KvTransaction"/> so they can accumulate acquired locks
/// and modified keys for the 2-phase commit.
/// </summary>
public sealed class KvTableStore
{
    private readonly IKahuna kahuna;
    private readonly ILogger logger;
    private readonly string tableId;
    private readonly string tableName;
    private readonly string tableKeyPrefix;        // "{dbId}:{tableId}" — shared prefix for row and index keys

    private readonly string rowBucketPrefix;       // "{dbId}:{tableId}:r"  — bucket prefix for LocateAndScanRange
    private readonly string rowKeyPrefix;          // "{dbId}:{tableId}:r/" — prepended to rowIdHex

    // Index IDs (names) registered as key-range routed by TableOpener.
    // Written once during table open (inside AsyncLazy, single-threaded), then only read.
    private readonly HashSet<string> rangedIndexIds = [];

    // Caches "{dbId}:{tableId}:i:{indexId}" per index so the bucket prefix is interpolated once
    // instead of on every lock/scan. The index-id set is small and bounded by the table's schema.
    private readonly ConcurrentDictionary<string, string> indexBucketPrefixCache = new();

    private const int RowIdHexLength = 24;
    private const int DefaultPageSize = 512;
    private const int MaxKahunaRetries = 32;
    private const int MaxRetryDelayMs   = 50;

    // Hard wall-clock cap for write and lock-acquisition retry loops. Once this many
    // milliseconds have elapsed, the loop throws TransactionMustRetry immediately rather
    // than spinning for the full MaxKahunaRetries budget. This caps deadlock / persistent
    // lock-conflict latency: in a reverse-order deadlock both transactions abort within
    // roughly CamusDBConfig.LockWaitDeadlineMs rather than after the full ~1.4 s retry budget.
    // Keep this shorter than MaxKahunaRetries × MaxRetryDelayMs (≈ 1.4 s) to be useful.

    // Safety-net expiry for an exclusive range (prefix) lock. The lock is released explicitly when
    // the owning transaction commits or rolls back; this expiry only bounds a leak if the client
    // dies mid-transaction. A range scan that needs serializable isolation should comfortably
    // finish inside this window.
    private static int RangeLockExpiresMs => CamusDBConfig.RangeLockExpiresMs;

    // Upper-bound sentinel appended to the encoded last value for non-unique index keys.
    // Non-unique stored key = "{encodedValue}{rowId24}" where rowId24 is exactly 24 lowercase
    // hex chars (code points 0x0030–0x0066). The sentinel U+FFFF is the highest BMP code point
    // and exceeds every character KeyEncoder can emit:
    //   • Integer64 / Float64 / Bool: uppercase hex digits 0x0030–0x0046
    //   • String / Id (AppendStringHex): 4-hex-per-code-unit 0x0030–0x0046, plus the
    //     field terminator pair U+0000 U+0001 (both far below U+FFFF)
    //   • NULL marker: 0x0030 ('0'); Present marker: 0x0031 ('1')
    // If KeyEncoder ever emits a character ≥ U+FFFF (surrogates are illegal in C# strings;
    // a future supplementary-plane encoding would need two code units) this sentinel would
    // under-cover the upper bound, letting phantom inserts escape the range lock.
    private const char IndexKeySentinel = '￿';

    // Exponential back-off: 1 ms, 2 ms, 4 ms, … capped at MaxRetryDelayMs.
    // Guard against int overflow: `1 << attempt` becomes negative for attempt >= 31.
    private static int RetryDelayMs(int attempt) => attempt < 6 ? 1 << attempt : MaxRetryDelayMs;

    public KvTableStore(IKahuna kahuna, string dbId, string tableId, string tableName = "", ILogger<ICamusDB>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        ArgumentException.ThrowIfNullOrEmpty(dbId);
        ArgumentException.ThrowIfNullOrEmpty(tableId);

        this.kahuna = kahuna;
        this.logger = logger ?? NullLogger<ICamusDB>.Instance;
        this.tableId = tableId;
        this.tableName = tableName;
        tableKeyPrefix  = $"{dbId}:{tableId}";
        rowBucketPrefix = $"{dbId}:{tableId}:r";
        rowKeyPrefix    = $"{dbId}:{tableId}:r/";
    }

    /// <summary>
    /// The Kahuna key space for this table's rows (<c>{dbId}:{tableId}:r</c>) — the prefix before the last
    /// <c>'/'</c> of every row key. This is the exact string to pass to
    /// <see cref="IKahuna.RegisterKeyRange"/> when opting the row space into key-range routing.
    /// </summary>
    public string RowKeySpace => rowBucketPrefix;

    /// <summary>
    /// The Kahuna key space for a secondary index (<c>{dbId}:{tableId}:i:{indexId}</c>). Pass to
    /// <see cref="IKahuna.RegisterKeyRange"/> when opting an index into key-range routing. All
    /// column types are order-safe for range routing (String included, via its hex encoding).
    /// </summary>
    public string IndexKeySpace(string indexId) => BuildIndexBucketPrefix(indexId);

    /// <summary>
    /// Marks <paramref name="indexId"/> as key-range routed on this node. Called by
    /// <c>TableOpener</c> after successfully registering the index space. Once marked,
    /// <see cref="AcquireIndexRangeLockAsync"/> uses a Kahuna range lock instead of a prefix lock.
    /// </summary>
    public void MarkIndexAsRanged(string indexId) => rangedIndexIds.Add(indexId);

    // -----------------------------------------------------------------------
    // Range (prefix) locking — opt-in serializable scans
    // -----------------------------------------------------------------------

    /// <summary>
    /// Acquires a <b>shared</b> lock over the table's entire row range for <paramref name="tx"/>,
    /// giving a serializable, phantom-free view: a concurrent transaction cannot insert, update,
    /// or delete any row in this table until <paramref name="tx"/> commits or rolls back (the
    /// write-path fence blocks mutations into the locked range), while other read scans over the
    /// same range coexist. Call this <b>before</b> a cursor scan (<see cref="ScanRows"/>) that must
    /// not observe concurrent mutations across page boundaries — the default scan path only
    /// guarantees a snapshot, not serializability. The lock is tracked on the transaction and
    /// released automatically on commit/rollback.
    ///
    /// In key-range-sharding mode the lock is always acquired (existing behaviour). In hash mode
    /// (single partition per table) the lock fires only for Serializable read-write transactions
    /// — the isolation level that requires phantom protection as part of strict two-phase locking.
    /// Read Committed and Serializable read-only transactions are exempt.
    /// </summary>
    public Task AcquireRowRangeLockAsync(KvTransaction tx, bool exclusive = false, CancellationToken cancellationToken = default)
    {
        if (!CamusDBConfig.KeyRangeShardingEnabled && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;
        return AcquireRangeLockAsync(tx, rowBucketPrefix, null, true, null, true, cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared);
    }

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
    public Task AcquireIndexRangeLockAsync(KvTransaction tx, string indexId, bool exclusive = false, CancellationToken cancellationToken = default)
    {
        if ((!CamusDBConfig.KeyRangeShardingEnabled || !rangedIndexIds.Contains(indexId)) && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;
        return AcquireRangeLockAsync(tx, BuildIndexBucketPrefix(indexId), null, true, null, true, cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared);
    }

    /// <summary>
    /// Like <see cref="AcquireIndexRangeLockAsync"/> but locks only the sub-range
    /// <c>[fromBound, toBound]</c> instead of the whole bucket. This delivers the per-range
    /// concurrency win promised by key-range sharding: two transactions scanning disjoint
    /// portions of the same index don't conflict. The bounds are encoded with
    /// <see cref="KeyEncoder"/> to match the stored key format. For non-unique indexes
    /// <see cref="IndexKeySentinel"/> (U+FFFF) is appended so all rowId suffixes of the
    /// last value are included — see the constant's declaration for the invariant this relies on.
    ///
    /// Pass <paramref name="exclusive"/> = true for UPDATE/DELETE locate scans (same rationale
    /// as <see cref="AcquireIndexRangeLockAsync"/>).
    ///
    /// Fires under the same conditions as <see cref="AcquireIndexRangeLockAsync"/>.
    /// </summary>
    public Task AcquireBoundedIndexRangeLockAsync(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue? fromBound, bool fromInclusive,
        CompositeColumnValue? toBound,   bool toInclusive,
        bool unique,
        bool exclusive = false,
        CancellationToken cancellationToken = default)
    {
        if ((!CamusDBConfig.KeyRangeShardingEnabled || !rangedIndexIds.Contains(indexId)) && !IsSerializableReadWrite(tx))
            return Task.CompletedTask;

        string bucketPrefix = BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";

        string? fromEncoded = fromBound is not null ? KeyEncoder.Encode(fromBound) : null;
        string? toEncoded   = toBound   is not null ? KeyEncoder.Encode(toBound)   : null;

        // Non-unique stored key = {encodedValue}{rowId24}. The IndexKeySentinel (U+FFFF) sorts after
        // every rowId suffix, so it is appended to push a bound past all of a value's row entries.
        //
        // Start bound: for an exclusive lower bound (fromInclusive=false, i.e. ">") the lock must
        // begin strictly after value V's rows. Without the sentinel, {encode(V)} exclusive still
        // covers {encode(V)+rowId} (those sort after {encode(V)}), wrongly locking value V and
        // overlapping a disjoint "<= V" lock. Appending the sentinel makes the bound clear V's rows.
        // For an inclusive lower bound (">=") value V is wanted, so no sentinel.
        string? startKey = fromEncoded is not null
            ? (unique || fromInclusive ? keyPrefix + fromEncoded : keyPrefix + fromEncoded + IndexKeySentinel)
            : null;
        // End bound: the sentinel is appended ONLY when toInclusive=true, to cover all rowId suffixes
        // of the last value. When toInclusive=false ("<") it must be omitted: appending it would
        // extend the exclusive bound from {encode(V)} to {encode(V)+U+FFFF}, which includes
        // {encode(V)} itself and any {encode(V)+rowId} entries, causing a false range-overlap with
        // the next disjoint lock whose fromBound is exactly {encode(V)}.
        string? endKey   = toEncoded is not null
            ? (unique || !toInclusive ? keyPrefix + toEncoded : keyPrefix + toEncoded + IndexKeySentinel)
            : null;

        return AcquireRangeLockAsync(tx, bucketPrefix, startKey, fromInclusive, endKey, toInclusive, cancellationToken,
            exclusive ? RangeLockMode.Exclusive : RangeLockMode.Shared);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSerializableReadWrite(KvTransaction tx)
        => tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite;

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
        if (IsSerializableReadWrite(tx) && tx.IsExpired(CamusDBConfig.MaxSerializableTransactionLifetimeMs))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionLifetimeExceeded,
                $"Serializable transaction {tx.UniqueId} exceeded the maximum lifetime " +
                $"({CamusDBConfig.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");

        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        while (true)
        {
            (KeyValueResponseType type, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
                tx.TransactionId, bucketPrefix,
                startKey, startInclusive, endKey, endInclusive,
                RangeLockExpiresMs, KeyValueDurability.Persistent, mode, cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Locked)
            {
                tx.TrackRangeLock(bucketPrefix, startKey, startInclusive, endKey, endInclusive, KeyValueDurability.Persistent, mode);
                Log.LogRangeLockAcquired(
                    logger, mode.ToString(), bucketPrefix,
                    startKey ?? "-∞", startInclusive, endKey ?? "+∞", endInclusive, tx.UniqueId);
                return;
            }

            // Transient (routing flip / replication-not-ready): bounded-wait, then surface MustRetry.
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);
                await Task.Delay(RetryDelayMs(retries++), cancellationToken).ConfigureAwait(false);
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
                    await Task.Delay(RetryDelayMs(retries++), cancellationToken).ConfigureAwait(false);
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
    /// <see cref="CamusDBConfig.LockEscalationThreshold"/>, a single whole-bucket Shared lock
    /// (<c>[null,null)</c>) is acquired instead. All subsequent reads on the same bucket skip the
    /// per-point RPC — the whole-bucket lock already covers them. Old per-point entries are kept in
    /// tracking and released at commit.</para>
    /// </summary>
    private Task AcquireSharedPointLockAsync(
        KvTransaction tx,
        string bucketPrefix,
        string key,
        CancellationToken cancellationToken)
    {
        // Whole-bucket lock already covers this point — no new RPC needed.
        if (tx.HasWholeBucketLock(bucketPrefix))
            return Task.CompletedTask;

        // Past the threshold: promote to a single whole-bucket Shared lock.
        // Old per-point entries remain tracked and are released at commit/rollback.
        if (tx.CountPointLocksForBucket(bucketPrefix) >= CamusDBConfig.LockEscalationThreshold)
            return AcquireRangeLockAsync(tx, bucketPrefix, null, true, null, true, cancellationToken);

        return AcquireRangeLockAsync(tx, bucketPrefix, key, true, key, true, cancellationToken);
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
    private Task UpgradeToExclusivePointLockAsync(
        KvTransaction tx,
        string bucketPrefix,
        string key,
        CancellationToken cancellationToken)
        => AcquireRangeLockAsync(tx, bucketPrefix, key, true, key, true, cancellationToken, RangeLockMode.Exclusive);

    /// <summary>
    /// Returns <c>true</c> if <paramref name="tx"/> holds a singleton shared point lock
    /// <c>[key, key]</c> for the given <paramref name="bucketPrefix"/>. Used by write paths
    /// to detect the read-then-write pattern and trigger the S→X upgrade.
    /// </summary>
    private static bool HasSharedPointLock(KvTransaction tx, string bucketPrefix, string key)
    {
        foreach (RangeLockBounds bounds in tx.GetAcquiredRangeLocks())
        {
            if (bounds.Prefix == bucketPrefix
                && bounds.StartKey == key && bounds.StartInclusive
                && bounds.EndKey   == key && bounds.EndInclusive)
                return true;
        }
        return false;
    }

    private static async Task<KeyValueResponseType> RetryOnMustRetry(
        Func<Task<KeyValueResponseType>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            type = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return type;
    }

    // -----------------------------------------------------------------------
    // Primary row operations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Point-read a single row. Returns the raw serialized bytes, or <c>null</c> if not found.
    ///
    /// When <paramref name="tx"/> carries a non-Zero <see cref="KvTransaction.ReadTimestamp"/>
    /// (a Serializable read-only transaction) every read is pinned to that snapshot so the
    /// whole transaction observes one consistent cut through the version history.
    /// All other transaction types pass Zero, leaving Kahuna on the read-committed fast path.
    ///
    /// In a Serializable read-write transaction a shared point lock is acquired before the read
    /// and held until commit, preventing a concurrent writer from committing a modification to
    /// the same key while this transaction is still live.
    /// </summary>
    public async Task<byte[]?> GetRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        string key = BuildRowKey(rowId);

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await AcquireSharedPointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(tx.TransactionId, key, -1, tx.ReadTimestamp, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry is null)
            return null;

        return entry.Value;
    }

    /// <summary>
    /// Full table scan. Yields every (rowId, rowBytes) pair in ascending rowId order
    /// (ObjectId hex is time-ordered and fixed-width so natural KV order is correct).
    ///
    /// When <paramref name="tx"/> carries a non-Zero <see cref="KvTransaction.ReadTimestamp"/>
    /// the scan is pinned to that snapshot for its entire duration, including across page
    /// boundaries. All other transaction types pass Zero (read-committed fast path, unchanged).
    /// </summary>
    public async IAsyncEnumerable<(ObjectIdValue rowId, byte[] data)> ScanRows(
        KvTransaction tx,
        long? maxRows = null,
        ObjectIdValue? afterRowId = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxRows is <= 0)
            yield break;

        long emitted = 0;
        int prefixLen = rowKeyPrefix.Length;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            rowBucketPrefix,
            null, true,
            null, true,
            DefaultPageSize,
            tx.ReadTimestamp,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            // Key format: "{dbId}:{tableId}:r/{hex24}" — the hex suffix starts after the prefix.
            ReadOnlySpan<char> hex = key.AsSpan(prefixLen);
            ObjectIdValue rowId = ObjectId.ToValue(hex.ToString());

            // Resume uses ObjectIdValue.CompareTo because ObjectId.ToString writes the
            // same unsigned a/b/c segments in big-endian hex. Keep that equivalence
            // pinned by tests before changing ObjectId formatting or comparison.
            if (afterRowId is not null && rowId.CompareTo(afterRowId.Value) <= 0)
                continue;

            if (maxRows is not null && emitted >= maxRows.Value)
                yield break;

            yield return (rowId, entry.Value);
            emitted++;
        }
    }

    /// <summary>
    /// Inserts a new row. Acquires a pessimistic exclusive lock then writes the key.
    /// Throws <see cref="CamusDBException"/> if the lock or set fails.
    /// </summary>
    public async Task InsertRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken = default)
        => await WriteRow(tx, rowId, data, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Updates an existing row. Same mechanics as insert — the KV store overwrites the value.
    /// </summary>
    public async Task UpdateRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken = default)
        => await WriteRow(tx, rowId, data, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Deletes a row. Acquires a pessimistic exclusive lock then issues the delete.
    /// </summary>
    public async Task DeleteRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        string key = BuildRowKey(rowId);

        tx.ReserveMutations(1);

        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, rowBucketPrefix, key))
            await UpgradeToExclusivePointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryLocked(
            () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, key, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.MustRetry)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Secondary index operations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Point-read a unique index entry. Returns the rowId the encoded key maps to, or
    /// <c>null</c> if no entry exists (the key is absent).
    ///
    /// Passes <see cref="KvTransaction.ReadTimestamp"/> to Kahuna so that a Serializable
    /// read-only transaction observes the same snapshot here as in <see cref="GetRow"/>.
    ///
    /// In a Serializable read-write transaction a shared point lock is acquired on the index
    /// entry before the read and held until commit, preventing a concurrent writer from
    /// inserting or deleting the same index key while this transaction is live.
    /// </summary>
    public async Task<ObjectIdValue?> LookupUnique(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        string kvKey = BuildUniqueIndexKey(indexId, key);

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await AcquireSharedPointLockAsync(tx, BuildIndexBucketPrefix(indexId), kvKey, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(tx.TransactionId, kvKey, -1, tx.ReadTimestamp, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return ObjectId.ToValue(Encoding.UTF8.GetString(entry.Value));
    }

    /// <summary>
    /// Ordered scan over a secondary index within optional inclusive bounds [from, to].
    /// Yields (decodedKey, rowId) pairs in ascending encoded-key order.
    ///
    /// <paramref name="keyTypes"/> must match the column types of the index key in order.
    /// When <paramref name="unique"/> is false the stored key has the rowId hex (24 chars)
    /// appended directly to the encoded key (no separator); the rowId is stripped before decoding.
    ///
    /// Passes <see cref="KvTransaction.ReadTimestamp"/> to Kahuna so the scan is pinned to
    /// the same consistent snapshot as <see cref="GetRow"/> and <see cref="LookupUnique"/>
    /// across all pages. Zero is passed for non-snapshot transactions (read-committed fast path).
    /// </summary>
    public async IAsyncEnumerable<(CompositeColumnValue key, ObjectIdValue rowId)> ScanIndex(
        KvTransaction tx,
        string indexId,
        ColumnType[] keyTypes,
        CompositeColumnValue? from,
        CompositeColumnValue? to,
        bool unique,
        bool fromInclusive = true,
        bool toInclusive = true,
        long? maxRows = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxRows is <= 0)
            yield break;

        long emitted = 0;
        string bucketPrefix = BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";
        int prefixLen = keyPrefix.Length;

        string? fromEncoded = from is not null ? KeyEncoder.Encode(from) : null;
        string? toEncoded   = to   is not null ? KeyEncoder.Encode(to)   : null;

        // Push bounds into the scan. For non-unique indexes the stored key is
        // {encodedKey}{rowIdHex24}, so the end key needs IndexKeySentinel to include all
        // possible rowId suffixes for the last encoded value.
        string? startKey = fromEncoded is not null ? keyPrefix + fromEncoded : null;
        string? endKey   = toEncoded   is not null
            ? (unique ? keyPrefix + toEncoded : keyPrefix + toEncoded + IndexKeySentinel)
            : null;

        await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            bucketPrefix,
            startKey, fromInclusive,
            endKey, toInclusive,
            DefaultPageSize,
            tx.ReadTimestamp,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            if (!kvKey.StartsWith(keyPrefix, StringComparison.Ordinal))
                continue;

            ReadOnlySpan<char> suffix = kvKey.AsSpan(prefixLen);

            string encodedKey;
            ObjectIdValue rowId;

            if (unique)
            {
                encodedKey = suffix.ToString();
                rowId = ObjectId.ToValue(Encoding.UTF8.GetString(entry.Value));
            }
            else
            {
                // Non-unique: suffix = {encodedKey}{rowIdHex24}; rowId is the last 24 chars.
                if (suffix.Length < RowIdHexLength)
                    continue;

                encodedKey = suffix[..^RowIdHexLength].ToString();
                rowId = ObjectId.ToValue(suffix[^RowIdHexLength..].ToString());
            }

            CompositeColumnValue decodedKey = KeyEncoder.Decode(encodedKey, keyTypes);

            // Bounds filter on the DECODED value, compared as a PREFIX (trailing columns of
            // decodedKey are ignored). This is correct for both shapes that carry extra trailing
            // columns beyond the bound:
            //   • non-unique single-column index: stored Encode([value, rowId]); a raw encoded
            //     string compare dropped value==upperBound (Encode([v,rowId]) > Encode([v])),
            //   • composite index with a prefix bound (e.g. year=2023 AND enabled>false): a
            //     length-tiebreaking compare leaked/!dropped later prefix values.
            // This in-range check is load-bearing: when the planner absorbs the predicate into
            // the scan it is not re-applied by the executor.
            if (from is not null)
            {
                int cmp = ComparePrefix(decodedKey, from);
                if (fromInclusive ? cmp < 0 : cmp <= 0)
                    continue;
            }
            if (to is not null)
            {
                int cmp = ComparePrefix(decodedKey, to);
                if (toInclusive ? cmp > 0 : cmp >= 0)
                    continue;
            }

            if (maxRows is not null && emitted >= maxRows.Value)
                yield break;

            yield return (decodedKey, rowId);
            emitted++;
        }
    }

    /// <summary>
    /// Writes a secondary index entry.
    /// For unique indexes enforces uniqueness via <c>SetIfNotExists</c>; throws
    /// <see cref="CamusDBException"/> with <c>DuplicateKey</c> if the entry already exists.
    /// Set <paramref name="backfillMode"/> to <c>true</c> during coordinator-driven backfill:
    /// a <c>NotSet</c> response on a unique index triggers a read-back check — if the
    /// existing entry maps to the same rowId (idempotent re-run after leader change) the
    /// write is silently skipped; if it maps to a different rowId, a genuine duplicate is
    /// detected and <see cref="CamusDBException"/> is thrown.
    /// </summary>
    public async Task PutIndexEntry(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        ObjectIdValue rowId,
        bool unique,
        bool backfillMode = false,
        CancellationToken cancellationToken = default)
    {
        string kvKey = unique
            ? BuildUniqueIndexKey(indexId, key)
            : BuildNonUniqueIndexKey(indexId, key, rowId);

        byte[] value = Encoding.UTF8.GetBytes(rowId.ToString());

        tx.ReserveMutations(1);

        string indexBucketPrefix = BuildIndexBucketPrefix(indexId);
        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, indexBucketPrefix, kvKey))
            await UpgradeToExclusivePointLockAsync(tx, indexBucketPrefix, kvKey, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

        KeyValueFlags flags = unique ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryLocked(
            () => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (unique && type == KeyValueResponseType.NotSet)
        {
            if (backfillMode)
            {
                // Backfill resume: a NotSet response may be an idempotent re-write of a row
                // already processed in a previous partial run, or a genuine duplicate key.
                // Read back the existing entry to distinguish: same rowId → skip; different → throw.
                (_, ReadOnlyKeyValueEntry? existing) = await RetryOnMustRetry(
                    () => kahuna.LocateAndTryGetValue(tx.TransactionId, kvKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken),
                    cancellationToken
                ).ConfigureAwait(false);
                string existingRowId = existing?.Value is not null ? Encoding.UTF8.GetString(existing.Value) : "";
                if (existingRowId != rowId.ToString())
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
                // else: same rowId — idempotent re-write on resume, continue
            }
            else
            {
                throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
            }
        }

        if (type == KeyValueResponseType.MustRetry)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"PutIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Batched write path (mass insert)
    // -----------------------------------------------------------------------

    /// <summary>A single row plus its secondary-index entries, to be written as part of a batch.</summary>
    public sealed class RowWrite
    {
        public required ObjectIdValue RowId { get; init; }
        public required byte[] RowData { get; init; }
        public List<IndexWrite> IndexEntries { get; } = [];
    }

    /// <summary>One secondary-index entry for a row in a batch write.</summary>
    public readonly record struct IndexWrite(string IndexId, CompositeColumnValue Key, bool Unique);

    /// <summary>
    /// Writes many rows (and their index entries) using two Kahuna round-trips for the whole
    /// batch — one <see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> and one
    /// <see cref="IKahuna.LocateAndTrySetManyKeyValue"/> — instead of an acquire+set per key.
    ///
    /// Preserves the per-key semantics of <see cref="WriteRow"/> / <see cref="PutIndexEntry"/>:
    /// unique index entries use <c>SetIfNotExists</c> and a <c>NotSet</c> result raises a
    /// duplicate-key error. All keys in a batch are distinct; a repeated <em>unique</em> key
    /// means a duplicate unique value within the same insert and is rejected up-front.
    /// </summary>
    public async Task WriteRowsBatch(KvTransaction tx, IReadOnlyList<RowWrite> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys = [];
        List<KahunaSetKeyValueRequestItem> setItems = [];
        Dictionary<string, bool> uniqueByKey = new();
        HashSet<string> seenUnique = [];

        void AddWrite(string key, byte[] value, bool unique)
        {
            lockKeys.Add((key, 0, KeyValueDurability.Persistent));
            uniqueByKey[key] = unique;
            setItems.Add(new KahunaSetKeyValueRequestItem
            {
                TransactionId = tx.TransactionId,
                Key = key,
                Value = value,
                CompareValue = null,
                CompareRevision = -1,
                Flags = unique ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set,
                ExpiresMs = 0,
                Durability = KeyValueDurability.Persistent
            });
        }

        foreach (RowWrite row in rows)
        {
            AddWrite(BuildRowKey(row.RowId), row.RowData, unique: false);

            foreach (IndexWrite ix in row.IndexEntries)
            {
                string kvKey = ix.Unique
                    ? BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : BuildNonUniqueIndexKey(ix.IndexId, ix.Key, row.RowId);

                if (ix.Unique && !seenUnique.Add(kvKey))
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(ix.IndexId)}'");

                AddWrite(kvKey, Encoding.UTF8.GetBytes(row.RowId.ToString()), ix.Unique);
            }
        }

        tx.ReserveMutations(lockKeys.Count);

        // Phase 1 — acquire every lock for the batch in one round-trip (retrying only transients).
        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);

        // Phase 2 — set every value for the batch in one round-trip (retrying only transients).
        await SetManyWithRetry(setItems, uniqueByKey, cancellationToken).ConfigureAwait(false);

        // Track modified keys for the 2PC commit. Locks were already tracked inside
        // AcquireManyWithRetry as each key was confirmed Locked.
        foreach ((string key, int _, KeyValueDurability durability) in lockKeys)
            tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireManyWithRetry(
        KvTransaction tx,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken ct)
    {
        List<(string, int, KeyValueDurability)> pending = new(keys);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses =
                await kahuna.LocateAndTryAcquireManyExclusiveLocks(tx.TransactionId, pending, ct).ConfigureAwait(false);

            // First pass: track every successfully locked key before any throw so that
            // the transaction rollback path can release them even if a later key fails.
            // Mirrors the coordinator's two-pass pattern in AcquireLocksPessimistically.
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, _) in responses)
            {
                if (type == KeyValueResponseType.Locked)
                {
                    tx.TrackLock(key, durability);
                    Log.LogPointLockAcquired(logger, key, tx.UniqueId);
                }
            }

            // Second pass: queue transient failures for retry; throw on hard failures.
            List<(string, int, KeyValueDurability)> retry = [];
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, _) in responses)
            {
                if (type == KeyValueResponseType.Locked)
                    continue;

                if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    retry.Add((key, 0, durability));
                    continue;
                }

                if (type == KeyValueResponseType.AlreadyLocked)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, $"Key {key} is locked by another transaction");

                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {type}");
            }

            if (retry.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {retry.Count} key(s); a concurrent transaction holds a lock — retry the operation from BeginAsync");

            await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            pending = retry;
        }
    }

    private async Task SetManyWithRetry(
        List<KahunaSetKeyValueRequestItem> items,
        Dictionary<string, bool> uniqueByKey,
        CancellationToken ct)
    {
        List<KahunaSetKeyValueRequestItem> pending = new(items);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        while (pending.Count > 0)
        {
            List<KahunaSetKeyValueResponseItem> responses =
                await kahuna.LocateAndTrySetManyKeyValue(pending, ct).ConfigureAwait(false);

            // Only rebuilt if a transient response forces a retry. Re-sending an already-Set
            // unique key would falsely report a duplicate (its MVCC entry now exists), so we
            // resend only the keys that came back MustRetry/WaitingForReplication.
            List<KahunaSetKeyValueRequestItem> retry = [];
            Dictionary<string, KahunaSetKeyValueRequestItem>? byKey = null;

            foreach (KahunaSetKeyValueResponseItem resp in responses)
            {
                string key = resp.Key ?? "";

                switch (resp.Type)
                {
                    case KeyValueResponseType.Set:
                        break;

                    case KeyValueResponseType.NotSet:
                        if (uniqueByKey.TryGetValue(key, out bool unique) && unique)
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{IndexNameFromKvKey(key)}'");
                        break; // non-unique NotSet mirrors the per-row path and is acceptable

                    case KeyValueResponseType.MustRetry:
                    case KeyValueResponseType.WaitingForReplication:
                        byKey ??= pending.ToDictionary(i => i.Key!, i => i);
                        if (byKey.TryGetValue(key, out KahunaSetKeyValueRequestItem? item))
                            retry.Add(item);
                        break;

                    default:
                        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch set failed for key {key}: {resp.Type}");
                }
            }

            if (retry.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {retry.Count} key(s); a concurrent transaction holds a lock — retry the operation from BeginAsync");

            await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            pending = retry;
        }
    }

    // -----------------------------------------------------------------------
    // Batched delete path (mass delete / drop index / drop table)
    // -----------------------------------------------------------------------

    /// <summary>A single row plus its secondary-index entries, to be deleted as part of a batch.</summary>
    public sealed class RowDelete
    {
        public required ObjectIdValue RowId { get; init; }
        public List<IndexDelete> IndexEntries { get; } = [];
    }

    /// <summary>One secondary-index entry for a row in a batch delete.</summary>
    public readonly record struct IndexDelete(string IndexId, CompositeColumnValue Key, ObjectIdValue RowId, bool Unique);

    /// <summary>
    /// Deletes many rows (and their index entries) using two Kahuna round-trips for the whole
    /// batch — one <see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> and one
    /// <see cref="IKahuna.LocateAndTryDeleteManyKeyValue"/> — instead of an acquire+delete per key.
    /// </summary>
    public async Task DeleteRowsBatch(KvTransaction tx, IReadOnlyList<RowDelete> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        List<string> keys = [];

        foreach (RowDelete row in rows)
        {
            keys.Add(BuildRowKey(row.RowId));

            foreach (IndexDelete ix in row.IndexEntries)
            {
                keys.Add(ix.Unique
                    ? BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : BuildNonUniqueIndexKey(ix.IndexId, ix.Key, ix.RowId));
            }
        }

        tx.ReserveMutations(keys.Count);

        await DeleteKeysBatch(tx, keys, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes a secondary index entry. No-ops silently if the entry does not exist.
    /// </summary>
    public async Task DeleteIndexEntry(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        ObjectIdValue rowId,
        bool unique,
        CancellationToken cancellationToken = default)
    {
        string kvKey = unique
            ? BuildUniqueIndexKey(indexId, key)
            : BuildNonUniqueIndexKey(indexId, key, rowId);

        tx.ReserveMutations(1);

        string indexBucketPrefixDel = BuildIndexBucketPrefix(indexId);
        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, indexBucketPrefixDel, kvKey))
            await UpgradeToExclusivePointLockAsync(tx, indexBucketPrefixDel, kvKey, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryLocked(
            () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, kvKey, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.MustRetry)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Deletes every KV entry belonging to the named index. Used by DROP INDEX to reclaim
    /// the <c>{dbId}:{tableId}:i:{indexName}/…</c> space. All deletes run under <paramref name="tx"/>
    /// so they are atomic with the schema-removal that follows in the same transaction.
    /// Returns the number of entries deleted.
    /// </summary>
    public async Task<int> DropIndexEntries(KvTransaction tx, string indexName, CancellationToken cancellationToken = default)
    {
        string bucketPrefix = BuildIndexBucketPrefix(indexName);
        string keyPrefix    = bucketPrefix + "/";

        // Collect all raw keys first; deleting during async iteration is unsafe.
        List<string> keysToDelete = [];

        await foreach ((string kvKey, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            bucketPrefix,
            null, true,
            null, true,
            DefaultPageSize,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (kvKey.StartsWith(keyPrefix, StringComparison.Ordinal))
                keysToDelete.Add(kvKey);
        }

        await DeleteKeysBatch(tx, keysToDelete, cancellationToken).ConfigureAwait(false);

        return keysToDelete.Count;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task DeleteKeysBatch(KvTransaction tx, List<string> keys, CancellationToken cancellationToken)
    {
        if (keys.Count == 0)
            return;

        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys =
            keys.Select(k => (k, 0, KeyValueDurability.Persistent)).ToList();

        List<KahunaDeleteKeyValueRequestItem> deleteItems = keys.Select(k => new KahunaDeleteKeyValueRequestItem
        {
            TransactionId = tx.TransactionId,
            Key = k,
            Durability = KeyValueDurability.Persistent
        }).ToList();

        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);
        await DeleteManyWithRetry(deleteItems, cancellationToken).ConfigureAwait(false);

        // Locks were already tracked inside AcquireManyWithRetry.
        foreach (string key in keys)
            tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task DeleteManyWithRetry(List<KahunaDeleteKeyValueRequestItem> items, CancellationToken ct)
    {
        List<KahunaDeleteKeyValueRequestItem> pending = new(items);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        while (pending.Count > 0)
        {
            List<KahunaDeleteKeyValueResponseItem> responses =
                await kahuna.LocateAndTryDeleteManyKeyValue(pending, ct).ConfigureAwait(false);

            List<KahunaDeleteKeyValueRequestItem> retry = [];
            Dictionary<string, KahunaDeleteKeyValueRequestItem>? byKey = null;

            foreach (KahunaDeleteKeyValueResponseItem resp in responses)
            {
                string key = resp.Key ?? "";

                switch (resp.Type)
                {
                    case KeyValueResponseType.Deleted:
                    case KeyValueResponseType.DoesNotExist:
                        break;

                    case KeyValueResponseType.MustRetry:
                    case KeyValueResponseType.WaitingForReplication:
                        byKey ??= pending.ToDictionary(i => i.Key!, i => i);
                        if (byKey.TryGetValue(key, out KahunaDeleteKeyValueRequestItem? item))
                            retry.Add(item);
                        break;

                    default:
                        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch delete failed for key {key}: {resp.Type}");
                }
            }

            if (retry.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Batch delete conflict on {retry.Count} key(s); a concurrent transaction holds a lock — retry the operation from BeginAsync");

            await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            pending = retry;
        }
    }

    private async Task WriteRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken)
    {
        tx.ReserveMutations(1);
        string key = BuildRowKey(rowId);

        // If this Serializable+RW transaction already holds a shared point lock on this key
        // (acquired during a preceding GetRow), promote it to exclusive so no new readers can
        // acquire a shared lock on K between now and commit.
        // AcquireLock is still called after the upgrade: the per-key write intent it sets is what
        // drives the 2PC commit path; the exclusive range lock drives predicate/reader exclusion.
        // Two different Kahuna mechanisms, each load-bearing for a different invariant.
        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, rowBucketPrefix, key))
            await UpgradeToExclusivePointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryLocked(
            () => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, data, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.MustRetry)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireLock(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        (KeyValueResponseType lockType, _, KeyValueDurability lockDurability, _) = await RetryOnMustRetryLocked(
            () => kahuna.LocateAndTryAcquireExclusiveLock(tx.TransactionId, key, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (lockType == KeyValueResponseType.AlreadyLocked)
            throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, $"Key {key} is locked by another transaction");

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {lockType}");

        tx.TrackLock(key, lockDurability);
        Log.LogPointLockAcquired(logger, key, tx.UniqueId);
    }

    /// <summary>
    /// Retries a Kahuna get call that returns <see cref="KeyValueResponseType.MustRetry"/> up to
    /// <see cref="MaxKahunaRetries"/> times with a 1 ms back-off. MustRetry is a transient
    /// condition that occurs when a key has an active write intent from a 2PC prepare phase
    /// that hasn't committed or rolled back yet.
    /// </summary>
    private static async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        ReadOnlyKeyValueEntry? entry;
        int retries = 0;

        do
        {
            (type, entry) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, entry);
    }

    /// <summary>
    /// Retries a Kahuna set/delete call that returns <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        long revision;
        HLCTimestamp ts;
        int retries = 0;

        do
        {
            (type, revision, ts) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, revision, ts);
    }

    /// <summary>
    /// Retries a Kahuna lock-acquire call that returns <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    private static async Task<(KeyValueResponseType, string, KeyValueDurability)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, string, KeyValueDurability)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        string endpoint;
        KeyValueDurability durability;
        int retries = 0;

        do
        {
            (type, endpoint, durability) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, endpoint, durability);
    }

    // -----------------------------------------------------------------------
    // Deadline-aware retry helpers (write and lock-acquisition paths)
    //
    // These mirror the RetryOnMustRetry overloads above but add a wall-clock
    // deadline check on every MustRetry iteration. When the deadline elapses
    // they throw TransactionMustRetry immediately, bounding deadlock and
    // persistent lock-conflict latency to ≤ LockWaitDeadlineMs per operation.
    //
    // Used by: AcquireRangeLockAsync, AcquireLock, WriteRow, DeleteRow,
    //          PutIndexEntry, DeleteIndexEntry, and the three manual batch loops.
    // NOT used by: read paths (GetRow, LookupUnique, ScanRows, ScanIndex)
    //   where MustRetry is a transient 2PC prepare-phase signal, not a lock conflict.
    // -----------------------------------------------------------------------

    private const string LockWaitDeadlineMessage =
        "Lock-wait deadline exceeded; the operation conflicts with a long-held lock or is in a deadlock — retry the transaction from BeginAsync";

    private static long LockWaitDeadlineTicks()
        => Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * (CamusDBConfig.LockWaitDeadlineMs / 1000.0));

    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetryLocked(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        long deadline = LockWaitDeadlineTicks();
        KeyValueResponseType type;
        long revision;
        HLCTimestamp ts;
        int retries = 0;

        do
        {
            (type, revision, ts) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, revision, ts);
    }

    private static async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RetryOnMustRetryLocked(
        Func<Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        long deadline = LockWaitDeadlineTicks();
        KeyValueResponseType type;
        string endpoint;
        KeyValueDurability durability;
        HLCTimestamp holder;
        int retries = 0;

        do
        {
            (type, endpoint, durability, holder) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, LockWaitDeadlineMessage);
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, endpoint, durability, holder);
    }

    /// <summary>
    /// Compares <paramref name="key"/> against <paramref name="bound"/> over the bound's columns
    /// only, ignoring any trailing columns in <paramref name="key"/> (the appended rowId on a
    /// non-unique index, or lower-significance columns when the bound is a composite prefix).
    /// Returns &lt;0 / 0 / &gt;0 like <see cref="IComparable{T}.CompareTo"/>.
    /// </summary>
    private static int ComparePrefix(CompositeColumnValue key, CompositeColumnValue bound)
    {
        int n = Math.Min(key.Values.Length, bound.Values.Length);

        for (int i = 0; i < n; i++)
        {
            int cmp = key.Values[i].CompareTo(bound.Values[i]);
            if (cmp != 0)
                return cmp;
        }

        // Every bound column matched: equal for range purposes (prefix match); trailing
        // columns in `key` do not push it outside an inclusive bound.
        return 0;
    }

    // Composes "{dbId}:{tableId}:r/{rowIdHex24}" directly into the new string's buffer — one
    // allocation, no separate rowId.ToString() temporary. The length is small and bounded
    // (compact db/table ids + 24 hex), so this is allocation-minimal on the per-row hot path.
    private string BuildRowKey(ObjectIdValue rowId)
        => string.Create(rowKeyPrefix.Length + 24, (rowKeyPrefix, rowId), static (span, state) =>
        {
            state.rowKeyPrefix.CopyTo(span);
            ObjectId.WriteHex(span[state.rowKeyPrefix.Length..], state.rowId.a, state.rowId.b, state.rowId.c);
        });

    // Returns "{dbId}:{tableId}:i:{indexId}" — the bucket prefix (no trailing slash) used for
    // LocateAndScanRange so that SimpleHash("{dbId}:{tableId}:i:{indexId}") matches the routing
    // hash of keys "{dbId}:{tableId}:i:{indexId}/{...}". Cached per index id (see field).
    private string BuildIndexBucketPrefix(string indexId)
        => indexBucketPrefixCache.TryGetValue(indexId, out string? cached)
            ? cached
            : indexBucketPrefixCache.GetOrAdd(indexId, static (id, prefix) => $"{prefix}:i:{id}", tableKeyPrefix);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildUniqueIndexKey(string indexId, CompositeColumnValue key)
        => $"{tableKeyPrefix}:i:{indexId}/{KeyEncoder.Encode(key)}";

    // Non-unique: rowIdHex appended directly (no separator) so the last slash in the full
    // key is always the one after {indexId}, keeping the routing hash stable.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildNonUniqueIndexKey(string indexId, CompositeColumnValue key, ObjectIdValue rowId)
        => $"{tableKeyPrefix}:i:{indexId}/{KeyEncoder.Encode(key)}{rowId}";

    // Formats a human-readable "table.index" key name for duplicate-key errors.
    private string DuplicateKeyLabel(string indexId)
        => string.IsNullOrEmpty(tableName) ? indexId : $"{tableName}.{indexId}";

    // Extracts the index name from a full KV key ("{dbId}:{tableId}:i:{indexId}/{...}").
    // Falls back to the raw key on unexpected formats.
    private string IndexNameFromKvKey(string kvKey)
    {
        string prefix = $"{tableKeyPrefix}:i:";
        if (!kvKey.StartsWith(prefix, StringComparison.Ordinal))
            return kvKey;
        string tail = kvKey[prefix.Length..];
        int slash = tail.IndexOf('/');
        string indexId = slash >= 0 ? tail[..slash] : tail;
        return DuplicateKeyLabel(indexId);
    }
}
