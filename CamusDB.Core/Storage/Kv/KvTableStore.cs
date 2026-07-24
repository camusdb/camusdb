
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
using CamusDB.Core.Diagnostics;
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
/// <b>Table id format:</b> newly created tables get a <em>short base-62</em> table id allocated from
/// a per-store persistent monotonic sequence (<c>_system/tableseq</c>) via
/// <see cref="CamusDB.Core.CommandsExecutor.Controllers.DatabaseRegistry.AllocateTableIdAsync"/>.
/// The id is typically 1–4 characters (e.g. <c>"1"</c>, <c>"A0"</c>) and contains none of the key
/// separators (<c>/</c>, <c>:</c>, <c>~</c>). Tables created before this change keep their original
/// 24-character lowercase-hex ObjectId (e.g. <c>"6849f3a1c2e7d50b4f8a91d3"</c>); the two forms
/// coexist safely because their lengths and character sets never overlap.
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

    // Index KvIds registered as key-range routed by TableOpener.
    // Written once during table open (inside AsyncLazy, single-threaded), then only read.
    private readonly HashSet<string> rangedIndexIds = [];

    // Caches "{dbId}:{tableId}:i:{indexId}" per index so the bucket prefix is interpolated once
    // instead of on every lock/scan. The index-id set is small and bounded by the table's schema.
    private readonly ConcurrentDictionary<string, string> indexBucketPrefixCache = new();

    // Maps each index KvId to its human-readable name for user-facing error messages.
    // Populated by TableOpener at open time; the KvId is the stable immutable id used in KV keys.
    private readonly Dictionary<string, string> indexIdToDisplayName = [];

    // Per-index KvId → column sort directions. Only indexes with at least one descending column
    // are present; an absent entry means all-ascending (the encoder's fast path).
    private readonly Dictionary<string, OrderType[]> indexDirections = [];

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
    //   • String / Id: ordered ASCII U+0002–U+007F excluding '/', plus the field
    //     terminator pair U+0000 U+0001 (all far below U+FFFF)
    //   • NULL marker: 0x0030 ('0'); Present marker: 0x0031 ('1')
    // If KeyEncoder ever emits a character ≥ U+FFFF (surrogates are illegal in C# strings;
    // a future supplementary-plane encoding would need two code units) this sentinel would
    // under-cover the upper bound, letting phantom inserts escape the range lock.
    private const char IndexKeySentinel = '￿';

    // Exponential back-off: 1 ms, 2 ms, 4 ms, … capped at MaxRetryDelayMs.
    // Guard against int overflow: `1 << attempt` becomes negative for attempt >= 31.
    private static int RetryDelayMs(int attempt) => attempt < 6 ? 1 << attempt : MaxRetryDelayMs;

    // Branch lineage stores, in nearest-parent-first order.  Each entry holds a KvTableStore
    // constructed for that ancestor's dbId (so its row/index key prefixes address the right
    // keyspace) together with the HLC timestamp at which this database was forked from that
    // ancestor.  Empty for root databases — the hot read path has no overhead in that case.
    private readonly (KvTableStore store, HLCTimestamp forkTimestamp)[] ancestorStores;

    /// <summary>
    /// Creates a table store for the given <paramref name="dbId"/> and <paramref name="tableId"/>.
    /// Pass <paramref name="ancestorStores"/> (nearest parent first) when the database is a branch;
    /// read methods will walk the lineage on a miss so inherited rows and index entries are visible
    /// without having been physically copied into the branch namespace.
    /// </summary>
    public KvTableStore(
        IKahuna kahuna,
        string dbId,
        string tableId,
        string tableName = "",
        ILogger<ICamusDB>? logger = null,
        (KvTableStore store, HLCTimestamp forkTimestamp)[]? ancestorStores = null)
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
        this.ancestorStores = ancestorStores ?? [];

        if (this.ancestorStores.Length >= BranchMetrics.LineageWarningThreshold)
        {
            BranchMetrics.RecordDeepLineageWarning();
            this.logger.LogWarning(
                "Table '{Table}' opened on a branch with lineage depth {Depth}. " +
                "Point reads probe every ancestor level on a miss and scans open one iterator per level. " +
                "Consider compacting the branch chain to reduce read amplification.",
                tableName, this.ancestorStores.Length);
        }
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
    /// column types are order-safe for range routing (String included, via its ordered ASCII encoding).
    /// </summary>
    public string IndexKeySpace(string indexId) => BuildIndexBucketPrefix(indexId);

    /// <summary>
    /// Returns the full KV key for the given row: <c>{dbId}:{tableId}:r/{rowIdHex24}</c>.
    /// Used by the dependency collector to record per-row point dependencies without exposing
    /// the internal key-prefix fields.
    /// </summary>
    public string RowPointKey(ObjectIdValue rowId) => BuildRowKey(rowId);

    /// <summary>
    /// Exposes the underlying <see cref="IKahuna"/> instance for use by the strict-validation
    /// path, which needs direct key probes with <c>LastModified</c> timestamps. Not for general
    /// DML/DDL use — those must go through <see cref="KvTransaction"/> to track lock and key sets.
    /// </summary>
    internal IKahuna Kahuna => kahuna;

    /// <summary>
    /// Number of ancestor levels this store is configured to walk on a read miss.
    /// Zero for root databases (no ancestry). This is the per-read amplification factor:
    /// a point read may probe up to <c>LineageDepth</c> extra levels, and a range scan
    /// opens <c>LineageDepth</c> extra iterators. Exposed for observability and tests;
    /// see <see cref="BranchMetrics"/> for process-wide counters.
    /// </summary>
    public int LineageDepth => ancestorStores.Length;

    /// <summary>
    /// Marks <paramref name="indexId"/> as key-range routed on this node. Called by
    /// <c>TableOpener</c> after successfully registering the index space. Once marked,
    /// <see cref="AcquireIndexRangeLockAsync"/> uses a Kahuna range lock instead of a prefix lock.
    /// </summary>
    public void MarkIndexAsRanged(string indexId) => rangedIndexIds.Add(indexId);

    /// <summary>
    /// Registers the human-readable display name for an index KvId so that duplicate-key
    /// errors show the mutable index name (e.g., <c>robots.name_idx</c>) rather than the
    /// immutable KvId stored in KV keys. Called by <c>TableOpener</c> for every index entry
    /// when loading a table, before any DML can reference the index.
    /// </summary>
    public void RegisterIndexName(string indexId, string displayName) => indexIdToDisplayName[indexId] = displayName;

    /// <summary>
    /// Registers the per-column sort directions for an index KvId so that key encoding and index
    /// scans invert the ordinal order of descending columns. A null or all-ascending vector is not
    /// stored — <see cref="DirectionsOf"/> then returns null and the encoder takes its ascending
    /// fast path, keeping every existing index byte-identical. Called by <c>TableOpener</c> for each
    /// loaded index and by the index-add path, before any DML can reference the index.
    /// </summary>
    public void RegisterIndexDirections(string indexId, OrderType[]? directions)
    {
        bool anyDescending = false;
        if (directions is not null)
            foreach (OrderType direction in directions)
                if (direction == OrderType.Descending) { anyDescending = true; break; }

        if (anyDescending)
            indexDirections[indexId] = directions!;
        else
            indexDirections.Remove(indexId);

        // Branch lineage: an ancestor namespace stores this index with the same descending encoding,
        // so a lineage lookup (BuildUniqueIndexKey on an ancestor store) must encode identically.
        foreach ((KvTableStore ancestorStore, HLCTimestamp _) in ancestorStores)
            ancestorStore.RegisterIndexDirections(indexId, directions);
    }

    /// <summary>Per-index sort directions, or null when the index is entirely ascending.</summary>
    private OrderType[]? DirectionsOf(string indexId)
        => indexDirections.TryGetValue(indexId, out OrderType[]? directions) ? directions : null;

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

        OrderType[]? directions = DirectionsOf(indexId);
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
        if (IsSerializableReadWrite(tx) && tx.IsExpired(CamusDBConfig.MaxSerializableTransactionLifetimeMs))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionLifetimeExceeded,
                $"Serializable transaction {tx.UniqueId} exceeded the maximum lifetime " +
                $"({CamusDBConfig.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");

        long deadline = LockWaitDeadlineTicks();
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
                tx.TrackRangeLock(bucketPrefix, startKey, startInclusive, endKey, endInclusive, KeyValueDurability.Persistent, mode);
                if (CamusDBConfig.LockTracingEnabled && logger.IsEnabled(LogLevel.Debug))
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
    /// <see cref="CamusDBConfig.LockEscalationThreshold"/>, a single whole-bucket Shared lock
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
    private Task AcquireSharedPointLockAsync(
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
    /// Returns <c>true</c> if <paramref name="tx"/> holds a singleton <b>Shared</b> point lock
    /// <c>[key, key]</c> for the given <paramref name="bucketPrefix"/>. Used by write paths
    /// to detect the read-then-write pattern and trigger the S→X upgrade.
    ///
    /// <para>The mode is part of the test on purpose: a key this transaction has already promoted
    /// to Exclusive (an earlier write in the same transaction) needs no second upgrade, and asking
    /// for one only spends a round trip re-asserting a lock it holds.</para>
    /// </summary>
    private static bool HasSharedPointLock(KvTransaction tx, string bucketPrefix, string key)
        => tx.HasPointLock(bucketPrefix, key, RangeLockMode.Shared);

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
    public async Task<ReadOnlyMemory<byte>?> GetRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        // Serializable+RW acquires a shared point lock: the session must be open first.
        // For all other transaction types this is a no-op (zero-snapshot reads proceed without a session).
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        string key = BuildRowKey(rowId);

        // Gated on isolation + mode only, NOT on tx.Locking: a Serializable+RW read takes this shared
        // predicate lock even when tx.Locking is Optimistic (that is the Serializable+Optimistic hybrid —
        // optimistic writes, but predicate locks still keep the read phantom-free).
        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await AcquireSharedPointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        BranchKvValue probe = await ProbeRaw(tx.TransactionId, tx.ReadTimestamp, key, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (probe.Kind == BranchKvKind.Tombstone) return null;   // explicitly deleted at this level
        if (probe.HasPayload) return probe.Payload;              // found at this level

        // Miss at level-0: walk ancestry levels until a hit or a tombstone (stop walking).
        foreach ((KvTableStore ancestorStore, HLCTimestamp forkTimestamp) in ancestorStores)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKey = ancestorStore.BuildRowKey(rowId);
            probe = await ancestorStore.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKey, cancellationToken).ConfigureAwait(false);
            if (probe.Kind == BranchKvKind.Tombstone) return null;
            if (probe.HasPayload) return probe.Payload;
        }

        return null;
    }

    /// <summary>
    /// Batch point-read for a list of row ids. Returns one <c>byte[]?</c> per input id in the
    /// same order: non-null = the raw serialized row bytes; null = row not found (index entry
    /// points at an absent or deleted row — callers must warn-and-skip).
    ///
    /// The batch is issued as a single <c>LocateAndTryGetManyValues</c> call so all N ids are
    /// resolved in one Kahuna round-trip instead of N sequential <see cref="GetRow"/> calls.
    /// The transaction's <see cref="KvTransaction.ReadTimestamp"/> is forwarded so every fetch
    /// reads at the same snapshot as the index scan that produced the ids (required for
    /// Serializable and promoted read-only transactions — mirrors the single-key path in
    /// <see cref="GetRow"/>).
    ///
    /// Branch ancestry: when the store has ancestor levels (database branching), ids that are
    /// absent or tombstoned at level-0 are walked through the ancestry chain individually, the
    /// same way <see cref="GetRow"/> does. This keeps correctness identical to the per-row path
    /// at the cost of one extra call per missing id — a rare case on branch reads.
    ///
    /// This is a read-only operation: no locks are acquired, and no keys are tracked as
    /// modified. Serializable read-write callers that need shared point locks must use
    /// <see cref="GetRow"/> per entry.
    /// </summary>
    public async Task<ReadOnlyMemory<byte>?[]> GetRowsBatch(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        CancellationToken cancellationToken = default)
    {
        if (rowIds.Count == 0)
            return [];

        // Build the Kahuna key list in input order.
        List<(string key, long revision, KeyValueDurability durability)> keys = new(rowIds.Count);
        for (int i = 0; i < rowIds.Count; i++)
            keys.Add((BuildRowKey(rowIds[i]), -1, KeyValueDurability.Persistent));

        // LocateAndTryGetManyValues fans keys out across leader nodes and returns results in
        // leader-group order, not input order. Build a key→result map and look up by key, not
        // by position, to avoid output[i] receiving another row's bytes in cluster mode.
        //
        // MustRetry/WaitingForReplication mean a partition is not yet ready; mirror ProbeRaw's
        // retry loop by retrying only the affected subset with exponential back-off, up to
        // MaxKahunaRetries. A persistent non-terminal response throws TransactionMustRetry so
        // the caller can restart the whole operation from BeginAsync — the same contract as the
        // single-key path.
        Dictionary<string, (KeyValueResponseType responseType, ReadOnlyKeyValueEntry? entry)> byKey =
            new(keys.Count, StringComparer.Ordinal);

        List<(string key, long revision, KeyValueDurability durability)> pending = keys;
        int retries = 0;

        // Register the batch read for read-set folding on the optimistic / TrackAndValidate path; empty
        // coordinatorKey leaves it unregistered (pessimistic / snapshot). One operation id is bound to the
        // pending read declaration: it is REUSED while the pending set is unchanged (an ack-loss resend of
        // the identical batch, which the coordinator can only replay idempotently under the same id) and a
        // fresh id is minted only when the set shrinks (confirmed reads removed). Unregistered reads carry
        // the default id (no folding), so the identity is immaterial there.
        string readCoordinatorKey = tx.FoldReads ? tx.CoordinatorKey : "";
        TransactionOperationId readOperationId = readCoordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType responseType, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
                await kahuna.LocateAndTryGetManyValues(tx.TransactionId, tx.ReadTimestamp, pending, cancellationToken, readCoordinatorKey, readOperationId)
                            .ConfigureAwait(false);

            List<(string key, long revision, KeyValueDurability durability)>? nextPending = null;

            foreach ((KeyValueResponseType responseType, string key, _, ReadOnlyKeyValueEntry? entry) in results)
            {
                if (responseType == KeyValueResponseType.Aborted)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                        $"Batch read of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

                if (responseType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    nextPending ??= [];
                    nextPending.Add((key, -1, KeyValueDurability.Persistent));
                    continue;
                }

                byKey[key] = (responseType, entry);
            }

            if (nextPending is null)
                break;

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                    $"Batch read was not ready after {MaxKahunaRetries} retries — retry the operation from BeginAsync");

            await Task.Delay(RetryDelayMs(retries), cancellationToken).ConfigureAwait(false);

            // Shrinking set (some reads confirmed) → new, smaller declaration under a fresh id on the
            // registered path. Unchanged set (every key transient) → identical resend of a lost ack →
            // keep the same id so the coordinator replays the read observation idempotently.
            if (readCoordinatorKey.Length != 0 && nextPending.Count != pending.Count)
                readOperationId = TransactionOperationId.NewRandom();
            pending = nextPending;
        }

        ReadOnlyMemory<byte>?[] output = new ReadOnlyMemory<byte>?[rowIds.Count];

        for (int i = 0; i < rowIds.Count; i++)
        {
            string rowKey = keys[i].key;

            BranchKvValue decoded = BranchKvValue.Miss;

            if (byKey.TryGetValue(rowKey, out (KeyValueResponseType responseType, ReadOnlyKeyValueEntry? entry) res)
                && res.responseType == KeyValueResponseType.Get
                && res.entry is not null)
            {
                decoded = BranchKvCodec.Decode(res.entry.Value);
            }

            if (decoded.Kind == BranchKvKind.Tombstone)
            {
                output[i] = null;
                continue;
            }

            if (decoded.HasPayload)
            {
                output[i] = decoded.Payload;
                continue;
            }

            // Miss at level-0: walk ancestry levels individually (uncommon on branch databases).
            if (ancestorStores.Length > 0)
            {
                foreach ((KvTableStore ancestorStore, HLCTimestamp forkTimestamp) in ancestorStores)
                {
                    BranchMetrics.RecordAncestorProbe();
                    string ancestorKey = ancestorStore.BuildRowKey(rowIds[i]);
                    decoded = await ancestorStore.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKey, cancellationToken).ConfigureAwait(false);
                    if (decoded.Kind == BranchKvKind.Tombstone) break;
                    if (decoded.HasPayload) break;
                }
            }

            // Cast the value branch to the nullable type explicitly. Without it, the bare `null`
            // literal binds to byte[] via ReadOnlyMemory's implicit array conversion, making the whole
            // conditional a non-nullable (empty) ReadOnlyMemory<byte> — so a miss would surface as an
            // empty present value instead of null.
            output[i] = decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null;
        }

        return output;
    }

    /// <summary>
    /// Batch point-read for a mutation (UPDATE/DELETE) write phase: identical to
    /// <see cref="GetRowsBatch"/> but, for a Serializable read-write transaction, first acquires the
    /// same shared point locks that a per-row <see cref="GetRow"/> would take on every row key, held
    /// until commit.
    ///
    /// <para>
    /// This is the lock-acquiring batch read the plain <see cref="GetRowsBatch"/> deliberately is not.
    /// A write phase that reads a row lock-free and only exclusively locks it later cannot detect a
    /// concurrent transaction that modified-and-committed that row in the gap: a read pinned to
    /// <see cref="KvTransaction.ReadTimestamp"/> records no read dependency and pins no server-side MVCC
    /// base, and the later write pins its base only after the other commit — so the stale read is used
    /// to compute which index entries to remove, orphaning the concurrently-written ones. Holding the
    /// shared point lock continuously from read to commit is the mechanism that forces the competing
    /// writer to <c>MustRetry</c> instead. The exclusive row-range lock a full-table locate scan already
    /// holds covers this, but a predicate-driven <em>index</em> locate scan only range-locks the index
    /// sub-range, leaving the row keys uncovered — which is exactly the set this method locks.
    /// </para>
    ///
    /// <para>
    /// The acquired locks are byte-for-byte the set a sequence of per-row <see cref="GetRow"/> calls
    /// would take (including whole-bucket escalation past
    /// <see cref="CamusDBConfig.LockEscalationThreshold"/>); only the reads are batched into one round
    /// trip. The subsequent exclusive batch write (<see cref="DeleteRowsBatch"/> /
    /// <see cref="UpdateRowsBatch"/>) then upgrades/covers these same keys, exactly as the per-row
    /// <see cref="GetRow"/> path already did.
    /// </para>
    /// </summary>
    public async Task<ReadOnlyMemory<byte>?[]> GetRowsBatchLockedForMutation(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        CancellationToken cancellationToken = default)
    {
        if (rowIds.Count == 0)
            return [];

        // Serializable+RW acquires the shared point locks first (session must be open before locking);
        // other transaction types skip locking, exactly as GetRow does.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (IsSerializableReadWrite(tx))
        {
            for (int i = 0; i < rowIds.Count; i++)
                await AcquireSharedPointLockAsync(tx, rowBucketPrefix, BuildRowKey(rowIds[i]), cancellationToken).ConfigureAwait(false);
        }

        return await GetRowsBatch(tx, rowIds, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Full table scan. Yields every (rowId, rowBytes) pair in ascending rowId order
    /// (ObjectId hex is time-ordered and fixed-width so natural KV order is correct).
    ///
    /// When <paramref name="tx"/> carries a non-Zero <see cref="KvTransaction.ReadTimestamp"/>
    /// the scan is pinned to that snapshot for its entire duration, including across page
    /// boundaries. All other transaction types pass Zero (read-committed fast path, unchanged).
    /// </summary>
    public async IAsyncEnumerable<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> ScanRows(
        KvTransaction tx,
        long? maxRows = null,
        ObjectIdValue? afterRowId = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxRows is <= 0)
            yield break;

        // Open the deferred Kahuna session before reading tx.FoldReads / tx.TransactionId below, so an
        // optimistic transaction that scans before its first write folds these reads (FoldReads is
        // false while TransactionId is Zero). No-op for eager, read-only, and already-started
        // transactions.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (ancestorStores.Length == 0)
        {
            // Root database (no ancestry): stream directly without materialization.
            long emitted = 0;
            int prefixLen = rowKeyPrefix.Length;

            // Register the scan for read-set folding on the optimistic / TrackAndValidate path so every
            // scanned row becomes a commit-time read observation; empty coordinatorKey leaves it
            // unregistered (pessimistic / snapshot), byte-for-byte the prior behavior.
            string scanCoordinatorKey = tx.FoldReads ? tx.CoordinatorKey : "";
            TransactionOperationId scanOperationId = scanCoordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                rowBucketPrefix,
                null, true,
                null, true,
                DefaultPageSize,
                tx.ReadTimestamp,
                KeyValueDurability.Persistent,
                cancellationToken,
                scanCoordinatorKey,
                scanOperationId).ConfigureAwait(false))
            {
                if (entry.Value is null)
                    continue;

                BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
                if (decoded.Kind == BranchKvKind.Tombstone || !decoded.HasPayload)
                    continue;

                // Key format: "{dbId}:{tableId}:r/{hex24}" — the hex suffix starts after the prefix.
                ReadOnlySpan<char> hex = key.AsSpan(prefixLen);
                ObjectIdValue rowId = ObjectId.ToValue(hex);

                // Resume uses ObjectIdValue.CompareTo because ObjectId.ToString writes the
                // same unsigned a/b/c segments in big-endian hex. Keep that equivalence
                // pinned by tests before changing ObjectId formatting or comparison.
                if (afterRowId is not null && rowId.CompareTo(afterRowId.Value) <= 0)
                    continue;

                if (maxRows is not null && emitted >= maxRows.Value)
                    yield break;

                yield return (rowId, decoded.Payload);
                emitted++;
            }
        }
        else
        {
            // Branch database: streaming k-way merge across all lineage levels.
            // Each level's raw iterator yields rows in ascending rowIdHex (ordinal) order — the
            // same order Kahuna's BTree scan produces.  A priority queue merges them without
            // materializing any level, so LIMIT 1 against a large parent reads exactly 1 row.
            // "Nearest-wins" semantics: for the same rowIdHex the entry from the lowest level
            // index (nearest) is processed first; the seen set skips all deeper-level entries
            // for the same key.

            int levelCount = 1 + ancestorStores.Length;
            var iters = new IAsyncEnumerator<(string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload)>[levelCount];

            iters[0] = ScanRowsRawAsync(tx.TransactionId, tx.ReadTimestamp, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").GetAsyncEnumerator(cancellationToken);
            for (int ai = 0; ai < ancestorStores.Length; ai++)
            {
                (KvTableStore ancestorStore, HLCTimestamp forkTimestamp) = ancestorStores[ai];
                iters[ai + 1] = ancestorStore.ScanRowsRawAsync(HLCTimestamp.Zero, forkTimestamp, cancellationToken).GetAsyncEnumerator(cancellationToken);
            }

            if (ancestorStores.Length > 0)
                BranchMetrics.RecordScanIterators(ancestorStores.Length);

            // Priority key: (rowIdHex ordinal-ascending, levelIndex ascending) so ties go to the nearest level.
            PriorityQueue<(int level, string hex, BranchKvKind kind, ReadOnlyMemory<byte>? payload),
                          (string hex, int level)> heap = new(
                Comparer<(string hex, int level)>.Create(static (a, b) =>
                {
                    int c = string.CompareOrdinal(a.hex, b.hex);
                    return c != 0 ? c : a.level.CompareTo(b.level);
                }));

            // afterRowId as ordinal-comparable hex; ordinal hex comparison matches ObjectId ordering
            // because ObjectId.ToString() writes all three unsigned segments as fixed-width big-endian hex.
            string? afterHex = afterRowId.HasValue ? afterRowId.Value.ToString() : null;

            // The heap priority is (hex, levelIndex) — for the same hex, level 0 (branch) dequeues
            // before level 1 (nearest ancestor) and so on. This guarantees all entries for a given
            // hex are dequeued consecutively, so a single lastHex string is enough to suppress
            // duplicate logical keys, replacing an O(distinct-rows) HashSet with O(1) memory.
            string? lastHex = null;
            long emitted = 0;

            try
            {
                for (int i = 0; i < levelCount; i++)
                {
                    if (await iters[i].MoveNextAsync().ConfigureAwait(false))
                    {
                        (string hex, BranchKvKind kind, ReadOnlyMemory<byte>? payload) = iters[i].Current;
                        heap.Enqueue((i, hex, kind, payload), (hex, i));
                    }
                }

                while (heap.Count > 0)
                {
                    (int levelIdx, string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload) = heap.Dequeue();

                    if (rowIdHex != lastHex)
                    {
                        lastHex = rowIdHex;

                        // Nearest level wins for this rowIdHex.
                        if (kind == BranchKvKind.Value && payload is not null &&
                            (afterHex is null || string.CompareOrdinal(rowIdHex, afterHex) > 0))
                        {
                            yield return (ObjectId.ToValue(rowIdHex), payload.Value);
                            emitted++;

                            if (maxRows is not null && emitted >= maxRows.Value)
                                yield break;
                        }
                        // Tombstone: lastHex is updated; deeper-level entries for this hex will be skipped.
                    }

                    if (await iters[levelIdx].MoveNextAsync().ConfigureAwait(false))
                    {
                        (string nextHex, BranchKvKind nextKind, ReadOnlyMemory<byte>? nextPayload) = iters[levelIdx].Current;
                        heap.Enqueue((levelIdx, nextHex, nextKind, nextPayload), (nextHex, levelIdx));
                    }
                }
            }
            finally
            {
                for (int i = 0; i < levelCount; i++)
                    await iters[i].DisposeAsync().ConfigureAwait(false);
            }
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
    /// Deletes a row.
    ///
    /// For root databases (no ancestry), issues a physical KV delete — the key is gone.
    /// For branch databases, writes a <see cref="BranchKvKind.Tombstone"/> to the level-0
    /// overlay instead of deleting the key.  A physical delete would leave no level-0 entry,
    /// which would let the ancestry merge surface the row again from an ancestor namespace.
    /// The tombstone is the "deleted in this branch" signal that the merge respects.
    /// </summary>
    public async Task DeleteRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        string key = BuildRowKey(rowId);

        tx.ReserveMutations(1);

        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, rowBucketPrefix, key))
            await UpgradeToExclusivePointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        if (ancestorStores.Length > 0)
        {
            // Branch: write a tombstone so the ancestry merge suppresses the inherited row.
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.MustRetry)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type != KeyValueResponseType.Set)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteRow failed for key {key}: {type}");
        }
        else
        {
            // Root: physical delete — no ancestor namespace to suppress.
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                (coordinatorKey, operationId) => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, key, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.MustRetry)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteRow failed for key {key}: {type}");
        }

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
        // Serializable+RW acquires a shared point lock on the index entry before reading.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        // A lookup key that cannot be encoded (e.g. an Id equality against a non-ObjectId literal)
        // can match no stored row — return a miss rather than throwing on the invalid value.
        if (!TryBuildUniqueIndexKey(indexId, key, out string kvKey))
            return null;

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await AcquireSharedPointLockAsync(tx, BuildIndexBucketPrefix(indexId), kvKey, cancellationToken).ConfigureAwait(false);

        BranchKvValue idx = await ProbeRaw(tx.TransactionId, tx.ReadTimestamp, kvKey, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (idx.Kind == BranchKvKind.Tombstone) return null;   // tombstone at this level
        if (idx.HasPayload) return RowIdFromPayload(idx.Payload.Span);

        // Miss at level-0: walk ancestry.
        foreach ((KvTableStore ancestorStore, HLCTimestamp forkTimestamp) in ancestorStores)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKvKey = ancestorStore.BuildUniqueIndexKey(indexId, key);
            idx = await ancestorStore.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);
            if (idx.Kind == BranchKvKind.Tombstone) return null;
            if (idx.HasPayload) return RowIdFromPayload(idx.Payload.Span);
        }

        return null;
    }

    /// <summary>
    /// Point-read a unique index entry for a covering (index-only) lookup, returning both the rowId
    /// and the stored/payload (INCLUDE) tuple bytes from the entry value. Mirrors
    /// <see cref="LookupUnique"/> exactly for locking, snapshot, and ancestry semantics; the only
    /// difference is that it also surfaces the include tuple so the covering read can synthesize the
    /// payload columns without a primary-row fetch. Returns <c>null</c> when the key is absent.
    /// </summary>
    public async Task<(ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)?> LookupUniqueCovering(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (!TryBuildUniqueIndexKey(indexId, key, out string kvKey))
            return null;

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await AcquireSharedPointLockAsync(tx, BuildIndexBucketPrefix(indexId), kvKey, cancellationToken).ConfigureAwait(false);

        BranchKvValue idx = await ProbeRaw(tx.TransactionId, tx.ReadTimestamp, kvKey, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (idx.Kind == BranchKvKind.Tombstone) return null;
        if (idx.HasPayload) return (RowIdFromPayload(idx.Payload.Span), IncludeTupleFromPayload(idx.Payload));

        foreach ((KvTableStore ancestorStore, HLCTimestamp forkTimestamp) in ancestorStores)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKvKey = ancestorStore.BuildUniqueIndexKey(indexId, key);
            idx = await ancestorStore.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);
            if (idx.Kind == BranchKvKind.Tombstone) return null;
            if (idx.HasPayload) return (RowIdFromPayload(idx.Payload.Span), IncludeTupleFromPayload(idx.Payload));
        }

        return null;
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
    /// <summary>
    /// Extracts the stored/payload (INCLUDE) tuple from a secondary-index entry value payload. The
    /// payload is <c>rowId (24 bytes) ‖ includeTuple</c>; anything past the fixed 24-byte rowId is the
    /// tuple. Returns an empty span for a plain (non-covering) index whose payload is exactly the rowId.
    /// The returned memory is a zero-copy slice of the Kahuna-owned value buffer, valid for the life of
    /// the scan iteration.
    /// </summary>
    private static ReadOnlyMemory<byte> IncludeTupleFromPayload(ReadOnlyMemory<byte> payload)
        => payload.Length > BranchKvCodec.IndexRowIdPayloadLength
            ? payload[BranchKvCodec.IndexRowIdPayloadLength..]
            : ReadOnlyMemory<byte>.Empty;

    /// <summary>
    /// Reads the rowId from a unique-index entry value payload. The payload is
    /// <c>rowId (24 bytes) ‖ includeTuple</c>; the rowId is the fixed 24-byte prefix, so a covering
    /// index whose payload also carries a tuple must slice the prefix rather than parse the whole
    /// payload (which would fail the exact-24-byte requirement of <see cref="ObjectId.ToValue(ReadOnlySpan{byte})"/>).
    /// </summary>
    private static ObjectIdValue RowIdFromPayload(ReadOnlySpan<byte> payload)
        => ObjectId.ToValue(payload.Length > BranchKvCodec.IndexRowIdPayloadLength
            ? payload[..BranchKvCodec.IndexRowIdPayloadLength]
            : payload);

    public async IAsyncEnumerable<(CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)> ScanIndex(
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

        // Open the deferred Kahuna session before the fold decision below, so an optimistic
        // transaction that scans an index before its first write folds these reads. No-op for eager,
        // read-only, and already-started transactions.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        long emitted = 0;
        string bucketPrefix = BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";
        int prefixLen = keyPrefix.Length;

        OrderType[]? directions = DirectionsOf(indexId);

        string? fromEncoded = from is not null ? KeyEncoder.Encode(from, directions) : null;
        string? toEncoded   = to   is not null ? KeyEncoder.Encode(to, directions)   : null;

        // Push bounds into the scan. For non-unique indexes the stored key is
        // {encodedKey}{rowIdHex24}, so the end key needs IndexKeySentinel to include all
        // possible rowId suffixes for the last encoded value.
        string? startKey = fromEncoded is not null ? keyPrefix + fromEncoded : null;
        string? endKey   = toEncoded   is not null
            ? (unique ? keyPrefix + toEncoded : keyPrefix + toEncoded + IndexKeySentinel)
            : null;

        if (ancestorStores.Length == 0)
        {
            // Root database: stream directly without materialization. Register for read-set folding
            // on the optimistic / TrackAndValidate path; empty coordinatorKey = unregistered.
            string scanCoordinatorKey = tx.FoldReads ? tx.CoordinatorKey : "";
            TransactionOperationId scanOperationId = scanCoordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

            await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                bucketPrefix,
                startKey, fromInclusive,
                endKey, toInclusive,
                DefaultPageSize,
                tx.ReadTimestamp,
                KeyValueDurability.Persistent,
                cancellationToken,
                scanCoordinatorKey,
                scanOperationId).ConfigureAwait(false))
            {
                if (entry.Value is null)
                    continue;

                BranchKvValue scanDecoded = BranchKvCodec.Decode(entry.Value);
                if (scanDecoded.Kind == BranchKvKind.Tombstone)
                    continue;

                if (!kvKey.StartsWith(keyPrefix, StringComparison.Ordinal))
                    continue;

                ReadOnlySpan<char> suffix = kvKey.AsSpan(prefixLen);

                string encodedKey;
                ObjectIdValue rowId;

                if (unique)
                {
                    // Unique index value = the row-id as UTF-8 bytes (wrapped in the envelope).
                    // Skip entries whose payload is empty — they indicate a corrupt or partially
                    // written entry and should not surface to callers.
                    if (!scanDecoded.HasPayload)
                        continue;

                    encodedKey = suffix.ToString();
                    rowId = RowIdFromPayload(scanDecoded.Payload.Span);
                }
                else
                {
                    // Non-unique: suffix = {encodedKey}{rowIdHex24}; rowId is the last 24 chars.
                    if (suffix.Length < RowIdHexLength)
                        continue;

                    encodedKey = suffix[..^RowIdHexLength].ToString();
                    rowId = ObjectId.ToValue(suffix[^RowIdHexLength..]);
                }

                CompositeColumnValue decodedKey = KeyEncoder.Decode(encodedKey, keyTypes, directions);

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

                yield return (decodedKey, rowId, IncludeTupleFromPayload(scanDecoded.Payload));
                emitted++;
            }
        }
        else
        {
            // Branch database: streaming k-way merge across all lineage levels.
            // Each level's raw iterator yields index entries in ascending suffix (ordinal) order.
            // A priority queue merges them without materializing any level; bounds (fromEncoded/
            // toEncoded) are pushed into the per-level scans by ScanIndexRawAsync so Kahuna
            // itself skips out-of-range pages.  The decoded-key in-range check below is a
            // second filter for the ComparePrefix edge cases documented on the root path above.

            int levelCount = 1 + ancestorStores.Length;
            var iters = new IAsyncEnumerator<(string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload)>[levelCount];

            iters[0] = ScanIndexRawAsync(tx.TransactionId, tx.ReadTimestamp, indexId, fromEncoded, fromInclusive, toEncoded, toInclusive, unique, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").GetAsyncEnumerator(cancellationToken);
            for (int ai = 0; ai < ancestorStores.Length; ai++)
            {
                (KvTableStore ancestorStore, HLCTimestamp forkTimestamp) = ancestorStores[ai];
                iters[ai + 1] = ancestorStore.ScanIndexRawAsync(HLCTimestamp.Zero, forkTimestamp, indexId, fromEncoded, fromInclusive, toEncoded, toInclusive, unique, cancellationToken).GetAsyncEnumerator(cancellationToken);
            }

            if (ancestorStores.Length > 0)
                BranchMetrics.RecordScanIterators(ancestorStores.Length);

            PriorityQueue<(int level, string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload),
                          (string suffix, int level)> heap = new(
                Comparer<(string suffix, int level)>.Create(static (a, b) =>
                {
                    int c = string.CompareOrdinal(a.suffix, b.suffix);
                    return c != 0 ? c : a.level.CompareTo(b.level);
                }));

            // Same O(1) lastSuffix dedup as ScanRows (heap orders by (suffix, levelIndex) so all
            // entries for a given suffix are consecutive; the branch level-0 entry always dequeues
            // first and wins, suppressing ancestor entries for the same suffix).
            string? lastSuffix = null;

            try
            {
                for (int i = 0; i < levelCount; i++)
                {
                    if (await iters[i].MoveNextAsync().ConfigureAwait(false))
                    {
                        (string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload) = iters[i].Current;
                        heap.Enqueue((i, suffix, kind, payload), (suffix, i));
                    }
                }

                while (heap.Count > 0)
                {
                    (int levelIdx, string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload) = heap.Dequeue();

                    if (suffix != lastSuffix)
                    {
                        lastSuffix = suffix;

                        if (kind == BranchKvKind.Value)
                        {
                            // Decode the suffix to (encodedKey, rowId).
                            string? encKey = null;
                            ObjectIdValue rowId = default;

                            if (unique)
                            {
                                if (payload is not null)
                                {
                                    encKey = suffix;
                                    rowId = RowIdFromPayload(payload.Value.Span);
                                }
                            }
                            else if (suffix.Length >= RowIdHexLength)
                            {
                                encKey = suffix[..^RowIdHexLength];
                                rowId = ObjectId.ToValue(suffix[^RowIdHexLength..]);
                            }

                            if (encKey is not null)
                            {
                                CompositeColumnValue decodedKey = KeyEncoder.Decode(encKey, keyTypes, directions);

                                bool inRange = true;
                                if (from is not null)
                                {
                                    int cmp = ComparePrefix(decodedKey, from);
                                    if (fromInclusive ? cmp < 0 : cmp <= 0) inRange = false;
                                }
                                if (inRange && to is not null)
                                {
                                    int cmp = ComparePrefix(decodedKey, to);
                                    if (toInclusive ? cmp > 0 : cmp >= 0) inRange = false;
                                }

                                if (inRange)
                                {
                                    yield return (decodedKey, rowId, IncludeTupleFromPayload(payload ?? default));
                                    emitted++;

                                    if (maxRows is not null && emitted >= maxRows.Value)
                                        yield break;
                                }
                            }
                        }
                        // Tombstone: lastSuffix is updated; deeper-level entries for this suffix are skipped.
                    }

                    if (await iters[levelIdx].MoveNextAsync().ConfigureAwait(false))
                    {
                        (string nextSuffix, BranchKvKind nextKind, ReadOnlyMemory<byte>? nextPayload) = iters[levelIdx].Current;
                        heap.Enqueue((levelIdx, nextSuffix, nextKind, nextPayload), (nextSuffix, levelIdx));
                    }
                }
            }
            finally
            {
                for (int i = 0; i < levelCount; i++)
                    await iters[i].DisposeAsync().ConfigureAwait(false);
            }
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
        byte[]? includeTuple = null,
        CancellationToken cancellationToken = default)
    {
        string kvKey = unique
            ? BuildUniqueIndexKey(indexId, key)
            : BuildNonUniqueIndexKey(indexId, key, rowId);

        byte[] value = BranchKvCodec.EncodeIndexRowId(rowId, includeTuple ?? default);

        tx.ReserveMutations(1);

        string indexBucketPrefix = BuildIndexBucketPrefix(indexId);
        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, indexBucketPrefix, kvKey))
            await UpgradeToExclusivePointLockAsync(tx, indexBucketPrefix, kvKey, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

        // For unique entries on branch databases, SetIfNotExists alone is not sufficient:
        // (a) a level-0 tombstone causes NotSet, blocking re-insert of a slot that was
        //     deliberately cleared (tombstone-replace must succeed), and
        // (b) SetIfNotExists only checks level-0, so an ancestor value for a different row
        //     goes undetected, creating a cross-lineage duplicate.
        // ResolveBranchUniqueFlagsAsync probes level-0 and ancestry post-lock so it sees
        // the committed state after any lock contention resolved, then returns the correct
        // write flag or throws DuplicateUniqueKeyValue.
        KeyValueFlags flags;
        if (unique && ancestorStores.Length > 0)
            flags = await ResolveBranchUniqueFlagsAsync(tx, indexId, key, kvKey, rowId, cancellationToken).ConfigureAwait(false);
        else
            flags = unique ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
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
                BranchKvValue existingDecoded = BranchKvCodec.Decode(existing?.Value);
                // The payload may carry an INCLUDE tuple after the rowId; the rowId is the fixed
                // 24-byte prefix, so compare only that slice — never the whole payload.
                string existingRowId = existingDecoded.HasPayload
                    ? Encoding.UTF8.GetString(existingDecoded.Payload.Span.Slice(0, Math.Min(BranchKvCodec.IndexRowIdPayloadLength, existingDecoded.Payload.Length)))
                    : "";
                if (existingRowId != rowId.ToString())
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
                // else: same rowId — idempotent re-write on resume, continue
            }
            else
            {
                throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
            }
        }

        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"PutIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Batched write path (mass insert)
    // -----------------------------------------------------------------------

    /// <summary>
    /// A single row plus its secondary-index entries, to be written as part of a batch.
    /// A <see langword="readonly struct"/> so the per-row descriptor lives in the batch list's backing
    /// array rather than as its own heap object; <see cref="IndexEntries"/> is null (not an empty list)
    /// for a no-index write, so such writes allocate no per-row index collection.
    /// </summary>
    public readonly struct RowWrite
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's final Kahuna storage value — the <see cref="BranchKvKind.Value"/>-enveloped row
        /// bytes as produced by <see cref="RowEncoder.EncodeStorageValue"/>. It is written to KV
        /// verbatim (no further enveloping), so the whole write costs one allocation, not raw-row plus
        /// a re-enveloped copy.
        /// </summary>
        public required byte[] RowData { get; init; }

        /// <summary>
        /// The row's secondary-index entries, or <see langword="null"/> when the table has no writable
        /// index applicable to this row. Built by the producer only once the first entry exists and then
        /// never mutated (this is a value type — mutating a copied struct's list would be a bug).
        /// </summary>
        public IReadOnlyList<IndexWrite>? IndexEntries { get; init; }
    }

    /// <summary>
    /// One secondary-index entry for a row in a batch write.
    /// <para>
    /// <see cref="IncludeTuple"/> carries the serialized stored/payload (INCLUDE) column values for a
    /// covering index (see <see cref="IndexIncludeValueCodec"/>); it is null/empty for a plain index,
    /// keeping the entry value byte-identical to the historical rowId-only form.
    /// </para>
    /// <para>
    /// <see cref="Overwrite"/> forces the write to use <see cref="KeyValueFlags.Set"/> even for a
    /// unique index. It is set only when an UPDATE changed an included column but not the key, so the
    /// entry key already exists and belongs to this same row: the value must be overwritten in place,
    /// and <c>SetIfNotExists</c> (the normal unique-insert flag) would wrongly no-op. It never bypasses
    /// duplicate detection for a genuinely new key.
    /// </para>
    /// </summary>
    public readonly record struct IndexWrite(string IndexId, CompositeColumnValue Key, bool Unique, byte[]? IncludeTuple = null, bool Overwrite = false);

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

        // Ensure the Kahuna session is open before building request items that embed
        // tx.TransactionId — the items are built into batch lists before AcquireManyWithRetry
        // runs, so a deferred-start session must start here rather than inside that call.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        bool isBranch = ancestorStores.Length > 0;

        // Collect lock keys and — for root databases — the batch write items in one pass.
        // Branch databases require per-item writes for unique entries (see branch phase below),
        // so their write items are built during the write phase, not here.
        // Pre-size to the row-count floor: every row contributes at least its own row key (plus any
        // index entries), so this avoids the first few list regrowths for the common no-/few-index case.
        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys = new(rows.Count);
        List<KahunaSetKeyValueRequestItem> setItems = new(rows.Count);   // root only
        Dictionary<string, bool> uniqueByKey = new();       // root only
        HashSet<string> seenUnique = [];                    // within-batch duplicate guard (both paths)

        foreach (RowWrite row in rows)
        {
            string rowKey = BuildRowKey(row.RowId);
            byte[] rowValue = row.RowData;   // already the enveloped storage value (EncodeStorageValue)
            lockKeys.Add((rowKey, 0, KeyValueDurability.Persistent));

            if (!isBranch)
            {
                uniqueByKey[rowKey] = false;
                setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
            }

            foreach (IndexWrite ix in row.IndexEntries ?? [])
            {
                string kvKey = ix.Unique
                    ? BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : BuildNonUniqueIndexKey(ix.IndexId, ix.Key, row.RowId);
                lockKeys.Add((kvKey, 0, KeyValueDurability.Persistent));

                if (ix.Unique && !seenUnique.Add(kvKey))
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(ix.IndexId)}'");

                if (!isBranch)
                {
                    // An include-only rewrite (Overwrite) targets an existing unique key owned by this
                    // same row, so it must Set (overwrite), not SetIfNotExists which would no-op.
                    uniqueByKey[kvKey] = ix.Unique && !ix.Overwrite;
                    setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = BranchKvCodec.EncodeIndexRowId(row.RowId, ix.IncludeTuple ?? default), CompareValue = null, CompareRevision = -1, Flags = (ix.Unique && !ix.Overwrite) ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                }
            }
        }

        tx.ReserveMutations(lockKeys.Count);

        // Phase 1 — acquire every lock for the batch in one round-trip (retrying only transients).
        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);

        if (!isBranch)
        {
            // Phase 2 (root) — set every value in one round-trip.
            await SetManyWithRetry(tx, setItems, uniqueByKey, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Phase 2 (branch) — write non-unique entries and row data in a batch (using Set);
            // write unique entries individually with a post-lock ancestry probe so that:
            //   (a) tombstoned unique slots can be replaced (tombstone-replace), and
            //   (b) ancestor values for a different rowId are detected as conflicts.
            // The probe happens post-lock so it sees the committed state after any competing
            // writer released the lock, not a stale pre-lock snapshot.
            List<KahunaSetKeyValueRequestItem> batchItems = [];
            Dictionary<string, bool> batchByKey = new();

            foreach (RowWrite row in rows)
            {
                string rowKey = BuildRowKey(row.RowId);
                byte[] rowValue = row.RowData;   // already the enveloped storage value (EncodeStorageValue)
                batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                batchByKey[rowKey] = false;

                foreach (IndexWrite ix in row.IndexEntries ?? [])
                {
                    string kvKey = ix.Unique
                        ? BuildUniqueIndexKey(ix.IndexId, ix.Key)
                        : BuildNonUniqueIndexKey(ix.IndexId, ix.Key, row.RowId);
                    byte[] value = BranchKvCodec.EncodeIndexRowId(row.RowId, ix.IncludeTuple ?? default);

                    if (!ix.Unique || ix.Overwrite)
                    {
                        // Non-unique entries, and include-only rewrites of an existing unique key owned
                        // by this same row, both write with a plain Set (overwrite in place).
                        batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = value, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                        batchByKey[kvKey] = false;
                    }
                    else
                    {
                        KeyValueFlags flags = await ResolveBranchUniqueFlagsAsync(
                            tx, ix.IndexId, ix.Key, kvKey, row.RowId, cancellationToken).ConfigureAwait(false);

                        (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                            cancellationToken
                        ).ConfigureAwait(false);

                        if (type == KeyValueResponseType.NotSet)
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(ix.IndexId)}'");
                        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");
                        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
                            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch set failed for branch unique key {kvKey}: {type}");
                    }
                }
            }

            await SetManyWithRetry(tx, batchItems, batchByKey, cancellationToken).ConfigureAwait(false);
        }

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
        // Start the deferred session before the optimistic check so that the write calls that
        // follow (SetManyWithRetry / DeleteManyWithRetry) have a valid TransactionId.
        await tx.EnsureSessionStartedAsync(ct).ConfigureAwait(false);

        // Optimistic transactions take no explicit exclusive WRITE locks: each confirmed write folds an
        // implicit point lock into the coordinator working set, and write-write conflicts are detected at
        // commit (write-intent conflict on the modified keys plus read-set validation). Skipping the
        // acquire here makes the WRITE path non-blocking: a concurrent writer is not blocked and the loser
        // aborts at prepare instead of at lock time.
        //
        // This is fully lock-free only under Read Committed. Under the default Serializable isolation the
        // read/scan paths still take SHARED range/point locks and a following write UPGRADES them to
        // Exclusive — both gated on the isolation level, NOT on tx.Locking (see GetRow / WriteRow). So a
        // Serializable+Optimistic transaction is a HYBRID: optimistic write + read-set validation plus the
        // retained predicate locks that keep the scan phantom-free. Serializable is deliberately not
        // weakened to lock-free, because that would silently break its isolation guarantee.
        if (tx.Locking == KeyValueTransactionLocking.Optimistic)
            return;

        List<(string, int, KeyValueDurability)> pending = new(keys);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration. It folds every acquired
        // exclusive point lock into the coordinator working set as one registered operation, so
        // commit/rollback release them. The id is REUSED while the pending set is unchanged — an
        // ack-loss resend of the identical batch, which the coordinator can only replay idempotently
        // under the same id — and a fresh id is minted only when the set actually shrinks (confirmed
        // locks were removed), a genuinely new, smaller declaration. Minting a new id for an unchanged
        // resend would strand the original registration (blocking finalize drain) and could double-fold.
        TransactionOperationId lockBatchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses =
                await kahuna.LocateAndTryAcquireManyExclusiveLocks(tx.TransactionId, pending, ct, tx.CoordinatorKey, lockBatchOperationId).ConfigureAwait(false);

            // First pass: trace every successfully locked key (when lock tracing is on). The coordinator
            // owns the folded point locks and releases them at finalize, so no client-side tracking is
            // needed for cleanup.
            if (CamusDBConfig.LockTracingEnabled)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability _, _) in responses)
                {
                    if (type == KeyValueResponseType.Locked)
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

            // A shrinking pending set (some keys were confirmed Locked) is a new, smaller declaration —
            // register it under a fresh id. An unchanged set (every key transient) is the identical batch
            // resent (e.g. a lost completion ack) — keep the id so the coordinator replays idempotently.
            if (retry.Count != pending.Count)
                lockBatchOperationId = TransactionOperationId.NewRandom();
            pending = retry;
        }
    }

    private async Task SetManyWithRetry(
        KvTransaction tx,
        List<KahunaSetKeyValueRequestItem> items,
        Dictionary<string, bool> uniqueByKey,
        CancellationToken ct)
    {
        List<KahunaSetKeyValueRequestItem> pending = new(items);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration. It folds the whole batch into
        // the coordinator working set as one registered operation. The id is REUSED while the pending set
        // is unchanged — an ack-loss resend of the identical batch, which the coordinator can only replay
        // idempotently under the same id — and a fresh id is minted only when the set actually shrinks
        // (confirmed keys removed), a genuinely new, smaller declaration. Resending the identical unique
        // batch under a NEW id would let an already-staged SetIfNotExists read back as a false duplicate
        // and strand the original pending op (blocking finalize drain).
        TransactionOperationId batchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<KahunaSetKeyValueResponseItem> responses =
                await kahuna.LocateAndTrySetManyKeyValue(pending, ct, tx.CoordinatorKey, batchOperationId).ConfigureAwait(false);

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

            // Shrinking set (some keys confirmed Set/NotSet) → new, smaller declaration under a fresh id.
            // Unchanged set (every key transient) → identical resend of a lost ack → keep the same id.
            if (retry.Count != pending.Count)
                batchOperationId = TransactionOperationId.NewRandom();
            pending = retry;
        }
    }

    // -----------------------------------------------------------------------
    // Batched update path (mass UPDATE)
    // -----------------------------------------------------------------------

    /// <summary>
    /// A single row update: the new row data plus index entries to remove (old) and to add (new).
    /// Only entries whose indexed columns actually changed should be included; unchanged entries
    /// are intentionally omitted so the batch skips needless delete+put round-trips and avoids
    /// the branch tombstone-replace correctness trap (see <see cref="UpdateRowsBatch"/>).
    /// </summary>
    public readonly struct RowUpdate
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's final Kahuna storage value — the <see cref="BranchKvKind.Value"/>-enveloped row
        /// bytes from <see cref="RowEncoder.EncodeStorageValue"/>. Written verbatim (no re-enveloping).
        /// </summary>
        public required byte[] NewRowData { get; init; }

        /// <summary>
        /// Old secondary-index entries to remove (only for changed keys), or <see langword="null"/>
        /// when no indexed value changed. Built lazily so an unchanged-index update allocates no list.
        /// </summary>
        public IReadOnlyList<IndexDelete>? OldIndexEntries { get; init; }

        /// <summary>
        /// New secondary-index entries to put (only for changed keys), or <see langword="null"/> when
        /// no indexed value changed. Built lazily so an unchanged-index update allocates no list.
        /// </summary>
        public IReadOnlyList<IndexWrite>? NewIndexEntries { get; init; }
    }

    /// <summary>
    /// Updates many rows (and their changed secondary-index entries) using two Kahuna round-trips
    /// per chunk — one <see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> and one (or two
    /// for root databases with old-index deletes) <see cref="IKahuna.LocateAndTrySetManyKeyValue"/>
    /// — instead of one acquire+delete/set per key.
    ///
    /// <para>For root databases: old index entries are physically deleted via
    /// <see cref="IKahuna.LocateAndTryDeleteManyKeyValue"/> then row blobs and new index entries
    /// are written via <see cref="IKahuna.LocateAndTrySetManyKeyValue"/>.</para>
    ///
    /// <para>For branch databases: old entries receive a tombstone (same reasoning as
    /// <see cref="DeleteRowsBatch"/>), row blobs and non-unique new entries are written in a
    /// batch, and unique new entries are written individually via
    /// <see cref="ResolveBranchUniqueFlagsAsync"/> (post-lock ancestry probe).</para>
    ///
    /// <para>Unique-index semantics: new unique entries use <c>SetIfNotExists</c>; a
    /// <c>NotSet</c> result surfaces <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>.
    /// Within-batch uniqueness is checked up front (duplicate new unique key → immediate error).
    /// NULL-distinct semantics are the caller's responsibility: entries with NULL key columns
    /// must not be included in either <see cref="RowUpdate.OldIndexEntries"/> or
    /// <see cref="RowUpdate.NewIndexEntries"/>.</para>
    ///
    /// <para>Lock keys are deduplicated before acquisition so that a key appearing as both an
    /// old entry for one row and a new entry for another row within the same batch is only locked
    /// once, avoiding a false <c>AlreadyLocked</c> from Kahuna on repeated lock requests.</para>
    /// </summary>
    public async Task UpdateRowsBatch(KvTransaction tx, IReadOnlyList<RowUpdate> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        bool isBranch = ancestorStores.Length > 0;

        // Collect lock keys (deduplicated), delete items (old index entries for root),
        // set items (row blob + new index entries for root), and the within-batch
        // unique-new-key guard.
        // Pre-size to the row-count floor: every row contributes at least its own row-blob key.
        HashSet<string> seenLockKeys = new(StringComparer.Ordinal);
        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys = new(rows.Count);
        List<KahunaSetKeyValueRequestItem> setItems = new(rows.Count);          // row + new index
        List<KahunaDeleteKeyValueRequestItem> deleteItems = [];    // old index (root only)
        Dictionary<string, bool> uniqueByKey = new();             // root only
        HashSet<string> seenUniqueNew = [];                        // within-batch new unique guard

        void AddLockKey(string key)
        {
            if (seenLockKeys.Add(key))
                lockKeys.Add((key, 0, KeyValueDurability.Persistent));
        }

        foreach (RowUpdate row in rows)
        {
            string rowKey = BuildRowKey(row.RowId);
            byte[] rowValue = row.NewRowData;   // already the enveloped storage value (EncodeStorageValue)
            AddLockKey(rowKey);

            if (!isBranch)
            {
                uniqueByKey[rowKey] = false;
                setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
            }

            foreach (IndexDelete old in row.OldIndexEntries ?? [])
            {
                string kvKey = old.Unique
                    ? BuildUniqueIndexKey(old.IndexId, old.Key)
                    : BuildNonUniqueIndexKey(old.IndexId, old.Key, old.RowId);
                AddLockKey(kvKey);

                if (!isBranch)
                    deleteItems.Add(new KahunaDeleteKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Durability = KeyValueDurability.Persistent });
            }

            foreach (IndexWrite newIx in row.NewIndexEntries ?? [])
            {
                string kvKey = newIx.Unique
                    ? BuildUniqueIndexKey(newIx.IndexId, newIx.Key)
                    : BuildNonUniqueIndexKey(newIx.IndexId, newIx.Key, row.RowId);

                if (newIx.Unique && !seenUniqueNew.Add(kvKey))
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(newIx.IndexId)}'");

                AddLockKey(kvKey);

                if (!isBranch)
                {
                    // Overwrite = include-only rewrite of an existing unique key owned by this row:
                    // Set (overwrite), not SetIfNotExists which would no-op on the existing key.
                    uniqueByKey[kvKey] = newIx.Unique && !newIx.Overwrite;
                    setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = BranchKvCodec.EncodeIndexRowId(row.RowId, newIx.IncludeTuple ?? default), CompareValue = null, CompareRevision = -1, Flags = (newIx.Unique && !newIx.Overwrite) ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                }
            }
        }

        tx.ReserveMutations(lockKeys.Count);

        // Phase 1 — acquire every exclusive lock in one round-trip.
        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);

        if (!isBranch)
        {
            // Phase 2a (root) — physically delete old index entries.
            await DeleteManyWithRetry(tx, deleteItems, cancellationToken).ConfigureAwait(false);

            // Phase 2b (root) — write row blobs and new index entries.
            await SetManyWithRetry(tx, setItems, uniqueByKey, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Phase 2 (branch) — tombstone old entries; write row blobs and non-unique new
            // entries in a batch; handle unique new entries individually (post-lock ancestry probe).
            List<KahunaSetKeyValueRequestItem> batchItems = [];
            Dictionary<string, bool> batchByKey = new();

            foreach (RowUpdate row in rows)
            {
                string rowKey = BuildRowKey(row.RowId);
                byte[] rowValue = row.NewRowData;   // already the enveloped storage value (EncodeStorageValue)
                batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                batchByKey[rowKey] = false;

                foreach (IndexDelete old in row.OldIndexEntries ?? [])
                {
                    string kvKey = old.Unique
                        ? BuildUniqueIndexKey(old.IndexId, old.Key)
                        : BuildNonUniqueIndexKey(old.IndexId, old.Key, old.RowId);
                    batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = BranchKvCodec.EncodeTombstone(), CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                    batchByKey[kvKey] = false;
                }

                foreach (IndexWrite newIx in row.NewIndexEntries ?? [])
                {
                    string kvKey = newIx.Unique
                        ? BuildUniqueIndexKey(newIx.IndexId, newIx.Key)
                        : BuildNonUniqueIndexKey(newIx.IndexId, newIx.Key, row.RowId);
                    byte[] value = BranchKvCodec.EncodeIndexRowId(row.RowId, newIx.IncludeTuple ?? default);

                    if (!newIx.Unique || newIx.Overwrite)
                    {
                        // Non-unique, or an include-only rewrite of an existing unique key owned by this
                        // row: plain Set (overwrite in place).
                        batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = value, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                        batchByKey[kvKey] = false;
                    }
                    else
                    {
                        KeyValueFlags flags = await ResolveBranchUniqueFlagsAsync(
                            tx, newIx.IndexId, newIx.Key, kvKey, row.RowId, cancellationToken).ConfigureAwait(false);

                        (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                            cancellationToken
                        ).ConfigureAwait(false);

                        if (type == KeyValueResponseType.NotSet)
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{DuplicateKeyLabel(newIx.IndexId)}'");
                        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");
                        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
                            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch update failed for branch unique key {kvKey}: {type}");

                        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
                    }
                }
            }

            await SetManyWithRetry(tx, batchItems, batchByKey, cancellationToken).ConfigureAwait(false);
        }

        // Track all modified keys for 2PC commit. Locks were already tracked inside AcquireManyWithRetry.
        foreach ((string key, _, _) in lockKeys)
            tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Batched delete path (mass delete / drop index / drop table)
    // -----------------------------------------------------------------------

    /// <summary>
    /// A single row plus its secondary-index entries, to be deleted as part of a batch.
    /// A <see langword="readonly struct"/>; <see cref="IndexEntries"/> is null (not an empty list) for a
    /// no-index delete, so such deletes allocate no per-row index collection.
    /// </summary>
    public readonly struct RowDelete
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's secondary-index entries to remove, or <see langword="null"/> when the table has no
        /// writable index applicable to this row. Built lazily; never mutated after construction.
        /// </summary>
        public IReadOnlyList<IndexDelete>? IndexEntries { get; init; }
    }

    /// <summary>One secondary-index entry for a row in a batch delete.</summary>
    public readonly record struct IndexDelete(string IndexId, CompositeColumnValue Key, ObjectIdValue RowId, bool Unique);

    /// <summary>
    /// Deletes many rows (and their index entries).
    ///
    /// For root databases, issues a physical batch KV delete via two round-trips
    /// (<see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> then
    /// <see cref="IKahuna.LocateAndTryDeleteManyKeyValue"/>).
    ///
    /// For branch databases, writes a <see cref="BranchKvKind.Tombstone"/> to every key
    /// in the level-0 overlay instead.  Physical deletes would leave no level-0 entry,
    /// letting the ancestry merge re-surface the rows from ancestor namespaces.
    /// </summary>
    public async Task DeleteRowsBatch(KvTransaction tx, IReadOnlyList<RowDelete> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        List<string> keys = [];

        foreach (RowDelete row in rows)
        {
            keys.Add(BuildRowKey(row.RowId));

            foreach (IndexDelete ix in row.IndexEntries ?? [])
            {
                keys.Add(ix.Unique
                    ? BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : BuildNonUniqueIndexKey(ix.IndexId, ix.Key, ix.RowId));
            }
        }

        tx.ReserveMutations(keys.Count);

        if (ancestorStores.Length > 0)
        {
            // Branch: write tombstones so the ancestry merge suppresses inherited rows.
            List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys =
                keys.Select(k => (k, 0, KeyValueDurability.Persistent)).ToList();

            List<KahunaSetKeyValueRequestItem> tombstoneItems = keys.Select(k => new KahunaSetKeyValueRequestItem
            {
                TransactionId = tx.TransactionId,
                Key = k,
                Value = BranchKvCodec.EncodeTombstone(),
                CompareValue = null,
                CompareRevision = -1,
                Flags = KeyValueFlags.Set,
                ExpiresMs = 0,
                Durability = KeyValueDurability.Persistent
            }).ToList();

            await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);
            await SetManyWithRetry(tx, tombstoneItems, new Dictionary<string, bool>(), cancellationToken).ConfigureAwait(false);

            foreach (string key in keys)
                tx.TrackModified(key, KeyValueDurability.Persistent);
        }
        else
        {
            await DeleteKeysBatch(tx, keys, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Removes a secondary index entry.
    ///
    /// For root databases, issues a physical KV delete.
    /// For branch databases, writes a <see cref="BranchKvKind.Tombstone"/> to the level-0
    /// overlay so that <see cref="LookupUnique"/> and <see cref="ScanIndex"/> suppress the
    /// inherited index entry in the ancestry merge — the same reason <see cref="DeleteRow"/>
    /// writes a tombstone rather than a physical delete on branch stores.
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

        if (ancestorStores.Length > 0)
        {
            // Branch: write a tombstone so the ancestry merge suppresses the inherited index entry.
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.MustRetry)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type != KeyValueResponseType.Set)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");
        }
        else
        {
            // Root: physical delete.
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
                (coordinatorKey, operationId) => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, kvKey, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.MustRetry)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");
        }

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
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

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

    /// <summary>
    /// Physically deletes every KV entry in this database's row overlay for this table
    /// (<c>{dbId}:{tableId}:r/…</c>). Used by <c>DROP TABLE</c> on a branch database to reclaim
    /// branch-local row entries without scanning or tombstoning inherited ancestor rows — those
    /// become unreachable through the schema once the table is dropped from the branch, so no
    /// tombstone is required. Returns the number of entries deleted.
    /// </summary>
    public async Task<int> PurgeLocalRowOverlayAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        List<string> keysToDelete = [];

        await foreach ((string kvKey, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            rowBucketPrefix,
            null, true,
            null, true,
            DefaultPageSize,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (kvKey.StartsWith(rowKeyPrefix, StringComparison.Ordinal))
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

        // Start the deferred session before building delete items that embed tx.TransactionId.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys =
            keys.Select(k => (k, 0, KeyValueDurability.Persistent)).ToList();

        List<KahunaDeleteKeyValueRequestItem> deleteItems = keys.Select(k => new KahunaDeleteKeyValueRequestItem
        {
            TransactionId = tx.TransactionId,
            Key = k,
            Durability = KeyValueDurability.Persistent
        }).ToList();

        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);
        await DeleteManyWithRetry(tx, deleteItems, cancellationToken).ConfigureAwait(false);

        // Locks were already tracked inside AcquireManyWithRetry.
        foreach (string key in keys)
            tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task DeleteManyWithRetry(KvTransaction tx, List<KahunaDeleteKeyValueRequestItem> items, CancellationToken ct)
    {
        List<KahunaDeleteKeyValueRequestItem> pending = new(items);
        long deadline = LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration. It folds the confirmed deletes
        // into the coordinator working set as one registered operation. The id is REUSED while the pending
        // set is unchanged — an ack-loss resend of the identical batch, which the coordinator can only
        // replay idempotently under the same id — and a fresh id is minted only when the set actually
        // shrinks (confirmed deletes removed), a genuinely new, smaller declaration.
        TransactionOperationId deleteBatchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<KahunaDeleteKeyValueResponseItem> responses =
                await kahuna.LocateAndTryDeleteManyKeyValue(pending, ct, tx.CoordinatorKey, deleteBatchOperationId).ConfigureAwait(false);

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

            // Shrinking set (some deletes confirmed) → new, smaller declaration under a fresh id.
            // Unchanged set (every key transient) → identical resend of a lost ack → keep the same id.
            if (retry.Count != pending.Count)
                deleteBatchOperationId = TransactionOperationId.NewRandom();
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
        // Gated on Serializable+RW, NOT on tx.Locking: even an optimistic transaction upgrades its
        // retained shared predicate lock here — the hybrid keeps the read-then-write phantom-free while
        // the write itself skips the explicit exclusive lock in AcquireLock.
        if (IsSerializableReadWrite(tx) && HasSharedPointLock(tx, rowBucketPrefix, key))
            await UpgradeToExclusivePointLockAsync(tx, rowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        byte[] encodedData = BranchKvCodec.EncodeValue(data);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, encodedData, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.MustRetry)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Test-only: overwrites the row entry for <paramref name="rowId"/> with a
    /// <see cref="BranchKvKind.Tombstone"/> envelope, mirroring the locked write path of
    /// <see cref="WriteRow"/>. Useful for storage-layer tests that need to inject a tombstone
    /// directly without going through a DELETE DML statement.
    /// </summary>
    internal async Task WriteRowTombstoneForTesting(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        tx.ReserveMutations(1);
        string key = BuildRowKey(rowId);
        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);
        await SetTombstoneForTesting(tx, key, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Test-only: overwrites the unique-index entry for <paramref name="key"/> with a
    /// <see cref="BranchKvKind.Tombstone"/> envelope. See <see cref="WriteRowTombstoneForTesting"/>.
    /// </summary>
    internal async Task WriteUniqueIndexTombstoneForTesting(KvTransaction tx, string indexId, CompositeColumnValue key, CancellationToken cancellationToken = default)
    {
        tx.ReserveMutations(1);
        string kvKey = BuildUniqueIndexKey(indexId, key);
        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);
        await SetTombstoneForTesting(tx, kvKey, cancellationToken).ConfigureAwait(false);
    }

    private async Task SetTombstoneForTesting(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryRegistered(tx,
            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRowTombstoneForTesting failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireLock(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        // Start the deferred Kahuna session before the optimistic/pessimistic branch so the
        // session is open for the write that follows even when optimistic skips the lock.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        // Optimistic transactions take no explicit exclusive WRITE lock — see AcquireManyWithRetry (the
        // write's implicit point lock and commit-time validation replace it). Note this skips only the
        // write lock: under Serializable, the read/scan predicate (range/point) locks still apply, so
        // Serializable+Optimistic remains a hybrid rather than fully lock-free.
        if (tx.Locking == KeyValueTransactionLocking.Optimistic)
            return;

        (KeyValueResponseType lockType, _, _, _) = await RetryOnMustRetryRegistered(tx,
            (coordinatorKey, operationId) => kahuna.LocateAndTryAcquireExclusiveLock(tx.TransactionId, key, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (lockType == KeyValueResponseType.AlreadyLocked)
            throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, $"Key {key} is locked by another transaction");

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {lockType}");

        if (CamusDBConfig.LockTracingEnabled)
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

    // -----------------------------------------------------------------------
    // Coordinator-registered write/lock helpers
    //
    // A transactional mutation or exclusive point lock must fold into the Kahuna coordinator's
    // server-owned working set, or LocateAndCommitTransaction(handle) would finalize an empty set and
    // the write would never commit. Folding requires passing the transaction's coordinator key and a
    // per-operation id: the coordinator registers the operation, records its confirmed effect, and
    // caches the response keyed by that id. These overloads mint the operation id ONCE and reuse it
    // across every lock-wait retry, so a retry after a lost response replays the cached effect instead
    // of applying the mutation twice — this is the idempotent-retry guarantee. Each logical operation
    // gets its own id (distinct key/digest under one id would be rejected as a duplicate).
    // -----------------------------------------------------------------------

    private static Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetryRegistered(
        KvTransaction tx,
        Func<string, TransactionOperationId, Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        TransactionOperationId operationId = TransactionOperationId.NewRandom();
        return RetryOnMustRetryLocked(() => fn(tx.CoordinatorKey, operationId), ct);
    }

    private static Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RetryOnMustRetryRegistered(
        KvTransaction tx,
        Func<string, TransactionOperationId, Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        TransactionOperationId operationId = TransactionOperationId.NewRandom();
        return RetryOnMustRetryLocked(() => fn(tx.CoordinatorKey, operationId), ct);
    }

    // -----------------------------------------------------------------------
    // Branch lineage helpers (used by the branch-aware read paths above)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Resolves the <see cref="KeyValueFlags"/> to use when writing a unique index entry on a
    /// branch database.
    ///
    /// <para>
    /// This method MUST be called AFTER <see cref="AcquireLock"/> for <paramref name="kvKey"/>
    /// has returned.  Because Kahuna creates each transaction's MVCC snapshot lazily on first
    /// access, probing <paramref name="kvKey"/> here — the first access for this transaction —
    /// reflects the committed state that exists once the lock was acquired, not a stale snapshot
    /// captured before lock contention resolved.
    /// </para>
    ///
    /// Three cases:
    /// <list type="bullet">
    ///   <item>Level-0 tombstone → <see cref="KeyValueFlags.Set"/> (tombstone-replace: the slot was
    ///     explicitly cleared in this branch and is available for re-use).</item>
    ///   <item>Level-0 live value, same rowId → <see cref="KeyValueFlags.Set"/> (idempotent write).</item>
    ///   <item>Level-0 live value, different rowId → throws <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>.</item>
    ///   <item>Level-0 miss → walks ancestor chain; ancestor live value for a different rowId → throws
    ///     <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>; otherwise → <see cref="KeyValueFlags.SetIfNotExists"/>
    ///     (concurrent-insert fence for same-branch races).</item>
    /// </list>
    /// </summary>
    private async Task<KeyValueFlags> ResolveBranchUniqueFlagsAsync(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        string kvKey,
        ObjectIdValue rowId,
        CancellationToken cancellationToken)
    {
        // Post-lock probe: first access to this key in the transaction, so Kahuna creates the
        // MVCC snapshot from the current committed state — after any competing writer released
        // the lock by committing or rolling back.
        BranchKvValue existing = await ProbeRaw(
            tx.TransactionId, HLCTimestamp.Zero, kvKey, cancellationToken).ConfigureAwait(false);

        if (existing.Kind == BranchKvKind.Tombstone)
            // Slot was explicitly cleared in this branch; replace the tombstone with the new value.
            return KeyValueFlags.Set;

        if (existing.Kind == BranchKvKind.Value && existing.HasPayload)
        {
            // Live value at level-0: idempotent same-row write (e.g. backfill resume or an UPDATE
            // that touches non-indexed columns) is allowed; a different rowId is a conflict.
            if (Encoding.UTF8.GetString(existing.Payload.Span) != rowId.ToString())
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateUniqueKeyValue,
                    $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
            return KeyValueFlags.Set;
        }

        // Miss at level-0: walk ancestry to enforce uniqueness over the full union.
        // Nearest ancestor first; stop on the first hit (tombstone or live value).
        foreach ((KvTableStore ancestorStore, HLCTimestamp forkTimestamp) in ancestorStores)
        {
            string ancestorKvKey = ancestorStore.BuildUniqueIndexKey(indexId, key);
            BranchKvValue ancestor = await ancestorStore.ProbeRaw(
                HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);

            if (ancestor.Kind == BranchKvKind.Tombstone)
                break;   // an ancestor branch cleared this slot; treat as available

            if (ancestor.Kind == BranchKvKind.Value && ancestor.HasPayload)
            {
                if (Encoding.UTF8.GetString(ancestor.Payload.Span) != rowId.ToString())
                    throw new CamusDBException(
                        CamusDBErrorCodes.DuplicateUniqueKeyValue,
                        $"Duplicate entry for key '{DuplicateKeyLabel(indexId)}'");
                break;   // same rowId in ancestor (branch shadows its own inherited entry); allow
            }
        }

        // Slot absent from level-0 and all ancestors.  SetIfNotExists is a concurrent-insert
        // fence: if two branch transactions race to insert the same unique key, one wins the
        // lock and writes; the other, when it retries the lock and probes, sees the winner's
        // committed value and throws DuplicateUniqueKeyValue above.  As an additional safety net,
        // SetIfNotExists here ensures the second writer's set also fails if the MVCC snapshot
        // somehow missed the intermediate commit.
        return KeyValueFlags.SetIfNotExists;
    }

    // -----------------------------------------------------------------------

    // Probes a single Kahuna key at the given transaction identity and read timestamp, returning the
    // raw (kind, payload) pair decoded by BranchKvCodec.  A Kahuna miss returns (Value, null) —
    // the same signal as BranchKvCodec.Decode(null) — so callers treat null payload as "not here,
    // continue walking ancestry."  A Tombstone payload means "deleted at this level; stop walking."
    // When coordinatorKey is non-empty the read is registered with the transaction coordinator so its
    // existence/base-revision observation folds into the working set and is validated at commit — the
    // optimistic / TrackAndValidate read path. Empty coordinatorKey (the default, and every ancestor
    // snapshot probe) issues an unregistered read, identical to the pessimistic behavior. The operation
    // id is minted once, outside the retry, so a transient MustRetry replays under the same id rather
    // than registering a second read.
    private async Task<BranchKvValue> ProbeRaw(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string key,
        CancellationToken cancellationToken,
        string coordinatorKey = "")
    {
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(txId, key, -1, readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                $"Read of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

        if (type != KeyValueResponseType.Get || entry is null)
            return BranchKvValue.Miss;  // Kahuna miss — continue ancestry walk

        return BranchKvCodec.Decode(entry.Value);
    }

    // Scans all rows in this store's keyspace at the given read timestamp (no live transaction).
    // Yields every (rowIdHex, kind, payload) triple, including tombstones, so the branch-merge
    // caller can apply the nearest-wins/tombstone-suppression rule across levels.
    // Yields every row entry from this store's namespace at the given snapshot.
    // txId is the live transaction id for level-0 reads (use HLCTimestamp.Zero for ancestor snapshots).
    private async IAsyncEnumerable<(string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload)> ScanRowsRawAsync(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string coordinatorKey = "")
    {
        int prefixLen = rowKeyPrefix.Length;

        // Non-empty coordinatorKey registers the scan so every row it returns folds as a read
        // observation for commit-time validation; the coordinator derives a distinct operation id per
        // page from this base id. Empty (default, and all ancestor snapshots) scans unregistered.
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId, rowBucketPrefix, null, true, null, true, DefaultPageSize,
            readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey, operationId).ConfigureAwait(false))
        {
            if (entry.Value is null) continue;
            string rowIdHex = key.AsSpan(prefixLen).ToString();
            BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
            yield return (rowIdHex, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
        }
    }

    // Yields every index entry within optional encoded bounds from this store's namespace at the
    // given snapshot.  txId is the live transaction id for level-0 reads (HLCTimestamp.Zero for
    // ancestor snapshots).  fromEncoded/toEncoded are the raw encoded key strings (no prefix) so
    // this store builds its own start/end keys in its own keyspace ({dbId}:{tableId}:i:{indexId}/…).
    private async IAsyncEnumerable<(string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload)> ScanIndexRawAsync(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string indexId,
        string? fromEncoded, bool fromInclusive,
        string? toEncoded, bool toInclusive,
        bool unique,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string coordinatorKey = "")
    {
        string bucketPrefix = BuildIndexBucketPrefix(indexId);
        string keyPrefix = bucketPrefix + "/";
        int prefixLen = keyPrefix.Length;

        string? startKey = fromEncoded is not null ? keyPrefix + fromEncoded : null;
        string? endKey   = toEncoded   is not null
            ? (unique ? keyPrefix + toEncoded : keyPrefix + toEncoded + IndexKeySentinel)
            : null;

        // See ScanRowsRawAsync: non-empty coordinatorKey registers the index scan for read-set folding.
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId, bucketPrefix, startKey, fromInclusive, endKey, toInclusive,
            DefaultPageSize, readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey, operationId).ConfigureAwait(false))
        {
            if (entry.Value is null || !kvKey.StartsWith(keyPrefix, StringComparison.Ordinal)) continue;
            string suffix = kvKey.Substring(prefixLen);
            BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
            yield return (suffix, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
        }
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

    // Composes "{bucket}/{encodedKey}" straight into the final string's buffer — one allocation, no
    // intermediate KeyEncoder.Encode(...) string interpolated into a second string. `bucket` is the
    // cached "{dbId}:{tableId}:i:{indexId}" prefix (== "{tableKeyPrefix}:i:{indexId}").
    private string BuildUniqueIndexKey(string indexId, CompositeColumnValue key)
    {
        string bucket = BuildIndexBucketPrefix(indexId);
        OrderType[]? directions = DirectionsOf(indexId);
        int encodedLen = KeyEncoder.Measure(key, directions);
        return string.Create(bucket.Length + 1 + encodedLen, (bucket, key, directions), static (span, state) =>
        {
            state.bucket.CopyTo(span);
            span[state.bucket.Length] = '/';
            int pos = state.bucket.Length + 1;
            KeyEncoder.Write(span, ref pos, state.key, state.directions);
        });
    }

    // Read-path variant: returns false when the key cannot be encoded (an invalid Id value that no
    // stored row can equal), so a point lookup treats it as a miss instead of throwing.
    private bool TryBuildUniqueIndexKey(string indexId, CompositeColumnValue key, out string kvKey)
    {
        if (!KeyEncoder.TryEncode(key, DirectionsOf(indexId), out string encoded))
        {
            kvKey = string.Empty;
            return false;
        }

        kvKey = $"{tableKeyPrefix}:i:{indexId}/{encoded}";
        return true;
    }

    // Non-unique: rowIdHex appended directly (no separator) so the last slash in the full
    // key is always the one after {indexId}, keeping the routing hash stable. Composed straight into
    // the final buffer — one allocation, no encoded-key string and no rowId.ToString() temporary.
    private string BuildNonUniqueIndexKey(string indexId, CompositeColumnValue key, ObjectIdValue rowId)
    {
        string bucket = BuildIndexBucketPrefix(indexId);
        OrderType[]? directions = DirectionsOf(indexId);
        int encodedLen = KeyEncoder.Measure(key, directions);
        return string.Create(bucket.Length + 1 + encodedLen + 24, (bucket, key, directions, rowId), static (span, state) =>
        {
            state.bucket.CopyTo(span);
            span[state.bucket.Length] = '/';
            int pos = state.bucket.Length + 1;
            KeyEncoder.Write(span, ref pos, state.key, state.directions);
            ObjectId.WriteHex(span[pos..], state.rowId.a, state.rowId.b, state.rowId.c);
        });
    }

    // Formats a human-readable "table.index" key name for duplicate-key errors.
    // Resolves the immutable KvId to the mutable display name via the table-open-time registry.
    private string DuplicateKeyLabel(string indexId)
    {
        string display = indexIdToDisplayName.TryGetValue(indexId, out string? name) ? name : indexId;
        return string.IsNullOrEmpty(tableName) ? display : $"{tableName}.{display}";
    }

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
