/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The primary-row access paths for one table: point reads, batched point reads, the full ordered
/// scan, and the single-row write and delete.
///
/// <para><b>Snapshot and lock behaviour is decided by the transaction, never by the call site.</b>
/// A non-Zero <see cref="KvTransaction.ReadTimestamp"/> pins every read here to one consistent cut
/// through the version history, including across scan page boundaries; every other transaction type
/// passes Zero and stays on Kahuna's read-committed fast path. A Serializable read-write transaction
/// additionally takes a shared point lock before a read and holds it to commit, which is what stops a
/// concurrent writer from committing a modification to the same key while this transaction is live.</para>
///
/// <para><b>Read-set folding likewise follows <see cref="KvTransaction.FoldReads"/>.</b> A
/// transaction that promises commit-time read validation must fold <em>every</em> read it performs,
/// scans included. Folding only point reads would make isolation depend on plan choice: the same
/// predicate answered by a primary-key lookup would be validated at commit while a table scan of it
/// would not. The price is that such a transaction's commit cost scales with the rows its scans
/// observed; a transaction that cannot afford that should use pessimistic locking, whose scans
/// register nothing.</para>
///
/// <para><b>A branch merges its ancestry as it reads.</b> A miss at level 0 walks the lineage, and a
/// scan runs a streaming k-way merge across one iterator per level so a <c>LIMIT 1</c> against a
/// large parent still reads one row. A delete on a branch writes a tombstone rather than deleting the
/// key, because a physical delete would leave no level-0 entry and the merge would resurface the
/// inherited row.</para>
/// </summary>
internal sealed class KvRowAccessor
{
    private readonly IKahuna kahuna;
    private readonly KvKeyBuilder keys;
    private readonly KvRangeLockManager locks;
    private readonly KvBranchReader branch;
    private readonly KahunaRetryPolicy retry;

    internal KvRowAccessor(
        IKahuna kahuna,
        KvKeyBuilder keys,
        KvRangeLockManager locks,
        KvBranchReader branch,
        KahunaRetryPolicy retry)
    {
        this.kahuna = kahuna;
        this.keys = keys;
        this.locks = locks;
        this.branch = branch;
        this.retry = retry;
    }

    /// <summary>
    /// Point-read a single row. Returns the raw serialized bytes, or <c>null</c> if not found.
    /// See the class summary for the snapshot, locking and ancestry rules that apply.
    /// </summary>
    internal async Task<ReadOnlyMemory<byte>?> GetRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        // Serializable+RW acquires a shared point lock: the session must be open first.
        // For all other transaction types this is a no-op (zero-snapshot reads proceed without a session).
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        string key = keys.BuildRowKey(rowId);

        // Gated on isolation + mode only, NOT on tx.Locking: a Serializable+RW read takes this shared
        // predicate lock even when tx.Locking is Optimistic (that is the Serializable+Optimistic hybrid —
        // optimistic writes, but predicate locks still keep the read phantom-free).
        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await locks.AcquireSharedPointLockAsync(tx, keys.RowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        BranchKvValue probe = await branch.ProbeRaw(tx.TransactionId, tx.ReadTimestamp, key, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (probe.Kind == BranchKvKind.Tombstone)
            return null;   // explicitly deleted at this level

        if (probe.HasPayload)
            return probe.Payload;              // found at this level

        // Miss at level-0: walk ancestry levels until a hit or a tombstone (stop walking).
        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in branch.Levels)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKey = ancestorKeys.BuildRowKey(rowId);

            probe = await ancestorReader.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKey, cancellationToken).ConfigureAwait(false);
            if (probe.Kind == BranchKvKind.Tombstone)
                return null;

            if (probe.HasPayload)
                return probe.Payload;
        }

        return null;
    }

    /// <summary>
    /// Batch point-read for a list of row ids. Returns one entry per input id in the same order:
    /// non-null = the raw serialized row bytes; null = row not found (an index entry points at an
    /// absent or deleted row — callers must warn-and-skip).
    ///
    /// The batch is issued as a single <c>LocateAndTryGetManyValues</c> call so all N ids are
    /// resolved in one Kahuna round-trip instead of N sequential <see cref="GetRow"/> calls, and the
    /// transaction's <see cref="KvTransaction.ReadTimestamp"/> is forwarded so every fetch reads at
    /// the same snapshot as the index scan that produced the ids.
    ///
    /// <para>Branch ancestry: ids that miss at level-0 are carried forward as input positions and
    /// resolved one bounded batch per ancestry level (see
    /// <see cref="KvBranchReader.ResolveRowsFromAncestorsAsync"/>), not one round trip per id per
    /// level. A page from a branch index scan is full of inherited rows by construction, so this is
    /// the common shape on a branch, not a rare one.</para>
    ///
    /// <para>This is a read-only operation: no locks are acquired and no keys are tracked as
    /// modified. Serializable read-write callers that need shared point locks must use
    /// <see cref="GetRow"/> per entry, or <see cref="GetRowsBatchLockedForMutation"/>.</para>
    /// </summary>
    internal async Task<ReadOnlyMemory<byte>?[]> GetRowsBatch(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        CancellationToken cancellationToken = default)
    {
        if (rowIds.Count == 0)
            return [];

        // Build the Kahuna key list in input order.
        string[] rowKeys = new string[rowIds.Count];
        for (int i = 0; i < rowIds.Count; i++)
            rowKeys[i] = keys.BuildRowKey(rowIds[i]);

        // Register the batch read for read-set folding on the optimistic / TrackAndValidate path; empty
        // coordinatorKey leaves it unregistered (pessimistic / snapshot).
        BranchKvValue[] level0 = await branch.ProbeManyRaw(
            tx.TransactionId,
            tx.ReadTimestamp,
            rowKeys,
            tx.FoldReads ? tx.CoordinatorKey : "",
            "get_rows_batch",
            cancellationToken).ConfigureAwait(false);

        ReadOnlyMemory<byte>?[] output = new ReadOnlyMemory<byte>?[rowIds.Count];

        // Input positions still unanswered after level 0. Kept as positions, not ids, so a repeated id
        // resolves once per position and the output stays aligned with the input.
        List<int>? unresolved = null;

        for (int i = 0; i < rowIds.Count; i++)
        {
            BranchKvValue decoded = level0[i];

            if (decoded.Kind == BranchKvKind.Tombstone)
            {
                output[i] = null;
                continue;
            }

            if (decoded.HasPayload)
            {
                // Cast the value branch to the nullable type explicitly. Without it, the bare `null`
                // literal binds to byte[] via ReadOnlyMemory's implicit array conversion, making the
                // whole conditional a non-nullable (empty) ReadOnlyMemory<byte> — so a miss would
                // surface as an empty present value instead of null.
                output[i] = (ReadOnlyMemory<byte>?)decoded.Payload;
                continue;
            }

            if (!branch.IsBranch)
            {
                output[i] = null;
                continue;
            }

            (unresolved ??= []).Add(i);
        }

        if (unresolved is not null)
            await branch.ResolveRowsFromAncestorsAsync(rowIds, unresolved, output, cancellationToken).ConfigureAwait(false);

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
    /// <see cref="CamusDBOptions.LockEscalationThreshold"/>); only the reads are batched into one round
    /// trip. The subsequent exclusive batch write then upgrades/covers these same keys, exactly as the
    /// per-row <see cref="GetRow"/> path already did.
    /// </para>
    ///
    /// <para>
    /// A read-only caller that has no range lock to fall back on uses this method too, for the same
    /// reason: a join leaf scan holds no range lock at all, so the per-row shared point locks are its
    /// only Serializable protection and batching the reads must not drop them. The name records where
    /// the requirement first arose, not an exclusive caller.
    /// </para>
    /// </summary>
    internal async Task<ReadOnlyMemory<byte>?[]> GetRowsBatchLockedForMutation(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        CancellationToken cancellationToken = default)
    {
        if (rowIds.Count == 0)
            return [];

        // Serializable+RW acquires the shared point locks first (session must be open before locking);
        // other transaction types skip locking, exactly as GetRow does.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (KvRangeLockManager.IsSerializableReadWrite(tx))
        {
            for (int i = 0; i < rowIds.Count; i++)
                await locks.AcquireSharedPointLockAsync(tx, keys.RowBucketPrefix, keys.BuildRowKey(rowIds[i]), cancellationToken).ConfigureAwait(false);
        }

        return await GetRowsBatch(tx, rowIds, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Full table scan. Yields every (rowId, rowBytes) pair in ascending rowId order
    /// (ObjectId hex is time-ordered and fixed-width so natural KV order is correct).
    ///
    /// <para><paramref name="fromRowId"/> (inclusive), <paramref name="afterRowId"/> (exclusive) and
    /// <paramref name="untilRowId"/> (exclusive) bound the scan. They exist so a caller can own one
    /// span of a keyspace partitioned across workers and read only that span; without them every
    /// worker would scan the whole table and discard, which costs exactly the parallelism the
    /// partitioning bought. All three are pushed down as key bounds so the store <em>seeks</em> to the
    /// span rather than returning rows this method then drops — the difference between each worker
    /// reading its own slice and each worker re-reading the table from its start. Bounds compare hex
    /// ordinally, which matches <see cref="ObjectIdValue"/> ordering because <c>ObjectId.ToString</c>
    /// writes fixed-width big-endian hex.</para>
    ///
    /// <para>Snapshot pinning and read-set folding follow the transaction; see the class summary. The
    /// shared range lock is not a substitute for folding here — it fires only for Serializable
    /// read-write transactions or under key-range sharding (see
    /// <see cref="KvRangeLockManager.AcquireRowRangeLockAsync"/>), so a Read Committed optimistic scan
    /// would otherwise carry no read protection at all.</para>
    /// </summary>
    internal async IAsyncEnumerable<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> ScanRows(
        KvTransaction tx,
        long? maxRows = null,
        ObjectIdValue? afterRowId = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        ObjectIdValue? untilRowId = null,
        ObjectIdValue? fromRowId = null)
    {
        if (maxRows is <= 0)
            yield break;

        // Two lower bounds with different meanings, deliberately kept separate.
        //
        // fromRowId is INCLUSIVE and describes a partition: it is the first row id that belongs to this
        // caller's slice of the keyspace. afterRowId is EXCLUSIVE and describes progress: it is the last
        // row already dealt with. Collapsing them into one exclusive bound loses the row that sits
        // exactly on a partition edge — the slice below it excludes it as its end, the slice above
        // excludes it as its start, and nothing ever visits it.
        //
        // untilRowId is EXCLUSIVE, so a partition is [from, until) and adjacent partitions share an
        // endpoint without overlapping. Rows arrive in ascending order on both the root and branch
        // paths, so reaching the upper bound ends the scan rather than filtering the tail: a caller that
        // owns one span must not read its neighbours' rows, or every worker reads the whole table.
        string? untilHex = untilRowId?.ToString();
        string? fromHex = fromRowId?.ToString();

        // Resume position as ordinal-comparable hex. Ordinal hex comparison matches ObjectId ordering
        // because ObjectId.ToString writes all three unsigned segments as fixed-width big-endian hex,
        // which is the same equivalence the partition bounds above rely on.
        string? afterHex = afterRowId?.ToString();

        // All three bounds are pushed into the KV scan instead of being applied to what comes back.
        // The store seeks to the start key, so a worker that owns one slice reads only that slice;
        // filtering client-side made every slice re-read the prefix from its beginning, so a table
        // divided into N slices was read N times over. Ordinal comparison on the whole key is the
        // same comparison as on the hex suffix, since every row key shares the row key prefix.
        //
        // The two lower bounds collapse into the single one the store accepts: whichever is higher
        // wins, and on a tie the exclusive progress cursor does, because the row it names has already
        // been dealt with.
        string? startHex = fromHex;
        bool startInclusive = true;

        if (afterHex is not null && (startHex is null || string.CompareOrdinal(afterHex, startHex) >= 0))
        {
            startHex = afterHex;
            startInclusive = false;
        }

        string rowKeyPrefix = keys.RowKeyPrefix;
        string? scanStartKey = startHex is not null ? rowKeyPrefix + startHex : null;
        string? scanEndKey = untilHex is not null ? rowKeyPrefix + untilHex : null;

        // Open the deferred Kahuna session before reading tx.TransactionId below: the scan must run
        // under the transaction's own identity, and a tracked scan can only fold reads once the
        // session exists (FoldReads is false while TransactionId is Zero). No-op for eager,
        // read-only, and already-started transactions.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (!branch.IsBranch)
        {
            // Root database (no ancestry): stream directly without materialization.
            long emitted = 0;
            int prefixLen = rowKeyPrefix.Length;

            // Register the scan for read-set folding whenever the transaction folds reads at all
            // (optimistic / TrackAndValidate); the empty coordinatorKey leaves every scanned row
            // out of the commit-time read set for transactions guarded by locks instead.
            string scanCoordinatorKey = tx.FoldReads ? tx.CoordinatorKey : "";
            TransactionOperationId scanOperationId = scanCoordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                keys.RowBucketPrefix,
                scanStartKey, startInclusive,
                // The upper bound is exclusive when there is one; with no end key the flag carries no
                // meaning, and true is what every other unbounded scan in the engine passes.
                scanEndKey, scanEndKey is null,
                KvStoreConstants.DefaultPageSize,
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
                // Every key the scan returns already lies within the bounds; they are enforced once,
                // by the store, and deliberately not re-checked here.
                ObjectIdValue rowId = ObjectId.ToValue(key.AsSpan(prefixLen));

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

            BranchLevel[] levels = branch.Levels;
            int levelCount = 1 + levels.Length;
            IAsyncEnumerator<(string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload)>[] iters = new IAsyncEnumerator<(string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload)>[levelCount];

            // Every level is bounded identically, and each level builds the bound keys in its own
            // namespace. Applying the same bounds everywhere is what keeps nearest-wins intact: a
            // tombstone and the value it suppresses share a row id, so they are either both inside
            // the bounds or both outside — a bound can never admit one and drop the other.
            iters[0] = branch.ScanRowsRawAsync(
                tx.TransactionId,
                tx.ReadTimestamp,
                startHex,
                startInclusive,
                untilHex,
                cancellationToken,
                tx.FoldReads ? tx.CoordinatorKey : ""
            ).GetAsyncEnumerator(cancellationToken);

            for (int ai = 0; ai < levels.Length; ai++)
            {
                (KvKeyBuilder _, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) = levels[ai];

                iters[ai + 1] = ancestorReader.ScanRowsRawAsync(
                    HLCTimestamp.Zero,
                    forkTimestamp,
                    startHex,
                    startInclusive,
                    untilHex,
                    cancellationToken
                ).GetAsyncEnumerator(cancellationToken);
            }

            BranchMetrics.RecordScanIterators(levels.Length);

            // Priority key: (rowIdHex ordinal-ascending, levelIndex ascending) so ties go to the nearest level.
            PriorityQueue<(int level, string hex, BranchKvKind kind, ReadOnlyMemory<byte>? payload),
                          (string hex, int level)> heap = new(
                Comparer<(string hex, int level)>.Create(static (a, b) =>
                {
                    int c = string.CompareOrdinal(a.hex, b.hex);
                    return c != 0 ? c : a.level.CompareTo(b.level);
                }));

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

                    // No bound checks here: every level was scanned within the same bounds, so the
                    // heap only ever holds keys that already satisfy them.
                    if (rowIdHex != lastHex)
                    {
                        lastHex = rowIdHex;

                        // Nearest level wins for this rowIdHex.
                        if (kind == BranchKvKind.Value && payload is not null)
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
    /// Writes a row: takes the exclusive key lock then sets the value. Insert and update share this
    /// path because the KV store overwrites either way.
    ///
    /// <para>If this Serializable+RW transaction already holds a shared point lock on the key
    /// (acquired during a preceding <see cref="GetRow"/>), it is promoted to exclusive first so no new
    /// reader can acquire a shared lock on the key between now and commit. The exclusive key lock is
    /// still taken afterwards: the per-key write intent it sets is what drives the 2PC commit path,
    /// while the exclusive range lock drives predicate/reader exclusion. Two different Kahuna
    /// mechanisms, each load-bearing for a different invariant.</para>
    ///
    /// <para>The upgrade is gated on Serializable+RW, NOT on <see cref="KvTransaction.Locking"/>: even
    /// an optimistic transaction upgrades its retained shared predicate lock here, which is what keeps
    /// the hybrid's read-then-write phantom-free while the write itself skips the explicit exclusive
    /// lock.</para>
    /// </summary>
    internal async Task WriteRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken)
    {
        tx.ReserveMutations(1);
        string key = keys.BuildRowKey(rowId);

        if (KvRangeLockManager.IsSerializableReadWrite(tx) && KvRangeLockManager.HasSharedPointLock(tx, keys.RowBucketPrefix, key))
            await locks.UpgradeToExclusivePointLockAsync(tx, keys.RowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await locks.AcquireExclusiveKeyLockAsync(tx, key, cancellationToken).ConfigureAwait(false);

        byte[] encodedData = BranchKvCodec.EncodeValue(data);

        (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "row write", key,
            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, encodedData, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Deletes a row.
    ///
    /// For root databases, issues a physical KV delete — the key is gone.
    /// For branch databases, writes a <see cref="BranchKvKind.Tombstone"/> to the level-0
    /// overlay instead of deleting the key.  A physical delete would leave no level-0 entry,
    /// which would let the ancestry merge surface the row again from an ancestor namespace.
    /// The tombstone is the "deleted in this branch" signal that the merge respects.
    /// </summary>
    internal async Task DeleteRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        string key = keys.BuildRowKey(rowId);

        tx.ReserveMutations(1);

        if (KvRangeLockManager.IsSerializableReadWrite(tx) && KvRangeLockManager.HasSharedPointLock(tx, keys.RowBucketPrefix, key))
            await locks.UpgradeToExclusivePointLockAsync(tx, keys.RowBucketPrefix, key, cancellationToken).ConfigureAwait(false);

        await locks.AcquireExclusiveKeyLockAsync(tx, key, cancellationToken).ConfigureAwait(false);

        if (branch.IsBranch)
        {
            // Branch: write a tombstone so the ancestry merge suppresses the inherited row.
            (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "row delete (branch tombstone)", key,
                (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type != KeyValueResponseType.Set)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteRow failed for key {key}: {type}");
        }
        else
        {
            // Root: physical delete — no ancestor namespace to suppress.
            (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "row delete", key,
                (coordinatorKey, operationId) => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, key, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {key}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteRow failed for key {key}: {type}");
        }

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }
}
