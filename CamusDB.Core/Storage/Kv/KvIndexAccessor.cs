/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The secondary-index access paths for one table: unique point lookups, ordered range scans, and
/// the single-entry write and delete.
///
/// <para><b>Entry shape.</b> A unique entry's key is <c>{bucket}/{encodedKey}</c> and its value is
/// <c>rowId (24 bytes) ‖ includeTuple</c>. A non-unique entry appends the row-id hex to the key with
/// no separator, so the row id is sliced off by fixed width. The include tuple is the stored/payload
/// column set of a covering index; it is empty for a plain index, which keeps such entries
/// byte-identical to the historical rowId-only form.</para>
///
/// <para><b>Bounds are enforced twice, on purpose.</b> The encoded bounds pushed into the KV scan are
/// deliberately widened — with <see cref="KvStoreConstants.IndexKeySentinel"/> — whenever a stored key
/// can extend past the bound, which happens for every non-unique index and for any prefix bound, a
/// unique index included. Widening is always safe because the authoritative filter is the
/// <see cref="ComparePrefix"/> check on the <em>decoded</em> key, which trims whatever the raw bound
/// over-reads. That second check is load-bearing rather than defensive: when the planner absorbs a
/// predicate into the scan, the executor does not re-apply it.</para>
///
/// <para><b>Uniqueness is enforced by the KV layer, not by a read-then-write.</b> A unique entry is
/// written with <c>SetIfNotExists</c> so two racing inserts cannot both succeed; the loser surfaces
/// <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>. On a branch that flag alone is wrong in
/// two ways, and <see cref="KvBranchReader.ResolveBranchUniqueFlagsAsync"/> resolves the correct one
/// after the lock is held.</para>
/// </summary>
internal sealed class KvIndexAccessor
{
    private readonly IKahuna kahuna;
    private readonly KvKeyBuilder keys;
    private readonly KvRangeLockManager locks;
    private readonly KvBranchReader branch;
    private readonly KahunaRetryPolicy retry;

    internal KvIndexAccessor(
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
    /// Point-read a unique index entry. Returns the rowId the encoded key maps to, or
    /// <c>null</c> if no entry exists (the key is absent).
    ///
    /// Passes <see cref="KvTransaction.ReadTimestamp"/> to Kahuna so that a Serializable
    /// read-only transaction observes the same snapshot here as a row read does.
    ///
    /// In a Serializable read-write transaction a shared point lock is acquired on the index
    /// entry before the read and held until commit, preventing a concurrent writer from
    /// inserting or deleting the same index key while this transaction is live.
    /// </summary>
    internal async Task<ObjectIdValue?> LookupUnique(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        // Serializable+RW acquires a shared point lock on the index entry before reading.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        // A lookup key that cannot be encoded (e.g. an Id equality against a non-ObjectId literal)
        // can match no stored row — return a miss rather than throwing on the invalid value.
        if (!keys.TryBuildUniqueIndexKey(indexId, key, out string kvKey))
            return null;

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await locks.AcquireSharedPointLockAsync(tx, keys.BuildIndexBucketPrefix(indexId), kvKey, cancellationToken).ConfigureAwait(false);

        BranchKvValue idx = await branch.ProbeRaw(tx.TransactionId, tx.ReadTimestamp, kvKey, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (idx.Kind == BranchKvKind.Tombstone)
            return null;   // tombstone at this level

        if (idx.HasPayload)
            return RowIdFromPayload(idx.Payload.Span);

        // Miss at level-0: walk ancestry.
        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in branch.Levels)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKvKey = ancestorKeys.BuildUniqueIndexKey(indexId, key);
            idx = await ancestorReader.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);
            if (idx.Kind == BranchKvKind.Tombstone) return null;
            if (idx.HasPayload) return RowIdFromPayload(idx.Payload.Span);
        }

        return null;
    }

    /// <summary>
    /// Point-reads a unique index entry <b>outside any transaction</b>: it acquires no lock, folds
    /// nothing into a read set, and starts no session. Returns the row id, or null when the key is
    /// absent.
    ///
    /// <para>For introspection only — today, resolving a primary key to its row id so
    /// <c>SHOW RANGE … FOR ROW</c> can name the row's KV key. Never use it for DML or for anything
    /// whose result a transaction may act on: it reads at the advisory read-committed context
    /// (<see cref="HLCTimestamp.Zero"/>), so it does not see the caller's own uncommitted writes and
    /// gives no isolation guarantee at all.</para>
    ///
    /// <para><b>The lock-free part is the point, not an optimization.</b> Going through
    /// <see cref="LookupUnique"/> instead would take a shared point lock under serializable
    /// read-write, and that observation would join the transaction's read set — so an inspection
    /// statement could decide whether the surrounding transaction commits. A <c>SHOW</c> that can
    /// abort its caller's transaction is a bug users would spend a long time not believing.</para>
    ///
    /// <para>Ancestry is still walked, so a branched database answers for rows it inherited rather
    /// than reporting them missing.</para>
    /// </summary>
    internal async Task<ObjectIdValue?> LookupUniqueUntracked(
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        if (!keys.TryBuildUniqueIndexKey(indexId, key, out string kvKey))
            return null;

        BranchKvValue idx = await branch.ProbeRaw(HLCTimestamp.Zero, HLCTimestamp.Zero, kvKey, cancellationToken).ConfigureAwait(false);
        if (idx.Kind == BranchKvKind.Tombstone)
            return null;

        if (idx.HasPayload)
            return RowIdFromPayload(idx.Payload.Span);

        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in branch.Levels)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKvKey = ancestorKeys.BuildUniqueIndexKey(indexId, key);
            idx = await ancestorReader.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);
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
    internal async Task<(ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)?> LookupUniqueCovering(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        if (!keys.TryBuildUniqueIndexKey(indexId, key, out string kvKey))
            return null;

        if (tx.IsolationLevel == CamusIsolationLevel.Serializable && tx.TransactionMode == CamusTransactionMode.ReadWrite)
            await locks.AcquireSharedPointLockAsync(tx, keys.BuildIndexBucketPrefix(indexId), kvKey, cancellationToken).ConfigureAwait(false);

        BranchKvValue idx = await branch.ProbeRaw(tx.TransactionId, tx.ReadTimestamp, kvKey, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "").ConfigureAwait(false);
        if (idx.Kind == BranchKvKind.Tombstone) return null;
        if (idx.HasPayload) return (RowIdFromPayload(idx.Payload.Span), IncludeTupleFromPayload(idx.Payload));

        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in branch.Levels)
        {
            BranchMetrics.RecordAncestorProbe();
            string ancestorKvKey = ancestorKeys.BuildUniqueIndexKey(indexId, key);
            idx = await ancestorReader.ProbeRaw(HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);
            if (idx.Kind == BranchKvKind.Tombstone) return null;
            if (idx.HasPayload) return (RowIdFromPayload(idx.Payload.Span), IncludeTupleFromPayload(idx.Payload));
        }

        return null;
    }

    /// <summary>
    /// Ordered scan over a secondary index within optional inclusive bounds [from, to].
    /// Yields (decodedKey, rowId, includeTuple) triples in ascending encoded-key order.
    ///
    /// <paramref name="keyTypes"/> must match the column types of the index key in order.
    /// When <paramref name="unique"/> is false the stored key has the rowId hex (24 chars)
    /// appended directly to the encoded key (no separator); the rowId is stripped before decoding.
    ///
    /// Passes <see cref="KvTransaction.ReadTimestamp"/> to Kahuna so the scan is pinned to the same
    /// consistent snapshot as the point reads across all pages, and folds reads according to
    /// <see cref="KvTransaction.FoldReads"/> — see <see cref="KvRowAccessor.ScanRows"/> for why
    /// folding is a transaction property rather than a call-site choice.
    /// </summary>
    internal async IAsyncEnumerable<(CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)> ScanIndex(
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

        // Open the deferred Kahuna session before reading tx.TransactionId below: the scan must run
        // under the transaction's own identity, and a tracked scan can only fold reads once the
        // session exists. No-op for eager, read-only, and already-started transactions.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        long emitted = 0;
        string bucketPrefix = keys.BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";
        int prefixLen = keyPrefix.Length;

        OrderType[]? directions = keys.DirectionsOf(indexId);

        string? fromEncoded = from is not null ? KeyEncoder.Encode(from, directions) : null;
        string? toEncoded   = to   is not null ? KeyEncoder.Encode(to, directions)   : null;

        // Push bounds into the scan. The end key needs IndexKeySentinel whenever a stored key can
        // extend *past* the encoded bound, which happens two ways:
        //   • non-unique index: the stored key is {encodedKey}{rowIdHex24}, so every value carries
        //     rowId suffixes that sort after {encodedKey};
        //   • prefix bound: the bound pins only the leading key columns, so real keys carry the
        //     remaining columns' encodings after it — true for unique indexes too. Without the
        //     sentinel a prefix bound on a unique index excludes every matching row (the range
        //     [enc(prefix), enc(prefix)] contains no key of the form enc(prefix)+enc(rest)).
        // Widening here is always safe: the decoded-key ComparePrefix filter below is authoritative
        // and trims whatever the raw bound over-reads.
        bool fromIsPrefixBound = from is not null && from.Values.Length < keyTypes.Length;
        bool toIsPrefixBound   = to   is not null && to.Values.Length   < keyTypes.Length;

        // Start bound: an exclusive lower bound ('>') must also clear every stored key that extends
        // the encoded bound (the rowId suffix on a non-unique index, the remaining columns under a
        // prefix bound). A bare exclusive start still admits those keys — they sort after the bound —
        // and the decoded ComparePrefix filter below then discards them, so the over-read is invisible
        // in the results. It is NOT harmless to the raw window: those keys belong to the adjacent
        // ('<= V') predicate's range, and a foreign transaction holding an exclusive range lock there
        // has planted write intents on them. A scan page that includes such a key cannot settle until
        // that transaction finishes, so two deliberately disjoint predicates ('<= V' and '> V')
        // serialize against each other for the whole intent lease. Appending IndexKeySentinel pushes
        // the start past every extension of the bound value — the exact rule
        // <see cref="KvRangeLockManager.AcquireBoundedIndexRangeLockAsync"/> applies to the lock's own
        // start bound — so the raw scan window is as disjoint as the lock that protects it.
        string? startKey = fromEncoded is not null
            ? ((unique && !fromIsPrefixBound) || fromInclusive ? keyPrefix + fromEncoded : keyPrefix + fromEncoded + KvStoreConstants.IndexKeySentinel)
            : null;
        string? endKey   = toEncoded   is not null
            ? (unique && !toIsPrefixBound ? keyPrefix + toEncoded : keyPrefix + toEncoded + KvStoreConstants.IndexKeySentinel)
            : null;

        if (!branch.IsBranch)
        {
            // Root database: stream directly without materialization. Register for read-set folding
            // whenever the transaction folds reads at all (optimistic / TrackAndValidate); empty
            // coordinatorKey = unregistered.
            string scanCoordinatorKey = tx.FoldReads ? tx.CoordinatorKey : "";
            TransactionOperationId scanOperationId = scanCoordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

            await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                bucketPrefix,
                startKey, fromInclusive,
                endKey, toInclusive,
                KvStoreConstants.DefaultPageSize,
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
                    if (suffix.Length < KvStoreConstants.RowIdHexLength)
                        continue;

                    encodedKey = suffix[..^KvStoreConstants.RowIdHexLength].ToString();
                    rowId = ObjectId.ToValue(suffix[^KvStoreConstants.RowIdHexLength..]);
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

            BranchLevel[] levels = branch.Levels;
            int levelCount = 1 + levels.Length;
            var iters = new IAsyncEnumerator<(string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload)>[levelCount];

            iters[0] = branch.ScanIndexRawAsync(tx.TransactionId, tx.ReadTimestamp, indexId, fromEncoded, fromInclusive, toEncoded, toInclusive, unique, cancellationToken, tx.FoldReads ? tx.CoordinatorKey : "", toIsPrefixBound, fromIsPrefixBound).GetAsyncEnumerator(cancellationToken);
            for (int ai = 0; ai < levels.Length; ai++)
            {
                (KvKeyBuilder _, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) = levels[ai];
                iters[ai + 1] = ancestorReader.ScanIndexRawAsync(HLCTimestamp.Zero, forkTimestamp, indexId, fromEncoded, fromInclusive, toEncoded, toInclusive, unique, cancellationToken, "", toIsPrefixBound, fromIsPrefixBound).GetAsyncEnumerator(cancellationToken);
            }

            BranchMetrics.RecordScanIterators(levels.Length);

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
                            else if (suffix.Length >= KvStoreConstants.RowIdHexLength)
                            {
                                encKey = suffix[..^KvStoreConstants.RowIdHexLength];
                                rowId = ObjectId.ToValue(suffix[^KvStoreConstants.RowIdHexLength..]);
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
    /// <see cref="CamusDBException"/> with <c>DuplicateUniqueKeyValue</c> if the entry already exists.
    /// Set <paramref name="backfillMode"/> to <c>true</c> during coordinator-driven backfill:
    /// a <c>NotSet</c> response on a unique index triggers a read-back check — if the
    /// existing entry maps to the same rowId (idempotent re-run after leader change) the
    /// write is silently skipped; if it maps to a different rowId, a genuine duplicate is
    /// detected and <see cref="CamusDBException"/> is thrown.
    /// </summary>
    internal async Task PutIndexEntry(
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
            ? keys.BuildUniqueIndexKey(indexId, key)
            : keys.BuildNonUniqueIndexKey(indexId, key, rowId);

        byte[] value = BranchKvCodec.EncodeIndexRowId(rowId, includeTuple ?? default);

        tx.ReserveMutations(1);

        string indexBucketPrefix = keys.BuildIndexBucketPrefix(indexId);
        if (KvRangeLockManager.IsSerializableReadWrite(tx) && KvRangeLockManager.HasSharedPointLock(tx, indexBucketPrefix, kvKey))
            await locks.UpgradeToExclusivePointLockAsync(tx, indexBucketPrefix, kvKey, cancellationToken).ConfigureAwait(false);

        await locks.AcquireExclusiveKeyLockAsync(tx, kvKey, cancellationToken).ConfigureAwait(false);

        // For unique entries on branch databases, SetIfNotExists alone is not sufficient:
        // (a) a level-0 tombstone causes NotSet, blocking re-insert of a slot that was
        //     deliberately cleared (tombstone-replace must succeed), and
        // (b) SetIfNotExists only checks level-0, so an ancestor value for a different row
        //     goes undetected, creating a cross-lineage duplicate.
        // ResolveBranchUniqueFlagsAsync probes level-0 and ancestry post-lock so it sees
        // the committed state after any lock contention resolved, then returns the correct
        // write flag or throws DuplicateUniqueKeyValue.
        KeyValueFlags flags;
        if (unique && branch.IsBranch)
            flags = await branch.ResolveBranchUniqueFlagsAsync(tx, indexId, key, kvKey, rowId, cancellationToken).ConfigureAwait(false);
        else
            flags = unique ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;

        (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "index entry write", kvKey,
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
                (_, ReadOnlyKeyValueEntry? existing) = await KahunaRetryPolicy.RetryOnMustRetry(
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
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(indexId)}'");
                // else: same rowId — idempotent re-write on resume, continue
            }
            else
            {
                throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(indexId)}'");
            }
        }

        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"PutIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Removes a secondary index entry.
    ///
    /// For root databases, issues a physical KV delete.
    /// For branch databases, writes a <see cref="BranchKvKind.Tombstone"/> to the level-0
    /// overlay so that <see cref="LookupUnique"/> and <see cref="ScanIndex"/> suppress the
    /// inherited index entry in the ancestry merge — the same reason
    /// <see cref="KvRowAccessor.DeleteRow"/> writes a tombstone rather than a physical delete
    /// on branch stores.
    /// </summary>
    internal async Task DeleteIndexEntry(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        ObjectIdValue rowId,
        bool unique,
        CancellationToken cancellationToken = default)
    {
        string kvKey = unique
            ? keys.BuildUniqueIndexKey(indexId, key)
            : keys.BuildNonUniqueIndexKey(indexId, key, rowId);

        tx.ReserveMutations(1);

        string indexBucketPrefixDel = keys.BuildIndexBucketPrefix(indexId);
        if (KvRangeLockManager.IsSerializableReadWrite(tx) && KvRangeLockManager.HasSharedPointLock(tx, indexBucketPrefixDel, kvKey))
            await locks.UpgradeToExclusivePointLockAsync(tx, indexBucketPrefixDel, kvKey, cancellationToken).ConfigureAwait(false);

        await locks.AcquireExclusiveKeyLockAsync(tx, kvKey, cancellationToken).ConfigureAwait(false);

        if (branch.IsBranch)
        {
            // Branch: write a tombstone so the ancestry merge suppresses the inherited index entry.
            (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "index entry delete (branch tombstone)", kvKey,
                (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type != KeyValueResponseType.Set)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");
        }
        else
        {
            // Root: physical delete.
            (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "index entry delete", kvKey,
                (coordinatorKey, operationId) => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, kvKey, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                cancellationToken
            ).ConfigureAwait(false);

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Write conflict on {kvKey}; a concurrent transaction holds a lock — retry the operation from BeginAsync");

            if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");
        }

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

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
}
