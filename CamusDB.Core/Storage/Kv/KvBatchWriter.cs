/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The set-based write paths for one table: mass insert, mass update, mass delete, and the bulk
/// key-space purges that DROP INDEX and a branch DROP TABLE need.
///
/// <para><b>Why these exist alongside the per-row paths.</b> Each batch costs two Kahuna round trips
/// for the whole set — one <see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> and one
/// set/delete — instead of an acquire plus a write per key. Per-key semantics are unchanged: unique
/// index entries still use <c>SetIfNotExists</c>, and a <c>NotSet</c> on one still raises
/// <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>.</para>
///
/// <para><b>Root and branch databases take different phase-2 shapes.</b> A root database deletes old
/// index entries physically and writes everything else in one batch. A branch writes a tombstone
/// where a root would delete — a physical delete leaves no level-0 entry, so the ancestry merge would
/// resurface the inherited row — and writes each <em>unique</em> index entry individually, because
/// <c>SetIfNotExists</c> alone is wrong on a branch twice over: a level-0 tombstone makes it refuse a
/// slot that was deliberately cleared, and it never sees an ancestor value, so a cross-lineage
/// duplicate would go undetected. <see cref="KvBranchReader.ResolveBranchUniqueFlagsBatchAsync"/>
/// resolves the correct flag per entry, and it runs only after every lock of the batch is held.</para>
///
/// <para><b>Retry identity.</b> Each retry loop binds one operation id to its current pending
/// declaration. The id is reused while the pending set is unchanged — that is an ack-loss resend of
/// an identical batch, which the coordinator can only replay idempotently under the same id — and a
/// fresh id is minted only when the set actually shrinks. Resending an identical unique batch under a
/// new id would let an already-staged <c>SetIfNotExists</c> read back as a false duplicate and strand
/// the original pending operation, blocking finalize drain.</para>
/// </summary>
internal sealed class KvBatchWriter
{
    private readonly IKahuna kahuna;
    private readonly ILogger logger;
    private readonly KvKeyBuilder keys;
    private readonly KvBranchReader branch;
    private readonly KahunaRetryPolicy retry;
    private readonly KvConflictMessageBuilder messages;

    /// <summary>Configuration snapshot; swapped atomically by <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    internal KvBatchWriter(
        IKahuna kahuna,
        ILogger logger,
        KvKeyBuilder keys,
        KvBranchReader branch,
        KahunaRetryPolicy retry,
        KvConflictMessageBuilder messages,
        CamusDBOptions options)
    {
        this.kahuna = kahuna;
        this.logger = logger;
        this.keys = keys;
        this.branch = branch;
        this.retry = retry;
        this.messages = messages;
        this.options = options;
    }

    /// <summary>Swaps in a newly published configuration snapshot. See <see cref="KvTableStore.ApplyOptions"/>.</summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    // -----------------------------------------------------------------------
    // Batched write path (mass insert)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Writes many rows (and their index entries) using two Kahuna round-trips for the whole
    /// batch — one <see cref="IKahuna.LocateAndTryAcquireManyExclusiveLocks"/> and one
    /// <see cref="IKahuna.LocateAndTrySetManyKeyValue"/> — instead of an acquire+set per key.
    ///
    /// Preserves the per-key semantics of the single-row write and index-entry write paths:
    /// unique index entries use <c>SetIfNotExists</c> and a <c>NotSet</c> result raises a
    /// duplicate-key error. All keys in a batch are distinct; a repeated <em>unique</em> key
    /// means a duplicate unique value within the same insert and is rejected up-front.
    /// </summary>
    internal async Task WriteRowsBatch(KvTransaction tx, IReadOnlyList<KvTableStore.RowWrite> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        // Ensure the Kahuna session is open before building request items that embed
        // tx.TransactionId — the items are built into batch lists before AcquireManyWithRetry
        // runs, so a deferred-start session must start here rather than inside that call.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        bool isBranch = branch.IsBranch;

        // Collect lock keys and — for root databases — the batch write items in one pass.
        // Branch databases require per-item writes for unique entries (see branch phase below),
        // so their write items are built during the write phase, not here.
        // Pre-size to the row-count floor: every row contributes at least its own row key (plus any
        // index entries), so this avoids the first few list regrowths for the common no-/few-index case.
        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys = new(rows.Count);
        List<KahunaSetKeyValueRequestItem> setItems = new(rows.Count);   // root only
        Dictionary<string, bool> uniqueByKey = new();       // root only
        HashSet<string> seenUnique = [];                    // within-batch duplicate guard (both paths)

        foreach (KvTableStore.RowWrite row in rows)
        {
            string rowKey = keys.BuildRowKey(row.RowId);
            byte[] rowValue = row.RowData;   // already the enveloped storage value (EncodeStorageValue)
            lockKeys.Add((rowKey, 0, KeyValueDurability.Persistent));

            if (!isBranch)
            {
                uniqueByKey[rowKey] = false;
                setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
            }

            // Indexed loop: foreach over the IReadOnlyList interface boxes the list enumerator
            // on the heap once per row; same for the sibling batch loops below.
            IReadOnlyList<KvTableStore.IndexWrite>? indexEntries = row.IndexEntries;
            for (int e = 0; indexEntries is not null && e < indexEntries.Count; e++)
            {
                KvTableStore.IndexWrite ix = indexEntries[e];
                string kvKey = ix.Unique
                    ? keys.BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : keys.BuildNonUniqueIndexKey(ix.IndexId, ix.Key, row.RowId);
                lockKeys.Add((kvKey, 0, KeyValueDurability.Persistent));

                if (ix.Unique && !seenUnique.Add(kvKey))
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(ix.IndexId)}'");

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

            // Every unique entry of the batch is resolved first, in one level-0 probe plus one probe
            // per ancestry level, so the write loop below never waits on a per-entry round trip. The
            // locks are all held at this point, which is what the resolution requires.
            List<KvBranchReader.BranchUniqueFlagRequest> flagRequests = [];

            foreach (KvTableStore.RowWrite row in rows)
            {
                IReadOnlyList<KvTableStore.IndexWrite>? uniqueCandidates = row.IndexEntries;
                for (int e = 0; uniqueCandidates is not null && e < uniqueCandidates.Count; e++)
                {
                    KvTableStore.IndexWrite ix = uniqueCandidates[e];

                    if (ix.Unique && !ix.Overwrite)
                        flagRequests.Add(new KvBranchReader.BranchUniqueFlagRequest(ix.IndexId, ix.Key, keys.BuildUniqueIndexKey(ix.IndexId, ix.Key), row.RowId));
                }
            }

            Dictionary<string, KeyValueFlags> uniqueFlags =
                await branch.ResolveBranchUniqueFlagsBatchAsync(tx, flagRequests, cancellationToken).ConfigureAwait(false);

            foreach (KvTableStore.RowWrite row in rows)
            {
                string rowKey = keys.BuildRowKey(row.RowId);
                byte[] rowValue = row.RowData;   // already the enveloped storage value (EncodeStorageValue)
                batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                batchByKey[rowKey] = false;

                IReadOnlyList<KvTableStore.IndexWrite>? indexEntries = row.IndexEntries;
                for (int e = 0; indexEntries is not null && e < indexEntries.Count; e++)
                {
                    KvTableStore.IndexWrite ix = indexEntries[e];
                    string kvKey = ix.Unique
                        ? keys.BuildUniqueIndexKey(ix.IndexId, ix.Key)
                        : keys.BuildNonUniqueIndexKey(ix.IndexId, ix.Key, row.RowId);
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
                        KeyValueFlags flags = uniqueFlags[kvKey];

                        (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "unique index entry write", kvKey,
                            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                            cancellationToken
                        ).ConfigureAwait(false);

                        if (type == KeyValueResponseType.NotSet)
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(ix.IndexId)}'");
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
        tx.TrackModifiedRange(lockKeys, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Batched update path (mass UPDATE)
    // -----------------------------------------------------------------------

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
    /// batch, and unique new entries are written individually after a post-lock ancestry probe
    /// (<see cref="KvBranchReader.ResolveBranchUniqueFlagsBatchAsync"/>).</para>
    ///
    /// <para>Unique-index semantics: new unique entries use <c>SetIfNotExists</c>; a
    /// <c>NotSet</c> result surfaces <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>.
    /// Within-batch uniqueness is checked up front (duplicate new unique key → immediate error).
    /// NULL-distinct semantics are the caller's responsibility: entries with NULL key columns
    /// must not be included in either <see cref="KvTableStore.RowUpdate.OldIndexEntries"/> or
    /// <see cref="KvTableStore.RowUpdate.NewIndexEntries"/>.</para>
    ///
    /// <para>Lock keys are deduplicated before acquisition so that a key appearing as both an
    /// old entry for one row and a new entry for another row within the same batch is only locked
    /// once, avoiding a false <c>AlreadyLocked</c> from Kahuna on repeated lock requests.</para>
    /// </summary>
    internal async Task UpdateRowsBatch(KvTransaction tx, IReadOnlyList<KvTableStore.RowUpdate> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        bool isBranch = branch.IsBranch;

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

        foreach (KvTableStore.RowUpdate row in rows)
        {
            string rowKey = keys.BuildRowKey(row.RowId);
            byte[] rowValue = row.NewRowData;   // already the enveloped storage value (EncodeStorageValue)
            AddLockKey(rowKey);

            if (!isBranch)
            {
                uniqueByKey[rowKey] = false;
                setItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
            }

            // Indexed loops: foreach over the IReadOnlyList interface boxes the list enumerator
            // on the heap once per row per loop.
            IReadOnlyList<KvTableStore.IndexDelete>? oldIndexEntries = row.OldIndexEntries;
            for (int e = 0; oldIndexEntries is not null && e < oldIndexEntries.Count; e++)
            {
                KvTableStore.IndexDelete old = oldIndexEntries[e];
                string kvKey = old.Unique
                    ? keys.BuildUniqueIndexKey(old.IndexId, old.Key)
                    : keys.BuildNonUniqueIndexKey(old.IndexId, old.Key, old.RowId);
                AddLockKey(kvKey);

                if (!isBranch)
                    deleteItems.Add(new KahunaDeleteKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Durability = KeyValueDurability.Persistent });
            }

            IReadOnlyList<KvTableStore.IndexWrite>? newIndexEntries = row.NewIndexEntries;
            for (int e = 0; newIndexEntries is not null && e < newIndexEntries.Count; e++)
            {
                KvTableStore.IndexWrite newIx = newIndexEntries[e];
                string kvKey = newIx.Unique
                    ? keys.BuildUniqueIndexKey(newIx.IndexId, newIx.Key)
                    : keys.BuildNonUniqueIndexKey(newIx.IndexId, newIx.Key, row.RowId);

                if (newIx.Unique && !seenUniqueNew.Add(kvKey))
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(newIx.IndexId)}'");

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

            // Same shape as the batch insert path: resolve every unique entry's write flags up front,
            // one probe per ancestry level for the whole batch, with all locks already held.
            List<KvBranchReader.BranchUniqueFlagRequest> flagRequests = [];

            foreach (KvTableStore.RowUpdate row in rows)
            {
                IReadOnlyList<KvTableStore.IndexWrite>? uniqueCandidates = row.NewIndexEntries;
                for (int e = 0; uniqueCandidates is not null && e < uniqueCandidates.Count; e++)
                {
                    KvTableStore.IndexWrite newIx = uniqueCandidates[e];

                    if (newIx.Unique && !newIx.Overwrite)
                        flagRequests.Add(new KvBranchReader.BranchUniqueFlagRequest(newIx.IndexId, newIx.Key, keys.BuildUniqueIndexKey(newIx.IndexId, newIx.Key), row.RowId));
                }
            }

            Dictionary<string, KeyValueFlags> uniqueFlags =
                await branch.ResolveBranchUniqueFlagsBatchAsync(tx, flagRequests, cancellationToken).ConfigureAwait(false);

            foreach (KvTableStore.RowUpdate row in rows)
            {
                string rowKey = keys.BuildRowKey(row.RowId);
                byte[] rowValue = row.NewRowData;   // already the enveloped storage value (EncodeStorageValue)
                batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = rowKey, Value = rowValue, CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                batchByKey[rowKey] = false;

                IReadOnlyList<KvTableStore.IndexDelete>? oldIndexEntries = row.OldIndexEntries;
                for (int e = 0; oldIndexEntries is not null && e < oldIndexEntries.Count; e++)
                {
                    KvTableStore.IndexDelete old = oldIndexEntries[e];
                    string kvKey = old.Unique
                        ? keys.BuildUniqueIndexKey(old.IndexId, old.Key)
                        : keys.BuildNonUniqueIndexKey(old.IndexId, old.Key, old.RowId);
                    batchItems.Add(new KahunaSetKeyValueRequestItem { TransactionId = tx.TransactionId, Key = kvKey, Value = BranchKvCodec.EncodeTombstone(), CompareValue = null, CompareRevision = -1, Flags = KeyValueFlags.Set, ExpiresMs = 0, Durability = KeyValueDurability.Persistent });
                    batchByKey[kvKey] = false;
                }

                IReadOnlyList<KvTableStore.IndexWrite>? newIndexEntries = row.NewIndexEntries;
                for (int e = 0; newIndexEntries is not null && e < newIndexEntries.Count; e++)
                {
                    KvTableStore.IndexWrite newIx = newIndexEntries[e];
                    string kvKey = newIx.Unique
                        ? keys.BuildUniqueIndexKey(newIx.IndexId, newIx.Key)
                        : keys.BuildNonUniqueIndexKey(newIx.IndexId, newIx.Key, row.RowId);
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
                        KeyValueFlags flags = uniqueFlags[kvKey];

                        (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "unique index entry write", kvKey,
                            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
                            cancellationToken
                        ).ConfigureAwait(false);

                        if (type == KeyValueResponseType.NotSet)
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.DuplicateKeyLabel(newIx.IndexId)}'");
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
        tx.TrackModifiedRange(lockKeys, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Batched delete path (mass delete / drop index / drop table)
    // -----------------------------------------------------------------------

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
    internal async Task DeleteRowsBatch(KvTransaction tx, IReadOnlyList<KvTableStore.RowDelete> rows, CancellationToken cancellationToken = default)
    {
        if (rows.Count == 0)
            return;

        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        List<string> deleteKeys = [];

        foreach (KvTableStore.RowDelete row in rows)
        {
            deleteKeys.Add(keys.BuildRowKey(row.RowId));

            IReadOnlyList<KvTableStore.IndexDelete>? indexEntries = row.IndexEntries;
            for (int e = 0; indexEntries is not null && e < indexEntries.Count; e++)
            {
                KvTableStore.IndexDelete ix = indexEntries[e];
                deleteKeys.Add(ix.Unique
                    ? keys.BuildUniqueIndexKey(ix.IndexId, ix.Key)
                    : keys.BuildNonUniqueIndexKey(ix.IndexId, ix.Key, ix.RowId));
            }
        }

        tx.ReserveMutations(deleteKeys.Count);

        if (branch.IsBranch)
        {
            // Branch: write tombstones so the ancestry merge suppresses inherited rows.
            List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys =
                deleteKeys.Select(k => (k, 0, KeyValueDurability.Persistent)).ToList();

            List<KahunaSetKeyValueRequestItem> tombstoneItems = deleteKeys.Select(k => new KahunaSetKeyValueRequestItem
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

            tx.TrackModifiedRange(deleteKeys, KeyValueDurability.Persistent);
        }
        else
        {
            await DeleteKeysBatch(tx, deleteKeys, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deletes every KV entry belonging to the named index. Used by DROP INDEX to reclaim
    /// the <c>{dbId}:{tableId}:i:{indexName}/…</c> space. All deletes run under <paramref name="tx"/>
    /// so they are atomic with the schema-removal that follows in the same transaction.
    /// Returns the number of entries deleted.
    /// </summary>
    internal async Task<int> DropIndexEntries(KvTransaction tx, string indexName, CancellationToken cancellationToken = default)
    {
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        string bucketPrefix = keys.BuildIndexBucketPrefix(indexName);
        string keyPrefix    = bucketPrefix + "/";

        // Collect all raw keys first; deleting during async iteration is unsafe.
        List<string> keysToDelete = [];

        await foreach ((string kvKey, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            bucketPrefix,
            null, true,
            null, true,
            KvStoreConstants.DefaultPageSize,
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
    internal async Task<int> PurgeLocalRowOverlayAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        List<string> keysToDelete = [];

        await foreach ((string kvKey, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            keys.RowBucketPrefix,
            null, true,
            null, true,
            KvStoreConstants.DefaultPageSize,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (kvKey.StartsWith(keys.RowKeyPrefix, StringComparison.Ordinal))
                keysToDelete.Add(kvKey);
        }

        await DeleteKeysBatch(tx, keysToDelete, cancellationToken).ConfigureAwait(false);

        return keysToDelete.Count;
    }

    // -----------------------------------------------------------------------
    // Shared batch primitives
    // -----------------------------------------------------------------------

    /// <summary>
    /// Locks and physically deletes an arbitrary key list in two round-trips. Callers must have
    /// already decided that a physical delete is correct here — on a branch that is only true for a
    /// key space being abandoned outright, never for a row a scan could still inherit.
    /// </summary>
    private async Task DeleteKeysBatch(KvTransaction tx, List<string> deleteKeys, CancellationToken cancellationToken)
    {
        if (deleteKeys.Count == 0)
            return;

        // Start the deferred session before building delete items that embed tx.TransactionId.
        await tx.EnsureSessionStartedAsync(cancellationToken).ConfigureAwait(false);

        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys =
            deleteKeys.Select(k => (k, 0, KeyValueDurability.Persistent)).ToList();

        List<KahunaDeleteKeyValueRequestItem> deleteItems = deleteKeys.Select(k => new KahunaDeleteKeyValueRequestItem
        {
            TransactionId = tx.TransactionId,
            Key = k,
            Durability = KeyValueDurability.Persistent
        }).ToList();

        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);
        await DeleteManyWithRetry(tx, deleteItems, cancellationToken).ConfigureAwait(false);

        // Locks were already tracked inside AcquireManyWithRetry.
        tx.TrackModifiedRange(deleteKeys, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Acquires every exclusive lock of a batch in one round-trip, retrying only the transient
    /// responses and bounded by both the wall-clock deadline and the retry budget.
    ///
    /// <para>Optimistic transactions take no explicit exclusive WRITE locks: each confirmed write folds
    /// an implicit point lock into the coordinator working set, and write-write conflicts are detected
    /// at commit (write-intent conflict on the modified keys plus read-set validation). Skipping the
    /// acquire makes the WRITE path non-blocking — a concurrent writer is not blocked and the loser
    /// aborts at prepare instead of at lock time.</para>
    ///
    /// <para>This is fully lock-free only under Read Committed. Under Serializable the read/scan paths
    /// still take SHARED range/point locks and a following write upgrades them to Exclusive, both gated
    /// on the isolation level rather than on <see cref="KvTransaction.Locking"/>. Serializable +
    /// Optimistic is therefore a hybrid, deliberately not weakened to lock-free.</para>
    /// </summary>
    private async Task AcquireManyWithRetry(
        KvTransaction tx,
        List<(string key, int expiresMs, KeyValueDurability durability)> lockKeys,
        CancellationToken ct)
    {
        // Start the deferred session before the optimistic check so that the write calls that
        // follow (SetManyWithRetry / DeleteManyWithRetry) have a valid TransactionId.
        await tx.EnsureSessionStartedAsync(ct).ConfigureAwait(false);

        if (tx.Locking == KeyValueTransactionLocking.Optimistic)
            return;

        List<(string, int, KeyValueDurability)> pending = new(lockKeys);
        long deadline = retry.LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration. It folds every acquired
        // exclusive point lock into the coordinator working set as one registered operation, so
        // commit/rollback release them. See the class summary for the reuse-versus-mint rule.
        TransactionOperationId lockBatchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses =
                await kahuna.LocateAndTryAcquireManyExclusiveLocks(tx.TransactionId, pending, ct, tx.CoordinatorKey, lockBatchOperationId).ConfigureAwait(false);

            // First pass: trace every successfully locked key (when lock tracing is on). The coordinator
            // owns the folded point locks and releases them at finalize, so no client-side tracking is
            // needed for cleanup.
            if (options.LockTracingEnabled)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability _, _) in responses)
                {
                    if (type == KeyValueResponseType.Locked)
                        Log.LogPointLockAcquired(logger, key, tx.UniqueId);
                }
            }

            // Second pass: queue transient failures for retry; throw on hard failures.
            List<(string, int, KeyValueDurability)> retryBatch = [];
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, _) in responses)
            {
                if (type == KeyValueResponseType.Locked)
                    continue;

                if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    retryBatch.Add((key, 0, durability));
                    continue;
                }

                if (type == KeyValueResponseType.AlreadyLocked)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, $"Key {key} is locked by another transaction");

                // A range that lost quorum or changed leadership (e.g. under a partition) aborts the
                // lock acquisition; that is transient and retryable from BeginAsync, not corruption.
                if (type == KeyValueResponseType.Aborted)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"Lock acquisition on {key} was aborted by Kahuna — retry the operation from BeginAsync");

                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {type}");
            }

            if (retryBatch.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.LockWaitDeadlineMessage(tx, "batched exclusive lock acquisition", retryBatch.Select(r => r.Item1).ToList(), retryBatch.Count));

            if (++retries >= KahunaRetryPolicy.MaxKahunaRetries)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.WriteConflictMessage(tx, "batched exclusive lock acquisition", retryBatch.Select(r => r.Item1).ToList(), retryBatch.Count));

            ServerDiagnostics.AddKvRetryWait("acquire_many");
            await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries), ct).ConfigureAwait(false);

            // A shrinking pending set (some keys were confirmed Locked) is a new, smaller declaration —
            // register it under a fresh id. An unchanged set (every key transient) is the identical batch
            // resent (e.g. a lost completion ack) — keep the id so the coordinator replays idempotently.
            if (retryBatch.Count != pending.Count)
                lockBatchOperationId = TransactionOperationId.NewRandom();
            pending = retryBatch;
        }
    }

    /// <summary>
    /// Writes every item of a batch in one round-trip, resending only the keys that came back
    /// transient. <paramref name="uniqueByKey"/> marks the keys whose <c>NotSet</c> means a genuine
    /// duplicate; for every other key a <c>NotSet</c> mirrors the per-row path and is acceptable.
    /// </summary>
    private async Task SetManyWithRetry(
        KvTransaction tx,
        List<KahunaSetKeyValueRequestItem> items,
        Dictionary<string, bool> uniqueByKey,
        CancellationToken ct)
    {
        List<KahunaSetKeyValueRequestItem> pending = new(items);
        long deadline = retry.LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration; see the class summary.
        TransactionOperationId batchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<KahunaSetKeyValueResponseItem> responses =
                await kahuna.LocateAndTrySetManyKeyValue(pending, ct, tx.CoordinatorKey, batchOperationId).ConfigureAwait(false);

            // Only rebuilt if a transient response forces a retry. Re-sending an already-Set
            // unique key would falsely report a duplicate (its MVCC entry now exists), so we
            // resend only the keys that came back MustRetry/WaitingForReplication.
            List<KahunaSetKeyValueRequestItem> retryBatch = [];
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
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{keys.IndexNameFromKvKey(key)}'");
                        break; // non-unique NotSet mirrors the per-row path and is acceptable

                    case KeyValueResponseType.MustRetry:
                    case KeyValueResponseType.WaitingForReplication:
                        byKey ??= pending.ToDictionary(i => i.Key!, i => i);
                        if (byKey.TryGetValue(key, out KahunaSetKeyValueRequestItem? item))
                            retryBatch.Add(item);
                        break;

                    case KeyValueResponseType.Aborted:
                        // Kahuna aborted this batched set — the range lost quorum or changed leadership
                        // (e.g. under a network partition), so its transaction state is gone and the
                        // operation cannot be replayed in place under the same coordinator. Surface a
                        // retryable TransactionMustRetry so the client restarts from BeginAsync; falling
                        // through to SystemSpaceCorrupt would report a transient partition as durable
                        // corruption and turn a recoverable blip into a fatal error.
                        throw new CamusDBException(
                            CamusDBErrorCodes.TransactionMustRetry,
                            $"Batched set of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

                    default:
                        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch set failed for key {key}: {resp.Type}");
                }
            }

            if (retryBatch.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.LockWaitDeadlineMessage(tx, "batched write", retryBatch.Select(i => i.Key ?? "").ToList(), retryBatch.Count));

            if (++retries >= KahunaRetryPolicy.MaxKahunaRetries)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.WriteConflictMessage(tx, "batched write", retryBatch.Select(i => i.Key ?? "").ToList(), retryBatch.Count));

            ServerDiagnostics.AddKvRetryWait("set_many");
            await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries), ct).ConfigureAwait(false);

            // Shrinking set (some keys confirmed Set/NotSet) → new, smaller declaration under a fresh id.
            // Unchanged set (every key transient) → identical resend of a lost ack → keep the same id.
            if (retryBatch.Count != pending.Count)
                batchOperationId = TransactionOperationId.NewRandom();
            pending = retryBatch;
        }
    }

    /// <summary>
    /// Physically deletes every item of a batch in one round-trip, resending only the keys that came
    /// back transient. A key that does not exist is a success, not an error.
    /// </summary>
    private async Task DeleteManyWithRetry(KvTransaction tx, List<KahunaDeleteKeyValueRequestItem> items, CancellationToken ct)
    {
        List<KahunaDeleteKeyValueRequestItem> pending = new(items);
        long deadline = retry.LockWaitDeadlineTicks();
        int retries = 0;

        // Bind one operation id to the current pending batch declaration; see the class summary.
        TransactionOperationId deleteBatchOperationId = TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<KahunaDeleteKeyValueResponseItem> responses =
                await kahuna.LocateAndTryDeleteManyKeyValue(pending, ct, tx.CoordinatorKey, deleteBatchOperationId).ConfigureAwait(false);

            List<KahunaDeleteKeyValueRequestItem> retryBatch = [];
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
                            retryBatch.Add(item);
                        break;

                    case KeyValueResponseType.Aborted:
                        // Kahuna aborted this batched delete — the range lost quorum or changed
                        // leadership (e.g. under a network partition), so the transaction cannot
                        // continue and must restart from BeginAsync. Surface a retryable
                        // TransactionMustRetry rather than falling through to SystemSpaceCorrupt, which
                        // would report a transient partition as durable corruption.
                        throw new CamusDBException(
                            CamusDBErrorCodes.TransactionMustRetry,
                            $"Batched delete of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

                    default:
                        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch delete failed for key {key}: {resp.Type}");
                }
            }

            if (retryBatch.Count == 0)
                return;

            if (Stopwatch.GetTimestamp() >= deadline)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.LockWaitDeadlineMessage(tx, "batched delete", retryBatch.Select(i => i.Key ?? "").ToList(), retryBatch.Count));

            if (++retries >= KahunaRetryPolicy.MaxKahunaRetries)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    messages.WriteConflictMessage(tx, "batched delete", retryBatch.Select(i => i.Key ?? "").ToList(), retryBatch.Count));

            ServerDiagnostics.AddKvRetryWait("delete_many");
            await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries), ct).ConfigureAwait(false);

            // Shrinking set (some deletes confirmed) → new, smaller declaration under a fresh id.
            // Unchanged set (every key transient) → identical resend of a lost ack → keep the same id.
            if (retryBatch.Count != pending.Count)
                deleteBatchOperationId = TransactionOperationId.NewRandom();
            pending = retryBatch;
        }
    }
}
