
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Per-table data access layer built on top of <see cref="IKahuna"/>.
///
/// Key layout (all keys share the leading <c>{tableId}</c> segment so Kommander routes
/// the whole table to one partition):
///
///   Primary rows:      {tableId}:r/{rowIdHex24}                         → serialized row bytes
///   Unique index:      {tableId}:i:{indexId}/{encodedKey}               → rowIdHex24 (UTF-8)
///   Non-unique index:  {tableId}:i:{indexId}/{encodedKey}{rowIdHex24}   → rowIdHex24 (UTF-8)
///     (rowId appended without separator; it is always exactly 24 lowercase hex chars)
///
/// Routing constraint (T0.5):
///   LocateAndScanRange routes via SimpleHash(prefix) while individual TrySet/Delete
///   route via InversePrefixedStaticHash(key, '/') = SimpleHash(key[..lastSlash]).
///   For rows: bucket prefix "{tableId}:r" → SimpleHash("{tableId}:r") matches writes.
///   For indexes: bucket prefix "{tableId}:i:{indexId}" → SimpleHash("{tableId}:i:{indexId}")
///   matches writes whose key is "{tableId}:i:{indexId}/{...}" (last slash before the suffix).
///   Note: non-unique keys are "{tableId}:i:{indexId}/{encodedKey}{rowId}" with no extra slash,
///   so the routing invariant holds for both unique and non-unique on a single partition.
///   With multiple partitions (Phase 6) this requires review.
///
/// All write methods take a <see cref="KvTransaction"/> so they can accumulate acquired locks
/// and modified keys for the 2-phase commit.
/// </summary>
public sealed class KvTableStore
{
    private readonly IKahuna kahuna;
    private readonly string tableId;

    private readonly string rowBucketPrefix;       // "{tableId}:r"  — bucket prefix for LocateAndScanRange
    private readonly string rowKeyPrefix;          // "{tableId}:r/" — prepended to rowIdHex

    private const int RowIdHexLength = 24;
    private const int DefaultPageSize = 512;
    private const int MaxKahunaRetries = 32;
    private const int MaxRetryDelayMs   = 50;

    // Exponential back-off: 1 ms, 2 ms, 4 ms, … capped at MaxRetryDelayMs.
    private static int RetryDelayMs(int attempt) => Math.Min(1 << attempt, MaxRetryDelayMs);

    public KvTableStore(IKahuna kahuna, string tableId)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        ArgumentException.ThrowIfNullOrEmpty(tableId);

        this.kahuna = kahuna;
        this.tableId = tableId;
        rowBucketPrefix = $"{tableId}:r";
        rowKeyPrefix    = $"{tableId}:r/";
    }

    // -----------------------------------------------------------------------
    // Primary row operations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Point-read a single row. Returns the raw serialized bytes, or <c>null</c> if not found.
    /// </summary>
    public async Task<byte[]?> GetRow(HLCTimestamp txId, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        string key = BuildRowKey(rowId);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(txId, key, -1, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry is null)
            return null;

        return entry.Value;
    }

    /// <summary>
    /// Full table scan. Yields every (rowId, rowBytes) pair in ascending rowId order
    /// (ObjectId hex is time-ordered and fixed-width so natural KV order is correct).
    /// </summary>
    public async IAsyncEnumerable<(ObjectIdValue rowId, byte[] data)> ScanRows(
        HLCTimestamp txId,
        long? maxRows = null,
        ObjectIdValue? afterRowId = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxRows is <= 0)
            yield break;

        long emitted = 0;
        int prefixLen = rowKeyPrefix.Length;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId,
            rowBucketPrefix,
            null, true,
            null, true,
            DefaultPageSize,
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            // Key format: "{tableId}:r/{hex24}" — the hex suffix starts after the prefix.
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

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, key, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

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
    /// </summary>
    public async Task<ObjectIdValue?> LookupUnique(
        HLCTimestamp txId,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
    {
        string kvKey = BuildUniqueIndexKey(indexId, key);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(txId, kvKey, -1, KeyValueDurability.Persistent, cancellationToken),
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
    /// </summary>
    public async IAsyncEnumerable<(CompositeColumnValue key, ObjectIdValue rowId)> ScanIndex(
        HLCTimestamp txId,
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
        // {encodedKey}{rowIdHex24}, so the end key needs a high sentinel (￿) to
        // include all possible rowId suffixes for the last encoded value.
        string? startKey = fromEncoded is not null ? keyPrefix + fromEncoded : null;
        string? endKey   = toEncoded   is not null
            ? (unique ? keyPrefix + toEncoded : keyPrefix + toEncoded + "￿")
            : null;

        await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId,
            bucketPrefix,
            startKey, fromInclusive,
            endKey, toInclusive,
            DefaultPageSize,
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
    /// </summary>
    public async Task PutIndexEntry(
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

        byte[] value = Encoding.UTF8.GetBytes(rowId.ToString());

        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

        KeyValueFlags flags = unique ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;

        (KeyValueResponseType type, _, _) = await RetryOnMustRetry(
            () => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, kvKey, value, null, -1, flags, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (unique && type == KeyValueResponseType.NotSet)
            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{indexId}'");

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
                    throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{ix.IndexId}'");

                AddWrite(kvKey, Encoding.UTF8.GetBytes(row.RowId.ToString()), ix.Unique);
            }
        }

        // Phase 1 — acquire every lock for the batch in one round-trip (retrying only transients).
        await AcquireManyWithRetry(tx, lockKeys, cancellationToken).ConfigureAwait(false);

        // Phase 2 — set every value for the batch in one round-trip (retrying only transients).
        await SetManyWithRetry(setItems, uniqueByKey, cancellationToken).ConfigureAwait(false);

        // Track locks + modified keys for the 2PC commit.
        foreach ((string key, int _, KeyValueDurability durability) in lockKeys)
        {
            tx.TrackLock(key, durability);
            tx.TrackModified(key, KeyValueDurability.Persistent);
        }
    }

    private async Task AcquireManyWithRetry(
        KvTransaction tx,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken ct)
    {
        List<(string, int, KeyValueDurability)> pending = new(keys);
        int retries = 0;

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses =
                await kahuna.LocateAndTryAcquireManyExclusiveLocks(tx.TransactionId, pending, ct).ConfigureAwait(false);

            List<(string, int, KeyValueDurability)> retry = [];
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in responses)
            {
                if (type == KeyValueResponseType.Locked)
                    continue;

                // Re-acquiring keys already held by this transaction is idempotent, so retrying
                // only the transient ones is safe and avoids re-walking the whole list.
                if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    retry.Add((key, 0, durability));
                    continue;
                }

                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {type}");
            }

            if (retry.Count == 0)
                return;

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire {retry.Count} lock(s) after {MaxKahunaRetries} retries");

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
                            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{key}'");
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

            if (++retries >= MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Batch set failed for {retry.Count} key(s) after {MaxKahunaRetries} retries");

            await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            pending = retry;
        }
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

        await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, kvKey, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Deletes every KV entry belonging to the named index. Used by DROP INDEX to reclaim
    /// the <c>{tableId}:i:{indexName}/…</c> space. All deletes run under <paramref name="tx"/>
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
            KeyValueDurability.Persistent,
            cancellationToken).ConfigureAwait(false))
        {
            if (kvKey.StartsWith(keyPrefix, StringComparison.Ordinal))
                keysToDelete.Add(kvKey);
        }

        foreach (string kvKey in keysToDelete)
        {
            await AcquireLock(tx, kvKey, cancellationToken).ConfigureAwait(false);

            (KeyValueResponseType type, _, _) = await RetryOnMustRetry(
                () => kahuna.LocateAndTryDeleteKeyValue(tx.TransactionId, kvKey, KeyValueDurability.Persistent, cancellationToken),
                cancellationToken
            ).ConfigureAwait(false);

            if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DropIndexEntries failed for key {kvKey}: {type}");

            tx.TrackModified(kvKey, KeyValueDurability.Persistent);
        }

        return keysToDelete.Count;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task WriteRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken)
    {
        string key = BuildRowKey(rowId);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetry(
            () => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, data, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireLock(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        (KeyValueResponseType lockType, _, KeyValueDurability lockDurability) = await RetryOnMustRetry(
            () => kahuna.LocateAndTryAcquireExclusiveLock(tx.TransactionId, key, 0, KeyValueDurability.Persistent, cancellationToken),
            cancellationToken
        ).ConfigureAwait(false);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {lockType}");

        tx.TrackLock(key, lockDurability);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildRowKey(ObjectIdValue rowId) => rowKeyPrefix + rowId.ToString();

    // Returns "{tableId}:i:{indexId}" — the bucket prefix (no trailing slash) used for
    // LocateAndScanRange so that SimpleHash("{tableId}:i:{indexId}") matches the routing
    // hash of keys "{tableId}:i:{indexId}/{...}".
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildIndexBucketPrefix(string indexId) => $"{tableId}:i:{indexId}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildUniqueIndexKey(string indexId, CompositeColumnValue key)
        => $"{tableId}:i:{indexId}/{KeyEncoder.Encode(key)}";

    // Non-unique: rowIdHex appended directly (no separator) so the last slash in the full
    // key is always the one after {indexId}, keeping the routing hash stable.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildNonUniqueIndexKey(string indexId, CompositeColumnValue key, ObjectIdValue rowId)
        => $"{tableId}:i:{indexId}/{KeyEncoder.Encode(key)}{rowId}";
}
