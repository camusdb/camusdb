
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
///   Primary rows:      {tableId}/r/{rowIdHex24}                         → serialized row bytes
///   Unique index:      {tableId}/i/{indexId}/{encodedKey}               → rowIdHex24 (UTF-8)
///   Non-unique index:  {tableId}/i/{indexId}/{encodedKey}{rowIdHex24}   → rowIdHex24 (UTF-8)
///     (rowId appended without separator; it is always exactly 24 lowercase hex chars)
///
/// Routing constraint (T0.5):
///   LocateAndGetByBucket routes via SimpleHash(prefix) while individual TrySet/Delete
///   route via InversePrefixedStaticHash(key, '/') = SimpleHash(key[..lastSlash]).
///   For rows: bucket prefix "{tableId}/r" → SimpleHash("{tableId}/r") matches writes.
///   For indexes: bucket prefix "{tableId}/i/{indexId}" → SimpleHash("{tableId}/i/{indexId}")
///   matches writes whose key is "{tableId}/i/{indexId}/{...}" (last slash before the suffix).
///   Note: non-unique keys are "{tableId}/i/{indexId}/{encodedKey}{rowId}" with no extra slash,
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

    private readonly string rowBucketPrefix;       // "{tableId}/r"  — used for LocateAndGetByBucket
    private readonly string rowKeyPrefix;          // "{tableId}/r/" — prepended to rowIdHex

    private const int RowIdHexLength = 24;

    public KvTableStore(IKahuna kahuna, string tableId)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        ArgumentException.ThrowIfNullOrEmpty(tableId);

        this.kahuna = kahuna;
        this.tableId = tableId;
        rowBucketPrefix = $"{tableId}/r";
        rowKeyPrefix    = $"{tableId}/r/";
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

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            txId,
            key,
            -1,
            KeyValueDurability.Persistent,
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
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        KeyValueGetByBucketResult result;
        int retries = 0;

        do
        {
            result = await kahuna.LocateAndGetByBucket(
                txId,
                rowBucketPrefix,
                KeyValueDurability.Persistent,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Type == KeyValueResponseType.MustRetry)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
        while (result.Type == KeyValueResponseType.MustRetry && ++retries < 32);

        if (result.Type != KeyValueResponseType.Get)
            yield break;

        int prefixLen = rowKeyPrefix.Length;

        foreach ((string key, ReadOnlyKeyValueEntry entry) in result.Items)
        {
            if (entry.Value is null)
                continue;

            // Key format: "{tableId}/r/{hex24}" — the hex suffix starts after the prefix.
            ReadOnlySpan<char> hex = key.AsSpan(prefixLen);
            ObjectIdValue rowId = ObjectId.ToValue(hex.ToString());

            yield return (rowId, entry.Value);
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

        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId,
            key,
            KeyValueDurability.Persistent,
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

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            txId,
            kvKey,
            -1,
            KeyValueDurability.Persistent,
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
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string bucketPrefix = BuildIndexBucketPrefix(indexId);
        string keyPrefix    = bucketPrefix + "/";

        KeyValueGetByBucketResult result;
        int retries = 0;

        do
        {
            result = await kahuna.LocateAndGetByBucket(
                txId,
                bucketPrefix,
                KeyValueDurability.Persistent,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Type == KeyValueResponseType.MustRetry)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
        while (result.Type == KeyValueResponseType.MustRetry && ++retries < 32);

        if (result.Type != KeyValueResponseType.Get)
            yield break;

        string? fromEncoded = from is not null ? KeyEncoder.Encode(from) : null;
        string? toEncoded   = to   is not null ? KeyEncoder.Encode(to)   : null;

        int prefixLen = keyPrefix.Length;

        foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in result.Items)
        {
            if (entry.Value is null)
                continue;

            // Strip the "{tableId}/i/{indexId}/" prefix to obtain the raw suffix.
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

            // Apply bounds filter.
            if (fromEncoded is not null)
            {
                int cmp = string.CompareOrdinal(encodedKey, fromEncoded);
                if (fromInclusive ? cmp < 0 : cmp <= 0)
                    continue;
            }
            if (toEncoded is not null)
            {
                int cmp = string.CompareOrdinal(encodedKey, toEncoded);
                if (toInclusive ? cmp > 0 : cmp >= 0)
                    continue;
            }

            CompositeColumnValue decodedKey = KeyEncoder.Decode(encodedKey, keyTypes);
            yield return (decodedKey, rowId);
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

        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId,
            kvKey,
            value,
            null,
            -1,
            flags,
            0,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (unique && type == KeyValueResponseType.NotSet)
            throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, $"Duplicate entry for key '{indexId}'");

        if (type is not (KeyValueResponseType.Set or KeyValueResponseType.NotSet))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"PutIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
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

        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId,
            kvKey,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (type is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"DeleteIndexEntry failed for key {kvKey}: {type}");

        tx.TrackModified(kvKey, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task WriteRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken)
    {
        string key = BuildRowKey(rowId);

        await AcquireLock(tx, key, cancellationToken).ConfigureAwait(false);

        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId,
            key,
            data,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRow failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireLock(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        (KeyValueResponseType lockType, _, KeyValueDurability lockDurability) = await kahuna.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId,
            key,
            0,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to acquire lock on {key}: {lockType}");

        tx.TrackLock(key, lockDurability);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildRowKey(ObjectIdValue rowId) => rowKeyPrefix + rowId.ToString();

    // Returns "{tableId}/i/{indexId}" — the bucket prefix (no trailing slash) used for
    // LocateAndGetByBucket so that SimpleHash("{tableId}/i/{indexId}") matches the routing
    // hash of keys "{tableId}/i/{indexId}/{...}".
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildIndexBucketPrefix(string indexId) => $"{tableId}/i/{indexId}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildUniqueIndexKey(string indexId, CompositeColumnValue key)
        => $"{tableId}/i/{indexId}/{KeyEncoder.Encode(key)}";

    // Non-unique: rowIdHex appended directly (no separator) so the last slash in the full
    // key is always the one after {indexId}, keeping the routing hash stable.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string BuildNonUniqueIndexKey(string indexId, CompositeColumnValue key, ObjectIdValue rowId)
        => $"{tableId}/i/{indexId}/{KeyEncoder.Encode(key)}{rowId}";
}
