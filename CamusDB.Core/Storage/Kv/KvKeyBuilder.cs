/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Composes every KV key one table occupies, and holds the per-index metadata the composition needs.
///
/// <para>Key layout (all keys share the leading <c>{dbId}:{tableId}</c> segment so databases are
/// isolated in the shared keyspace and every key of one table routes together):</para>
///
/// <code>
///   Primary rows:      {dbId}:{tableId}:r/{rowIdHex24}                         -> serialized row bytes
///   Unique index:      {dbId}:{tableId}:i:{indexId}/{encodedKey}               -> rowIdHex24 (UTF-8)
///   Non-unique index:  {dbId}:{tableId}:i:{indexId}/{encodedKey}{rowIdHex24}   -> rowIdHex24 (UTF-8)
/// </code>
///
/// <para>The rowId is appended to a non-unique key without a separator, so the last <c>'/'</c> of a
/// full key is always the one after <c>{indexId}</c>. Under hash routing the bucket prefix a scan
/// uses and the key a point write uses must hash to the same partition, and that equality is what
/// the missing separator preserves. Do not add a slash to a key or a bucket prefix without
/// re-deriving both sides of it — see <see cref="KvTableStore"/> for the full routing argument.</para>
///
/// <para>One instance addresses one <c>(database, table)</c> pair. A branch database's lineage holds
/// one builder per ancestor level, because an ancestor owns its own key namespace and a probe of an
/// inherited row must be composed there rather than in the branch.</para>
///
/// <para><b>Threading:</b> <see cref="RegisterIndexName"/> and <see cref="RegisterIndexDirections"/>
/// are called by the table-open path before any DML can reference the index, and never afterwards,
/// so the two metadata dictionaries are single-writer-then-read-only and need no synchronization.
/// The bucket-prefix cache is written on the read path and is concurrent.</para>
/// </summary>
internal sealed class KvKeyBuilder
{
    // Caches "{dbId}:{tableId}:i:{indexId}" per index so the bucket prefix is interpolated once
    // instead of on every lock/scan. The index-id set is small and bounded by the table's schema.
    private readonly ConcurrentDictionary<string, string> indexBucketPrefixCache = new();

    // Maps each index KvId to its human-readable name for user-facing error messages.
    // Populated by TableOpener at open time; the KvId is the stable immutable id used in KV keys.
    private readonly Dictionary<string, string> indexIdToDisplayName = [];

    // Per-index KvId -> column sort directions. Only indexes with at least one descending column
    // are present; an absent entry means all-ascending (the encoder's fast path).
    private readonly Dictionary<string, OrderType[]> indexDirections = [];

    /// <summary>Immutable id of the database this builder addresses; the first key segment.</summary>
    internal string DbId { get; }

    /// <summary>
    /// User-facing database name, carried purely for diagnostics. It is never part of a KV key, so an
    /// empty value only degrades an error message.
    /// </summary>
    internal string DbName { get; }

    /// <summary>Immutable storage id of the table; the second key segment.</summary>
    internal string TableId { get; }

    /// <summary>User-facing table name, carried purely for diagnostics. Never part of a KV key.</summary>
    internal string TableName { get; }

    /// <summary><c>{dbId}:{tableId}</c> — the segment every row and index key of this table starts with.</summary>
    internal string TableKeyPrefix { get; }

    /// <summary><c>{dbId}:{tableId}:r</c> — the bucket prefix (no trailing slash) for row scans and row range locks.</summary>
    internal string RowBucketPrefix { get; }

    /// <summary><c>{dbId}:{tableId}:r/</c> — prepended to a row-id hex to form a full row key.</summary>
    internal string RowKeyPrefix { get; }

    internal KvKeyBuilder(string dbId, string dbName, string tableId, string tableName)
    {
        DbId = dbId;
        DbName = dbName;
        TableId = tableId;
        TableName = tableName;
        TableKeyPrefix = $"{dbId}:{tableId}";
        RowBucketPrefix = $"{dbId}:{tableId}:r";
        RowKeyPrefix = $"{dbId}:{tableId}:r/";
    }

    /// <summary>
    /// Registers the human-readable display name for an index KvId so that duplicate-key errors show
    /// the mutable index name (e.g., <c>robots.name_idx</c>) rather than the immutable KvId stored in
    /// KV keys.
    /// </summary>
    internal void RegisterIndexName(string indexId, string displayName) => indexIdToDisplayName[indexId] = displayName;

    /// <summary>
    /// Registers the per-column sort directions for an index KvId so that key encoding and index scans
    /// invert the ordinal order of descending columns. A null or all-ascending vector is not stored —
    /// <see cref="DirectionsOf"/> then returns null and the encoder takes its ascending fast path,
    /// keeping every existing index byte-identical.
    /// </summary>
    internal void RegisterIndexDirections(string indexId, OrderType[]? directions)
    {
        bool anyDescending = false;
        if (directions is not null)
            foreach (OrderType direction in directions)
                if (direction == OrderType.Descending) { anyDescending = true; break; }

        if (anyDescending)
            indexDirections[indexId] = directions!;
        else
            indexDirections.Remove(indexId);
    }

    /// <summary>Per-index sort directions, or null when the index is entirely ascending.</summary>
    internal OrderType[]? DirectionsOf(string indexId)
        => indexDirections.TryGetValue(indexId, out OrderType[]? directions) ? directions : null;

    /// <summary>
    /// The index's user-facing name, or the immutable KvId when no name was registered. Used by
    /// diagnostics that must name an index a user declared rather than an opaque id.
    /// </summary>
    internal string DisplayNameOf(string indexId)
        => indexIdToDisplayName.TryGetValue(indexId, out string? name) ? name : indexId;

    // Composes "{dbId}:{tableId}:r/{rowIdHex24}" directly into the new string's buffer — one
    // allocation, no separate rowId.ToString() temporary. The length is small and bounded
    // (compact db/table ids + 24 hex), so this is allocation-minimal on the per-row hot path.
    internal string BuildRowKey(ObjectIdValue rowId)
        => string.Create(RowKeyPrefix.Length + KvStoreConstants.RowIdHexLength, (RowKeyPrefix, rowId), static (span, state) =>
        {
            state.RowKeyPrefix.CopyTo(span);
            ObjectId.WriteHex(span[state.RowKeyPrefix.Length..], state.rowId.a, state.rowId.b, state.rowId.c);
        });

    // Returns "{dbId}:{tableId}:i:{indexId}" — the bucket prefix (no trailing slash) used for
    // LocateAndScanRange so that SimpleHash("{dbId}:{tableId}:i:{indexId}") matches the routing
    // hash of keys "{dbId}:{tableId}:i:{indexId}/{...}". Cached per index id (see field).
    internal string BuildIndexBucketPrefix(string indexId)
        => indexBucketPrefixCache.TryGetValue(indexId, out string? cached)
            ? cached
            : indexBucketPrefixCache.GetOrAdd(indexId, static (id, prefix) => $"{prefix}:i:{id}", TableKeyPrefix);

    // Composes "{bucket}/{encodedKey}" straight into the final string's buffer — one allocation, no
    // intermediate KeyEncoder.Encode(...) string interpolated into a second string. `bucket` is the
    // cached "{dbId}:{tableId}:i:{indexId}" prefix (== "{TableKeyPrefix}:i:{indexId}").
    internal string BuildUniqueIndexKey(string indexId, CompositeColumnValue key)
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
    // stored row can equal), so a point lookup treats it as a miss instead of throwing. Validation is
    // allocation-free and the key is composed by BuildUniqueIndexKey in a single string.Create — no
    // intermediate encoded-key string interpolated into a second string on the per-lookup hot path.
    internal bool TryBuildUniqueIndexKey(string indexId, CompositeColumnValue key, out string kvKey)
    {
        if (!KeyEncoder.CanEncode(key))
        {
            kvKey = string.Empty;
            return false;
        }

        kvKey = BuildUniqueIndexKey(indexId, key);
        return true;
    }

    // Non-unique: rowIdHex appended directly (no separator) so the last slash in the full
    // key is always the one after {indexId}, keeping the routing hash stable. Composed straight into
    // the final buffer — one allocation, no encoded-key string and no rowId.ToString() temporary.
    internal string BuildNonUniqueIndexKey(string indexId, CompositeColumnValue key, ObjectIdValue rowId)
    {
        string bucket = BuildIndexBucketPrefix(indexId);
        OrderType[]? directions = DirectionsOf(indexId);
        int encodedLen = KeyEncoder.Measure(key, directions);
        return string.Create(bucket.Length + 1 + encodedLen + KvStoreConstants.RowIdHexLength, (bucket, key, directions, rowId), static (span, state) =>
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
    internal string DuplicateKeyLabel(string indexId)
    {
        string display = DisplayNameOf(indexId);
        return string.IsNullOrEmpty(TableName) ? display : $"{TableName}.{display}";
    }

    // Extracts the index name from a full KV key ("{dbId}:{tableId}:i:{indexId}/{...}").
    // Falls back to the raw key on unexpected formats.
    internal string IndexNameFromKvKey(string kvKey)
    {
        string prefix = $"{TableKeyPrefix}:i:";
        if (!kvKey.StartsWith(prefix, StringComparison.Ordinal))
            return kvKey;
        string tail = kvKey[prefix.Length..];
        int slash = tail.IndexOf('/');
        string indexId = slash >= 0 ? tail[..slash] : tail;
        return DuplicateKeyLabel(indexId);
    }
}
