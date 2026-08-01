
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Config;

namespace CamusDB.Core.Cache;

/// <summary>
/// Validates a strict cache hit against the current state of the KV store.
///
/// <para><b>When validation is needed:</b> a hit is strict when
/// <see cref="CachedQueryResult.HintIsStrict"/> is true. Strict hits must pass this check
/// before rows are served. A failed validation evicts the stale entry; the request falls
/// through to a live execution path, which may or may not store a new entry depending on
/// whether a stable read snapshot is available.</para>
///
/// <para><b>Checks performed (in order):</b></para>
/// <list type="number">
///   <item><description>
///     <b>Schema deps:</b> for each <c>(tableId, version)</c> in the dep set, confirm the
///     table still exists and its current schema version matches. A version mismatch means a
///     column add/drop/rename happened since the entry was computed — the stored rows may be
///     decoded under a stale column layout.
///   </description></item>
///   <item><description>
///     <b>Point deps:</b> for each row key in the dep set, probe Kahuna for the latest
///     committed value. If the key is absent (row deleted) or has
///     <c>LastModified &gt; CachedAt</c> (row updated), the entry is stale.
///   </description></item>
///   <item><description>
///     <b>Range deps:</b> for each keyspace prefix in the dep set, scan Kahuna for the latest
///     committed keys. If any key has <c>LastModified &gt; CachedAt</c>, a phantom insert or
///     an untracked update occurred since the entry was computed.
///   </description></item>
///   <item><description>
///     <b>Probe limit:</b> if the total number of keys probed across point and range checks
///     exceeds <see cref="CamusDBOptions.QueryResultCacheStrictValidationMaxKeys"/>, validation
///     fails closed — the stale entry is evicted and the request falls through to live execution.
///   </description></item>
/// </list>
///
/// <para><b>All reads are non-transactional</b> (<c>txId = HLCTimestamp.Zero</c>,
/// <c>readTimestamp = HLCTimestamp.Zero</c>) so the validator always sees the latest committed
/// state with no Kahuna transaction overhead.</para>
/// </summary>
internal static class StrictValidator
{
    /// <summary>
    /// Validates <paramref name="result"/> against current KV state.
    /// Returns <c>true</c> if the entry is still valid and can be served; <c>false</c> if
    /// it is stale and must be evicted. A <c>false</c> return always causes the stale entry
    /// to be evicted at the call site before falling through to live execution.
    /// </summary>
    internal static async Task<bool> ValidateAsync(
        CachedQueryResult result,
        QueryDependencySet deps,
        DatabaseDescriptor database,
        IKahuna kahuna,
        CancellationToken ct)
    {
        int probeCount = 0;
        int maxKeys = database.Options.QueryResultCacheStrictValidationMaxKeys;

        // ── Schema deps ────────────────────────────────────────────────────────
        // Check that every table referenced by the cached plan still exists at the
        // same schema version. A schema version bump means a column layout change
        // that affects how row bytes are decoded — serve the cached (old-layout) rows
        // would be incorrect.
        // Note: index adds and drops also bump the table schema version, so index
        // identity changes are caught transitively through the same version check.
        foreach ((string tableId, int expectedVersion) in deps.SchemaDeps)  // compiler sees (TableId, SchemaVersion)
        {
            TableSchema? schema = FindTableById(database, tableId);
            if (schema is null)
                return false;  // table dropped

            if (schema.Version != expectedVersion)
                return false;  // schema changed (column add/drop/rename/index add/drop)
        }

        // ── Point deps ─────────────────────────────────────────────────────────
        // Each point dep is the full KV key of a row that was fetched at cache time.
        // If the row was deleted (key absent) or updated (LastModified > CachedAt),
        // the cached result is stale.
        foreach (string pointKey in deps.PointDeps)
        {
            if (++probeCount > maxKeys)
                return false;  // limit exceeded — fail closed

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, pointKey, -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)
                .ConfigureAwait(false);

            if (type == KeyValueResponseType.DoesNotExist || entry is null)
                return false;  // row deleted since T_cache

            if (entry.LastModified > result.CachedAt)
                return false;  // row updated since T_cache
        }

        // ── Range deps ─────────────────────────────────────────────────────────
        // Each range dep is a bucket prefix (e.g. "{dbId}:{tableId}:r") identifying a
        // keyspace scanned for membership. If any key in the range has
        // LastModified > CachedAt, a phantom insert occurred — the cached row set is
        // missing at least one row that would now match.
        // Fixed page size for range scans. The probe-limit counter still enforces the budget
        // mid-scan; using a fixed page rather than `remaining` avoids requesting an
        // unnecessarily large first batch when the budget is large.
        const int rangeScanPageSize = 256;

        foreach (string rangePrefix in deps.RangeDeps)
        {
            if (probeCount >= maxKeys)
                return false;  // already at limit before starting this range

            await foreach ((string _, ReadOnlyKeyValueEntry rangeEntry) in
                kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero, rangePrefix,
                    null, false, null, false,
                    rangeScanPageSize, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, ct)
                .ConfigureAwait(false))
            {
                if (++probeCount > maxKeys)
                    return false;  // limit exceeded mid-scan

                if (rangeEntry.LastModified > result.CachedAt)
                    return false;  // phantom insert or untracked update
            }
        }

        return true;
    }

    private static TableSchema? FindTableById(DatabaseDescriptor database, string tableId)
    {
        foreach (TableSchema schema in database.Schema.Tables.Values)
        {
            if (schema.Id == tableId)
                return schema;
        }
        return null;
    }
}
