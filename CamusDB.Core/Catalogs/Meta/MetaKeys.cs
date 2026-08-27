
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Builds every catalog metadata key for one database. Each key family lives here so the routing
/// invariant below is stated once and can be verified in one place.
///
/// <para><b>Every meta key shares a single Kahuna routing bucket: <c>{dbId}/meta</c></b> — the
/// substring before the last <c>'/'</c>. Kahuna partitions a point write by that bucket
/// (<c>InversePrefixedStaticHash</c> on the last <c>'/'</c>), and <c>GetByBucket</c> matches it
/// exactly. So a meta key must keep <c>{dbId}/meta</c> as its last-slash prefix, and must separate
/// its sub-fields with <c>':'</c> rather than <c>'/'</c>. A <c>'/'</c> in a sub-field would split
/// the family into per-table or per-version buckets scattered across partitions, which a single
/// <c>{dbId}/meta</c> scan cannot reach. Both the load path and the whole-database purge depend on
/// that one scan reaching everything.</para>
///
/// <para>This is the opposite of the rule for table <em>data</em> keys, where the single <c>'/'</c>
/// marks the routing split. See the <c>KvTableStore</c> class summary for that side.</para>
///
/// <para><b>Two related key families deliberately sit outside this bucket and are not built here.</b>
/// The row-level TTL run manifest (<c>{dbId}/meta/ttl/{tableId}</c>) uses <c>'/'</c>, so its bucket
/// is <c>{dbId}/meta/ttl</c>. Collected table statistics (<c>{dbId}:stats:{tableId}</c>) are not
/// meta keys at all. Neither belongs to this class.</para>
/// </summary>
internal static class MetaKeys
{
    /// <summary>
    /// The routing bucket shared by every key this class builds. A scan of this prefix reaches the
    /// whole metadata family for one database, which is what the load path and the database purge
    /// both rely on.
    /// </summary>
    internal static string MetaBucketPrefix(string dbId) => $"{dbId}/meta";

    /// <summary>Holds the legacy <c>SystemSchema</c> (index ownership).</summary>
    internal static string SystemKey(string dbId) => $"{dbId}/meta/system";

    /// <summary>Holds the database <c>SchemaVersion</c> counter.</summary>
    internal static string VersionKey(string dbId) => $"{dbId}/meta/version";

    internal static string TableKeyPrefix(string dbId) => $"{dbId}/meta/table:";

    /// <summary>
    /// Holds one <c>TableSchema</c>, keyed by the immutable relation id rather than the mutable
    /// table name, so a rename is a metadata-only change.
    /// </summary>
    internal static string TableKey(string dbId, string tableId) => $"{TableKeyPrefix(dbId)}{tableId}";

    internal static string HistoryKeyPrefix(string dbId, string tableId) => $"{dbId}/meta/history:{tableId}:";

    /// <summary>
    /// Holds one past column layout for a relation. The family is append-only: a row written under
    /// an older schema version is decoded through the entry for that version.
    /// </summary>
    internal static string HistoryKey(string dbId, string tableId, int version) => $"{HistoryKeyPrefix(dbId, tableId)}{version}";

    internal static string ViewKeyPrefix(string dbId) => $"{dbId}/meta/view:";

    /// <summary>Holds one view definition, keyed by the immutable view id.</summary>
    internal static string ViewKey(string dbId, string viewId) => $"{ViewKeyPrefix(dbId)}{viewId}";

    internal static string CoordinatorKeyPrefix(string dbId) => $"{dbId}/meta/coordinator:";

    /// <summary>
    /// Holds one persisted schema-change coordinator job. The key embeds the immutable table id,
    /// not the mutable table name, so a rename mid-job does not strand the record. <c>'~'</c>
    /// separates the table id from the element name; neither part can contain it.
    /// </summary>
    internal static string CoordinatorKey(string dbId, string tableId, string elementName) => $"{CoordinatorKeyPrefix(dbId)}{tableId}~{elementName}";

    internal static string RefreshJobKeyPrefix(string dbId) => $"{dbId}/meta/mvrefresh:";

    /// <summary>
    /// Holds one materialized-view refresh job, so a relation being built into staging has a
    /// durable owner even if the process that started the refresh never runs again.
    /// </summary>
    internal static string RefreshJobKey(string dbId, string viewTableId) => $"{RefreshJobKeyPrefix(dbId)}{viewTableId}";

    internal static string OrphanKeyPrefix(string dbId) => $"{dbId}/meta/orphan:";

    /// <summary>
    /// Holds one <c>OrphanTableRecord</c> for a deferred-dropped relation, which keeps its data
    /// reclaimable by the garbage collector and re-attachable by a relink.
    /// </summary>
    internal static string OrphanKey(string dbId, string tableId) => $"{OrphanKeyPrefix(dbId)}{tableId}";

    internal static string KeyspaceCatalogKeyPrefix(string dbId) => $"{dbId}/meta/keyspace:";

    /// <summary>
    /// Holds the grow-only set of every index id ever allocated for one <b>storage generation</b>.
    /// The catalog is written on every schema persist, so a database drop can discover and purge
    /// overlay entries for indexes that were dropped before the database was.
    ///
    /// <para><paramref name="storageId"/> is the <c>EffectiveStorageId</c>, not the relation id. The
    /// catalog names the key-space it describes, and a relation may own several over its life — one
    /// per contents swap. Keying it by identity instead would make every generation of one relation
    /// share a single entry. Retiring a generation could then not record what that generation owned,
    /// and the whole-database purge (which reads the key suffix as the bucket to sweep) would sweep
    /// an empty prefix named after the identity while the real rows stayed behind.</para>
    ///
    /// <para>The two ids coincide for every relation that never swapped, which is why existing
    /// catalogs keep working. Where they differ, the keyspace-catalog writer falls back to the
    /// legacy identity-keyed entry and copies it forward.</para>
    /// </summary>
    internal static string KeyspaceCatalogKey(string dbId, string storageId) => $"{KeyspaceCatalogKeyPrefix(dbId)}{storageId}";

    /// <summary>
    /// Returns the exclusive ordinal upper bound for every key beginning with <paramref name="prefix"/>.
    /// Incrementing the last available UTF-16 code unit keeps a prefix scan inside its logical subrange
    /// even though several key families share the one routing bucket. Returns <c>null</c> when no
    /// bound exists, which happens only if every code unit is already <see cref="char.MaxValue"/>.
    /// </summary>
    internal static string? PrefixUpperBound(string prefix)
    {
        for (int i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
                return string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i] + 1)).ToString());
        }

        return null;
    }
}
