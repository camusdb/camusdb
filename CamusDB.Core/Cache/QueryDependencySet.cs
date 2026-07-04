
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// Immutable dependency set captured during the execution of a cacheable query.
/// Holds three kinds of facts about which storage state the result depends on:
///
/// <list type="bullet">
///   <item><description>
///     <b>RangeDeps</b>: keyspace bucket strings scanned for membership (e.g.
///     <c>"{dbId}:{tableId}:r"</c> for a full table scan or
///     <c>"{dbId}:{tableId}:i:{indexId}"</c> for an index range scan). A write to
///     any key in the bucket invalidates this entry, catching phantoms and inserts.
///   </description></item>
///   <item><description>
///     <b>PointDeps</b>: individual KV key strings whose values were read while
///     producing the result. Catches row-content updates and deletes for rows that
///     were fetched but whose index entry would be unchanged by the update.
///   </description></item>
///   <item><description>
///     <b>SchemaDeps</b>: (tableId, schemaVersion) pairs recording the schema version
///     used by the plan and row decoder. A schema change (DDL) invalidates any entry
///     whose schema version no longer matches. Strict hits also re-check these live.
///   </description></item>
/// </list>
///
/// <para><b>Note:</b> entries created before dependency capture is wired carry <see cref="Empty"/>.
/// The cache and dependency index are already structured to accept real deps without further
/// changes once the cached-read path assigns a collector.</para>
/// </summary>
public sealed class QueryDependencySet
{
    /// <summary>
    /// An empty dependency set for entries whose deps were not captured.
    /// An entry with empty deps is never invalidated by key-based events; it expires only
    /// through TTL or explicit eviction. Using this outside of tests or before the
    /// cached-read path is wired risks serving stale results.
    /// </summary>
    public static readonly QueryDependencySet Empty = new([], [], []);

    /// <summary>
    /// Keyspace bucket strings scanned for membership. Each string is a table-bucket
    /// prefix of the form <c>"{dbId}:{tableId}:r"</c> or
    /// <c>"{dbId}:{tableId}:i:{indexId}"</c>. A committed write to any key whose prefix
    /// matches one of these strings invalidates this entry.
    /// </summary>
    public IReadOnlyList<string> RangeDeps { get; }

    /// <summary>
    /// Individual KV key strings whose byte values were fetched and used to produce the
    /// result. A write that changes or deletes any of these keys invalidates this entry
    /// even if the key's bucket range is unchanged.
    /// </summary>
    public IReadOnlyList<string> PointDeps { get; }

    /// <summary>
    /// Schema version facts. Each entry is a (tableId, schemaVersion) pair. The plan was
    /// built and the rows were decoded against this schema version. An increment to any
    /// of these versions (DDL, online column/index state change) invalidates this entry.
    /// </summary>
    public IReadOnlyList<(string TableId, int SchemaVersion)> SchemaDeps { get; }

    public QueryDependencySet(
        IReadOnlyList<string> rangeDeps,
        IReadOnlyList<string> pointDeps,
        IReadOnlyList<(string TableId, int SchemaVersion)> schemaDeps)
    {
        RangeDeps  = rangeDeps;
        PointDeps  = pointDeps;
        SchemaDeps = schemaDeps;
    }
}
