
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Cache;

/// <summary>
/// Per-request mutable accumulator for cache dependency facts collected during query execution.
/// Call <see cref="RecordRange"/>, <see cref="RecordPoint"/>, and <see cref="RecordSchema"/>
/// as rows are read; then call <see cref="Build"/> once after execution completes to obtain the
/// immutable <see cref="QueryDependencySet"/> to attach to the published cache entry.
///
/// <para><b>Design contract:</b></para>
/// <list type="bullet">
///   <item><description>
///     Table-scan paths record the entire row-bucket keyspace as a range dep
///     (<c>{dbId}:{tableId}:r</c>) plus individual row point deps for each row fetched, up to
///     <see cref="CamusDBOptions.QueryResultCacheMaxPointDeps"/>. When the point-dep cap is
///     reached, further point deps are silently dropped and <see cref="PointDepsTruncated"/> is
///     set. The range dep remains for non-strict entries (it provides conservative coverage for
///     inserts and updates). Strict entries must bypass publication when point deps were truncated
///     because a physical delete removes the deleted key from future range scans — no
///     <c>LastModified</c> entry exists for a deleted key, so strict validation cannot detect
///     the deletion without the individual point dep.
///   </description></item>
///   <item><description>
///     Index-scan paths record the index bucket keyspace (<c>{dbId}:{tableId}:i:{indexId}</c>)
///     as a range dep plus each fetched row's point dep. Both must be present: the index range
///     catches phantom inserts / membership changes; the row point catches updates to projected
///     non-indexed columns.
///   </description></item>
///   <item><description>
///     <see cref="CamusDBOptions.QueryResultCacheMaxDeps"/> caps the total dep count across all
///     three categories. If the total is exceeded, <see cref="CapExceeded"/> is set and
///     <see cref="Build"/> returns <see cref="QueryDependencySet.Empty"/> — the caller must
///     bypass publish rather than store an incomplete dep set.
///   </description></item>
///   <item><description>
///     <see cref="CamusDBOptions.QueryResultCacheMaxRanges"/> caps the number of distinct range
///     deps. Unlike point deps, a truncated range set cannot provide conservative coverage —
///     a missing range means writes into that keyspace go undetected. Overflow sets
///     <see cref="CapExceeded"/> and the caller must bypass publish.
///   </description></item>
/// </list>
///
/// <para><b>Thread safety:</b> not thread-safe; one instance per request.</para>
/// </summary>
internal sealed class QueryDependencyCollector
{
    private readonly HashSet<string> _rangeDeps  = new(StringComparer.Ordinal);
    private readonly HashSet<string> _pointDeps  = new(StringComparer.Ordinal);
    private readonly List<(string TableId, int SchemaVersion)> _schemaDeps = new();

    private bool _capExceeded;
    private bool _pointDepsTruncated;

    /// <summary>
    /// True if any dep cap was exceeded. When true, <see cref="Build"/> returns
    /// <see cref="QueryDependencySet.Empty"/> and the caller must bypass publish.
    /// </summary>
    public bool CapExceeded => _capExceeded;

    /// <summary>
    /// True if <see cref="RecordPoint"/> silently dropped at least one point dep because the
    /// per-query cap (<see cref="CamusDBOptions.QueryResultCacheMaxPointDeps"/>) was reached.
    ///
    /// <para>Non-strict entries: this flag is informational only. The range dep provides
    /// conservative coverage for rows not individually tracked — inserts and updates in the
    /// range are caught by same-node <c>InvalidateByModifiedKeys</c> range matching.</para>
    ///
    /// <para>Strict entries: this flag is blocking. A physical delete removes the key from
    /// Kahuna entirely — there is no <c>LastModified</c> tombstone for the strict validator
    /// to compare against. Without a point dep for the deleted row, the range scan sees nothing
    /// newer than <c>CachedAt</c> and incorrectly reports the entry as valid. The caller
    /// (<see cref="CachedQueryRunner"/>) must bypass publish for strict entries when this flag
    /// is set.</para>
    /// </summary>
    public bool PointDepsTruncated => _pointDepsTruncated;

    /// <summary>
    /// Records a keyspace range as a dependency (table row bucket or index bucket).
    /// The bucket string must use the real KvTableStore format:
    /// <c>{dbId}:{tableId}:r</c> for rows, <c>{dbId}:{tableId}:i:{indexId}</c> for indexes.
    ///
    /// <para>Unlike the point-dep cap, a range-dep overflow cannot be silently truncated: dropping
    /// a range dep means any write into that keyspace would go undetected, violating the
    /// "dependency-complete or bypass" invariant. When <see cref="CamusDBOptions.QueryResultCacheMaxRanges"/>
    /// is reached, <see cref="CapExceeded"/> is set and <see cref="Build"/> returns
    /// <see cref="QueryDependencySet.Empty"/> so the caller bypasses publish.</para>
    /// </summary>
    public void RecordRange(string keyspaceBucket)
    {
        if (_capExceeded) return;

        // If the bucket is already recorded, adding it to the HashSet is a no-op — skip the cap
        // check to avoid counting duplicates as new additions.
        if (_rangeDeps.Contains(keyspaceBucket))
            return;

        if (_rangeDeps.Count >= CamusDBConfig.QueryResultCacheMaxRanges)
        {
            _capExceeded = true;
            return;
        }

        _rangeDeps.Add(keyspaceBucket);
        CheckTotalCap();
    }

    /// <summary>
    /// Records a concrete row point key (<c>{dbId}:{tableId}:r/{rowIdHex24}</c>).
    /// If the point-dep cap is reached the call is a no-op; the previously recorded range dep
    /// (which must already have been added via <see cref="RecordRange"/>) provides conservative
    /// coverage.
    /// </summary>
    public void RecordPoint(string rowKey)
    {
        if (_capExceeded) return;

        // Drop point deps beyond the cap. For non-strict entries the range dep provides coverage;
        // for strict entries this truncation is unsafe (deletes are invisible without the point dep)
        // and PointDepsTruncated is set so FinalizeAsync can bypass publish for strict entries.
        if (_pointDeps.Count >= CamusDBConfig.QueryResultCacheMaxPointDeps)
        {
            _pointDepsTruncated = true;
            return;
        }

        _pointDeps.Add(rowKey);
        CheckTotalCap();
    }

    /// <summary>
    /// Records a schema version dependency — the table ID and the schema version the plan was
    /// built against. Duplicate (tableId, version) pairs are collapsed.
    /// </summary>
    public void RecordSchema(string tableId, int schemaVersion)
    {
        if (_capExceeded) return;

        // Avoid duplicates
        for (int i = 0; i < _schemaDeps.Count; i++)
        {
            if (_schemaDeps[i].TableId == tableId)
                return;
        }

        _schemaDeps.Add((tableId, schemaVersion));
        CheckTotalCap();
    }

    /// <summary>
    /// Builds and returns an immutable <see cref="QueryDependencySet"/> from the collected facts.
    /// Returns <see cref="QueryDependencySet.Empty"/> if <see cref="CapExceeded"/> is true —
    /// the caller must bypass publish in that case.
    /// </summary>
    public QueryDependencySet Build()
    {
        if (_capExceeded)
            return QueryDependencySet.Empty;

        return new QueryDependencySet(
            [.. _rangeDeps],
            [.. _pointDeps],
            [.. _schemaDeps]);
    }

    private void CheckTotalCap()
    {
        int total = _rangeDeps.Count + _pointDeps.Count + _schemaDeps.Count;
        if (total > CamusDBConfig.QueryResultCacheMaxDeps)
            _capExceeded = true;
    }
}
