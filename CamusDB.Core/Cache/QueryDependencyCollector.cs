
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
///     <see cref="CamusDBConfig.QueryResultCacheMaxPointDeps"/>. When the point-dep cap is
///     reached, further point deps are silently dropped — the range dep and the point deps
///     collected so far are both retained, and the range dep provides conservative coverage
///     for rows not individually tracked.
///   </description></item>
///   <item><description>
///     Index-scan paths record the index bucket keyspace (<c>{dbId}:{tableId}:i:{indexId}</c>)
///     as a range dep plus each fetched row's point dep. Both must be present: the index range
///     catches phantom inserts / membership changes; the row point catches updates to projected
///     non-indexed columns.
///   </description></item>
///   <item><description>
///     <see cref="CamusDBConfig.QueryResultCacheMaxDeps"/> caps the total dep count across all
///     three categories. If the total is exceeded, <see cref="CapExceeded"/> is set and
///     <see cref="Build"/> returns <see cref="QueryDependencySet.Empty"/> — the caller must
///     bypass publish rather than store an incomplete dep set.
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

    /// <summary>
    /// True if any dep cap was exceeded. When true, <see cref="Build"/> returns
    /// <see cref="QueryDependencySet.Empty"/> and the caller must bypass publish.
    /// </summary>
    public bool CapExceeded => _capExceeded;

    /// <summary>
    /// Records a keyspace range as a dependency (table row bucket or index bucket).
    /// The bucket string must use the real KvTableStore format:
    /// <c>{dbId}:{tableId}:r</c> for rows, <c>{dbId}:{tableId}:i:{indexId}</c> for indexes.
    /// </summary>
    public void RecordRange(string keyspaceBucket)
    {
        if (_capExceeded) return;

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

        // Silently drop point deps beyond the per-query cap; range dep already covers the bucket.
        if (_pointDeps.Count >= CamusDBConfig.QueryResultCacheMaxPointDeps)
            return;

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
