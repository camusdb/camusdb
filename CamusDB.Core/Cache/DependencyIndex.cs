
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// Maps dependency facts (keyspace ranges, point keys, schema versions) to the cache entry
/// IDs that depend on them. Used by the invalidation path to find the exact entries to drop
/// on a committed write without scanning the full entry dictionary.
///
/// <para><b>Thread safety:</b> all mutations and reads on this index must be performed while
/// the caller holds its own cache lock. This class is not internally synchronized — it is a
/// data structure, not a concurrent container.</para>
///
/// <para><b>Keyspace bucket convention — must match <c>QueryResultCache.ExtractKeyspaceBucket</c>:</b>
/// range deps must be table-bucket strings without a trailing slash. The exact formats are:
/// <c>"{dbId}:{tableId}:r"</c> for row ranges, <c>"{dbId}:{tableId}:i:{indexId}"</c> for index
/// ranges (note the colon before indexId, matching the real <c>KvTableStore</c> key format).
/// Point deps are full KV keys. Schema deps are <c>"{databaseId}:{tableId}"</c> composite keys
/// for easy lookup by databaseId prefix.</para>
///
/// <para><b>Test coverage:</b> the keyspace and point matching paths (<see cref="FindByKeyspace"/>
/// and <see cref="FindByPoint"/>) are covered by the dep-collector unit tests. The schema path
/// (<see cref="FindByTableSchema"/>) is exercised via <c>InvalidateDatabase</c>, which drives
/// <see cref="Remove"/> entry-by-entry through <c>RemoveEntryLocked</c>.</para>
///
/// <para><b>Idempotency:</b> removing an entry ID that was never added (or already removed)
/// is a no-op. Cleanup after partial add is safe.</para>
/// </summary>
internal sealed class DependencyIndex
{
    // keyspace bucket → set of entry IDs
    private readonly Dictionary<string, HashSet<string>> _rangeIndex  = new(StringComparer.Ordinal);
    
    // KV point key → set of entry IDs
    private readonly Dictionary<string, HashSet<string>> _pointIndex  = new(StringComparer.Ordinal);
    
    // "{databaseId}:{tableId}" → set of entry IDs
    private readonly Dictionary<string, HashSet<string>> _schemaIndex = new(StringComparer.Ordinal);

    /// <summary>
    /// Registers all dependency facts from <paramref name="deps"/> under
    /// <paramref name="entryId"/>. Must be called while the caller holds its cache lock.
    /// </summary>
    public void Add(string entryId, QueryDependencySet deps, string databaseId)
    {
        foreach (string range in deps.RangeDeps)
        {
            if (!_rangeIndex.TryGetValue(range, out HashSet<string>? ids))
                _rangeIndex[range] = ids = new HashSet<string>(StringComparer.Ordinal);
            
            ids.Add(entryId);
        }

        foreach (string point in deps.PointDeps)
        {
            if (!_pointIndex.TryGetValue(point, out HashSet<string>? ids))
                _pointIndex[point] = ids = new HashSet<string>(StringComparer.Ordinal);
            
            ids.Add(entryId);
        }

        foreach ((string tableId, _, _) in deps.SchemaDeps)
        {
            string key = databaseId + ":" + tableId;
            
            if (!_schemaIndex.TryGetValue(key, out HashSet<string>? ids))
                _schemaIndex[key] = ids = new HashSet<string>(StringComparer.Ordinal);
            
            ids.Add(entryId);
        }
    }

    /// <summary>
    /// Removes all index entries for <paramref name="entryId"/>. Must be called while the
    /// caller holds its cache lock.
    /// </summary>
    public void Remove(string entryId, QueryDependencySet deps, string databaseId)
    {
        foreach (string range in deps.RangeDeps)
        {
            if (_rangeIndex.TryGetValue(range, out HashSet<string>? ids))
            {
                ids.Remove(entryId);
                if (ids.Count == 0) _rangeIndex.Remove(range);
            }
        }

        foreach (string point in deps.PointDeps)
        {
            if (_pointIndex.TryGetValue(point, out HashSet<string>? ids))
            {
                ids.Remove(entryId);
                // Point keys are per-row — delete the dictionary entry when the set empties
                // to prevent unbounded growth as rows churn through the cache.
                if (ids.Count == 0) _pointIndex.Remove(point);
            }
        }

        foreach ((string tableId, _, _) in deps.SchemaDeps)
        {
            string key = databaseId + ":" + tableId;
            if (_schemaIndex.TryGetValue(key, out HashSet<string>? ids))
            {
                ids.Remove(entryId);
                if (ids.Count == 0) _schemaIndex.Remove(key);
            }
        }
    }

    /// <summary>
    /// Returns all entry IDs whose range dep set contains <paramref name="keyspace"/>.
    /// Empty if nothing matches. Must be called while the caller holds its cache lock.
    /// </summary>
    public IEnumerable<string> FindByKeyspace(string keyspace)
    {
        return _rangeIndex.TryGetValue(keyspace, out HashSet<string>? ids)
            ? (IEnumerable<string>)ids
            : [];
    }

    /// <summary>
    /// Returns all entry IDs whose point dep set contains <paramref name="kvKey"/>.
    /// Empty if nothing matches.
    /// </summary>
    public IEnumerable<string> FindByPoint(string kvKey)
    {
        return _pointIndex.TryGetValue(kvKey, out HashSet<string>? ids)
            ? (IEnumerable<string>)ids
            : [];
    }

    /// <summary>
    /// Returns all entry IDs with a schema dep on <paramref name="tableId"/> in
    /// <paramref name="databaseId"/>. Empty if nothing matches.
    /// </summary>
    public IEnumerable<string> FindByTableSchema(string databaseId, string tableId)
    {
        string key = databaseId + ":" + tableId;
        return _schemaIndex.TryGetValue(key, out HashSet<string>? ids)
            ? (IEnumerable<string>)ids
            : [];
    }
}
