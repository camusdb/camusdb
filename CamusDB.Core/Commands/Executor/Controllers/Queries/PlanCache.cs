/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Thread-safe LRU plan cache.
///
/// Keyed by <c>"{databaseId}:{queryShapeId}"</c> so queries from different databases with
/// identical structure don't share entries. On every cache hit the entry's
/// <see cref="PlanCacheEntry.SchemaDeps"/> are validated against the current schema versions;
/// a version mismatch evicts the stale entry and counts as a miss.
///
/// The cache stores the *optimization decision* — which access path or join ordering to use —
/// without any literal or parameter values. On a hit callers re-bind the current query's
/// literals into the cached structural choice, skipping the expensive cost-enumeration and
/// DP join-order search.
/// </summary>
internal sealed class PlanCache
{
    private sealed class Slot
    {
        public readonly string Key;
        public readonly PlanCacheEntry Entry;
        public Slot(string key, PlanCacheEntry entry) { Key = key; Entry = entry; }
    }

    private readonly int _maxEntries;
    private readonly object _lock = new();
    private readonly Dictionary<string, LinkedListNode<Slot>> _map;
    private readonly LinkedList<Slot> _lru = new();

    private long _hits;
    private long _misses;
    private long _evictions;

    public long Hits    => _hits;
    public long Misses  => _misses;
    public long Evictions => _evictions;

    public PlanCache(int maxEntries = 512)
    {
        _maxEntries = maxEntries;
        _map = new(maxEntries);
    }

    /// <summary>
    /// Attempts to retrieve a plan cache entry for <paramref name="shapeId"/> in
    /// <paramref name="databaseId"/>. Returns false (and increments <see cref="Misses"/>)
    /// when no entry exists OR when the cached entry's schema dependencies no longer match
    /// the current schema versions supplied in <paramref name="currentDeps"/>.
    /// </summary>
    public bool TryGet(
        string databaseId,
        string shapeId,
        IReadOnlyList<PlanCacheDep> currentDeps,
        out PlanCacheEntry? entry)
    {
        string key = MakeKey(databaseId, shapeId);
        lock (_lock)
        {
            if (!_map.TryGetValue(key, out LinkedListNode<Slot>? node))
            {
                entry = null;
                _misses++;
                return false;
            }

            PlanCacheEntry cached = node.Value.Entry;

            if (!SchemaDepsMatch(cached.SchemaDeps, currentDeps))
            {
                _lru.Remove(node);
                _map.Remove(key);
                entry = null;
                _misses++;
                return false;
            }

            // Promote to MRU position.
            _lru.Remove(node);
            _lru.AddFirst(node);

            entry = cached;
            _hits++;
            return true;
        }
    }

    /// <summary>Inserts or replaces the cache entry for the given key.</summary>
    public void Put(string databaseId, string shapeId, PlanCacheEntry entry)
    {
        string key = MakeKey(databaseId, shapeId);
        lock (_lock)
        {
            if (_map.TryGetValue(key, out LinkedListNode<Slot>? existing))
            {
                _lru.Remove(existing);
                _map.Remove(key);
            }

            LinkedListNode<Slot> node = _lru.AddFirst(new Slot(key, entry));
            _map[key] = node;

            while (_lru.Count > _maxEntries && _lru.Last is { } tail)
            {
                _map.Remove(tail.Value.Key);
                _lru.RemoveLast();
                _evictions++;
            }
        }
    }

    /// <summary>Removes all entries (used by tests to isolate state).</summary>
    public void Clear()
    {
        lock (_lock)
        {
            _map.Clear();
            _lru.Clear();
        }
    }

    private static string MakeKey(string databaseId, string shapeId) =>
        string.Concat(databaseId, ":", shapeId);

    private static bool SchemaDepsMatch(
        IReadOnlyList<PlanCacheDep> cached,
        IReadOnlyList<PlanCacheDep> current)
    {
        if (cached.Count != current.Count)
            return false;

        // Use a dictionary so that a reordering of identical deps does not produce a
        // spurious miss (safe direction: a miss causes a replan, never a stale hit).
        Dictionary<string, PlanCacheDep> cachedMap = new(cached.Count, StringComparer.Ordinal);
        foreach (PlanCacheDep dep in cached)
            cachedMap[dep.TableId] = dep;

        foreach (PlanCacheDep dep in current)
        {
            if (!cachedMap.TryGetValue(dep.TableId, out PlanCacheDep cachedDep)
                || cachedDep.SchemaVersion != dep.SchemaVersion
                || cachedDep.IndexSetGeneration != dep.IndexSetGeneration
                || cachedDep.AnalyzeGeneration != dep.AnalyzeGeneration)
                return false;
        }

        return true;
    }
}

/// <summary>
/// One table's contribution to a cached plan's dependency fingerprint.
///
/// <see cref="SchemaVersion"/> alone is insufficient: index DDL (add/drop/rename/state change)
/// deliberately does not bump <see cref="Catalogs.Models.TableSchema.Version"/> (indexes are not
/// part of row encoding), so <see cref="IndexSetGeneration"/> (bumped on every
/// <see cref="CommandsExecutor.Models.TableDescriptor.MutateIndexes"/> swap) invalidates cached
/// access-path decisions when the index set changes — otherwise a CREATE INDEX would never be
/// considered for an already-cached shape. <see cref="AnalyzeGeneration"/> (bumped on every
/// histogram/NDV publish) does the same for statistics refreshes. Both generations are
/// process-unique and never reused, so a descriptor rebuild cannot alias a stale value.
/// </summary>
internal readonly record struct PlanCacheDep(
    string TableId,
    int SchemaVersion,
    long IndexSetGeneration,
    long AnalyzeGeneration);

/// <summary>
/// Immutable optimization decision stored in <see cref="PlanCache"/>.
/// Exactly one of <see cref="SingleTable"/> or <see cref="JoinAliasOrder"/> is non-null.
///
/// Join queries store only the table-alias ordering (e.g. <c>["sessions", "events", "users"]</c>),
/// not the full <see cref="QuerySource"/> AST. On a cache hit the planner re-applies the cached
/// ordering to the <em>current</em> query's source tree (via
/// <see cref="JoinOrderOptimizer.ReorderByAliases"/>), so ON-predicate literals from the current
/// query are always used. Storing the AST directly would freeze the first query's literal values
/// and silently return wrong rows for subsequent queries with the same shape but different ON literals.
/// </summary>
internal sealed record PlanCacheEntry(
    IReadOnlyList<PlanCacheDep> SchemaDeps,
    SingleTableDecision? SingleTable,
    IReadOnlyList<string>? JoinAliasOrder);

/// <summary>
/// Cached access-path decision for a single-table query.
///
/// Policy: <strong>chosen-index replay</strong>.
/// Only <see cref="IndexName"/> is stored — null means full primary-row scan.
/// The scan type (range, lookup, full) is intentionally <em>not</em> cached; instead
/// <see cref="IndexScanSelector.TrySelectScanForForcedIndex"/> re-derives it from the
/// current query's predicates on each cache hit.  This lets a forced-index or order-only
/// scan on the first query replay as a more selective range scan on a subsequent query that
/// has matching predicates — without ever producing wrong results.
///
/// Literal values (lookup keys, range bounds) are never stored; they are always re-bound
/// from the current query at replay time.
/// </summary>
internal sealed record SingleTableDecision(string? IndexName);
