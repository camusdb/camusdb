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
        IReadOnlyList<(string TableName, int SchemaVersion)> currentDeps,
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
        IReadOnlyList<(string, int)> cached,
        IReadOnlyList<(string, int)> current)
    {
        if (cached.Count != current.Count)
            return false;

        // Use a dictionary so that a reordering of identical deps does not produce a
        // spurious miss (safe direction: a miss causes a replan, never a stale hit).
        Dictionary<string, int> cachedMap = new(cached.Count, StringComparer.Ordinal);
        foreach ((string name, int version) in cached)
            cachedMap[name] = version;

        foreach ((string name, int version) in current)
        {
            if (!cachedMap.TryGetValue(name, out int cachedVersion) || cachedVersion != version)
                return false;
        }

        return true;
    }
}

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
    IReadOnlyList<(string TableName, int SchemaVersion)> SchemaDeps,
    SingleTableDecision? SingleTable,
    IReadOnlyList<string>? JoinAliasOrder);

/// <summary>
/// Cached access-path decision for a single-table query.
/// <see cref="IndexName"/> is null when the chosen path is a full table scan.
/// Literal values (lookup keys, range bounds) are <em>not</em> stored; callers re-bind them
/// from the current query's predicates using <see cref="IndexScanSelector.TrySelectScanForForcedIndex"/>.
/// </summary>
internal sealed record SingleTableDecision(
    QueryPlanStepType ScanType,
    string? IndexName);
