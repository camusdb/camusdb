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

    // Not readonly: SetMaxEntries swaps the cap at runtime. Mutated and read under _lock on the
    // Put/trim paths; volatile so no stale cap is observed if a read ever happens outside it.
    private volatile int _maxEntries;

    private readonly object _lock = new();
    private readonly Dictionary<string, LinkedListNode<Slot>> _map;
    private readonly LinkedList<Slot> _lru = new();

    private long _hits;
    private long _misses;
    private long _evictions;
    private long _replayFailures;

    public long Hits    => _hits;
    public long Misses  => _misses;
    public long Evictions => _evictions;

    /// <summary>
    /// Hits whose cached decision could not be rebuilt and fell back to a full replan. A hit is
    /// counted at lookup time, before replay, so <see cref="Hits"/> alone cannot tell a cache that
    /// serves plans from one that only pays lookup plus replan on every query; a test or an
    /// operator must read this counter alongside it.
    /// </summary>
    public long ReplayFailures => _replayFailures;

    /// <summary>Records one failed replay. Called by the planner when a hit falls back to <c>BuildScanNode</c>.</summary>
    public void RecordReplayFailure() => Interlocked.Increment(ref _replayFailures);

    /// <summary>Current number of cached plans; for tests and diagnostics.</summary>
    public int Count { get { lock (_lock) return _map.Count; } }

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

            EvictOverCapacityLocked();
        }
    }

    /// <summary>
    /// Swaps the entry cap at runtime and immediately trims the LRU tail down to it, so lowering
    /// the cap actually frees memory rather than only constraining future inserts. Evictions are
    /// counted exactly as Put-path evictions are.
    /// </summary>
    public void SetMaxEntries(int maxEntries)
    {
        lock (_lock)
        {
            _maxEntries = maxEntries;
            EvictOverCapacityLocked();
        }
    }

    /// <summary>Evicts LRU-tail entries while the cache is over its cap. Callers must hold <c>_lock</c>.</summary>
    private void EvictOverCapacityLocked()
    {
        while (_lru.Count > _maxEntries && _lru.Last is { } tail)
        {
            _map.Remove(tail.Value.Key);
            _lru.RemoveLast();
            _evictions++;
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

    /// <summary>
    /// True when the cached dependency set still describes the current one. Order-insensitive: a
    /// reordering of identical deps must not evict a usable entry.
    /// <para>
    /// Runs under <c>_lock</c> on every lookup, so the two common shapes avoid the dictionary
    /// entirely — a single dependency (an unjoined query, by far the most frequent) is one direct
    /// comparison, and lists that are already in the same order compare positionally. Only a genuine
    /// reordering falls through to the dictionary. Every path compares the full descriptor: table
    /// identity, schema version, index-set generation and analyze generation.
    /// </para>
    /// </summary>
    private static bool SchemaDepsMatch(
        IReadOnlyList<PlanCacheDep> cached,
        IReadOnlyList<PlanCacheDep> current)
    {
        int count = cached.Count;

        if (count != current.Count)
            return false;

        if (count == 0)
            return true;

        if (count == 1)
            return DepMatches(cached[0], current[0]);

        // Same order is the norm: both lists come from the same traversal of the same query shape.
        bool sameOrder = true;

        for (int i = 0; i < count; i++)
        {
            if (!DepMatches(cached[i], current[i]))
            {
                sameOrder = false;
                break;
            }
        }

        if (sameOrder)
            return true;

        // Reordered (or genuinely different): fall back to matching by table identity. A duplicated
        // table id keeps its last cached descriptor, exactly as before; duplicates of one table always
        // carry the same descriptor, so which one wins cannot change the answer.
        Dictionary<string, PlanCacheDep> cachedMap = new(count, StringComparer.Ordinal);

        for (int i = 0; i < count; i++)
            cachedMap[cached[i].TableId] = cached[i];

        for (int i = 0; i < count; i++)
        {
            if (!cachedMap.TryGetValue(current[i].TableId, out PlanCacheDep cachedDep) || !DepMatches(cachedDep, current[i]))
                return false;
        }

        return true;
    }

    /// <summary>Full descriptor comparison for one dependency, table identity included.</summary>
    private static bool DepMatches(in PlanCacheDep cached, in PlanCacheDep current) =>
        cached.SchemaVersion == current.SchemaVersion
        && cached.IndexSetGeneration == current.IndexSetGeneration
        && cached.AnalyzeGeneration == current.AnalyzeGeneration
        && string.Equals(cached.TableId, current.TableId, StringComparison.Ordinal);
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
/// Policy: <strong>chosen-index replay</strong>. <see cref="IndexName"/> names the index (null
/// means the primary-row scan) and <see cref="Kind"/> says how it was used, because the replay
/// differs per kind:
/// <list type="bullet">
///   <item><see cref="ScanDecisionKind.PredicateScan"/>: the bounds are re-derived from the
///   current query's predicates by <see cref="IndexScanSelector.TrySelectScanForForcedIndex"/>.
///   Literal values are never stored, so a later query of the same shape gets its own keys.</item>
///   <item><see cref="ScanDecisionKind.OrderByIndexScan"/> and
///   <see cref="ScanDecisionKind.FullIndexScan"/>: the query has no predicate the index can
///   absorb (ORDER BY elision, the streaming DISTINCT/GROUP BY override, a user-forced index),
///   so the unbounded step is rebuilt directly after re-checking that the index is still
///   readable and, for a unique index, still holds every row. Re-running the predicate matcher
///   here would find no comparisons, report a failed replay, and re-select on every hit.</item>
/// </list>
/// The dependency fingerprint (schema version, index-set generation) invalidates the entry on
/// any DDL that could change which kind applies.
/// </summary>
internal sealed record SingleTableDecision(string? IndexName, ScanDecisionKind Kind);

/// <summary>How a cached single-table decision used its index. See <see cref="SingleTableDecision"/>.</summary>
internal enum ScanDecisionKind
{
    /// <summary>Primary-row scan; no index.</summary>
    TableScan,

    /// <summary>A lookup or bounded range scan whose bounds come from the query's predicates.</summary>
    PredicateScan,

    /// <summary>An unbounded index scan chosen to satisfy ORDER BY.</summary>
    OrderByIndexScan,

    /// <summary>An end-to-end index scan: the streaming DISTINCT/GROUP BY override or a user-forced index.</summary>
    FullIndexScan,
}
