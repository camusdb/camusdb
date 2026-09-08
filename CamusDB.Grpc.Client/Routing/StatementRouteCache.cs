
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Grpc.Client.Routing;

/// <summary>Which operation family a route key describes; part of the cache identity.</summary>
internal enum RouteOpKind : byte
{
    Query = 0,
    NonQuery = 1,
}

/// <summary>
/// Identity of one learned route: the exact statement and binding context advice may be reused
/// for. Database and SQL are kept separate (never concatenated) for the same delimiter-forgery
/// reason as <see cref="Batching.PreparedStatementKey"/>, the SQL text is compared verbatim —
/// no lowercasing, comment stripping, or literal normalization, because exact SQL semantics are
/// the reuse contract — and the hash is precomputed so the hot-path lookup never re-hashes the
/// statement text. Parameter identity needs no extra component: the key holds the exact SQL, and
/// a prepared statement's declared parameter set is a function of that SQL.
/// </summary>
internal readonly struct StatementRouteKey : IEquatable<StatementRouteKey>
{
    public readonly string Database;

    public readonly string Sql;

    public readonly RouteOpKind Kind;

    private readonly int hash;

    public StatementRouteKey(string database, string sql, RouteOpKind kind)
    {
        Database = database;
        Sql = sql;
        Kind = kind;
        hash = HashCode.Combine(
            StringComparer.Ordinal.GetHashCode(database),
            StringComparer.Ordinal.GetHashCode(sql),
            (int)kind);
    }

    /// <summary>Retained-bytes estimate for the cache's byte budget (UTF-16 plus entry overhead).</summary>
    public int ApproximateBytes => (Database.Length + Sql.Length) * 2;

    public bool Equals(StatementRouteKey other)
        => Kind == other.Kind
           && string.Equals(Database, other.Database, StringComparison.Ordinal)
           && string.Equals(Sql, other.Sql, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is StatementRouteKey other && Equals(other);

    public override int GetHashCode() => hash;
}

/// <summary>
/// Bounded, TTL-expiring cache of learned statement destinations.
///
/// <para><b>Revisions defend against late responses.</b> Every stored entry carries a
/// monotonically increasing revision. A caller captures the entry's revision when it dispatches a
/// request and passes it back with the response's advice; the write is applied only when the
/// revision is unchanged, so a slow response — or a late <c>clear</c> — can never replace or
/// remove a route a faster response already refreshed.</para>
///
/// <para><b>Bounded twice.</b> Both an entry count and a retained-byte budget cap the cache, so a
/// workload of high-cardinality SQL text cannot retain unbounded memory. Eviction drops expired
/// entries first, then the least-recently-touched live ones.</para>
///
/// <para>All time values are monotonic milliseconds from the connection's clock (a test seam);
/// wall-clock time is never consulted. Internally locked — the cache is shared by every caller of
/// one connection.</para>
/// </summary>
internal sealed class StatementRouteCache
{
    private sealed class Entry
    {
        public string NodeId = "";
        public string? DependencyToken;
        public long ExpiresAt;
        public long Revision;
        public long LastTouched;
        public int Bytes;
    }

    private readonly object gate = new();

    private readonly Dictionary<StatementRouteKey, Entry> entries = new();

    private readonly int maxEntries;

    private readonly long maxBytes;

    private long retainedBytes;

    private long revisionSeq;

    public StatementRouteCache(int maxEntries, long maxBytes)
    {
        this.maxEntries = Math.Max(1, maxEntries);
        this.maxBytes = Math.Max(1, maxBytes);
    }

    /// <summary>The revision to capture before dispatch; 0 when the key has no entry.</summary>
    public long RevisionOf(in StatementRouteKey key)
    {
        lock (gate)
            return entries.TryGetValue(key, out Entry? entry) ? entry.Revision : 0;
    }

    /// <summary>
    /// The learned node identity when a fresh entry exists, else null (expired entries are
    /// dropped). Also captures the entry's revision under the same lock, so the caller's
    /// dispatch-time observation cannot be torn by a concurrent learn between two lookups.
    /// An expired-and-dropped entry reports revision 0 — the key is gone, and a later learn
    /// against it starts a fresh entry.
    /// </summary>
    public string? TryGet(in StatementRouteKey key, long now, out long revision)
    {
        lock (gate)
        {
            if (!entries.TryGetValue(key, out Entry? entry))
            {
                revision = 0;
                return null;
            }

            if (now >= entry.ExpiresAt)
            {
                RemoveLocked(key, entry);
                revision = 0;
                return null;
            }

            entry.LastTouched = now;
            revision = entry.Revision;
            return entry.NodeId;
        }
    }

    /// <summary>
    /// Stores or refreshes a route. Applied only when the entry's revision still equals
    /// <paramref name="observedRevision"/> (captured at dispatch), so a stale response cannot
    /// overwrite a newer route.
    /// </summary>
    public void Learn(
        in StatementRouteKey key, string nodeId, string? dependencyToken,
        long expiresAt, long observedRevision, long now)
    {
        lock (gate)
        {
            bool exists = entries.TryGetValue(key, out Entry? entry);
            if (exists && entry!.Revision != observedRevision)
                return;
            if (!exists && observedRevision != 0)
                return;

            if (!exists)
            {
                entry = new Entry();
                entries[key] = entry;
            }
            else
            {
                retainedBytes -= entry!.Bytes;
            }

            entry.NodeId = nodeId;
            entry.DependencyToken = dependencyToken;
            entry.ExpiresAt = expiresAt;
            entry.Revision = ++revisionSeq;
            entry.LastTouched = now;
            entry.Bytes = key.ApproximateBytes + (nodeId.Length + (dependencyToken?.Length ?? 0)) * 2 + 64;
            retainedBytes += entry.Bytes;

            EvictLocked(now);
        }
    }

    /// <summary>
    /// Removes a route on a server <c>clear</c>, but only when the entry's revision still equals
    /// what the clearing request observed — a late clear must not delete a newer route.
    /// </summary>
    public void Clear(in StatementRouteKey key, long observedRevision)
    {
        lock (gate)
        {
            if (entries.TryGetValue(key, out Entry? entry) && entry.Revision == observedRevision)
                RemoveLocked(key, entry);
        }
    }

    /// <summary>Current entry count, for tests and diagnostics.</summary>
    public int Count
    {
        get { lock (gate) return entries.Count; }
    }

    /// <summary>Current retained-byte estimate, for tests and diagnostics.</summary>
    public long RetainedBytes
    {
        get { lock (gate) return retainedBytes; }
    }

    private void RemoveLocked(in StatementRouteKey key, Entry entry)
    {
        entries.Remove(key);
        retainedBytes -= entry.Bytes;
    }

    private void EvictLocked(long now)
    {
        if (entries.Count <= maxEntries && retainedBytes <= maxBytes)
            return;

        // First pass: drop everything already expired.
        List<StatementRouteKey>? expired = null;
        foreach (KeyValuePair<StatementRouteKey, Entry> pair in entries)
        {
            if (now >= pair.Value.ExpiresAt)
                (expired ??= new List<StatementRouteKey>()).Add(pair.Key);
        }
        if (expired is not null)
        {
            foreach (StatementRouteKey key in expired)
            {
                if (entries.TryGetValue(key, out Entry? entry))
                    RemoveLocked(key, entry);
            }
        }

        // Second pass: least-recently-touched until under both budgets. An O(n) scan per evicted
        // entry is fine here — eviction happens only at the cap, and the cap is small.
        while (entries.Count > maxEntries || retainedBytes > maxBytes)
        {
            StatementRouteKey? victim = null;
            long oldest = long.MaxValue;
            foreach (KeyValuePair<StatementRouteKey, Entry> pair in entries)
            {
                if (pair.Value.LastTouched < oldest)
                {
                    oldest = pair.Value.LastTouched;
                    victim = pair.Key;
                }
            }

            if (victim is null)
                return;

            if (entries.TryGetValue(victim.Value, out Entry? entry))
                RemoveLocked(victim.Value, entry);
        }
    }
}
