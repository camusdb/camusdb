
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// A single entry in the SQL parser AST cache.
/// </summary>
/// <remarks>
/// <see cref="ExpirationTicks"/> is mutable so the cache can extend it on each hit (sliding TTL)
/// without allocating a new entry. All other fields are immutable after construction.
/// </remarks>
internal sealed class ParsedSqlCacheEntry
{
    public NodeAst Ast { get; }

    /// <summary>
    /// Absolute expiry expressed as <see cref="Environment.TickCount64"/> milliseconds.
    /// A monotonic source avoids wall-clock adjustment surprises.
    /// </summary>
    public long ExpirationTicks { get; set; }

    public ParsedSqlCacheEntry(NodeAst ast, long expirationTicks)
    {
        Ast = ast;
        ExpirationTicks = expirationTicks;
    }
}

/// <summary>
/// Process-wide cache of parsed SQL ASTs, keyed by the exact SQL string.
/// </summary>
/// <remarks>
/// Entries use a sliding TTL: each cache hit extends <see cref="ParsedSqlCacheEntry.ExpirationTicks"/>
/// by <c>ttlMs</c> so a frequently-used statement is never evicted while it is in use.
/// <para>
/// Thread safety: <see cref="ConcurrentDictionary{TKey,TValue}"/> provides safe concurrent reads and
/// writes. A brief double-parse is acceptable when two threads race to insert the same new key (last
/// writer wins; both trees are structurally equal). No lock is held on the parse path.
/// </para>
/// <para>
/// Caching is correct because a <see cref="NodeAst"/> returned by
/// <see cref="SQLParserProcessor.Parse"/> is immutable after it is returned — see the invariant
/// documented on <see cref="NodeAst"/> and <see cref="SQLParserProcessor.Parse"/>.
/// </para>
/// </remarks>
internal static class SqlParserCache
{
    private static readonly ConcurrentDictionary<string, ParsedSqlCacheEntry> _cache = new();

    /// <summary>Current number of entries in the cache (for observability / tests).</summary>
    public static int Count => _cache.Count;

    /// <summary>
    /// Tries to retrieve a live (non-expired) cached AST for <paramref name="sql"/>.
    /// On a hit the entry's expiration is slid forward by <paramref name="ttlMs"/>.
    /// </summary>
    public static bool TryGet(string sql, long ttlMs, out NodeAst? ast)
    {
        if (_cache.TryGetValue(sql, out ParsedSqlCacheEntry? entry))
        {
            long now = Environment.TickCount64;
            if (entry.ExpirationTicks >= now)
            {
                entry.ExpirationTicks = now + ttlMs;
                ast = entry.Ast;
                return true;
            }
            // Expired — remove so the sweep doesn't have to race with us.
            _cache.TryRemove(sql, out _);
        }
        ast = null;
        return false;
    }

    /// <summary>
    /// Inserts a successfully-parsed AST into the cache with a fresh expiration.
    /// If another thread inserted the same key first (thundering-herd), the winner's entry
    /// expiration is refreshed instead — no extra allocation.
    /// </summary>
    public static void Store(string sql, NodeAst ast, long ttlMs)
    {
        long expiresAt = Environment.TickCount64 + ttlMs;
        ParsedSqlCacheEntry newEntry = new(ast, expiresAt);

        if (!_cache.TryAdd(sql, newEntry))
        {
            // Another thread inserted first; slide its expiration so it stays hot.
            if (_cache.TryGetValue(sql, out ParsedSqlCacheEntry? existing))
                existing.ExpirationTicks = expiresAt;
        }
    }

    /// <summary>
    /// Removes all entries. Used by tests to produce a clean-slate cache between runs.
    /// </summary>
    public static void Clear() => _cache.Clear();
}
