/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Remembers which row id a primary-key value resolved to, so a later point lookup by the same key
/// can read the row directly instead of reading the <c>~pk</c> index entry first.
///
/// <para><b>Why it is safe without invalidation.</b> A row id is allocated once, on INSERT, and
/// never reused; the primary key of a row cannot be updated; a DELETE removes both the row and its
/// index entry; a re-INSERT of the same key allocates a new row id. So a row id names one key value
/// for all time, and a remembered mapping can be wrong in exactly one way: the row it points to has
/// been deleted, on this node or on another. That case is observable: the row read answers "no row",
/// and the caller then falls back to the index read, which is authoritative, and drops the entry.
/// A mapping is therefore never trusted for a negative answer, only for a positive one.</para>
///
/// <para><b>Scope.</b> One instance per <see cref="KvTableStore"/>, so it dies with the store on
/// TRUNCATE, materialized-view refresh, index DDL, drop and eviction. Root databases only: a branch
/// walks ancestor levels and carries tombstones, which this shortcut does not model. Bounded by
/// entry count; when full it is cleared rather than evicted entry by entry, since the next lookups
/// repopulate it and the cost of a cleared cache is one extra index read per key.</para>
/// </summary>
internal sealed class UniqueRowIdCache
{
    private readonly ConcurrentDictionary<string, ObjectIdValue> entries = new();
    private readonly int maxEntries;
    private int count;

    internal UniqueRowIdCache(int maxEntries)
    {
        this.maxEntries = Math.Max(1, maxEntries);
    }

    internal int Count => Volatile.Read(ref count);

    /// <summary>The remembered keys (diagnostics and tests; a racing snapshot, not a consistent one).</summary>
    internal ICollection<string> Keys => entries.Keys;

    internal bool TryGet(string kvKey, out ObjectIdValue rowId) => entries.TryGetValue(kvKey, out rowId);

    internal void Set(string kvKey, ObjectIdValue rowId)
    {
        if (entries.TryAdd(kvKey, rowId))
        {
            if (Interlocked.Increment(ref count) > maxEntries)
                Clear();
            return;
        }

        entries[kvKey] = rowId;
    }

    internal void Remove(string kvKey)
    {
        if (entries.TryRemove(kvKey, out _))
            Interlocked.Decrement(ref count);
    }

    internal void Clear()
    {
        entries.Clear();
        Volatile.Write(ref count, 0);
    }
}
