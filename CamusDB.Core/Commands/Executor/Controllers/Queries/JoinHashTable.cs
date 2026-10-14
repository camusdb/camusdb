/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The in-memory table of a hash join build: build rows grouped by their equi-join key, each
/// group (bucket) in the order its rows were added.
///
/// <para><b>Why not a dictionary of lists.</b> Most builds have mostly unique keys. A list per
/// key costs a list object plus a backing array for a single row, roughly 88 bytes on top of the
/// row and its key, for every distinct key the build holds. Here a bucket value is the row itself
/// until a second row with the same key arrives; only then does it become a list. The dictionary
/// entry keeps the size of one reference either way, so its entry array does not grow.</para>
///
/// <para><b>Order.</b> Rows of one bucket enumerate in the order they were added. Broadcast joins
/// depend on it: the coordinator flattens the build in bucket order and ships it, a remote probe
/// rebuilds the same buckets from that flat list, and the match indices it returns point into
/// the flat list, so both sides must see the rows of a bucket in one order.</para>
///
/// <para>Keys are looked up as a span over the caller's scratch array, so a probe allocates no
/// key; an owned key is copied (by <see cref="CompositeColumnValueComparer"/>) only when a new
/// bucket is added. One instance belongs to one join execution and is used from one thread.</para>
/// </summary>
internal sealed class JoinHashTable
{
    /// <summary>One dictionary entry (hash, next, key and value references) plus its bucket slot.</summary>
    private const int BucketEntryBytes = 32;

    private readonly Dictionary<CompositeColumnValue, object> buckets = new(CompositeColumnValueComparer.Instance);

    private readonly Dictionary<CompositeColumnValue, object>.AlternateLookup<ReadOnlySpan<ColumnValue>> lookup;

    public JoinHashTable()
    {
        lookup = buckets.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
    }

    /// <summary>Total build rows across every bucket.</summary>
    public int RowCount { get; private set; }

    /// <summary>
    /// Adds <paramref name="row"/> to the bucket of <paramref name="key"/>. The key span is read
    /// during the call only, so the caller may reuse its scratch array afterward. Returns true when
    /// the key opened a new bucket, which is when the table copies and keeps the key.
    /// </summary>
    public bool Add(ReadOnlySpan<ColumnValue> key, IReadOnlyDictionary<string, ColumnValue> row)
    {
        bool newBucket = false;

        if (!lookup.TryGetValue(key, out object? existing))
        {
            lookup[key] = row;
            newBucket = true;
        }
        else if (existing is List<IReadOnlyDictionary<string, ColumnValue>> list)
        {
            list.Add(row);
        }
        else
        {
            lookup[key] = new List<IReadOnlyDictionary<string, ColumnValue>>(2) { (IReadOnlyDictionary<string, ColumnValue>)existing, row };
        }

        RowCount++;
        return newBucket;
    }

    /// <summary>
    /// Estimated heap bytes one <see cref="Add"/> keeps alive, for the byte cap of a spilling build:
    /// the row (<see cref="RowMemoryEstimator.EstimateRow"/>) and its place in a bucket, plus, when
    /// the row opened a new bucket, the copied key and the dictionary entry. A bucket that turns into
    /// a list is charged one reference per row, which covers the list's array but not the list object.
    /// </summary>
    public static long EstimateAddedBytes(
        ReadOnlySpan<ColumnValue> key,
        IReadOnlyDictionary<string, ColumnValue> row,
        bool newBucket)
    {
        long bytes = RowMemoryEstimator.ReferenceBytes + RowMemoryEstimator.EstimateRow(row);

        if (newBucket)
            bytes += BucketEntryBytes + RowMemoryEstimator.EstimateKey(key);

        return bytes;
    }

    /// <summary>Finds the bucket of <paramref name="key"/>; false when no build row has that key.</summary>
    public bool TryGetBucket(ReadOnlySpan<ColumnValue> key, out JoinBucket bucket)
    {
        if (lookup.TryGetValue(key, out object? value))
        {
            bucket = new JoinBucket(value);
            return true;
        }

        bucket = default;
        return false;
    }

    /// <summary>Every bucket, in the order the dictionary enumerates them.</summary>
    public IEnumerable<JoinBucket> Buckets
    {
        get
        {
            foreach (object value in buckets.Values)
                yield return new JoinBucket(value);
        }
    }
}

/// <summary>
/// The build rows of one <see cref="JoinHashTable"/> key, in the order they were added. A view:
/// it owns nothing and is valid while the table is not changed.
/// </summary>
internal readonly struct JoinBucket
{
    private readonly object value;

    internal JoinBucket(object value) => this.value = value;

    /// <summary>Number of rows in the bucket (at least one).</summary>
    public int Count => value is List<IReadOnlyDictionary<string, ColumnValue>> list ? list.Count : 1;

    public Enumerator GetEnumerator() => new(value);

    /// <summary>Allocation-free enumerator over the one row or the list of rows.</summary>
    public struct Enumerator
    {
        private readonly IReadOnlyDictionary<string, ColumnValue>? single;

        private readonly List<IReadOnlyDictionary<string, ColumnValue>>? list;

        private int index;

        internal Enumerator(object value)
        {
            list = value as List<IReadOnlyDictionary<string, ColumnValue>>;
            single = list is null ? (IReadOnlyDictionary<string, ColumnValue>)value : null;
            index = -1;
        }

        public IReadOnlyDictionary<string, ColumnValue> Current => list is not null ? list[index] : single!;

        public bool MoveNext()
        {
            index++;
            return list is not null ? index < list.Count : index == 0;
        }
    }
}
