/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The predicate locks of an IN-list scan that seeks one range for each value: a non-unique index,
/// or the leading column of a unique index of several columns. The single-table executor and the
/// join-leaf scanner both take their locks here, so the lock set of the list is the same in a join
/// as in a plain SELECT. A unique index of one column is not served here: its point lookups lock
/// each key themselves.
///
/// <para><b>Why one lock for each value.</b> The phantom fence must cover every value the scan
/// reads, present or absent, and nothing forces it to cover the keys between two values. A lock
/// from the smallest to the largest value does cover them: for list values spread over the key
/// space (random ids) it approaches the whole index, and the read then conflicts with every write
/// to the table. So each value locks exactly the range its seek reads, the bounds
/// <see cref="IndexScanSelector.EqualitySeekUpperBound"/> gives, which is the lock an equality
/// range scan of that one value takes.</para>
///
/// <para><b>The bound.</b> A long list must not take an unbounded number of locks, so
/// <see cref="KvTableStore.InListLocksEachValue"/> decides the granularity once, before the first
/// seek. When it refuses, <see cref="BeginAsync"/> takes one lock over the span of the values and
/// the seeks take none. The locks fire only for a Serializable read-write transaction; every call
/// here is a no-op for another transaction.</para>
/// </summary>
internal static class InListSeekLocks
{
    /// <summary>
    /// Runs once before the first seek. Returns true when each seek must call
    /// <see cref="AcquireValueAsync"/> for its own range. Returns false when the list needs no more
    /// locks: this call then took the span lock (or the transaction takes no predicate lock).
    /// <para>The span bounds are encoded in the index's own key order: on a descending leading
    /// column the largest value encodes first, so it must be the start bound or the lock covers an
    /// inverted, empty range.</para>
    /// </summary>
    internal static async ValueTask<bool> BeginAsync(
        TableDescriptor table,
        TableIndexSchema index,
        IReadOnlyList<ColumnValue> values,
        KvTransaction tx,
        bool exclusive,
        CancellationToken cancellationToken)
    {
        if (table.Store.InListLocksEachValue(tx, index.KvId, values.Count))
            return true;

        CompositeColumnValue? minVal = null, maxVal = null;
        foreach (ColumnValue v in values)
        {
            CompositeColumnValue cv = new(v);
            if (minVal is null || cv.CompareTo(minVal) < 0) minVal = cv;
            if (maxVal is null || cv.CompareTo(maxVal) > 0) maxVal = cv;
        }

        bool leadingDescending = index.DirectionAt(0) == OrderType.Descending;

        // keyColumnCount marks the bounds as a prefix of a unique index of several columns, so the
        // end bound spans every entry of the last value and not the bare point of its encoding.
        await table.Store.AcquireBoundedIndexRangeLockAsync(
            tx, index.KvId,
            leadingDescending ? maxVal : minVal, true,
            leadingDescending ? minVal : maxVal, true,
            unique: index.Type == IndexType.Unique, exclusive: exclusive,
            cancellationToken: cancellationToken,
            keyColumnCount: index.Columns.Length).ConfigureAwait(false);

        return false;
    }

    /// <summary>
    /// Locks the range one value's seek reads: <c>[lookupKey, toBound]</c> with the bounds the seek
    /// itself passes to the index scan. Call it before the seek, and only when
    /// <see cref="BeginAsync"/> returned true.
    /// </summary>
    internal static Task AcquireValueAsync(
        TableDescriptor table,
        TableIndexSchema index,
        CompositeColumnValue lookupKey,
        CompositeColumnValue toBound,
        bool toInclusive,
        KvTransaction tx,
        bool exclusive,
        CancellationToken cancellationToken)
        => table.Store.AcquireBoundedIndexRangeLockAsync(
            tx, index.KvId,
            lookupKey, true,
            toBound, toInclusive,
            unique: index.Type == IndexType.Unique, exclusive: exclusive,
            cancellationToken: cancellationToken,
            keyColumnCount: index.Columns.Length);
}
