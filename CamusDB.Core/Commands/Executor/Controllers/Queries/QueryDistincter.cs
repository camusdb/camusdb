
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Removes duplicate output tuples for <c>SELECT DISTINCT</c>.
/// Comparison uses SQL DISTINCT null semantics: two NULL values are equal.
///
/// <para><b>Hash path.</b> A <see cref="HashSet{T}"/> holds one row per distinct tuple, and each
/// new tuple is returned the moment it first arrives. Memory is O(distinct count), the output is in
/// first-seen order, and a downstream <c>LIMIT</c> stops the scan early.</para>
///
/// <para><b>Spill path (<see cref="CamusDBOptions.SpillEnabled"/> = <c>true</c>).</b> Identical to the
/// hash path until the count of <b>distinct</b> tuples reaches
/// <see cref="CamusDBOptions.SpillEffectiveThreshold"/> — so below the threshold the two paths return
/// the same rows in the same order, and spill on costs nothing. A low-cardinality DISTINCT over many
/// rows never spills at all. The trigger and the partitioning follow the GROUP BY spill path
/// (<see cref="QueryAggregator"/>); see <see cref="DistinctWithSpill"/> for how the rows already
/// returned are carried across the spill without being returned twice.</para>
///
/// <para><b>Ordered input.</b> When the input arrives with equal rows adjacent,
/// <see cref="StreamingDistinctRows"/> dedups in O(1) memory and never spills.</para>
/// </summary>
internal sealed class QueryDistincter
{
    // Maximum recursion depth for repartitioning a DISTINCT spill partition. Beyond this depth the
    // partition dedups in memory regardless of size: the hash cannot split it further, so memory is
    // bounded by the partition's distinct tuples, not by its raw rows. Same cap as GROUP BY.
    private const int MaxDistinctRecursionDepth = 3;

    internal IAsyncEnumerable<QueryResultRow> DistinctResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        QueryExecutionContext context)
    {
        if (!context.Options.SpillEnabled)
            return DistinctRows(dataCursor);

        return DistinctWithSpill(dataCursor, context, context.CancellationToken);
    }

    /// <summary>
    /// Streaming deduplication: compares each row to the previously emitted row.
    /// Requires the input to arrive in an order that groups equal rows adjacently (i.e. an
    /// index scan whose prefix covers all projected DISTINCT columns).
    /// Uses O(1) memory (one key) instead of the O(distinct-count) hash set.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> StreamingDistinctRows(
        IAsyncEnumerable<QueryResultRow> dataCursor) =>
        StreamingRows(dataCursor);

    private static async IAsyncEnumerable<QueryResultRow> StreamingRows(
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        IReadOnlyDictionary<string, ColumnValue>? lastRow = null;
        string[]? sortedNames = null;
        // Ordinal-parallel array resolved from the first row's RowLayout. The fast path is
        // guarded on ReferenceEquals(row layout, resolvedLayout) so that rows carrying a
        // different layout instance (different schema version, join node, future DDL reorder)
        // degrade safely to the dictionary path rather than reading the wrong column ordinals.
        int[]? sortedOrdinals = null;
        RowLayout? resolvedLayout = null;
        QueryRow? lastQr = null;

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            if (lastRow is null)
            {
                // Hoist sorted column names on first row — schema is fixed across the cursor.
                sortedNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                lastRow = row.Row;
                if (row.Row is QueryRow qrFirst)
                {
                    lastQr = qrFirst;
                    resolvedLayout = qrFirst.Layout;
                    sortedOrdinals = BuildSortedOrdinals(sortedNames, resolvedLayout);
                }
                yield return row;
                continue;
            }

            bool equal = true;

            if (sortedOrdinals is not null
                && row.Row  is QueryRow qrCurr && ReferenceEquals(qrCurr.Layout, resolvedLayout)
                && lastQr is not null          && ReferenceEquals(lastQr.Layout,  resolvedLayout))
            {
                for (int i = 0; i < sortedOrdinals.Length; i++)
                {
                    int ord = sortedOrdinals[i];
                    ColumnValue cv = ord >= 0 ? qrCurr.GetColumnValue(ord) : ((IReadOnlyDictionary<string, ColumnValue>)qrCurr)[sortedNames![i]];
                    ColumnValue lv = ord >= 0 ? lastQr.GetColumnValue(ord)  : ((IReadOnlyDictionary<string, ColumnValue>)lastQr)[sortedNames![i]];
                    if (!DistinctValuesEqual(cv, lv)) { equal = false; break; }
                }
            }
            else
            {
                for (int i = 0; i < sortedNames!.Length; i++)
                {
                    string name = sortedNames[i];
                    if (!DistinctValuesEqual(row.Row[name], lastRow[name]))
                    {
                        equal = false;
                        break;
                    }
                }
            }

            if (!equal)
            {
                lastRow = row.Row;
                lastQr = row.Row as QueryRow;
                yield return row;
            }
        }
    }

    /// <summary>
    /// Spill-aware DISTINCT. Rows dedup through a hash set exactly as <see cref="DistinctRows"/> does,
    /// and spilling starts only when admitting one more distinct tuple would push the set past
    /// <see cref="CamusDBOptions.SpillEffectiveThreshold"/>.
    ///
    /// <para><b>The rows already returned travel with their partition.</b> At overflow every row in the
    /// set has already gone to the caller, so it must never be returned again, yet a later duplicate of
    /// it must still be suppressed. Each such row is therefore handed, in memory, to the partition its
    /// tuple hashes to, and only the <em>remaining</em> input is written to the
    /// <see cref="CamusDBOptions.SpillMergeFanIn"/> partition files. A partition seeds its set with the
    /// rows it carried (without returning them) and then streams its file, returning only tuples that
    /// are new. Because the hash is deterministic, every later duplicate of a carried tuple lands in the
    /// partition that carried it. Each distinct tuple is returned exactly once.</para>
    ///
    /// <para><b>Order.</b> Below the threshold the output is in first-seen order, identical to the hash
    /// path. After a spill, the rows returned before the overflow come first, in first-seen order, and
    /// the rest follow partition by partition. SQL does not define a DISTINCT order without
    /// <c>ORDER BY</c>.</para>
    ///
    /// <para><b>Peak memory</b> is the threshold's worth of carried rows plus one partition's set — the
    /// same bound as the GROUP BY spill path. The <see cref="SpillScope"/> is disposed in a
    /// <c>finally</c> block, so spill files are deleted on completion, cancellation and error.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> DistinctWithSpill(
        IAsyncEnumerable<QueryResultRow> dataCursor,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct = context.Effective(ct);

        int threshold = context.Options.SpillEffectiveThreshold;
        int K = Math.Max(2, context.Options.SpillMergeFanIn);

        DistinctRowEqualityComparer comparer = new();
        HashSet<QueryResultRow> seen = new(comparer);

        SpillScope? scope = null;
        SpillWriteStream[]? writers = null;
        string[]? paths = null;
        List<QueryResultRow>?[]? carried = null;

        try
        {
            await foreach (QueryResultRow row in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                if (writers is not null)
                {
                    WriteToDistinctPartition(row, comparer, K, seed: 0, writers);
                    continue;
                }

                // Below the cap one Add both tests and admits the tuple. At the cap, a duplicate is
                // still suppressed in memory; only a new tuple triggers the spill.
                if (seen.Count < threshold)
                {
                    if (seen.Add(row))
                        yield return row;
                    continue;
                }

                if (seen.Contains(row))
                    continue;

                context.Probe?.NoteSpill();
                scope = SpillFileManager.CreateScope(context.SpillDirectory, context.Options);
                paths = new string[K];
                writers = new SpillWriteStream[K];
                for (int i = 0; i < K; i++)
                    paths[i] = scope.OpenWriter(out writers[i]);

                carried = PartitionReturnedRows(seen, comparer, K, seed: 0);
                seen.Clear();
                seen.TrimExcess();

                WriteToDistinctPartition(row, comparer, K, seed: 0, writers);
            }

            if (writers is null)
                yield break;

            for (int i = 0; i < K; i++)
            {
                await writers[i].FlushAsync(ct).ConfigureAwait(false);
                writers[i].Close();
            }

            for (int i = 0; i < K; i++)
            {
                List<QueryResultRow> partitionCarried = carried![i]!;
                carried[i] = null;

                await foreach (QueryResultRow row in DistinctPartitionAsync(
                    paths![i], partitionCarried, scope!, depth: 0, seed: 0, context, ct).ConfigureAwait(false))
                    yield return row;
            }
        }
        finally
        {
            if (writers is not null)
            {
                for (int i = 0; i < writers.Length; i++)
                    writers[i]?.Dispose();
            }

            if (scope is not null)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Dedups one spill partition: the rows carried in from the level that produced it (already
    /// returned, so they only seed the set), plus every row in its file. All copies of a tuple are in
    /// this one partition, so a tuple new to this set is new to the whole query.
    ///
    /// <para><b>Overflow works as it does at level 0.</b> When admitting a new tuple would push the set
    /// past <see cref="CamusDBOptions.SpillEffectiveThreshold"/> and <paramref name="depth"/> is below
    /// <see cref="MaxDistinctRecursionDepth"/>, the set's rows (all returned by now) are routed to
    /// sub-partitions under <paramref name="seed"/>+1, so tuples that collided here separate, and only
    /// the rest of the file is written to those sub-partitions. Beyond the depth cap the partition
    /// dedups in memory regardless of size.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> DistinctPartitionAsync(
        string path,
        List<QueryResultRow> carriedRows,
        SpillScope scope,
        int depth,
        int seed,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, context.Options.SpillMaxFrameBytes, ct: ct).ConfigureAwait(false);

        // No file: every row of this partition was carried, and all of them are already returned.
        if (reader is null)
            yield break;

        int threshold = context.Options.SpillEffectiveThreshold;
        int K = Math.Max(2, context.Options.SpillMergeFanIn);
        int newSeed = seed + 1;

        DistinctRowEqualityComparer comparer = new();
        HashSet<QueryResultRow> seen = new(carriedRows.Count, comparer);
        for (int i = 0; i < carriedRows.Count; i++)
            seen.Add(carriedRows[i]);

        string[]? subPaths = null;
        SpillWriteStream[]? subWriters = null;
        List<QueryResultRow>?[]? subCarried = null;

        try
        {
            await using (reader)
            {
                do
                {
                    QueryResultRow row = reader.Current;

                    if (subWriters is not null)
                    {
                        WriteToDistinctPartition(row, comparer, K, newSeed, subWriters);
                        continue;
                    }

                    if (depth >= MaxDistinctRecursionDepth || seen.Count < threshold)
                    {
                        if (seen.Add(row))
                            yield return row;
                        continue;
                    }

                    if (seen.Contains(row))
                        continue;

                    subPaths = new string[K];
                    subWriters = new SpillWriteStream[K];
                    for (int i = 0; i < K; i++)
                        subPaths[i] = scope.OpenWriter(out subWriters[i]);

                    subCarried = PartitionReturnedRows(seen, comparer, K, newSeed);
                    seen.Clear();
                    seen.TrimExcess();

                    WriteToDistinctPartition(row, comparer, K, newSeed, subWriters);
                }
                while (await reader.AdvanceAsync(ct).ConfigureAwait(false));
            }

            if (subWriters is null)
                yield break;

            for (int i = 0; i < K; i++)
            {
                await subWriters[i].FlushAsync(ct).ConfigureAwait(false);
                subWriters[i].Close();
            }
        }
        finally
        {
            if (subWriters is not null)
            {
                for (int i = 0; i < subWriters.Length; i++)
                    subWriters[i]?.Dispose();
            }
        }

        for (int i = 0; i < K; i++)
        {
            List<QueryResultRow> partitionCarried = subCarried![i]!;
            subCarried[i] = null;

            await foreach (QueryResultRow row in DistinctPartitionAsync(
                subPaths![i], partitionCarried, scope, depth + 1, newSeed, context, ct).ConfigureAwait(false))
                yield return row;
        }
    }

    /// <summary>
    /// Routes every row in <paramref name="returned"/> to the partition its tuple hashes to under
    /// <paramref name="seed"/>. Used when a level starts to spill: these rows are already returned,
    /// so they travel in memory to the partition that will receive any later duplicate of them.
    /// </summary>
    private static List<QueryResultRow>?[] PartitionReturnedRows(
        HashSet<QueryResultRow> returned,
        DistinctRowEqualityComparer comparer,
        int K,
        int seed)
    {
        List<QueryResultRow>?[] buckets = new List<QueryResultRow>?[K];

        for (int i = 0; i < K; i++)
            buckets[i] = [];

        foreach (QueryResultRow row in returned)
            buckets[QueryAggregator.PartitionFromHash(comparer.GetHashCode(row), K, seed)]!.Add(row);

        return buckets;
    }

    /// <summary>
    /// Writes <paramref name="row"/> to the partition its tuple hashes to under <paramref name="seed"/>.
    /// The schema-less record format is used because a later row may carry a different
    /// <see cref="RowLayout"/>; the comparer hashes a decoded dictionary row and a positional row with
    /// the same tuple to the same value, so routing stays stable across the round trip.
    /// </summary>
    private static void WriteToDistinctPartition(
        QueryResultRow row,
        DistinctRowEqualityComparer comparer,
        int K,
        int seed,
        SpillWriteStream[] writers)
    {
        int p = QueryAggregator.PartitionFromHash(comparer.GetHashCode(row), K, seed);
        SpillRowCodec.EncodeToStream(writers[p], row);
    }

    private static int[] BuildSortedOrdinals(string[] sortedNames, RowLayout layout)
    {
        int[] ordinals = new int[sortedNames.Length];
        for (int i = 0; i < sortedNames.Length; i++)
            ordinals[i] = layout.IndexOf(sortedNames[i]);
        return ordinals;
    }

    private static bool DistinctValuesEqual(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null && right.Type == ColumnType.Null)
            return true;

        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return false;

        return left.CompareTo(right) == 0;
    }

    private static int DistinctValueHash(ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
            return 0;

        return value.Type switch
        {
            ColumnType.Integer64 => HashCode.Combine(value.Type, value.LongValue),
            ColumnType.Float64 => HashCode.Combine(value.Type, value.FloatValue),
            ColumnType.Bool => HashCode.Combine(value.Type, value.BoolValue),
            ColumnType.String or ColumnType.Id => HashCode.Combine(value.Type, value.StrValue, StringComparer.Ordinal),
            _ => value.Type.GetHashCode(),
        };
    }

    private static async IAsyncEnumerable<QueryResultRow> DistinctRows(
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        HashSet<QueryResultRow> seen = new(new DistinctRowEqualityComparer());

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            if (seen.Add(row))
                yield return row;
        }
    }

    /// <summary>
    /// Deduplicates output rows for <c>SELECT DISTINCT</c> without building a per-row key object.
    /// The canonical sorted-name→ordinal mapping is resolved <b>once</b> from the first
    /// <see cref="QueryRow"/>'s fixed <see cref="RowLayout"/>; thereafter same-layout rows hash and
    /// compare each key cell by ordinal via <see cref="QueryRow.GetColumnValue(int)"/> (per-cell) — no
    /// per-row name sort, tuple array, or key allocation, and no whole-row materialization.
    /// <para>
    /// Rows whose layout differs from the resolved one (a dictionary-backed row, or a different
    /// schema version) fall back to a per-row canonical computation that compares column <b>names</b>
    /// as well as values — preserving the rule that DISTINCT equality includes names when layouts
    /// differ. Hash inputs are identical across both paths (sorted-name order; each column contributes
    /// its name plus <see cref="DistinctValueHash"/>), so two equal rows hash equally regardless of
    /// which path computed the hash. NULL semantics (<c>NULL = NULL</c>) come from
    /// <see cref="DistinctValuesEqual"/>, matching the streaming and sort-based dedup paths.
    /// </para>
    /// </summary>
    private sealed class DistinctRowEqualityComparer : IEqualityComparer<QueryResultRow>
    {
        private string[]? _sortedNames;
        private int[]? _sortedOrdinals;
        private RowLayout? _resolvedLayout;

        public int GetHashCode(QueryResultRow row)
        {
            EnsureResolved(row);

            if (_sortedOrdinals is not null
                && row.Row is QueryRow qr && ReferenceEquals(qr.Layout, _resolvedLayout))
            {
                HashCode hash = new();
                for (int i = 0; i < _sortedOrdinals.Length; i++)
                {
                    int ord = _sortedOrdinals[i];
                    ColumnValue v = ord >= 0 ? qr.GetColumnValue(ord) : ColumnValue.Null;
                    hash.Add(_sortedNames![i], StringComparer.Ordinal);
                    hash.Add(DistinctValueHash(v));
                }
                return hash.ToHashCode();
            }

            (string[] names, ColumnValue[] values) = Canonical(row);
            HashCode fallback = new();
            for (int i = 0; i < names.Length; i++)
            {
                fallback.Add(names[i], StringComparer.Ordinal);
                fallback.Add(DistinctValueHash(values[i]));
            }
            return fallback.ToHashCode();
        }

        public bool Equals(QueryResultRow x, QueryResultRow y)
        {
            // Fast path: both rows carry the resolved layout, so the sorted column names are identical
            // by construction — compare values by ordinal only.
            if (_sortedOrdinals is not null
                && x.Row is QueryRow qx && ReferenceEquals(qx.Layout, _resolvedLayout)
                && y.Row is QueryRow qy && ReferenceEquals(qy.Layout, _resolvedLayout))
            {
                for (int i = 0; i < _sortedOrdinals.Length; i++)
                {
                    int ord = _sortedOrdinals[i];
                    ColumnValue xv = ord >= 0 ? qx.GetColumnValue(ord) : ColumnValue.Null;
                    ColumnValue yv = ord >= 0 ? qy.GetColumnValue(ord) : ColumnValue.Null;
                    if (!DistinctValuesEqual(xv, yv))
                        return false;
                }
                return true;
            }

            // Fallback: a differing layout means the names may differ too, so compare names and values.
            (string[] xn, ColumnValue[] xvv) = Canonical(x);
            (string[] yn, ColumnValue[] yvv) = Canonical(y);
            if (xn.Length != yn.Length)
                return false;

            for (int i = 0; i < xn.Length; i++)
            {
                if (!string.Equals(xn[i], yn[i], StringComparison.Ordinal))
                    return false;
                if (!DistinctValuesEqual(xvv[i], yvv[i]))
                    return false;
            }
            return true;
        }

        private void EnsureResolved(QueryResultRow row)
        {
            if (_sortedNames is not null)
                return;

            _sortedNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
            if (row.Row is QueryRow qr)
            {
                _resolvedLayout = qr.Layout;
                _sortedOrdinals = BuildSortedOrdinals(_sortedNames, _resolvedLayout);
            }
        }

        /// <summary>
        /// Materializes a row's columns in canonical sorted-name order. Used only off the fixed-layout
        /// fast path (dictionary rows or a layout that does not match the resolved one), so its
        /// allocation is not on the steady-state DISTINCT path. Names follow the row's own key set, so
        /// two rows with different column names are correctly unequal.
        /// </summary>
        private static (string[] Names, ColumnValue[] Values) Canonical(QueryResultRow row)
        {
            if (row.Row is QueryRow qr)
            {
                string[] names = qr.Layout.OutputNames.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                ColumnValue[] values = new ColumnValue[names.Length];
                for (int i = 0; i < names.Length; i++)
                {
                    int ord = qr.Layout.IndexOf(names[i]);
                    values[i] = ord >= 0 ? qr.GetColumnValue(ord) : ColumnValue.Null;
                }
                return (names, values);
            }

            string[] dictNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
            ColumnValue[] dictValues = new ColumnValue[dictNames.Length];
            for (int i = 0; i < dictNames.Length; i++)
                dictValues[i] = row.Row[dictNames[i]];
            return (dictNames, dictValues);
        }
    }
}
