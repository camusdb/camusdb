
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Implements ORDER BY for the query pipeline.
///
/// <b>Small-input / flag-off path:</b> rows are materialised into a <c>List</c>, sorted
/// with <c>List.Sort</c>, and yielded — byte-identical to the previous implementation.
/// This path is always used when <see cref="context.Options.SpillEnabled"/> is <c>false</c>.
///
/// <b>External merge-sort path</b> (active when <see cref="context.Options.SpillEnabled"/> is
/// <c>true</c>): rows are accumulated in a buffer until the buffer reaches
/// <see cref="context.Options.SpillEffectiveThreshold"/>. When the threshold is hit the buffer
/// is sorted and written as a sorted run to a spill file; the buffer is cleared and filling
/// restarts. If no spill occurred when the input ends, the buffered rows are sorted and
/// yielded in-memory (same result as the small-input path). Otherwise the last partial buffer
/// is also spilled, and the runs are k-way merged (via a min-heap) with up to
/// <see cref="context.Options.SpillMergeFanIn"/> simultaneously-open readers per merge pass.
///
/// The shared <see cref="QueryResultRowOrderComparer"/> is used by both the in-memory
/// <c>List.Sort</c> and the k-way heap, so the two paths produce the same ordering for
/// distinct-key inputs. For tied keys, SQL leaves the relative order of equal rows
/// unspecified — <c>List.Sort</c> and the heap may resolve ties differently, both are
/// correct. Callers must not depend on the tie-breaking order of equal rows.
///
/// Spill files are cleaned up on normal completion, cancellation, and exception via the
/// <see cref="SpillScope"/> disposable wrapping each sort invocation.
/// </summary>
internal sealed class QuerySorter
{
    // ──────────────────────────────────────────────────────────────────────────
    // Public entry points
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Prefix of the internal column that carries a computed sort key. <c>~</c> is already reserved
    /// for engine-internal names — <c>ValidateNotReservedColumnName</c> refuses it for user columns
    /// and the primary-key index is <c>~pk</c> — so a carrier can never collide with a real column.
    /// </summary>
    private const string SortKeyPrefix = "~sort";

    internal async IAsyncEnumerable<QueryResultRow> SortResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        QueryExecutionContext context,
        long? boundedLimit = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Executed as part of a plan the token arrives on the context; driven directly it arrives
        // on the enumerator. See QueryExecutionContext.Effective for which one wins.
        cancellationToken = context.Effective(cancellationToken);

        if (ticket.OrderBy is null || ticket.OrderBy.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal sort context");

        // LIMIT 0 asks for no rows. Returning here means the input is never enumerated, so the scan
        // below does not run and no ordering key is ever evaluated.
        if (boundedLimit == 0)
            yield break;

        if (!HasComputedKey(ticket.OrderBy))
        {
            IAsyncEnumerable<QueryResultRow> plain = boundedLimit is { } plainBound
                ? TopKAsync(dataCursor, new QueryResultRowOrderComparer(ticket.OrderBy), (int)plainBound, cancellationToken)
                : SortByKeys(dataCursor, ticket.OrderBy, context, cancellationToken);

            await foreach (QueryResultRow row in plain.ConfigureAwait(false))
                yield return row;

            yield break;
        }

        // Bounded retention never spills — the heap holds only k rows — so the computed keys do
        // not need to survive a spill round trip. They ride beside the heap entries instead of
        // being attached as carrier columns, which removes both per-row dictionary copies the
        // carrier costs (the attach on every input row and the strip on every emitted row).
        if (boundedLimit is { } bound)
        {
            IAsyncEnumerable<RankedRow> ranked = EvaluateSortKeysAsync(
                ticket, ticket.OrderBy, dataCursor, cancellationToken);

            await foreach (RankedRow entry in TopKAsync(
                ranked, new RankedRowComparer(ticket.OrderBy), (int)bound, cancellationToken).ConfigureAwait(false))
            {
                yield return entry.Row;
            }

            yield break;
        }

        // The full sort can spill, so every ordering key becomes a materialized column on the row:
        // the existing spill encoder round-trips it with no format change, and the comparer below
        // is the same one an ordinary column sort uses — including its NULL placement, its
        // multi-key walk and its direction handling.
        List<QueryOrderBy> materializedOrder = BuildMaterializedOrder(ticket.OrderBy);

        IAsyncEnumerable<QueryResultRow> keyed = MaterializeSortKeysAsync(
            ticket, ticket.OrderBy, dataCursor, cancellationToken);

        await foreach (QueryResultRow row in SortByKeys(keyed, materializedOrder, context, cancellationToken).ConfigureAwait(false))
            yield return StripSortKeys(row);
    }

    /// <summary>
    /// A row paired with its evaluated ordering keys, one per ORDER BY clause. Used only by the
    /// bounded retention, which keeps at most k of these alive and never writes them to disk.
    /// </summary>
    private readonly record struct RankedRow(QueryResultRow Row, ColumnValue[] Keys);

    /// <summary>
    /// Orders <see cref="RankedRow"/> entries by their pre-evaluated keys. Each key comparison goes
    /// through <see cref="QueryResultRowOrderComparer.CompareKey"/> — the same definition the full
    /// sort's comparer uses — so the two paths agree on NULL placement, direction handling and the
    /// multi-key walk.
    /// </summary>
    private sealed class RankedRowComparer : IComparer<RankedRow>
    {
        private readonly IReadOnlyList<QueryOrderBy> orderBy;

        public RankedRowComparer(IReadOnlyList<QueryOrderBy> orderBy) => this.orderBy = orderBy;

        public int Compare(RankedRow left, RankedRow right)
        {
            for (int i = 0; i < orderBy.Count; i++)
            {
                int comparison = QueryResultRowOrderComparer.CompareKey(left.Keys[i], right.Keys[i], orderBy[i].Type);

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }

    /// <summary>
    /// Evaluates every ordering key <b>once per input row</b> and pairs the row with its key array,
    /// without touching the row itself. Key evaluation is shared with
    /// <see cref="MaterializeSortKeysAsync"/> through <see cref="EvaluateKey"/>, so the bounded and
    /// full-sort paths compute identical keys for identical rows.
    /// </summary>
    private static async IAsyncEnumerable<RankedRow> EvaluateSortKeysAsync(
        QueryTicket ticket,
        IReadOnlyList<QueryOrderBy> orderBy,
        IAsyncEnumerable<QueryResultRow> cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            ColumnValue[] keys = new ColumnValue[orderBy.Count];

            for (int i = 0; i < orderBy.Count; i++)
                keys[i] = EvaluateKey(ticket, orderBy[i], row);

            yield return new RankedRow(row, keys);
        }
    }

    /// <summary>
    /// Retains the <paramref name="k"/> rows that rank first under <paramref name="comparer"/>, in
    /// one pass over the input and with only <c>k</c> rows resident.
    ///
    /// <para>The alternative — sort everything, then throw all but <c>k</c> away — costs
    /// <c>O(n log n)</c> time, <c>O(n)</c> memory, and spills to disk on a large table. This is
    /// <c>O(n log k)</c> and never spills, which is what makes an exact nearest-neighbour query over
    /// a large table finish at all.</para>
    ///
    /// <para>The heap is ordered <b>against</b> the query's ordering, so its head is the worst row
    /// retained so far and a new row only has to beat that one to earn a place. The retained set is
    /// then sorted with the query's own comparer before it is emitted — a heap yields its contents in
    /// heap order, not sorted order, and emitting that directly would return the right k rows in the
    /// wrong sequence.</para>
    ///
    /// <para>Ties are resolved by whichever row arrived first, which SQL leaves unspecified and which
    /// need not match what the full sort would do. A query that needs a determinate order among equal
    /// keys has to name a tie-breaking key.</para>
    /// </summary>
    private static async IAsyncEnumerable<T> TopKAsync<T>(
        IAsyncEnumerable<T> input,
        IComparer<T> comparer,
        int k,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (k <= 0)
            yield break;

        IComparer<T> worstFirst = new ReversedComparer<T>(comparer);
        PriorityQueue<T, T> retained = new(worstFirst);

        await foreach (T row in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (retained.Count < k)
            {
                retained.Enqueue(row, row);
                continue;
            }

            // Strictly better than the worst retained row, so it displaces it. Using strict
            // comparison keeps an equal-ranking row out, which makes the first arrival win a tie.
            if (comparer.Compare(row, retained.Peek()) < 0)
                retained.DequeueEnqueue(row, row);
        }

        List<T> ordered = new(retained.Count);

        while (retained.Count > 0)
            ordered.Add(retained.Dequeue());

        ordered.Sort(comparer);

        foreach (T row in ordered)
            yield return row;
    }

    /// <summary>
    /// Inverts an ordering, turning the shared ascending comparer into the max-heap the bounded
    /// retention needs. Defined here rather than by flipping every clause's direction, so top-k and
    /// the full sort keep comparing rows through exactly the same code.
    /// </summary>
    private sealed class ReversedComparer<T> : IComparer<T>
    {
        private readonly IComparer<T> inner;

        public ReversedComparer(IComparer<T> inner) => this.inner = inner;

        public int Compare(T? left, T? right) => inner.Compare(right, left);
    }

    private static bool HasComputedKey(IReadOnlyList<QueryOrderBy> orderBy)
    {
        foreach (QueryOrderBy clause in orderBy)
        {
            if (clause.IsExpression)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Rewrites the ordering so every key names a materialized carrier column.
    ///
    /// <para>Column keys are carried too, not just computed ones. Reading them through the same
    /// mechanism keeps a single comparison path for a mixed ordering such as
    /// <c>ORDER BY l2_distance(v, @q), id</c>, where splitting the keys across two lookup rules
    /// would be the kind of asymmetry that produces a subtly different order on one of them.</para>
    /// </summary>
    private static List<QueryOrderBy> BuildMaterializedOrder(IReadOnlyList<QueryOrderBy> orderBy)
    {
        List<QueryOrderBy> materialized = new(orderBy.Count);

        for (int i = 0; i < orderBy.Count; i++)
            materialized.Add(new QueryOrderBy(SortKeyPrefix + i, orderBy[i].Type));

        return materialized;
    }

    /// <summary>
    /// Evaluates every ordering key <b>once per input row</b> and attaches the results as internal
    /// carrier columns.
    ///
    /// <para>Evaluating inside the comparer instead would call the expression O(n log n) times rather
    /// than n — for a distance over 768 dimensions that is the difference between a scan and a
    /// stall — and it would let one row's key be computed differently on two comparisons if the
    /// expression were ever non-deterministic.</para>
    ///
    /// <para>The carrier travels as an ordinary column, so the existing spill encoder and k-way merge
    /// round-trip it with no format change, and a spilled key is never recomputed. Only the full
    /// sort pays the attach copy here: the bounded retention never spills, so it takes
    /// <see cref="EvaluateSortKeysAsync"/> instead, and an ordinary column sort keeps the
    /// slot-native comparison path untouched.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MaterializeSortKeysAsync(
        QueryTicket ticket,
        IReadOnlyList<QueryOrderBy> orderBy,
        IAsyncEnumerable<QueryResultRow> cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> carried = new(row.Row.Count + orderBy.Count, StringComparer.Ordinal);

            foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
                carried[cell.Key] = cell.Value;

            for (int i = 0; i < orderBy.Count; i++)
                carried[SortKeyPrefix + i] = EvaluateKey(ticket, orderBy[i], row);

            yield return new QueryResultRow(row.RowId, carried);
        }
    }

    /// <summary>
    /// The one definition of an ordering key's value: a column clause reads the row, an expression
    /// clause evaluates per row. Both the carrier columns of the full sort and the key arrays of
    /// the bounded retention come from here, so the two paths cannot compute different keys for
    /// the same row.
    /// </summary>
    private static ColumnValue EvaluateKey(QueryTicket ticket, QueryOrderBy clause, QueryResultRow row) =>
        clause.Expression is null
            ? ReadColumnKey(row, clause.ColumnName)
            : SqlExecutor.EvalExpr(clause.Expression, row.Row, ticket.Parameters, ticket.RowNameResolver);

    /// <summary>
    /// Reads a column key for the carrier, applying the same qualified-to-bare fallback the
    /// name-based comparer uses (<c>u.position</c> falls back to <c>position</c>), and treating an
    /// absent column as NULL rather than raising. A missing column here is not a sort failure: the
    /// row simply has no value for that key, and SQL orders NULLs rather than rejecting them.
    /// </summary>
    private static ColumnValue ReadColumnKey(QueryResultRow row, string columnName)
    {
        if (row.Row.TryGetValue(columnName, out ColumnValue? value))
            return value;

        int dot = columnName.LastIndexOf('.');

        if (dot >= 0 && dot < columnName.Length - 1 && row.Row.TryGetValue(columnName[(dot + 1)..], out value))
            return value;

        return ColumnValue.Null;
    }

    /// <summary>
    /// Removes the carrier columns before the row leaves the sort operator, so a computed key can
    /// never surface as a column of a <c>SELECT *</c> result.
    /// </summary>
    private static QueryResultRow StripSortKeys(QueryResultRow row)
    {
        Dictionary<string, ColumnValue> stripped = new(row.Row.Count, StringComparer.Ordinal);

        foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
        {
            if (!cell.Key.StartsWith(SortKeyPrefix, StringComparison.Ordinal))
                stripped[cell.Key] = cell.Value;
        }

        return new QueryResultRow(row.RowId, stripped);
    }

    /// <summary>
    /// Materialises <paramref name="cursor"/>, sorts by <paramref name="orderBy"/>, and
    /// yields the sorted rows. Used by <see cref="QueryJoinExecutor"/> to execute a
    /// <see cref="CamusDB.Core.CommandsExecutor.Models.Plans.SortNode"/> inside a join tree
    /// without needing a full <see cref="QueryTicket"/> context.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> SortByKeys(
        IAsyncEnumerable<QueryResultRow> cursor,
        IReadOnlyList<QueryOrderBy> orderBy,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken = context.Effective(cancellationToken);

        IComparer<QueryResultRow> comparer = new QueryResultRowOrderComparer(orderBy);

        if (!context.Options.SpillEnabled)
        {
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
                rows.Add(row);
            rows.Sort(comparer);
            foreach (QueryResultRow row in rows)
                yield return row;
            yield break;
        }

        await foreach (QueryResultRow row in SortWithPossibleSpillAsync(cursor, comparer, context, cancellationToken).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Sorts <paramref name="input"/> using the supplied <paramref name="comparer"/>. When
    /// <see cref="context.Options.SpillEnabled"/> is <c>true</c> and the input exceeds
    /// <see cref="context.Options.SpillEffectiveThreshold"/>, sorted runs are spilled to temp
    /// files and k-way merged. Otherwise rows are sorted in memory.
    ///
    /// Intended for callers that supply their own comparison, such as the DISTINCT deduplication
    /// path, which sorts by all projected columns before streaming dedup.
    /// </summary>
    internal static async IAsyncEnumerable<QueryResultRow> SortAsync(
        IAsyncEnumerable<QueryResultRow> input,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!context.Options.SpillEnabled)
        {
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
                rows.Add(row);
            rows.Sort(comparer);
            foreach (QueryResultRow row in rows)
                yield return row;
            yield break;
        }

        await foreach (QueryResultRow row in SortWithPossibleSpillAsync(input, comparer, context, ct).ConfigureAwait(false))
            yield return row;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Core: drain + optional spill + merge
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Carries a spill run file path together with the optional <see cref="RowLayout"/> it was
    /// written with. When <see cref="Layout"/> is non-null the file was written in value-only
    /// format (no per-row column names) and must be decoded with
    /// <see cref="SpillRunReader.OpenAsync(string, RowLayout?, CancellationToken)"/> passing the
    /// same layout. When null the file uses the schema-less format.
    /// </summary>
    private readonly record struct SpillRun(string Path, RowLayout? Layout);

    private static async IAsyncEnumerable<QueryResultRow> SortWithPossibleSpillAsync(
        IAsyncEnumerable<QueryResultRow> input,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = context.Options.SpillEffectiveThreshold;
        List<QueryResultRow> buffer = new();
        List<SpillRun> runs = new();
        SpillScope? scope = null;

        try
        {
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                buffer.Add(row);
                if (buffer.Count >= threshold)
                {
                    scope ??= SpillFileManager.CreateScope(context.SpillDirectory);
                    buffer.Sort(comparer);
                    runs.Add(await SpillSortedBufferAsync(scope, buffer, context, ct).ConfigureAwait(false));
                    buffer.Clear();
                }
            }

            if (runs.Count == 0)
            {
                // Input fit entirely in the buffer — sort and yield in-memory.
                buffer.Sort(comparer);
                foreach (QueryResultRow row in buffer)
                    yield return row;
            }
            else
            {
                // Spill the remaining partial buffer (may be empty — that's fine).
                if (buffer.Count > 0)
                {
                    buffer.Sort(comparer);
                    runs.Add(await SpillSortedBufferAsync(scope!, buffer, context, ct).ConfigureAwait(false));
                    buffer.Clear();
                }

                // K-way merge: multi-pass if #runs > fanIn, then stream the final merge.
                await foreach (QueryResultRow row in MergeAndYieldAsync(scope!, runs, comparer, context, ct).ConfigureAwait(false))
                    yield return row;
            }
        }
        finally
        {
            if (scope is not null)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Run I/O helpers
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Serialises <paramref name="sortedBuffer"/> to a new spill file in <paramref name="scope"/>.
    /// When every row in the buffer is a <see cref="QueryRow"/> (scanner-emitted), the file is
    /// written in value-only format (column names omitted per row); the returned
    /// <see cref="SpillRun.Layout"/> carries the shared layout for the reader. Otherwise the
    /// schema-less format (column names per row) is used and <see cref="SpillRun.Layout"/> is null.
    /// The buffer must be sorted before calling.
    /// </summary>
    private static async Task<SpillRun> SpillSortedBufferAsync(
        SpillScope scope,
        List<QueryResultRow> sortedBuffer,
        QueryExecutionContext context,
        CancellationToken ct)
    {
        // Detect value-only format: all rows must be QueryRow with the same layout.
        RowLayout? layout = null;
        if (sortedBuffer.Count > 0 && sortedBuffer[0].Row is QueryRow firstQr)
        {
            layout = firstQr.Layout;
            for (int i = 1; i < sortedBuffer.Count; i++)
            {
                if (sortedBuffer[i].Row is not QueryRow qr || !ReferenceEquals(qr.Layout, layout))
                {
                    layout = null;
                    break;
                }
            }
        }

        string path = scope.OpenWriter(out FileStream writer);
        try
        {
            if (layout is not null)
            {
                foreach (QueryResultRow row in sortedBuffer)
                    SpillRowCodec.EncodeValueOnlyToStream(writer, (QueryRow)row.Row);
            }
            else
            {
                foreach (QueryResultRow row in sortedBuffer)
                    SpillRowCodec.EncodeToStream(writer, row);
            }
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writer.Close();
        }
        return new SpillRun(path, layout);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // K-way merge
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reduces <paramref name="runs"/> to at most <see cref="context.Options.SpillMergeFanIn"/>
    /// runs via intermediate merge passes, then yields the final sorted stream directly.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MergeAndYieldAsync(
        SpillScope scope,
        List<SpillRun> runs,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int fanIn = context.Options.SpillMergeFanIn;

        // Reduce to ≤ fanIn runs via intermediate passes (each pass writes new run files).
        while (runs.Count > fanIn)
        {
            List<SpillRun> nextRuns = new();
            for (int i = 0; i < runs.Count; i += fanIn)
            {
                int end = Math.Min(i + fanIn, runs.Count);
                List<SpillRun> batch = runs.GetRange(i, end - i);
                nextRuns.Add(await MergeRunsToFileAsync(scope, batch, comparer, context, ct).ConfigureAwait(false));
            }
            runs = nextRuns;
        }

        // Final pass: stream the merged output directly to the caller.
        await foreach (QueryResultRow row in MergeRunsLazilyAsync(runs, comparer, context, ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Merges the runs in <paramref name="runs"/> into a single new sorted run written to
    /// <paramref name="scope"/>, and returns a <see cref="SpillRun"/> describing the output.
    /// The merged output is written in value-only format when all input runs carry a layout.
    /// </summary>
    private static async Task<SpillRun> MergeRunsToFileAsync(
        SpillScope scope,
        IReadOnlyList<SpillRun> runs,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        CancellationToken ct)
    {
        // Propagate layout-header format when all input runs share it.
        RowLayout? sharedLayout = runs[0].Layout;
        if (sharedLayout is not null)
        {
            for (int i = 1; i < runs.Count; i++)
            {
                if (!ReferenceEquals(runs[i].Layout, sharedLayout))
                {
                    sharedLayout = null;
                    break;
                }
            }
        }

        string outPath = scope.OpenWriter(out FileStream writer);
        try
        {
            await foreach (QueryResultRow row in MergeRunsLazilyAsync(runs, comparer, context, ct).ConfigureAwait(false))
            {
                if (sharedLayout is not null && row.Row is QueryRow qr)
                    SpillRowCodec.EncodeValueOnlyToStream(writer, qr);
                else
                    SpillRowCodec.EncodeToStream(writer, row);
            }
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writer.Close();
        }
        return new SpillRun(outPath, sharedLayout);
    }

    /// <summary>
    /// Performs a k-way merge of the given sorted run files using a min-heap and yields the
    /// merged rows lazily. Open file handles are closed as each run is exhausted; all
    /// remaining handles are released in the <c>finally</c> block on cancellation or exception.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MergeRunsLazilyAsync(
        IReadOnlyList<SpillRun> runs,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        List<SpillRunReader> openReaders = new(runs.Count);
        try
        {
            foreach (SpillRun run in runs)
            {
                SpillRunReader? reader = await SpillRunReader.OpenAsync(run.Path, context.Options.SpillMaxFrameBytes, run.Layout, ct).ConfigureAwait(false);
                if (reader is not null)
                    openReaders.Add(reader);
            }

            PriorityQueue<SpillRunReader, QueryResultRow> heap = new(openReaders.Count, comparer);
            foreach (SpillRunReader r in openReaders)
                heap.Enqueue(r, r.Current);

            while (heap.Count > 0)
            {
                SpillRunReader top = heap.Dequeue();
                yield return top.Current;

                if (await top.AdvanceAsync(ct).ConfigureAwait(false))
                {
                    heap.Enqueue(top, top.Current);
                }
                else
                {
                    openReaders.Remove(top);
                    await top.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (SpillRunReader r in openReaders)
            {
                try { await r.DisposeAsync().ConfigureAwait(false); } catch { }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Sort-key helpers (shared by both paths)
    // ──────────────────────────────────────────────────────────────────────────

    private static ColumnValue GetSortValue(QueryResultRow row, string columnName)
    {
        // Joins key rows by the qualified name ("u.position") — try that first.
        if (row.Row.TryGetValue(columnName, out ColumnValue? value))
            return value;

        // Single-table scan rows are keyed by the bare column name, so an alias-qualified
        // ORDER BY column ("u.position") must fall back to its bare form ("position").
        int dot = columnName.LastIndexOf('.');
        if (dot >= 0 && dot < columnName.Length - 1
            && row.Row.TryGetValue(columnName[(dot + 1)..], out value))
            return value;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Sort column '{columnName}' is missing from result row");
    }

    /// <summary>
    /// Compares two <see cref="QueryResultRow"/> values according to an ORDER BY clause list.
    /// Used by both in-memory <c>List.Sort</c> and the external-sort min-heap, so the two paths
    /// share one comparison definition and agree on the relative order of any two distinct keys.
    /// Equal-key rows (ties) may be ordered differently by each path — SQL leaves that unspecified.
    ///
    /// <para>
    /// When both rows are <see cref="QueryRow"/> (scanner-emitted) and both share the same
    /// <see cref="RowLayout"/> instance, ordinals are resolved from that layout on the first call
    /// and cached. Subsequent comparisons read <see cref="QueryRow.Values"/><c>[ordinal]</c>
    /// directly, bypassing the <see cref="IReadOnlyDictionary{TKey,TValue}"/> adapter.
    /// The same qualified-to-bare fallback (<c>u.col → col</c>) is applied at resolve time.
    /// An ordinal of <c>-1</c> (column not in layout — e.g. a computed sort key) falls back to
    /// <see cref="GetSortValue"/> for that specific column.
    /// </para>
    /// <para>
    /// The fast path is gated on <see cref="object.ReferenceEquals"/> of both rows' layouts to
    /// the resolved layout. Rows that carry a different layout instance (join output rows with a
    /// different join-node layout, or rows from a different schema version) safely degrade to the
    /// dictionary path rather than silently reading the wrong column values.
    /// </para>
    /// </summary>
    private sealed class QueryResultRowOrderComparer : IComparer<QueryResultRow>
    {
        private readonly IReadOnlyList<QueryOrderBy> orderBy;

        // Lazily resolved on the first Compare call where the left row is a QueryRow.
        // Entry is -1 when the sort column is not in the layout (computed key, alias mismatch).
        private int[]? _ordinals;

        // The layout the ordinals were resolved from. The fast path is only taken when both
        // rows' layouts are this same instance — mismatched layouts silently degrade to the
        // dictionary path, which is always correct via the IReadOnlyDictionary adapter.
        private RowLayout? _resolvedLayout;

        public QueryResultRowOrderComparer(IReadOnlyList<QueryOrderBy> orderBy)
        {
            this.orderBy = orderBy;
        }

        /// <summary>
        /// The one definition of a single-key comparison: <see cref="ColumnValue.CompareTo"/> for
        /// the value order (including NULL placement) and a sign flip for direction. Shared with
        /// <see cref="RankedRowComparer"/> so the bounded retention and the full sort cannot
        /// disagree on how a key orders.
        /// </summary>
        internal static int CompareKey(ColumnValue left, ColumnValue right, OrderType direction)
        {
            int comparison = left.CompareTo(right);

            return direction == OrderType.Ascending ? comparison : -comparison;
        }

        public int Compare(QueryResultRow left, QueryResultRow right)
        {
            // Resolve ordinals lazily on first call with a QueryRow input.
            if (_ordinals is null && left.Row is QueryRow qrInit)
            {
                _resolvedLayout = qrInit.Layout;
                _ordinals = ResolveOrdinals(_resolvedLayout, orderBy);
            }

            // Fast path: both rows must carry the exact same layout instance that was used to
            // resolve ordinals. A different layout instance (different schema version, join node,
            // or future DDL reorder) means ordinals address different columns — use the adapter.
            if (_ordinals is not null
                && left.Row  is QueryRow qrL && ReferenceEquals(qrL.Layout, _resolvedLayout)
                && right.Row is QueryRow qrR && ReferenceEquals(qrR.Layout, _resolvedLayout))
            {
                for (int i = 0; i < orderBy.Count; i++)
                {
                    int ord = _ordinals[i];
                    // In-layout keys compare slot-native (no ColumnValue materialized when both rows are
                    // slot-backed); computed keys (ord < 0) fall back to the dictionary-resolved value.
                    int cmp = ord >= 0
                        ? qrL.CompareCellTo(ord, qrR, ord)
                        : GetSortValue(left, orderBy[i].ColumnName).CompareTo(GetSortValue(right, orderBy[i].ColumnName));
                    if (cmp == 0) continue;
                    return orderBy[i].Type == OrderType.Ascending ? cmp : -cmp;
                }
                return 0;
            }

            // Fallback: dictionary path for join rows, aggregation outputs, mixed-layout rows.
            foreach (QueryOrderBy clause in orderBy)
            {
                ColumnValue leftValue  = GetSortValue(left,  clause.ColumnName);
                ColumnValue rightValue = GetSortValue(right, clause.ColumnName);
                int comparison = CompareKey(leftValue, rightValue, clause.Type);
                if (comparison != 0)
                    return comparison;
            }
            return 0;
        }

        private static int[] ResolveOrdinals(RowLayout layout, IReadOnlyList<QueryOrderBy> orderBy)
        {
            int[] ordinals = new int[orderBy.Count];
            for (int i = 0; i < orderBy.Count; i++)
            {
                string col = orderBy[i].ColumnName;
                int ord = layout.IndexOf(col);
                if (ord < 0)
                {
                    // Try bare form of a qualified name ("u.position" → "position").
                    int dot = col.LastIndexOf('.');
                    if (dot >= 0 && dot < col.Length - 1)
                        ord = layout.IndexOf(col[(dot + 1)..]);
                }
                ordinals[i] = ord; // -1 → falls back to GetSortValue per comparison
            }
            return ordinals;
        }
    }
}
