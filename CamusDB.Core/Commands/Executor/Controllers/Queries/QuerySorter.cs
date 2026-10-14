
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
/// <see cref="context.Options.SpillEffectiveThreshold"/> rows or
/// <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/> estimated bytes
/// (<see cref="RowMemoryEstimator"/>). When the threshold is hit the buffer
/// is sorted and written as a sorted run to a spill file; the buffer is cleared and filling
/// restarts. If no spill occurred when the input ends, the buffered rows are sorted and
/// yielded in-memory (same result as the small-input path). Otherwise the last partial buffer
/// is also spilled, and the runs are k-way merged (via a min-heap) with up to
/// <see cref="context.Options.SpillMergeFanIn"/> simultaneously-open readers per merge pass.
///
/// <b>Computed ordering keys</b> (any ORDER BY clause carrying an expression) are evaluated
/// exactly once per row. The two computed-key paths that never spill — the bounded retention and
/// the flag-off full sort — keep the keys beside the rows (<see cref="RankedRow"/>) and emit the
/// input rows untouched. The spill-capable full sort materializes the keys as internal
/// <c>~sort</c> carrier columns so they round-trip through spill files without recomputation;
/// <see cref="SortKeyCarrier"/> keeps that attachment positional for <see cref="QueryRow"/>
/// inputs and strips it before any row leaves the operator.
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

        bool capRetainedBytes = context.Options.SpillEnabled && context.Options.SpillThresholdBytes > 0;

        if (!HasComputedKey(ticket.OrderBy))
        {
            IAsyncEnumerable<QueryResultRow> plain;

            if (boundedLimit is { } plainBound)
            {
                QueryResultRowOrderComparer comparer = new(ticket.OrderBy);
                CancellationToken token = cancellationToken;
                int k = (int)plainBound;

                TopKByteCap<QueryResultRow>? byteCap = capRetainedBytes
                    ? new(
                        context.Options.SpillEffectiveThresholdBytes,
                        static row => RowMemoryEstimator.Estimate(row),
                        rows => SelectWithSpillAsync(rows, comparer, k, context, token))
                    : null;

                plain = TopKAsync(dataCursor, comparer, k, byteCap, cancellationToken);
            }
            else
            {
                plain = SortByKeys(dataCursor, ticket.OrderBy, context, cancellationToken);
            }

            await foreach (QueryResultRow row in plain.ConfigureAwait(false))
                yield return row;

            yield break;
        }

        // Bounded retention holds only k rows, so the computed keys ride beside the heap entries
        // instead of being attached as carrier columns, which removes both per-row copies the
        // carrier costs (the attach on every input row and the strip on every emitted row). Only
        // when the k retained rows pass the byte cap does the retention hand over to the spilling
        // selection; the already-evaluated keys are attached as carriers at that point, never
        // evaluated again.
        if (boundedLimit is { } bound)
        {
            IAsyncEnumerable<RankedRow> ranked = EvaluateSortKeysAsync(
                ticket, ticket.OrderBy, dataCursor, cancellationToken);

            CancellationToken token = cancellationToken;
            int k = (int)bound;

            TopKByteCap<RankedRow>? byteCap = capRetainedBytes
                ? new(
                    context.Options.SpillEffectiveThresholdBytes,
                    static entry => RowMemoryEstimator.Estimate(entry.Row) + RowMemoryEstimator.EstimateKey(entry.Keys),
                    entries => SelectRankedWithSpillAsync(entries, ticket.OrderBy, k, context, token))
                : null;

            await foreach (RankedRow entry in TopKAsync(
                ranked, new RankedRowComparer(ticket.OrderBy), k, byteCap, cancellationToken).ConfigureAwait(false))
            {
                yield return entry.Row;
            }

            yield break;
        }

        // With spill disabled the full sort buffers every row in memory regardless, so the
        // evaluated keys can ride beside the rows exactly as the bounded retention's do: no
        // carrier column is attached, no strip copy is made, and each row keeps whatever backing
        // it arrived with — a positional (QueryRow) row stays positional for the projector and
        // the wire serializers downstream.
        if (!context.Options.SpillEnabled)
        {
            List<RankedRow> buffered = new();

            await foreach (RankedRow entry in EvaluateSortKeysAsync(
                ticket, ticket.OrderBy, dataCursor, cancellationToken).ConfigureAwait(false))
            {
                buffered.Add(entry);
            }

            buffered.Sort(new RankedRowComparer(ticket.OrderBy));

            foreach (RankedRow entry in buffered)
                yield return entry.Row;

            yield break;
        }

        // The full sort can spill, so every ordering key becomes a materialized column on the row:
        // the existing spill encoder round-trips it with no format change, and the comparer below
        // is the same one an ordinary column sort uses — including its NULL placement, its
        // multi-key walk and its direction handling. The carrier keeps positional rows positional
        // (see SortKeyCarrier), so the value-only spill format and the comparer's ordinal fast
        // path both survive the attach, and the strip hands back the input's own layout.
        List<QueryOrderBy> materializedOrder = BuildMaterializedOrder(ticket.OrderBy);

        SortKeyCarrier carrier = new(ticket.OrderBy.Count);

        IAsyncEnumerable<QueryResultRow> keyed = AttachSortKeysAsync(
            ticket, ticket.OrderBy, dataCursor, carrier, cancellationToken);

        await foreach (QueryResultRow row in SortByKeys(keyed, materializedOrder, context, cancellationToken).ConfigureAwait(false))
            yield return carrier.Strip(row);
    }

    /// <summary>
    /// A row paired with its evaluated ordering keys, one per ORDER BY clause. Used by the two
    /// computed-key paths that do not write rows to disk: the bounded retention (at most k alive)
    /// and the spill-disabled full sort (everything buffered in memory anyway). Keys held here
    /// do not become row columns, so the row itself travels untouched — except when a bounded
    /// retention passes its byte cap and hands its entries to the spilling selection, which attaches
    /// these same keys as carrier columns (<see cref="SelectRankedWithSpillAsync"/>).
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
    /// <see cref="AttachSortKeysAsync"/> through <see cref="EvaluateKey"/>, so the bounded and
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
    /// <c>O(n log k)</c> and does not spill while its k rows stay under the byte cap (below), which is
    /// what makes an exact nearest-neighbour query over a large table finish at all.</para>
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
    ///
    /// <para><b>Byte cap.</b> A row cap of k does not bound the heap when the rows are wide: k rows of
    /// 64 KiB each hold k times 64 KiB. With <paramref name="byteCap"/> set (spill enabled), each
    /// retained entry is charged its estimated bytes, and when the retained entries pass the cap the
    /// retention stops: it hands the retained entries, followed by the rest of the input, to the
    /// spilling selection (<see cref="SelectWithSpillAsync"/>), which returns the same first k rows
    /// (ties aside) with the heap under the cap and at most about k rows on disk. Without a cap the
    /// retention never spills.</para>
    /// </summary>
    private static async IAsyncEnumerable<T> TopKAsync<T>(
        IAsyncEnumerable<T> input,
        IComparer<T> comparer,
        int k,
        TopKByteCap<T>? byteCap,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (k <= 0)
            yield break;

        IComparer<T> worstFirst = new ReversedComparer<T>(comparer);
        PriorityQueue<(T Row, long Bytes), T> retained = new(worstFirst);
        long retainedBytes = 0;

        await using IAsyncEnumerator<T> rows = input.GetAsyncEnumerator(cancellationToken);

        while (await rows.MoveNextAsync().ConfigureAwait(false))
        {
            T row = rows.Current;

            if (retained.Count < k)
            {
                long bytes = byteCap?.SizeOf(row) ?? 0;
                retained.Enqueue((row, bytes), row);
                retainedBytes += bytes;
            }
            else
            {
                // Strictly better than the worst retained row, so it displaces it. Using strict
                // comparison keeps an equal-ranking row out, which makes the first arrival win a tie.
                retained.TryPeek(out _, out T? worst);

                if (comparer.Compare(row, worst) >= 0)
                    continue;

                long bytes = byteCap?.SizeOf(row) ?? 0;
                long evictedBytes = retained.DequeueEnqueue((row, bytes), row).Bytes;
                retainedBytes += bytes - evictedBytes;
            }

            if (byteCap is null || retainedBytes < byteCap.Limit)
                continue;

            // The retained rows passed the byte cap. Hand them, then the rest of the input, to the
            // spilling selection, and release the heap's references before it starts buffering.
            T[] held = new T[retained.Count];
            int heldCount = 0;

            foreach (((T Row, long Bytes) Element, T Priority) item in retained.UnorderedItems)
                held[heldCount++] = item.Element.Row;

            retained.Clear();

            int emitted = 0;

            await foreach (T selected in byteCap.Overflow(ResumeAsync(held, rows)).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return selected;

                if (++emitted == k)
                    yield break;
            }

            yield break;
        }

        List<T> ordered = new(retained.Count);

        while (retained.Count > 0)
            ordered.Add(retained.Dequeue().Row);

        ordered.Sort(comparer);

        foreach (T row in ordered)
            yield return row;
    }

    /// <summary>
    /// The byte cap of a bounded retention and its way out: the cap (<see cref="Limit"/>), how to
    /// charge one retained entry (<see cref="SizeOf"/>), and the spilling selection that takes over
    /// when the retained entries pass the cap (<see cref="Overflow"/>). The selection must return the
    /// first k rows of its input, in order, under the same comparison the retention uses.
    /// </summary>
    private sealed record TopKByteCap<T>(
        long Limit,
        Func<T, long> SizeOf,
        Func<IAsyncEnumerable<T>, IAsyncEnumerable<T>> Overflow);

    /// <summary>
    /// Yields <paramref name="held"/>, then every row <paramref name="rest"/> has not yet produced.
    /// Does not dispose <paramref name="rest"/>: the caller that opened it owns it.
    /// </summary>
    private static async IAsyncEnumerable<T> ResumeAsync<T>(T[] held, IAsyncEnumerator<T> rest)
    {
        for (int i = 0; i < held.Length; i++)
        {
            yield return held[i];
            held[i] = default!;
        }

        while (await rest.MoveNextAsync().ConfigureAwait(false))
            yield return rest.Current;
    }

    /// <summary>
    /// The spilling selection a computed-key bounded retention hands over to. The keys were evaluated
    /// once, when each row entered the retention, so they are attached as carrier columns from the
    /// entries (<see cref="SortKeyCarrier.Attach"/>), never evaluated again, and the selection orders on
    /// them exactly as the spill-capable full computed sort does. The returned entries carry the
    /// stripped row only; their key arrays are empty, because no comparison follows.
    /// </summary>
    private static async IAsyncEnumerable<RankedRow> SelectRankedWithSpillAsync(
        IAsyncEnumerable<RankedRow> entries,
        IReadOnlyList<QueryOrderBy> orderBy,
        int k,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        SortKeyCarrier carrier = new(orderBy.Count);
        QueryResultRowOrderComparer comparer = new(BuildMaterializedOrder(orderBy));

        await foreach (QueryResultRow row in SelectWithSpillAsync(
            AttachRankedKeysAsync(entries, carrier, cancellationToken), comparer, k, context, cancellationToken).ConfigureAwait(false))
        {
            yield return new RankedRow(carrier.Strip(row), []);
        }
    }

    /// <summary>
    /// The sorted run of the best rows a spilling selection has written so far: at most k rows, in
    /// order, with <see cref="Last"/> its final row and <see cref="ReservedBytes"/> the disk bytes its
    /// file holds.
    /// </summary>
    private sealed record KeptRun(SpillRun Run, int Count, QueryResultRow Last, long ReservedBytes);

    /// <summary>
    /// Returns the first <paramref name="k"/> rows of <paramref name="input"/> under
    /// <paramref name="comparer"/>, in order, with the heap held near the byte cap and at most about
    /// k rows on disk. This is the way out of a bounded retention whose k rows are wider than
    /// <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/>.
    ///
    /// <para><b>Why not the external sort.</b> The external sort writes every input row to disk. For a
    /// top-k over a large table that is the whole table, which can pass
    /// <see cref="CamusDBOptions.SpillMaxTotalBytes"/> and fail a query that only needs k rows. This
    /// selection writes only rows that can still be in the result.</para>
    ///
    /// <para><b>How.</b> Rows collect in a buffer. When the buffer reaches the byte cap, it is sorted
    /// and merged with the kept run (at most k rows, on disk) into a new kept run of the first k rows;
    /// the old run's file is deleted at once and its disk bytes are given back. Once the kept run holds
    /// k rows, its last row is a cutoff: k rows at least as good exist, so an input row that does not
    /// rank strictly before the cutoff cannot be in the result and is dropped without being buffered.
    /// The buffer is also cut to its first k rows whenever it holds 2k, which moves the cutoff the
    /// same way. At the end the kept run and the sorted buffer are merged, and the first k rows are
    /// returned.</para>
    ///
    /// <para>Ties: a row equal to the cutoff is dropped (strict comparison), and a merge takes the kept
    /// run's row first. The buffer sort is not stable, so among equal rows the selection can keep a
    /// different subset than the in-memory retention does. Both are correct: SQL leaves the choice
    /// among equal keys unspecified. The <see cref="SpillScope"/> is disposed in a <c>finally</c>
    /// block, so the files are deleted on completion, cancellation and error.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> SelectWithSpillAsync(
        IAsyncEnumerable<QueryResultRow> input,
        IComparer<QueryResultRow> comparer,
        int k,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (k <= 0)
            yield break;

        long byteThreshold = context.Options.SpillEffectiveThresholdBytes;
        List<QueryResultRow> buffer = new();
        long bufferedBytes = 0;
        QueryResultRow? cutoff = null;
        KeptRun? kept = null;
        SpillScope? scope = null;

        try
        {
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                if (cutoff is { } limit && comparer.Compare(row, limit) >= 0)
                    continue;

                buffer.Add(row);
                bufferedBytes += RowMemoryEstimator.Estimate(row);

                if (buffer.Count >= 2L * k)
                {
                    // Only the first k buffered rows can be in the result, so the rest is dropped, and
                    // the k-th row bounds every later row the same way the kept run's last row does.
                    buffer.Sort(comparer);
                    buffer.RemoveRange(k, buffer.Count - k);
                    cutoff = FirstOf(cutoff, buffer[k - 1], comparer);

                    bufferedBytes = 0;
                    foreach (QueryResultRow held in buffer)
                        bufferedBytes += RowMemoryEstimator.Estimate(held);
                }

                if (bufferedBytes < byteThreshold)
                    continue;

                if (scope is null)
                {
                    context.Probe?.NoteSpill();
                    scope = SpillFileManager.CreateScope(context.SpillDirectory, context.Options);
                }

                buffer.Sort(comparer);
                if (buffer.Count > k)
                    buffer.RemoveRange(k, buffer.Count - k);

                kept = await WriteKeptRunAsync(scope, kept, buffer, comparer, k, context, ct).ConfigureAwait(false);
                buffer.Clear();
                bufferedBytes = 0;

                if (kept.Count == k)
                    cutoff = FirstOf(cutoff, kept.Last, comparer);
            }

            buffer.Sort(comparer);

            int emitted = 0;

            await foreach (QueryResultRow row in MergeRunWithSortedAsync(kept?.Run, buffer, comparer, context, ct).ConfigureAwait(false))
            {
                yield return row;

                if (++emitted == k)
                    break;
            }
        }
        finally
        {
            if (scope is not null)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes the first <paramref name="k"/> rows of the merge of <paramref name="kept"/> and the sorted
    /// <paramref name="sortedBuffer"/> as a new kept run, then deletes the old run's file and gives back
    /// its disk bytes, so a spilling selection never holds more than two kept runs on disk. The new run
    /// uses the value-only format when every row it can hold shares one <see cref="RowLayout"/>.
    /// </summary>
    private static async Task<KeptRun> WriteKeptRunAsync(
        SpillScope scope,
        KeptRun? kept,
        List<QueryResultRow> sortedBuffer,
        IComparer<QueryResultRow> comparer,
        int k,
        QueryExecutionContext context,
        CancellationToken ct)
    {
        RowLayout? layout = SharedLayout(kept, sortedBuffer);
        string path = scope.OpenWriter(out SpillWriteStream writer);
        int count = 0;
        QueryResultRow last = default;

        try
        {
            await foreach (QueryResultRow row in MergeRunWithSortedAsync(kept?.Run, sortedBuffer, comparer, context, ct).ConfigureAwait(false))
            {
                if (layout is not null)
                    SpillRowCodec.EncodeValueOnlyToStream(writer, (QueryRow)row.Row);
                else
                    SpillRowCodec.EncodeToStream(writer, row);

                last = row;

                if (++count == k)
                    break;
            }

            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writer.Close();
        }

        // The merge above disposed its reader when the loop ended, so the old file is closed here.
        if (kept is not null)
            scope.DeleteFile(kept.Run.Path, kept.ReservedBytes);

        return new KeptRun(new SpillRun(path, layout), count, last, writer.ReservedBytes);
    }

    /// <summary>
    /// The layout a new kept run can be written under in the value-only format: the layout shared by
    /// the old run (when it has one) and every buffered row, or null when any row differs, in which
    /// case the run is written in the schema-less format.
    /// </summary>
    private static RowLayout? SharedLayout(KeptRun? kept, List<QueryResultRow> sortedBuffer)
    {
        RowLayout? layout = kept is null ? null : kept.Run.Layout;

        if (kept is not null && layout is null)
            return null;

        foreach (QueryResultRow row in sortedBuffer)
        {
            if (row.Row is not QueryRow qr)
                return null;

            layout ??= qr.Layout;

            if (!ReferenceEquals(qr.Layout, layout))
                return null;
        }

        return layout;
    }

    /// <summary>
    /// Merges the sorted run file <paramref name="run"/> (or nothing, when null) with the sorted list
    /// <paramref name="sorted"/>. On equal rows the run's row comes first: it arrived earlier. The run's
    /// reader is disposed when the enumeration ends, early or not.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MergeRunWithSortedAsync(
        SpillRun? run,
        List<QueryResultRow> sorted,
        IComparer<QueryResultRow> comparer,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        SpillRunReader? reader = run is { } spilled
            ? await SpillRunReader.OpenAsync(spilled.Path, context.Options.SpillMaxFrameBytes, spilled.Layout, ct).ConfigureAwait(false)
            : null;

        try
        {
            bool runHasRow = reader is not null;
            int next = 0;

            while (runHasRow || next < sorted.Count)
            {
                if (runHasRow && (next >= sorted.Count || comparer.Compare(reader!.Current, sorted[next]) <= 0))
                {
                    yield return reader!.Current;
                    runHasRow = await reader.AdvanceAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    yield return sorted[next++];
                }
            }
        }
        finally
        {
            if (reader is not null)
                await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>The row of the two that ranks first; <paramref name="current"/> wins a tie.</summary>
    private static QueryResultRow FirstOf(QueryResultRow? current, QueryResultRow candidate, IComparer<QueryResultRow> comparer) =>
        current is { } held && comparer.Compare(held, candidate) <= 0 ? held : candidate;

    private static async IAsyncEnumerable<QueryResultRow> AttachRankedKeysAsync(
        IAsyncEnumerable<RankedRow> entries,
        SortKeyCarrier carrier,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (RankedRow entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return carrier.Attach(entry.Row, entry.Keys);
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
    /// carrier columns through <paramref name="carrier"/>.
    ///
    /// <para>Evaluating inside the comparer instead would call the expression O(n log n) times rather
    /// than n — for a distance over 768 dimensions that is the difference between a scan and a
    /// stall — and it would let one row's key be computed differently on two comparisons if the
    /// expression were ever non-deterministic.</para>
    ///
    /// <para>The carrier travels as an ordinary column, so the existing spill encoder and k-way merge
    /// round-trip it with no format change, and a spilled key is never recomputed. Only the
    /// spill-capable full sort pays the attach copy here: the bounded retention and the
    /// spill-disabled full sort never write rows to disk, so they take
    /// <see cref="EvaluateSortKeysAsync"/> instead, and an ordinary column sort keeps the
    /// slot-native comparison path untouched.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AttachSortKeysAsync(
        QueryTicket ticket,
        IReadOnlyList<QueryOrderBy> orderBy,
        IAsyncEnumerable<QueryResultRow> cursor,
        SortKeyCarrier carrier,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            ColumnValue[] keys = new ColumnValue[orderBy.Count];

            for (int i = 0; i < orderBy.Count; i++)
                keys[i] = EvaluateKey(ticket, orderBy[i], row);

            yield return carrier.Attach(row, keys);
        }
    }

    /// <summary>
    /// Attaches, and later removes, the materialized ordering-key columns of the spill-capable
    /// full computed sort — keeping a positional row positional across the whole round trip.
    ///
    /// <para>A <see cref="QueryRow"/> input gets a positional carrier: one carrier
    /// <see cref="RowLayout"/> (the input's columns followed by the <c>~sort</c> keys) is built
    /// per input layout <b>instance</b> and shared by every row carrying that layout. Sharing one
    /// carrier layout instance is what preserves the two downstream fast paths a per-row
    /// dictionary defeats: the spill writer's value-only format and the order comparer's ordinal
    /// fast path both gate on <see cref="object.ReferenceEquals"/> of row layouts. The strip then
    /// hands back the <b>original</b> input layout instance, so the projector's
    /// <see cref="QueryRow"/> fast path (and any alias names the input layout carried) survive
    /// the sort. Any other row shape falls back to the dictionary carrier, mirroring how the
    /// comparer itself degrades on unknown shapes.</para>
    ///
    /// <para>The carrier row owns a fresh cell array (input cells copied once, keys appended), and
    /// the strip allocates a fresh array of exactly the input's width — a stripped row must never
    /// expose a longer backing array through <see cref="QueryRow.Values"/>, or the extra carrier
    /// cells would leak into whole-row consumers such as the spill encoder.</para>
    ///
    /// <para>Not thread-safe: one instance lives inside a single sort invocation, whose rows are
    /// produced and consumed on one pipeline path.</para>
    /// </summary>
    private sealed class SortKeyCarrier
    {
        private readonly int keyCount;

        /// <summary>Carrier layout per input layout instance (reference identity, matching the comparer's fast-path gate).</summary>
        private readonly Dictionary<RowLayout, RowLayout> carrierByInput = new(ReferenceEqualityComparer.Instance);

        /// <summary>Input layout per carrier layout instance — the strip's way back to the original shape.</summary>
        private readonly Dictionary<RowLayout, RowLayout> inputByCarrier = new(ReferenceEqualityComparer.Instance);

        public SortKeyCarrier(int keyCount) => this.keyCount = keyCount;

        /// <summary>
        /// Returns <paramref name="row"/> with the evaluated <paramref name="keys"/> attached as
        /// trailing <c>~sort</c> columns: positionally for a <see cref="QueryRow"/> input, via a
        /// dictionary copy for any other row shape. The input row itself is never mutated.
        /// </summary>
        public QueryResultRow Attach(QueryResultRow row, ColumnValue[] keys)
        {
            if (row.Row is QueryRow input)
            {
                RowLayout carrierLayout = GetCarrierLayout(input.Layout);
                int width = input.Layout.Count;

                // One owned positional copy. Values materializes a lazily-backed row's cells, which
                // the dictionary carrier paid too — but here they land in an array under a shared
                // layout instead of a per-row dictionary with per-cell hashing.
                ColumnValue[] cells = new ColumnValue[width + keyCount];
                Array.Copy(input.Values, cells, width);

                for (int i = 0; i < keyCount; i++)
                    cells[width + i] = keys[i];

                return new QueryResultRow(row.RowId, new QueryRow(row.RowId, carrierLayout, cells));
            }

            Dictionary<string, ColumnValue> carried = new(row.Row.Count + keyCount, StringComparer.Ordinal);

            foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
                carried[cell.Key] = cell.Value;

            for (int i = 0; i < keyCount; i++)
                carried[SortKeyPrefix + i] = keys[i];

            return new QueryResultRow(row.RowId, carried);
        }

        /// <summary>
        /// Removes the carrier columns before the row leaves the sort operator, so a computed key
        /// can never surface as a column of a <c>SELECT *</c> result. A positional carrier strips
        /// back to the original input layout instance — a spilled run's value-only decode hands
        /// rows back under the same carrier layout instance it was written with, so the reverse
        /// lookup holds across the spill round trip. Everything else (the dictionary carrier, and
        /// a mixed-shape run that spilled schema-less) strips by the <c>~sort</c> name prefix.
        /// </summary>
        public QueryResultRow Strip(QueryResultRow row)
        {
            if (row.Row is QueryRow qr && inputByCarrier.TryGetValue(qr.Layout, out RowLayout? inputLayout))
            {
                ColumnValue[] cells = new ColumnValue[inputLayout.Count];
                Array.Copy(qr.Values, cells, inputLayout.Count);

                return new QueryResultRow(row.RowId, new QueryRow(row.RowId, inputLayout, cells));
            }

            Dictionary<string, ColumnValue> stripped = new(row.Row.Count, StringComparer.Ordinal);

            foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
            {
                if (!cell.Key.StartsWith(SortKeyPrefix, StringComparison.Ordinal))
                    stripped[cell.Key] = cell.Value;
            }

            return new QueryResultRow(row.RowId, stripped);
        }

        private RowLayout GetCarrierLayout(RowLayout input)
        {
            if (carrierByInput.TryGetValue(input, out RowLayout? carrier))
                return carrier;

            string[] names = new string[input.Count + keyCount];
            Array.Copy(input.OutputNames, names, input.Count);

            for (int i = 0; i < keyCount; i++)
                names[input.Count + i] = SortKeyPrefix + i;

            carrier = new RowLayout(names);
            carrierByInput.Add(input, carrier);
            inputByCarrier.Add(carrier, input);

            return carrier;
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
        long byteThreshold = context.Options.SpillEffectiveThresholdBytes;
        long bufferedBytes = 0;
        List<QueryResultRow> buffer = new();
        List<SpillRun> runs = new();
        SpillScope? scope = null;

        try
        {
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                buffer.Add(row);
                bufferedBytes += RowMemoryEstimator.Estimate(row);

                // A run ends at the row cap or at the byte cap, whichever comes first, so a buffer of
                // wide rows spills before it holds the row cap's worth of them.
                if (buffer.Count >= threshold || bufferedBytes >= byteThreshold)
                {
                    bufferedBytes = 0;

                    if (scope is null)
                    {
                        context.Probe?.NoteSpill();
                        scope = SpillFileManager.CreateScope(context.SpillDirectory, context.Options);
                    }
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

        string path = scope.OpenWriter(out SpillWriteStream writer);
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

        string outPath = scope.OpenWriter(out SpillWriteStream writer);
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
