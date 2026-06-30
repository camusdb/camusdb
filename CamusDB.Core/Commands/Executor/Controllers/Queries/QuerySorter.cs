
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Implements ORDER BY for the query pipeline.
///
/// <b>Small-input / flag-off path:</b> rows are materialised into a <c>List</c>, sorted
/// with <c>List.Sort</c>, and yielded — byte-identical to the previous implementation.
/// This path is always used when <see cref="CamusDBConfig.SpillEnabled"/> is <c>false</c>.
///
/// <b>External merge-sort path</b> (active when <see cref="CamusDBConfig.SpillEnabled"/> is
/// <c>true</c>): rows are accumulated in a buffer until the buffer reaches
/// <see cref="CamusDBConfig.SpillEffectiveThreshold"/>. When the threshold is hit the buffer
/// is sorted and written as a sorted run to a spill file; the buffer is cleared and filling
/// restarts. If no spill occurred when the input ends, the buffered rows are sorted and
/// yielded in-memory (same result as the small-input path). Otherwise the last partial buffer
/// is also spilled, and the runs are k-way merged (via a min-heap) with up to
/// <see cref="CamusDBConfig.SpillMergeFanIn"/> simultaneously-open readers per merge pass.
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

    internal async IAsyncEnumerable<QueryResultRow> SortResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (ticket.OrderBy is null || ticket.OrderBy.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal sort context");

        IComparer<QueryResultRow> comparer = new QueryResultRowOrderComparer(ticket.OrderBy);

        if (!CamusDBConfig.SpillEnabled)
        {
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in dataCursor.WithCancellation(cancellationToken).ConfigureAwait(false))
                rows.Add(row);
            rows.Sort(comparer);
            foreach (QueryResultRow row in rows)
                yield return row;
            yield break;
        }

        await foreach (QueryResultRow row in SortWithPossibleSpillAsync(dataCursor, comparer, cancellationToken).ConfigureAwait(false))
            yield return row;
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
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IComparer<QueryResultRow> comparer = new QueryResultRowOrderComparer(orderBy);

        if (!CamusDBConfig.SpillEnabled)
        {
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
                rows.Add(row);
            rows.Sort(comparer);
            foreach (QueryResultRow row in rows)
                yield return row;
            yield break;
        }

        await foreach (QueryResultRow row in SortWithPossibleSpillAsync(cursor, comparer, cancellationToken).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Sorts <paramref name="input"/> using the supplied <paramref name="comparer"/>. When
    /// <see cref="CamusDBConfig.SpillEnabled"/> is <c>true</c> and the input exceeds
    /// <see cref="CamusDBConfig.SpillEffectiveThreshold"/>, sorted runs are spilled to temp
    /// files and k-way merged. Otherwise rows are sorted in memory.
    ///
    /// Intended for callers that supply their own comparison, such as the DISTINCT deduplication
    /// path, which sorts by all projected columns before streaming dedup.
    /// </summary>
    internal static async IAsyncEnumerable<QueryResultRow> SortAsync(
        IAsyncEnumerable<QueryResultRow> input,
        IComparer<QueryResultRow> comparer,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!CamusDBConfig.SpillEnabled)
        {
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
                rows.Add(row);
            rows.Sort(comparer);
            foreach (QueryResultRow row in rows)
                yield return row;
            yield break;
        }

        await foreach (QueryResultRow row in SortWithPossibleSpillAsync(input, comparer, ct).ConfigureAwait(false))
            yield return row;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Core: drain + optional spill + merge
    // ──────────────────────────────────────────────────────────────────────────

    private static async IAsyncEnumerable<QueryResultRow> SortWithPossibleSpillAsync(
        IAsyncEnumerable<QueryResultRow> input,
        IComparer<QueryResultRow> comparer,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = CamusDBConfig.SpillEffectiveThreshold;
        List<QueryResultRow> buffer = new();
        List<string> runPaths = new();
        SpillScope? scope = null;

        try
        {
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                buffer.Add(row);
                if (buffer.Count >= threshold)
                {
                    scope ??= SpillFileManager.CreateScope(CamusDBConfig.DataDirectory);
                    buffer.Sort(comparer);
                    runPaths.Add(await SpillSortedBufferAsync(scope, buffer, ct).ConfigureAwait(false));
                    buffer.Clear();
                }
            }

            if (runPaths.Count == 0)
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
                    runPaths.Add(await SpillSortedBufferAsync(scope!, buffer, ct).ConfigureAwait(false));
                    buffer.Clear();
                }

                // K-way merge: multi-pass if #runs > fanIn, then stream the final merge.
                await foreach (QueryResultRow row in MergeAndYieldAsync(scope!, runPaths, comparer, ct).ConfigureAwait(false))
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
    /// Serialises <paramref name="sortedBuffer"/> to a new spill file in <paramref name="scope"/>
    /// and returns the file path. The buffer must be sorted before calling.
    /// </summary>
    private static async Task<string> SpillSortedBufferAsync(
        SpillScope scope,
        List<QueryResultRow> sortedBuffer,
        CancellationToken ct)
    {
        string path = scope.OpenWriter(out FileStream writer);
        try
        {
            foreach (QueryResultRow row in sortedBuffer)
                SpillRowCodec.EncodeToStream(writer, row);
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writer.Close();
        }
        return path;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // K-way merge
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reduces <paramref name="runPaths"/> to at most <see cref="CamusDBConfig.SpillMergeFanIn"/>
    /// runs via intermediate merge passes, then yields the final sorted stream directly.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MergeAndYieldAsync(
        SpillScope scope,
        List<string> runPaths,
        IComparer<QueryResultRow> comparer,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int fanIn = CamusDBConfig.SpillMergeFanIn;

        // Reduce to ≤ fanIn runs via intermediate passes (each pass writes new run files).
        while (runPaths.Count > fanIn)
        {
            List<string> nextPaths = new();
            for (int i = 0; i < runPaths.Count; i += fanIn)
            {
                int end = Math.Min(i + fanIn, runPaths.Count);
                List<string> batch = runPaths.GetRange(i, end - i);
                nextPaths.Add(await MergeRunsToFileAsync(scope, batch, comparer, ct).ConfigureAwait(false));
            }
            runPaths = nextPaths;
        }

        // Final pass: stream the merged output directly to the caller.
        await foreach (QueryResultRow row in MergeRunsLazilyAsync(runPaths, comparer, ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Merges the runs at <paramref name="paths"/> into a single new sorted run written to
    /// <paramref name="scope"/>, and returns the path of the merged run.
    /// </summary>
    private static async Task<string> MergeRunsToFileAsync(
        SpillScope scope,
        IReadOnlyList<string> paths,
        IComparer<QueryResultRow> comparer,
        CancellationToken ct)
    {
        string outPath = scope.OpenWriter(out FileStream writer);
        try
        {
            await foreach (QueryResultRow row in MergeRunsLazilyAsync(paths, comparer, ct).ConfigureAwait(false))
                SpillRowCodec.EncodeToStream(writer, row);
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writer.Close();
        }
        return outPath;
    }

    /// <summary>
    /// Performs a k-way merge of the given sorted run files using a min-heap and yields the
    /// merged rows lazily. Open file handles are closed as each run is exhausted; all
    /// remaining handles are released in the <c>finally</c> block on cancellation or exception.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> MergeRunsLazilyAsync(
        IReadOnlyList<string> paths,
        IComparer<QueryResultRow> comparer,
        [EnumeratorCancellation] CancellationToken ct)
    {
        List<SpillRunReader> openReaders = new(paths.Count);
        try
        {
            foreach (string path in paths)
            {
                SpillRunReader? reader = await SpillRunReader.OpenAsync(path, ct).ConfigureAwait(false);
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
    /// </summary>
    private sealed class QueryResultRowOrderComparer : IComparer<QueryResultRow>
    {
        private readonly IReadOnlyList<QueryOrderBy> orderBy;

        public QueryResultRowOrderComparer(IReadOnlyList<QueryOrderBy> orderBy)
        {
            this.orderBy = orderBy;
        }

        public int Compare(QueryResultRow left, QueryResultRow right)
        {
            foreach (QueryOrderBy clause in orderBy)
            {
                ColumnValue leftValue = GetSortValue(left, clause.ColumnName);
                ColumnValue rightValue = GetSortValue(right, clause.ColumnName);
                int comparison = leftValue.CompareTo(rightValue);

                if (comparison == 0)
                    continue;

                return clause.Type == OrderType.Ascending ? comparison : -comparison;
            }

            return 0;
        }
    }
}
