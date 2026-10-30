/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Computes the window calls of a windowed SELECT over the rows of its inner query and adds one
/// result cell per call (<see cref="WindowFunctionCall.ResultColumn"/>) to every row. It runs inside
/// <see cref="DerivedTableExecutor"/>, between the inner query and the derived table's filter, so the
/// rows it reads have passed WHERE, GROUP BY and HAVING, and a filter on a window result applies
/// after the window.
///
/// <para><b>Spec groups.</b> Calls with the same PARTITION BY and ORDER BY columns share one pass: the
/// rows are sorted once on the partition columns, then the order columns, through
/// <see cref="QuerySorter.SortByKeys"/>, which spills under the usual threshold. A group with no
/// columns is one partition in the input order and is not sorted. Each further group sorts the
/// output of the one before, which already carries that group's results.</para>
///
/// <para><b>One partition at a time.</b> The full rows of a partition go into a
/// <see cref="SpillableRowList"/>, so a large partition spills. The cells the calls read — their
/// inputs and the order columns that decide peers — are kept in memory as arrays for the partition,
/// together with the results. That gives every frame and every offset random access. The cost is a
/// few values per row of the largest partition, not its full rows.</para>
///
/// <para><b>Peers and frames.</b> Rows with equal ORDER BY cells (<see cref="ColumnValue.CompareTo"/>)
/// are peers. Without an ORDER BY every row of the partition is a peer, which makes the default frame
/// the whole partition and every rank 1, as in PostgreSQL. A frame becomes a pair of row indexes per
/// row; both only grow from one row to the next, so an aggregate slides: it adds the rows that enter
/// on the right and removes the rows that leave on the left (see <see cref="ComputeAggregate"/>). A
/// Float64 sum slides exactly through <see cref="ExactDoubleSum"/>, so every frame shape costs O(n)
/// per partition.</para>
///
/// <para><b>Cancellation.</b> The computation of one partition has no await, so every per-row loop in
/// it checks the token every <see cref="CancellationCheckInterval"/> rows, and a cancel ends a large
/// partition in bounded time.</para>
///
/// <para><b>Cleanup.</b> A partition buffer is disposed when its rows have been emitted, and in a
/// <c>finally</c> when the enumeration stops early or throws, so its spill file is deleted on every
/// path. The sort cleans up its own runs.</para>
/// </summary>
internal static class WindowOperator
{
    private const int CancellationCheckInterval = 1024;

    /// <param name="columns">
    /// The derived table's columns, result columns included, as <see cref="WindowResultSchema"/> typed
    /// them. A call whose result type depends on a value known only now (a placeholder default of
    /// <c>lag</c>/<c>lead</c>) reads its declared type from here.
    /// </param>
    public static IAsyncEnumerable<QueryResultRow> Apply(
        IAsyncEnumerable<QueryResultRow> input,
        IReadOnlyList<WindowFunctionCall> windows,
        IReadOnlyList<DerivedColumnSchema> columns,
        QueryExecutionContext context,
        QuerySorter sorter,
        Dictionary<string, ColumnValue>? parameters,
        WindowStats? stats,
        CancellationToken cancellationToken)
    {
        foreach (SpecGroup group in SpecGroup.Build(windows))
            input = RunGroup(input, group, columns, context, sorter, parameters, stats, cancellationToken);

        return input;
    }

    /// <summary>The calls that share one PARTITION BY and ORDER BY, and so one sort.</summary>
    private sealed class SpecGroup
    {
        public required IReadOnlyList<string> PartitionColumns { get; init; }

        public required IReadOnlyList<QueryOrderBy> OrderColumns { get; init; }

        public List<WindowFunctionCall> Calls { get; } = [];

        public static List<SpecGroup> Build(IReadOnlyList<WindowFunctionCall> windows)
        {
            List<SpecGroup> groups = [];

            foreach (WindowFunctionCall window in windows)
            {
                SpecGroup? match = null;

                foreach (SpecGroup group in groups)
                {
                    if (group.Matches(window))
                    {
                        match = group;
                        break;
                    }
                }

                if (match is null)
                {
                    match = new SpecGroup { PartitionColumns = window.PartitionColumns, OrderColumns = window.OrderColumns };
                    groups.Add(match);
                }

                match.Calls.Add(window);
            }

            return groups;
        }

        private bool Matches(WindowFunctionCall window)
        {
            if (window.PartitionColumns.Count != PartitionColumns.Count || window.OrderColumns.Count != OrderColumns.Count)
                return false;

            for (int i = 0; i < PartitionColumns.Count; i++)
            {
                if (!string.Equals(window.PartitionColumns[i], PartitionColumns[i], StringComparison.Ordinal))
                    return false;
            }

            for (int i = 0; i < OrderColumns.Count; i++)
            {
                if (!string.Equals(window.OrderColumns[i].ColumnName, OrderColumns[i].ColumnName, StringComparison.Ordinal)
                    || window.OrderColumns[i].Type != OrderColumns[i].Type)
                    return false;
            }

            return true;
        }

        /// <summary>The sort that brings each partition together, in window order inside it.</summary>
        public List<QueryOrderBy> SortKeys()
        {
            List<QueryOrderBy> keys = new(PartitionColumns.Count + OrderColumns.Count);

            foreach (string column in PartitionColumns)
                keys.Add(new QueryOrderBy(column, OrderType.Ascending));

            keys.AddRange(OrderColumns);
            return keys;
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> RunGroup(
        IAsyncEnumerable<QueryResultRow> input,
        SpecGroup group,
        IReadOnlyList<DerivedColumnSchema> columns,
        QueryExecutionContext context,
        QuerySorter sorter,
        Dictionary<string, ColumnValue>? parameters,
        WindowStats? stats,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken = context.Effective(cancellationToken);

        List<QueryOrderBy> sortKeys = group.SortKeys();
        IAsyncEnumerable<QueryResultRow> sorted = sortKeys.Count == 0
            ? input
            : sorter.SortByKeys(input, sortKeys, context, cancellationToken);

        PartitionCollector collector = new(group, parameters, columns);
        ResultAppender appender = new(group.Calls);
        SpillableRowList? buffer = null;
        ColumnValue[]? partitionKeys = null;
        long seen = 0;

        try
        {
            await foreach (QueryResultRow row in sorted.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (++seen % CancellationCheckInterval == 0)
                    cancellationToken.ThrowIfCancellationRequested();

                ColumnValue[] keys = ReadCells(row, group.PartitionColumns);

                if (buffer is not null && !SameCells(partitionKeys!, keys))
                {
                    await foreach (QueryResultRow emitted in EmitAsync(buffer, collector, appender, stats, cancellationToken).ConfigureAwait(false))
                        yield return emitted;

                    await buffer.DisposeAsync().ConfigureAwait(false);
                    buffer = null;
                }

                if (buffer is null)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    buffer = new SpillableRowList(context);
                    collector.Reset();
                    partitionKeys = keys;
                }

                collector.Add(row);
                await buffer.AddAsync(row, cancellationToken).ConfigureAwait(false);
            }

            if (buffer is not null)
            {
                await foreach (QueryResultRow emitted in EmitAsync(buffer, collector, appender, stats, cancellationToken).ConfigureAwait(false))
                    yield return emitted;
            }
        }
        finally
        {
            if (buffer is not null)
                await buffer.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Computes the partition's results, then streams its rows back with the results added.</summary>
    private static async IAsyncEnumerable<QueryResultRow> EmitAsync(
        SpillableRowList buffer,
        PartitionCollector collector,
        ResultAppender appender,
        WindowStats? stats,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await buffer.SealAsync(cancellationToken).ConfigureAwait(false);

        ColumnValue[][] results = collector.Compute(cancellationToken);

        if (stats is not null)
        {
            stats.Partitions++;
            stats.Rows += buffer.Count;

            if (buffer.Spilled)
                stats.SpilledPartitions++;
        }

        int index = 0;

        await foreach (QueryResultRow row in buffer.EnumerateAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return appender.Append(row, results, index);
            index++;
        }
    }

    private static ColumnValue[] ReadCells(QueryResultRow row, IReadOnlyList<string> columns)
    {
        if (columns.Count == 0)
            return [];

        ColumnValue[] cells = new ColumnValue[columns.Count];

        for (int i = 0; i < columns.Count; i++)
            cells[i] = ReadCell(row, columns[i]);

        return cells;
    }

    private static ColumnValue ReadCell(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) && value is not null ? value : ColumnValue.Null;

    /// <summary>
    /// Checks the token once per <see cref="CancellationCheckInterval"/> rows of a loop that has no
    /// await, so a cancel ends the computation of a large partition in bounded time.
    /// </summary>
    private static void CheckCancellation(int row, CancellationToken cancellationToken)
    {
        if ((row & (CancellationCheckInterval - 1)) == 0)
            cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>Two partition keys are one partition when every cell compares equal, NULL to NULL included.</summary>
    private static bool SameCells(ColumnValue[] left, ColumnValue[] right)
    {
        for (int i = 0; i < left.Length; i++)
        {
            if (left[i].CompareTo(right[i]) != 0)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Adds the result cells of one spec group to a row. A positional row keeps a positional layout:
    /// the extended layout is built once per input layout, as the sort-key carrier does.
    /// </summary>
    private sealed class ResultAppender
    {
        private readonly string[] resultColumns;
        private readonly Dictionary<RowLayout, RowLayout> layouts = new(ReferenceEqualityComparer.Instance);

        public ResultAppender(List<WindowFunctionCall> calls)
        {
            resultColumns = new string[calls.Count];

            for (int i = 0; i < calls.Count; i++)
                resultColumns[i] = calls[i].ResultColumn;
        }

        public QueryResultRow Append(QueryResultRow row, ColumnValue[][] results, int index)
        {
            if (row.Row is QueryRow input)
            {
                if (!layouts.TryGetValue(input.Layout, out RowLayout? layout))
                {
                    string[] names = new string[input.Layout.Count + resultColumns.Length];
                    Array.Copy(input.Layout.OutputNames, names, input.Layout.Count);
                    Array.Copy(resultColumns, 0, names, input.Layout.Count, resultColumns.Length);
                    layout = new RowLayout(names);
                    layouts.Add(input.Layout, layout);
                }

                int width = input.Layout.Count;
                ColumnValue[] cells = new ColumnValue[width + resultColumns.Length];
                Array.Copy(input.Values, cells, width);

                for (int i = 0; i < resultColumns.Length; i++)
                    cells[width + i] = results[i][index];

                return new QueryResultRow(row.RowId, new QueryRow(row.RowId, layout, cells));
            }

            Dictionary<string, ColumnValue> extended = new(row.Row.Count + resultColumns.Length, StringComparer.OrdinalIgnoreCase);

            foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
                extended[cell.Key] = cell.Value;

            for (int i = 0; i < resultColumns.Length; i++)
                extended[resultColumns[i]] = results[i][index];

            return new QueryResultRow(row.RowId, extended);
        }
    }

    /// <summary>
    /// The in-memory part of one partition: the cells the group's calls read, by column, and the
    /// constants of the calls, evaluated once. <see cref="Compute"/> turns them into results.
    /// </summary>
    private sealed class PartitionCollector
    {
        private readonly SpecGroup group;
        private readonly Dictionary<string, List<ColumnValue>> cells = new(StringComparer.Ordinal);
        private readonly List<ColumnValue>[] orderCells;
        private readonly CallPlan[] plans;
        private int count;

        public PartitionCollector(SpecGroup group, Dictionary<string, ColumnValue>? parameters, IReadOnlyList<DerivedColumnSchema> columns)
        {
            this.group = group;
            orderCells = new List<ColumnValue>[group.OrderColumns.Count];

            for (int i = 0; i < orderCells.Length; i++)
                orderCells[i] = Column(group.OrderColumns[i].ColumnName);

            plans = new CallPlan[group.Calls.Count];

            for (int i = 0; i < plans.Length; i++)
                plans[i] = new CallPlan(group.Calls[i], parameters, Column, columns);
        }

        private List<ColumnValue> Column(string name)
        {
            if (!cells.TryGetValue(name, out List<ColumnValue>? column))
            {
                column = [];
                cells.Add(name, column);
            }

            return column;
        }

        public void Reset()
        {
            foreach (List<ColumnValue> column in cells.Values)
                column.Clear();

            count = 0;
        }

        public void Add(QueryResultRow row)
        {
            foreach (KeyValuePair<string, List<ColumnValue>> column in cells)
                column.Value.Add(ReadCell(row, column.Key));

            count++;
        }

        public ColumnValue[][] Compute(CancellationToken cancellationToken)
        {
            int n = count;
            int[] peerStart = new int[n];
            int[] peerEnd = new int[n];

            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);
                peerStart[i] = i > 0 && SamePeers(i - 1, i) ? peerStart[i - 1] : i;
            }

            for (int i = n - 1; i >= 0; i--)
            {
                CheckCancellation(i, cancellationToken);
                peerEnd[i] = i < n - 1 && peerStart[i + 1] == peerStart[i] ? peerEnd[i + 1] : i;
            }

            PartitionView view = new(n, peerStart, peerEnd);
            ColumnValue[][] results = new ColumnValue[plans.Length][];

            for (int i = 0; i < plans.Length; i++)
                results[i] = plans[i].Compute(view, cancellationToken);

            return results;
        }

        private bool SamePeers(int left, int right)
        {
            foreach (List<ColumnValue> column in orderCells)
            {
                if (column[left].CompareTo(column[right]) != 0)
                    return false;
            }

            return true;
        }
    }

    /// <summary>The size and the peer groups of one partition, shared by every call of the group.</summary>
    private sealed record PartitionView(int Count, int[] PeerStart, int[] PeerEnd);

    /// <summary>
    /// One argument of a call: a column of the partition, or a constant evaluated once per statement.
    /// </summary>
    private readonly struct Argument
    {
        private readonly List<ColumnValue>? column;
        private readonly ColumnValue? constant;

        public Argument(List<ColumnValue> column)
        {
            this.column = column;
            constant = null;
        }

        public Argument(ColumnValue constant)
        {
            column = null;
            this.constant = constant;
        }

        public ColumnValue this[int row] => column is not null ? column[row] : constant!;

        public bool IsColumn => column is not null;
    }

    /// <summary>One call of the group: its arguments resolved to columns or constants, and its frame offsets.</summary>
    private sealed class CallPlan
    {
        private readonly WindowFunctionCall call;
        private readonly Argument[] arguments;
        private readonly long startOffset;
        private readonly long endOffset;
        private readonly bool countAll;

        public CallPlan(
            WindowFunctionCall call,
            Dictionary<string, ColumnValue>? parameters,
            Func<string, List<ColumnValue>> column,
            IReadOnlyList<DerivedColumnSchema> columns)
        {
            this.call = call;
            arguments = new Argument[call.Arguments.Count];

            for (int i = 0; i < arguments.Length; i++)
            {
                NodeAst argument = call.Arguments[i];

                if (argument.nodeType == NodeType.ExprAllFields)
                {
                    countAll = true;
                    arguments[i] = new Argument(ColumnValue.Null);
                }
                else if (argument.nodeType == NodeType.Identifier)
                {
                    arguments[i] = new Argument(column(argument.yytext!));
                }
                else
                {
                    arguments[i] = new Argument(SqlExecutor.EvalExpr(argument, QueryResultRow.EmptyRow, parameters));
                }
            }

            if (call.Kind is WindowFunctionKind.Lag or WindowFunctionKind.Lead && arguments.Length == 3 && !arguments[2].IsColumn)
                arguments[2] = new Argument(ResolveDefault(call, arguments[2][0], DeclaredType(call, columns)));

            if (call.UsesFrame)
            {
                startOffset = FrameOffset(call.Frame.Start, parameters);
                endOffset = FrameOffset(call.Frame.End, parameters);
            }
        }

        /// <summary>The declared type of the call's result column, as <see cref="WindowResultSchema"/> set it.</summary>
        private static ColumnType DeclaredType(WindowFunctionCall call, IReadOnlyList<DerivedColumnSchema> columns)
        {
            foreach (DerivedColumnSchema schema in columns)
            {
                if (string.Equals(schema.Name, call.ResultColumn, StringComparison.Ordinal))
                    return schema.Type;
            }

            return ColumnType.Null;
        }

        /// <summary>
        /// A constant default of <c>lag</c>/<c>lead</c> in the declared type of the result, so the
        /// result column never mixes types (a mixed column breaks every later comparison). A literal
        /// default already shaped the declared type at bind time and is only widened to it here. A
        /// placeholder default is seen for the first time here: it is widened when it is narrower than
        /// the declared type and refused when it is wider, because the value cells cannot change type
        /// at this point.
        /// </summary>
        private static ColumnValue ResolveDefault(WindowFunctionCall call, ColumnValue constant, ColumnType declared)
        {
            ColumnType common = WindowResultSchema.DefaultResultType(call.Kind, declared, constant.Type);

            if (common == declared)
                return NumericWidening.Widen(constant, declared);

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The default of {(call.Kind == WindowFunctionKind.Lag ? "lag" : "lead")} has type {constant.Type}, "
                + $"which is wider than the {declared} type of its value. Write the default as a literal, or cast the value");
        }

        private static long FrameOffset(WindowBound bound, Dictionary<string, ColumnValue>? parameters)
        {
            if (bound.Offset is null)
                return 0;

            ColumnValue value = SqlExecutor.EvalExpr(bound.Offset, QueryResultRow.EmptyRow, parameters);

            if (value.Type != ColumnType.Integer64 || value.LongValue < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"A ROWS frame offset must be a non-negative integer, got {Describe(value)}");

            // A partition holds at most int.MaxValue rows, so any larger offset reaches the same rows
            // as int.MaxValue does. The cap keeps every row + offset sum in Frame inside a long: without
            // it an offset near long.MaxValue wraps negative and the frame clamps to empty.
            return Math.Min(value.LongValue, int.MaxValue);
        }

        public ColumnValue[] Compute(PartitionView view, CancellationToken cancellationToken)
        {
            int n = view.Count;
            ColumnValue[] results = new ColumnValue[n];

            switch (call.Kind)
            {
                case WindowFunctionKind.RowNumber:
                    for (int i = 0; i < n; i++)
                    {
                        CheckCancellation(i, cancellationToken);
                        results[i] = Int(i + 1);
                    }

                    break;

                case WindowFunctionKind.Rank:
                    for (int i = 0; i < n; i++)
                    {
                        CheckCancellation(i, cancellationToken);
                        results[i] = Int(view.PeerStart[i] + 1);
                    }

                    break;

                case WindowFunctionKind.DenseRank:
                {
                    long rank = 0;

                    for (int i = 0; i < n; i++)
                    {
                        CheckCancellation(i, cancellationToken);

                        if (view.PeerStart[i] == i)
                            rank++;

                        results[i] = Int(rank);
                    }

                    break;
                }

                case WindowFunctionKind.PercentRank:
                    for (int i = 0; i < n; i++)
                    {
                        CheckCancellation(i, cancellationToken);
                        results[i] = Float(n > 1 ? view.PeerStart[i] / (double)(n - 1) : 0.0);
                    }

                    break;

                case WindowFunctionKind.CumeDist:
                    for (int i = 0; i < n; i++)
                    {
                        CheckCancellation(i, cancellationToken);
                        results[i] = Float((view.PeerEnd[i] + 1) / (double)n);
                    }

                    break;

                case WindowFunctionKind.Ntile:
                    ComputeNtile(results, n, cancellationToken);
                    break;

                case WindowFunctionKind.Lag:
                case WindowFunctionKind.Lead:
                    ComputeOffset(results, n, call.Kind == WindowFunctionKind.Lag ? -1 : 1, cancellationToken);
                    break;

                case WindowFunctionKind.FirstValue:
                case WindowFunctionKind.LastValue:
                case WindowFunctionKind.NthValue:
                    ComputeValue(results, view, cancellationToken);
                    break;

                default:
                    ComputeAggregate(results, view, cancellationToken);
                    break;
            }

            return results;
        }

        /// <summary>
        /// <c>ntile(k)</c>: k buckets as equal as possible, the larger ones first. k is read from the
        /// first row of the partition, as PostgreSQL does.
        /// </summary>
        private void ComputeNtile(ColumnValue[] results, int n, CancellationToken cancellationToken)
        {
            if (n == 0)
                return;

            ColumnValue buckets = arguments[0][0];

            if (buckets.Type == ColumnType.Null)
            {
                Array.Fill(results, ColumnValue.Null);
                return;
            }

            long k = RequireInteger(buckets, "ntile");

            if (k <= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"The argument of ntile must be greater than zero, got {k}");

            long size = n / k;
            long remainder = n % k;
            long boundary = remainder * (size + 1);

            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);

                long bucket = i < boundary
                    ? i / (size + 1) + 1
                    : (i - boundary) / size + remainder + 1;

                results[i] = Int(bucket);
            }
        }

        /// <summary>
        /// <c>lag</c> (<paramref name="direction"/> -1) and <c>lead</c> (+1): the value
        /// <c>offset</c> rows away, or the default when that row is outside the partition. The offset
        /// and the default are read per row, so either may be an expression of the row.
        /// </summary>
        private void ComputeOffset(ColumnValue[] results, int n, int direction, CancellationToken cancellationToken)
        {
            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);

                long offset = 1;

                if (arguments.Length > 1)
                {
                    ColumnValue offsetValue = arguments[1][i];

                    if (offsetValue.Type == ColumnType.Null)
                    {
                        results[i] = ColumnValue.Null;
                        continue;
                    }

                    offset = RequireInteger(offsetValue, call.Kind == WindowFunctionKind.Lag ? "lag" : "lead");
                }

                // Any offset past the partition reads the default, so an offset is clamped to the int
                // range first: the product and the sum then stay inside a long for every offset,
                // long.MinValue included, instead of wrapping into a row that exists.
                long target = i + direction * Math.Clamp(offset, -int.MaxValue, int.MaxValue);

                results[i] = target >= 0 && target < n
                    ? arguments[0][(int)target]
                    : arguments.Length > 2 ? arguments[2][i] : ColumnValue.Null;
            }
        }

        private void ComputeValue(ColumnValue[] results, PartitionView view, CancellationToken cancellationToken)
        {
            for (int i = 0; i < view.Count; i++)
            {
                CheckCancellation(i, cancellationToken);

                (int start, int end) = Frame(view, i);

                if (start > end)
                {
                    results[i] = ColumnValue.Null;
                    continue;
                }

                switch (call.Kind)
                {
                    case WindowFunctionKind.FirstValue:
                        results[i] = arguments[0][start];
                        break;

                    case WindowFunctionKind.LastValue:
                        results[i] = arguments[0][end];
                        break;

                    default:
                    {
                        ColumnValue nth = arguments[1][i];

                        if (nth.Type == ColumnType.Null)
                        {
                            results[i] = ColumnValue.Null;
                            break;
                        }

                        long k = RequireInteger(nth, "nth_value");

                        if (k <= 0)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"The second argument of nth_value must be greater than zero, got {k}");

                        // k - 1 is non-negative and the frame length is an int, so the comparison
                        // needs no sum start + k - 1, which wraps for a k near long.MaxValue.
                        long position = k - 1;
                        results[i] = position <= end - start ? arguments[0][start + (int)position] : ColumnValue.Null;
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// The frame of row <paramref name="row"/> as inclusive indexes, clamped to the partition. An
        /// empty frame has a start after its end. Both ends only grow as the row grows, which the
        /// sliding aggregates rely on. The offsets are at most int.MaxValue (see
        /// <see cref="FrameOffset"/>), so no sum here can wrap.
        /// </summary>
        private (int Start, int End) Frame(PartitionView view, int row)
        {
            WindowFrameSpec frame = call.Frame;
            int n = view.Count;

            long start = frame.Start.Kind switch
            {
                WindowBoundKind.UnboundedPreceding => 0,
                WindowBoundKind.Preceding => row - startOffset,
                WindowBoundKind.CurrentRow => frame.IsRange ? view.PeerStart[row] : row,
                WindowBoundKind.Following => row + startOffset,
                _ => n,
            };

            long end = frame.End.Kind switch
            {
                WindowBoundKind.UnboundedFollowing => n - 1,
                WindowBoundKind.Following => row + endOffset,
                WindowBoundKind.CurrentRow => frame.IsRange ? view.PeerEnd[row] : row,
                WindowBoundKind.Preceding => row - endOffset,
                _ => -1,
            };

            return ((int)Math.Clamp(start, 0, n), (int)Math.Clamp(end, -1, n - 1));
        }

        /// <summary>
        /// <c>count</c>, <c>sum</c>, <c>avg</c>, <c>min</c> and <c>max</c> over each row's frame.
        ///
        /// <para>The frames slide: the window <c>[lo, hi]</c> adds each row as its end passes it and
        /// removes each row as its start passes it, so the whole partition costs O(n) whatever the
        /// frame. <c>count</c> and the sums of Integer64 and NUMERIC values remove exactly through
        /// integer totals; a Float64 sum removes exactly through <see cref="ExactDoubleSum"/>, which
        /// rounds the frame's exact total once; <c>min</c> and <c>max</c> keep a monotonic deque of
        /// row indexes.</para>
        /// </summary>
        private void ComputeAggregate(ColumnValue[] results, PartitionView view, CancellationToken cancellationToken)
        {
            int n = view.Count;

            if (n == 0)
                return;

            int[] starts = new int[n];
            int[] ends = new int[n];

            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);
                (starts[i], ends[i]) = Frame(view, i);
            }

            bool sumLike = call.Kind is WindowFunctionKind.Sum or WindowFunctionKind.Avg;
            SlidingState state = new(
                call.Kind,
                countAll,
                arguments.Length > 0 ? arguments[0] : default,
                trackDoubles: sumLike && HasFloat(n, cancellationToken));
            int lo = 0;
            int hi = -1;

            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);

                while (hi < ends[i])
                {
                    hi++;

                    if (hi >= lo)
                        state.Add(hi);
                }

                while (lo < starts[i])
                {
                    if (lo <= hi)
                        state.Remove(lo);

                    lo++;
                }

                results[i] = lo <= hi ? state.Result() : state.Empty();
            }
        }

        /// <summary>
        /// True when the partition holds a Float64 value. The sliding state then keeps the Float64
        /// total of every frame beside the integer one, because a frame's result type is known only
        /// when the frame is read (see <see cref="SlidingState"/>).
        /// </summary>
        private bool HasFloat(int n, CancellationToken cancellationToken)
        {
            if (arguments.Length == 0 || countAll)
                return false;

            Argument value = arguments[0];

            for (int i = 0; i < n; i++)
            {
                CheckCancellation(i, cancellationToken);

                if (value[i].Type == ColumnType.Float64)
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    /// The state of one aggregate over a sliding frame. Every operation names a row index of the
    /// partition, and a row is removed only after it was added.
    ///
    /// <para>The result type of a frame follows the plain aggregate and depends on the values the frame
    /// holds: a Float64 value makes a Float64 result, else a NUMERIC value makes a NUMERIC result, else
    /// the result is Integer64. The totals are kept exactly, per type, so that any of the three can be
    /// read after any sequence of adds and removes. When the partition holds a Float64 value at all,
    /// Integer64 values also enter the Float64 total, as the doubles a plain SUM converts them to, so a
    /// frame that mixes the two types gives the result the plain aggregate gives for its rows. A
    /// partition with no Float64 value never allocates the Float64 total.</para>
    /// </summary>
    private sealed class SlidingState
    {
        private readonly WindowFunctionKind kind;
        private readonly bool countAll;
        private readonly Argument value;

        private long rows;
        private long nonNull;

        // Exact totals: Integer64 values, NUMERIC (unscaled) values, and Float64 values (plus the
        // Integer64 values as doubles) when the partition has a Float64 value.
        private QueryAggregator.ExactTotal integers;
        private QueryAggregator.ExactTotal numerics;
        private readonly ExactDoubleSum? doubles;
        private long integerCount;
        private long numericCount;
        private long floatCount;

        // Monotonic deque of row indexes for min and max: values from front to back are increasing
        // for min (decreasing for max), so the front is the extreme of the current frame.
        private int[]? deque;
        private int head;
        private int tail;

        public SlidingState(WindowFunctionKind kind, bool countAll, Argument value, bool trackDoubles)
        {
            this.kind = kind;
            this.countAll = countAll;
            this.value = value;
            doubles = trackDoubles ? new ExactDoubleSum() : null;
        }

        public void Add(int row)
        {
            rows++;

            if (countAll)
                return;

            ColumnValue cell = value[row];

            if (cell.Type == ColumnType.Null)
                return;

            nonNull++;

            switch (kind)
            {
                case WindowFunctionKind.Sum:
                case WindowFunctionKind.Avg:
                    Accumulate(cell, 1);
                    break;

                case WindowFunctionKind.Min:
                case WindowFunctionKind.Max:
                    PushBack(row, cell);
                    break;
            }
        }

        public void Remove(int row)
        {
            rows--;

            if (countAll)
                return;

            ColumnValue cell = value[row];

            if (cell.Type == ColumnType.Null)
                return;

            nonNull--;

            switch (kind)
            {
                case WindowFunctionKind.Sum:
                case WindowFunctionKind.Avg:
                    Accumulate(cell, -1);
                    break;

                case WindowFunctionKind.Min:
                case WindowFunctionKind.Max:
                    if (head < tail && deque![head] == row)
                        head++;
                    break;
            }
        }

        private void Accumulate(ColumnValue cell, int sign)
        {
            switch (cell.Type)
            {
                case ColumnType.Integer64:
                    integers.Add(sign * (Int128)cell.LongValue);
                    integerCount += sign;

                    if (doubles is not null)
                        Accumulate(doubles, cell.LongValue, sign);

                    break;

                case ColumnType.Float64:
                    if (doubles is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            "A Float64 value reached a frame sum whose partition scan found none");

                    Accumulate(doubles, cell.FloatValue, sign);
                    floatCount += sign;
                    break;

                case ColumnType.Numeric:
                    numerics.Add(sign * cell.NumericUnscaled);
                    numericCount += sign;
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"{(kind == WindowFunctionKind.Sum ? "SUM" : "AVG")} requires a numeric column, got {cell.Type}");
            }
        }

        private static void Accumulate(ExactDoubleSum sum, double value, int sign)
        {
            if (sign > 0)
                sum.Add(value);
            else
                sum.Remove(value);
        }

        private void PushBack(int row, ColumnValue cell)
        {
            deque ??= new int[16];

            bool isMin = kind == WindowFunctionKind.Min;

            while (head < tail)
            {
                int comparison = value[deque[tail - 1]].CompareTo(cell);

                if (isMin ? comparison >= 0 : comparison <= 0)
                    tail--;
                else
                    break;
            }

            if (tail == deque.Length)
            {
                // Rows leave from the front only, so the live part is [head, tail): compact before growing.
                int live = tail - head;

                if (head > 0 && live < deque.Length / 2)
                {
                    Array.Copy(deque, head, deque, 0, live);
                }
                else
                {
                    int[] grown = new int[deque.Length * 2];
                    Array.Copy(deque, head, grown, 0, live);
                    deque = grown;
                }

                head = 0;
                tail = live;
            }

            deque[tail++] = row;
        }

        /// <summary>The result of an empty frame: zero for count, NULL for the others.</summary>
        public ColumnValue Empty() => kind == WindowFunctionKind.Count ? Int(0) : ColumnValue.Null;

        public ColumnValue Result()
        {
            switch (kind)
            {
                case WindowFunctionKind.Count:
                    return Int(countAll ? rows : nonNull);

                case WindowFunctionKind.Min:
                case WindowFunctionKind.Max:
                    return head < tail ? value[deque![head]] : ColumnValue.Null;

                case WindowFunctionKind.Sum:
                    if (nonNull == 0)
                        return ColumnValue.Null;

                    if (floatCount > 0)
                        return Float(FloatTotal());

                    if (numericCount > 0)
                        return ColumnValue.FromNumeric(QueryAggregator.ExactTotal.Combine(numerics, integers).ToNumeric("SUM"));

                    return Int(integers.ToWrappedInt64());

                default:
                    if (nonNull == 0)
                        return ColumnValue.Null;

                    if (floatCount > 0)
                        return Float(FloatTotal() / nonNull);

                    if (numericCount > 0)
                        return ColumnValue.FromNumeric(QueryAggregator.ExactTotal.Combine(numerics, integers).AverageToNumeric(nonNull, "AVG"));

                    return Float(integers.ToUnscaledDouble() / nonNull);
            }
        }

        /// <summary>
        /// The Float64 total of a frame that holds a Float64 value: the correctly rounded sum of its
        /// Float64 and Integer64 values, plus its NUMERIC values as a double, which is how the plain
        /// SUM combines the three.
        /// </summary>
        private double FloatTotal()
        {
            double total = doubles!.ToDouble();
            return numericCount > 0 ? total + numerics.ToDouble() : total;
        }
    }

    private static long RequireInteger(ColumnValue value, string function)
    {
        if (value.Type != ColumnType.Integer64)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"An argument of {function} must be an integer, got {Describe(value)}");

        return value.LongValue;
    }

    private static string Describe(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        _ => value.Type.ToString(),
    };

    private static ColumnValue Int(long value) => new(ColumnType.Integer64, value);

    private static ColumnValue Float(double value) => new(ColumnType.Float64, value);
}

/// <summary>
/// What the window operator did for one windowed SELECT, for EXPLAIN (ANALYZE): the partitions it
/// computed, the rows they held, and how many partition buffers spilled to disk. Filled by one
/// enumeration on one flow, so its counters are plain fields.
/// </summary>
internal sealed class WindowStats
{
    public long Partitions;

    public long Rows;

    public long SpilledPartitions;
}
