/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Runs a bound <c>UNION</c>, <c>INTERSECT</c> or <c>EXCEPT</c> and streams its rows.
///
/// <para><b>Operands.</b> Each operand runs through the same "bound query to rows" path as the inner
/// query of a derived table, under the caller's transaction, parameters and cancellation token, so
/// all operands read in the one snapshot of the statement. Every row is then re-keyed to the output
/// layout by position and each cell is widened to its output type (see
/// <see cref="BoundSetOperation"/>). After that step all rows of both operands share one
/// <see cref="RowLayout"/> instance, which is what lets the DISTINCT comparer match them.</para>
///
/// <para><b>Combine step.</b></para>
/// <list type="bullet">
/// <item><c>UNION ALL</c> streams the left rows, then the right rows, and holds nothing.</item>
/// <item><c>UNION</c> streams both through <see cref="QueryDistincter"/>, with its hash and spill
/// rules.</item>
/// <item><c>INTERSECT [ALL]</c> and <c>EXCEPT [ALL]</c> count the rows of the right operand in a hash
/// table, then stream the left operand and emit each row by the count rules (see
/// <see cref="SetOperationKind"/>). Memory is the distinct rows of the right operand, plus the
/// distinct left rows for <c>EXCEPT</c>, which must remember what it already emitted. With
/// <see cref="CamusDBOptions.SpillEnabled"/> on, a hash table that grows past
/// <see cref="CamusDBOptions.SpillEffectiveThreshold"/> rows or
/// <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/> bytes moves both operands into
/// <see cref="CamusDBOptions.SpillMergeFanIn"/> partition files by row hash, and each pair of files is
/// then combined in memory. Equal rows always hash to the same partition, so the counts stay exact.
/// A partition is not split again.</item>
/// </list>
///
/// <para><b>Order.</b> Without ORDER BY the order of the rows is not defined. In practice it is the
/// left rows first, and a spill reorders them partition by partition.</para>
/// </summary>
internal sealed class SetOperationExecutor
{
    private readonly QueryExecutor queryExecutor;

    private readonly QueryJoinExecutor queryJoinExecutor;

    private readonly QueryDistincter queryDistincter = new();

    public SetOperationExecutor(QueryExecutor queryExecutor, QueryJoinExecutor queryJoinExecutor)
    {
        this.queryExecutor = queryExecutor;
        this.queryJoinExecutor = queryJoinExecutor;
    }

    /// <summary>
    /// Streams the rows of <paramref name="setOperation"/>. <paramref name="stats"/> receives the row
    /// count of each operand when EXPLAIN (ANALYZE) collects it; null otherwise.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> Execute(
        DatabaseDescriptor database,
        BoundSetOperation setOperation,
        QueryTicket outerTicket,
        SetOperationStats? stats = null)
    {
        RowLayout layout = RowLayout.ForColumns(setOperation.Columns.Select(static column => column.RowKey));
        QueryExecutionContext context = QueryExecutionContext.For(database, outerTicket);

        IAsyncEnumerable<QueryResultRow> left = RunOperand(
            database, setOperation.Left, setOperation.LeftSubqueries, setOperation.LeftColumns, setOperation.Columns,
            layout, outerTicket, stats, isLeft: true);

        IAsyncEnumerable<QueryResultRow> right = RunOperand(
            database, setOperation.Right, setOperation.RightSubqueries, setOperation.RightColumns, setOperation.Columns,
            layout, outerTicket, stats, isLeft: false);

        SetOperationSource source = setOperation.Source;

        return source.Kind switch
        {
            SetOperationKind.Union when source.All => Concat(left, right),
            SetOperationKind.Union => queryDistincter.DistinctResultset(outerTicket, Concat(left, right), context),
            _ => HashCombine(source.Kind, source.All, left, right, layout, context),
        };
    }

    /// <summary>
    /// Runs one operand and re-keys its rows to the output layout. The operand ticket carries the
    /// caller's transaction, parameters, probe and cancellation token, as a derived table's inner
    /// ticket does, so a cancel stops the operand scan itself and not only this loop. It also
    /// carries the operand's own <paramref name="subqueries"/>, so an EXISTS in an operand's WHERE
    /// finds its prepared subquery and an IN lifted into a semi-join finds its spec.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> RunOperand(
        DatabaseDescriptor database,
        BoundSelectQuery operand,
        SubqueryBindProducts subqueries,
        IReadOnlyList<DerivedColumnSchema> operandColumns,
        IReadOnlyList<DerivedColumnSchema> outputColumns,
        RowLayout layout,
        QueryTicket outerTicket,
        SetOperationStats? stats,
        bool isLeft,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // The request-scoped name, for the reason DerivedTableExecutor gives.
        ExecuteSQLTicket executeTicket = new(
            txnState: outerTicket.TxnState,
            database: outerTicket.DatabaseName,
            sql: "",
            parameters: outerTicket.Parameters,
            cancellationToken: outerTicket.CancellationToken,
            probe: outerTicket.Probe);

        QueryTicket operandTicket = QueryTicketAdapter.ToQueryTicket(
            operand, executeTicket, subqueries.ExistsRegistry, subqueries.SemiJoinSpecs,
            exclusivePredicateLocks: outerTicket.ExclusivePredicateLocks);

        // EXPLAIN (ANALYZE) built this operand's plan in advance, to show and count it; that plan runs.
        QueryPlan? prebuilt = isLeft ? stats?.LeftPlan : stats?.RightPlan;

        // An operand that only passes a nested set operation through runs that one directly. The
        // result is the same as planning `SELECT *` over it, and every level of a chain then costs a
        // few stack frames instead of a whole join-plan pipeline: a chain is as deep as its
        // operators, and every level's cursor is open on the stack while its first row is read.
        BoundSetOperation? nested = prebuilt is null ? BoundSetOperation.PassedThrough(operand) : null;

        IAsyncEnumerable<QueryResultRow> cursor = nested is not null
            ? Execute(database, nested, outerTicket)
            : prebuilt is not null
                ? prebuilt.BoundQuery is not null ? queryJoinExecutor.ExecuteJoinPlan(prebuilt) : queryExecutor.ExecuteQueryPlan(prebuilt)
                : operand.IsMultiSource
                    ? queryJoinExecutor.ExecuteJoinQuery(database, operand, operandTicket)
                    : queryExecutor.Query(database, operand.PrimaryTable, operandTicket);

        int width = outputColumns.Count;
        string[] keys = new string[width];
        ColumnType[] types = new ColumnType[width];

        for (int i = 0; i < width; i++)
        {
            keys[i] = operandColumns[i].RowKey;
            types[i] = outputColumns[i].Type;
        }

        // The ordinals of the operand's own layout, resolved from its first positional row. A row with
        // another layout falls back to a lookup by key.
        RowLayout? operandLayout = null;
        int[]? ordinals = null;

        CancellationToken statementToken = outerTicket.CancellationToken;

        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
        {
            statementToken.ThrowIfCancellationRequested();

            ColumnValue[] values = new ColumnValue[width];

            if (row.Row is QueryRow positional)
            {
                if (!ReferenceEquals(positional.Layout, operandLayout))
                {
                    operandLayout = positional.Layout;
                    ordinals = new int[width];
                    for (int i = 0; i < width; i++)
                        ordinals[i] = operandLayout.IndexOf(keys[i]);
                }

                for (int i = 0; i < width; i++)
                {
                    int ordinal = ordinals![i];
                    values[i] = SetOperationSchema.Widen(
                        ordinal >= 0 ? positional.GetColumnValue(ordinal) : ColumnValue.Null, types[i]);
                }
            }
            else
            {
                for (int i = 0; i < width; i++)
                {
                    values[i] = SetOperationSchema.Widen(
                        row.Row.TryGetValue(keys[i], out ColumnValue? value) ? value : ColumnValue.Null, types[i]);
                }
            }

            if (stats is not null)
                (isLeft ? stats.Left : stats.Right).RowsEmitted++;

            yield return new QueryResultRow(default, new QueryRow(default, layout, values));
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> Concat(
        IAsyncEnumerable<QueryResultRow> left,
        IAsyncEnumerable<QueryResultRow> right,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (QueryResultRow row in left.WithCancellation(ct).ConfigureAwait(false))
            yield return row;

        await foreach (QueryResultRow row in right.WithCancellation(ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// <c>INTERSECT [ALL]</c> and <c>EXCEPT [ALL]</c>: counts the right operand, then streams the left
    /// one through <see cref="CountTable.Probe"/>. Spills both operands into partition files when the
    /// count table outgrows the thresholds; see the class summary.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> HashCombine(
        SetOperationKind kind,
        bool all,
        IAsyncEnumerable<QueryResultRow> left,
        IAsyncEnumerable<QueryResultRow> right,
        RowLayout layout,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct = context.Effective(ct);

        bool spillEnabled = context.Options.SpillEnabled;
        int threshold = context.Options.SpillEffectiveThreshold;
        long byteThreshold = context.Options.SpillEffectiveThresholdBytes;
        int K = Math.Max(2, context.Options.SpillMergeFanIn);

        CountTable counts = new(kind, all);

        SpillScope? scope = null;
        SpillWriteStream[]? rightWriters = null;
        SpillWriteStream[]? leftWriters = null;
        string[]? rightPaths = null;
        string[]? leftPaths = null;

        try
        {
            await foreach (QueryResultRow row in right.WithCancellation(ct).ConfigureAwait(false))
            {
                QueryRow values = (QueryRow)row.Row;

                if (rightWriters is not null)
                {
                    WriteToPartition(values, K, rightWriters);
                    continue;
                }

                counts.Add(values);

                if (spillEnabled && (counts.Count >= threshold || counts.Bytes >= byteThreshold))
                {
                    context.Probe?.NoteSpill();
                    scope = SpillFileManager.CreateScope(context.SpillDirectory, context.Options);
                    (rightPaths, rightWriters) = OpenWriters(scope, K);
                    counts.SpillTo(K, rightWriters);
                }
            }

            if (rightWriters is null)
            {
                await foreach (QueryResultRow row in left.WithCancellation(ct).ConfigureAwait(false))
                {
                    if (counts.Probe((QueryRow)row.Row))
                        yield return row;
                }

                yield break;
            }

            (leftPaths, leftWriters) = OpenWriters(scope!, K);

            await foreach (QueryResultRow row in left.WithCancellation(ct).ConfigureAwait(false))
                WriteToPartition((QueryRow)row.Row, K, leftWriters);

            await CloseWritersAsync(rightWriters, ct).ConfigureAwait(false);
            await CloseWritersAsync(leftWriters, ct).ConfigureAwait(false);

            int maxFrameBytes = context.Options.SpillMaxFrameBytes;

            for (int i = 0; i < K; i++)
            {
                CountTable partition = new(kind, all);

                SpillRunReader? rightReader = await SpillRunReader.OpenAsync(rightPaths![i], maxFrameBytes, layout, ct).ConfigureAwait(false);

                if (rightReader is not null)
                {
                    await using (rightReader)
                    {
                        do
                        {
                            partition.Add((QueryRow)rightReader.Current.Row);
                        }
                        while (await rightReader.AdvanceAsync(ct).ConfigureAwait(false));
                    }
                }

                SpillRunReader? leftReader = await SpillRunReader.OpenAsync(leftPaths![i], maxFrameBytes, layout, ct).ConfigureAwait(false);

                if (leftReader is null)
                    continue;

                await using (leftReader)
                {
                    do
                    {
                        QueryResultRow row = leftReader.Current;

                        if (partition.Probe((QueryRow)row.Row))
                            yield return row;
                    }
                    while (await leftReader.AdvanceAsync(ct).ConfigureAwait(false));
                }
            }
        }
        finally
        {
            DisposeWriters(rightWriters);
            DisposeWriters(leftWriters);

            if (scope is not null)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static (string[] Paths, SpillWriteStream[] Writers) OpenWriters(SpillScope scope, int K)
    {
        string[] paths = new string[K];
        SpillWriteStream[] writers = new SpillWriteStream[K];

        for (int i = 0; i < K; i++)
            paths[i] = scope.OpenWriter(out writers[i]);

        return (paths, writers);
    }

    private static void WriteToPartition(QueryRow row, int K, SpillWriteStream[] writers)
    {
        int partition = QueryAggregator.PartitionFromHash(RowValueComparer.HashOf(row), K, seed: 0);
        SpillRowCodec.EncodeValueOnlyToStream(writers[partition], row);
    }

    private static async ValueTask CloseWritersAsync(SpillWriteStream[] writers, CancellationToken ct)
    {
        for (int i = 0; i < writers.Length; i++)
        {
            await writers[i].FlushAsync(ct).ConfigureAwait(false);
            writers[i].Close();
        }
    }

    private static void DisposeWriters(SpillWriteStream[]? writers)
    {
        if (writers is null)
            return;

        for (int i = 0; i < writers.Length; i++)
            writers[i]?.Dispose();
    }

    /// <summary>
    /// The hash table of <c>INTERSECT</c> and <c>EXCEPT</c>: one entry per distinct row of the right
    /// operand, with its count. <see cref="Probe"/> applies the emit rule for one left row and
    /// updates the count. For the DISTINCT forms an entry also remembers that a row was emitted:
    /// INTERSECT sets the count of an emitted row to 0, and EXCEPT adds an emitted left row with the
    /// count -1, so a second copy of the row is not emitted again.
    /// </summary>
    private sealed class CountTable
    {
        private const int Emitted = -1;

        private readonly SetOperationKind kind;

        private readonly bool all;

        private readonly Dictionary<QueryRow, int> counts = new(RowValueComparer.Instance);

        public CountTable(SetOperationKind kind, bool all)
        {
            this.kind = kind;
            this.all = all;
        }

        /// <summary>The number of distinct rows in the table.</summary>
        public int Count => counts.Count;

        /// <summary>The estimated bytes of the rows the table holds.</summary>
        public long Bytes { get; private set; }

        public void Add(QueryRow row)
        {
            ref int count = ref CollectionsMarshal.GetValueRefOrAddDefault(counts, row, out bool exists);
            count++;

            if (!exists)
                Bytes += RowMemoryEstimator.EstimateRow(row);
        }

        /// <summary>True when <paramref name="row"/>, a left row, is part of the result.</summary>
        public bool Probe(QueryRow row)
        {
            if (kind == SetOperationKind.Intersect)
            {
                ref int count = ref CollectionsMarshal.GetValueRefOrNullRef(counts, row);

                if (Unsafe.IsNullRef(ref count) || count <= 0)
                    return false;

                count = all ? count - 1 : 0;
                return true;
            }

            if (all)
            {
                ref int remaining = ref CollectionsMarshal.GetValueRefOrNullRef(counts, row);

                if (Unsafe.IsNullRef(ref remaining) || remaining <= 0)
                    return true;

                remaining--;
                return false;
            }

            // EXCEPT: a row in the right operand, or one already emitted, is not emitted.
            ref int entry = ref CollectionsMarshal.GetValueRefOrAddDefault(counts, row, out bool exists);

            if (exists)
                return false;

            entry = Emitted;
            Bytes += RowMemoryEstimator.EstimateRow(row);
            return true;
        }

        /// <summary>
        /// Writes every counted row to its partition, once per count, and empties the table. Called
        /// once, when the table first goes past a threshold; the rest of the right operand then goes
        /// to the files directly.
        /// </summary>
        public void SpillTo(int K, SpillWriteStream[] writers)
        {
            foreach ((QueryRow row, int count) in counts)
            {
                for (int i = 0; i < count; i++)
                    WriteToPartition(row, K, writers);
            }

            counts.Clear();
            counts.TrimExcess();
            Bytes = 0;
        }
    }

    /// <summary>
    /// Compares two rows of one set operation by position, with the DISTINCT rules: NULL equals NULL,
    /// two values of different types are not equal, and two values of one type compare with
    /// <see cref="ColumnValue.CompareTo"/>. The hash agrees with that rule
    /// (<see cref="QueryDistincter.DistinctValueHash"/>). Every row it sees is positional and has
    /// the output width, because the executor re-keys each operand row before it gets here.
    /// </summary>
    private sealed class RowValueComparer : IEqualityComparer<QueryRow>
    {
        public static readonly RowValueComparer Instance = new();

        public static int HashOf(QueryRow row)
        {
            HashCode hash = new();

            for (int i = 0; i < row.Count; i++)
                hash.Add(QueryDistincter.DistinctValueHash(row.GetColumnValue(i)));

            return hash.ToHashCode();
        }

        public bool Equals(QueryRow? x, QueryRow? y)
        {
            if (ReferenceEquals(x, y))
                return true;

            if (x is null || y is null || x.Count != y.Count)
                return false;

            for (int i = 0; i < x.Count; i++)
            {
                ColumnValue left = x.GetColumnValue(i);
                ColumnValue right = y.GetColumnValue(i);

                if (left.Type != right.Type)
                    return false;

                if (left.Type != ColumnType.Null && left.CompareTo(right) != 0)
                    return false;
            }

            return true;
        }

        public int GetHashCode(QueryRow row) => HashOf(row);
    }
}

/// <summary>
/// What EXPLAIN (ANALYZE) hands a set operation before it runs: the operand plans it built, and the
/// counters of the rows each operand produced. Written by the one enumeration that runs the
/// operands, read after it ends.
/// </summary>
internal sealed class SetOperationStats
{
    /// <summary>The plan the left operand runs, instead of one planned when it starts.</summary>
    public QueryPlan? LeftPlan { get; init; }

    /// <summary>The plan the right operand runs.</summary>
    public QueryPlan? RightPlan { get; init; }

    /// <summary>The rows the left operand produced.</summary>
    public PlanNodeStats Left { get; } = new();

    /// <summary>The rows the right operand produced.</summary>
    public PlanNodeStats Right { get; } = new();
}
