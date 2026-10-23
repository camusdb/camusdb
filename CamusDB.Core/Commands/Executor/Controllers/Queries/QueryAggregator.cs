
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryAggregator
{
    private readonly StatisticsManager? _stats;

    public QueryAggregator(StatisticsManager? stats = null)
    {
        _stats = stats;
    }

    internal IAsyncEnumerable<QueryResultRow> AggregateResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor, QueryExecutionContext context)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be aggregated");

        if (ticket.GroupBy is { Count: > 0 })
            return AggregateGrouped(ticket, dataCursor, _stats, context, context.CancellationToken);

        // The single-aggregate fast paths below compute projection[0] only. Several aggregates, a
        // compound over aggregates, or a HAVING that needs hidden aggregates all go through the
        // workspace, which runs one accumulator over every projection in a single pass.
        if (ticket.Projection.Count > 1
            || QueryHavingWorkspace.NeedsExpandedGlobalAggregate(ticket)
            || HasCompoundProjection(ticket.Projection))
            return AggregateGlobalWorkspace(ticket, dataCursor);

        NodeAst funcCall = GetSingleAggregationFuncCall(ticket.Projection);
        QueryAggregationType aggregationType = GetAggregationType(funcCall);

        // An extension aggregate has no single-aggregate fast path; the workspace runs its state.
        if (aggregationType == QueryAggregationType.Extension)
            return AggregateGlobalWorkspace(ticket, dataCursor);

        return aggregationType switch
        {
            QueryAggregationType.Count => AggregateGlobalCount(funcCall, ticket, dataCursor),
            QueryAggregationType.Sum => AggregateGlobalSum(funcCall, ticket, dataCursor),
            QueryAggregationType.Average => AggregateGlobalAverage(funcCall, ticket, dataCursor),
            QueryAggregationType.Min => AggregateGlobalMin(funcCall, ticket, dataCursor),
            QueryAggregationType.Max => AggregateGlobalMax(funcCall, ticket, dataCursor),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This aggregation type is not supported"),
        };
    }

    /// <summary>
    /// Aggregates grouped rows. When <see cref="context.Options.SpillEnabled"/> is <c>false</c>
    /// (the default), all groups are accumulated in a single in-memory dictionary — same as the
    /// original implementation. When spilling is enabled rows fold into accumulators until the
    /// distinct group count reaches <see cref="context.Options.SpillEffectiveThreshold"/> or the
    /// estimated group bytes reach <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/>; if
    /// overflow occurs the remaining rows are partitioned into <see cref="context.Options.SpillMergeFanIn"/> spill files by
    /// group-key hash, and each partition is then aggregated in-memory independently. Because
    /// the hash is deterministic, all rows that share a group key land in the same partition and
    /// the per-partition result is identical to the global result.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregateGrouped(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        StatisticsManager? stats,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        IReadOnlyList<NodeAst> groupBy = ticket.GroupBy!;
        AggregateLayout layout = AggregateLayout.Build(AnalyzeGroupedWorkspace(ticket));

        if (!context.Options.SpillEnabled)
        {
            GroupKeyBuilder keyBuilder = new();
            Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
            var groupLookup = groups.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
            ColumnValue[] keyScratch = new ColumnValue[groupBy.Count];
            await foreach (QueryResultRow resultRow in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                ReadOnlySpan<ColumnValue> groupKey = keyBuilder.BuildInto(groupBy, resultRow.Row, ticket, keyScratch);
                if (!groupLookup.TryGetValue(groupKey, out GroupAccumulator? accumulator))
                {
                    accumulator = new GroupAccumulator(layout);
                    groupLookup[groupKey] = accumulator;
                }
                accumulator.AddRow(resultRow.Row, ticket);
            }
            foreach (GroupAccumulator accumulator in groups.Values)
                yield return accumulator.ToResultRow(ticket);
            yield break;
        }

        await foreach (QueryResultRow row in AggregateGroupedWithPossibleSpill(
            groupBy, layout, ticket, dataCursor, stats, context, ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Streaming grouped aggregation for the O(1)-memory path.
    /// Requires the input to arrive ordered so that all rows of the same group are adjacent.
    /// Emits each completed group's result row as soon as the group key changes.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> AggregateStreamingGrouped(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        QueryExecutionContext context)
    {
        IReadOnlyList<NodeAst> groupBy = ticket.GroupBy
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation,
                "Streaming GROUP BY requires GROUP BY expressions");
        AggregateLayout layout = AggregateLayout.Build(AnalyzeGroupedWorkspace(ticket));
        return StreamingAggregateRows(groupBy, layout, ticket, dataCursor, context, context.CancellationToken);
    }

    private static async IAsyncEnumerable<QueryResultRow> StreamingAggregateRows(
        IReadOnlyList<NodeAst> groupBy,
        AggregateLayout layout,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        GroupKeyBuilder keyBuilder = new();
        CompositeColumnValue? currentKey = null;
        GroupAccumulator? currentAccumulator = null;

        await foreach (QueryResultRow resultRow in dataCursor.WithCancellation(ct).ConfigureAwait(false))
        {
            CompositeColumnValue rowKey = keyBuilder.Build(groupBy, resultRow.Row, ticket);

            bool sameGroup = currentKey is not null
                && GroupKeyComparer.Instance.Equals(currentKey, rowKey);

            if (!sameGroup)
            {
                if (currentAccumulator is not null)
                    yield return currentAccumulator.ToResultRow(ticket);

                currentKey = rowKey;
                currentAccumulator = new GroupAccumulator(layout);
            }

            currentAccumulator!.AddRow(resultRow.Row, ticket);
        }

        if (currentAccumulator is not null)
            yield return currentAccumulator.ToResultRow(ticket);
    }

    /// <summary>
    /// Spill-aware GROUP BY aggregation. Rows fold into accumulators as they arrive, and spilling
    /// starts only when the number of <b>distinct groups</b> reaches
    /// <see cref="context.Options.SpillEffectiveThreshold"/> — the same rule
    /// <see cref="AggregatePartitionAsync"/> applies to a partition. A million rows that collapse into
    /// three groups therefore hold three accumulators and write nothing, instead of buffering the raw
    /// rows and writing every one of them to a partition file.
    ///
    /// <para><b>Overflow carries state forward rather than discarding it.</b> When the group count
    /// crosses the threshold, the rows seen so far already live inside their accumulators and exist
    /// nowhere else, so they cannot be re-read. Each accumulator is instead handed to the partition its
    /// group key hashes to, and only the <em>remaining</em> input is written to the
    /// <see cref="context.Options.SpillMergeFanIn"/> partition files. Because the hash is deterministic,
    /// every later row of a carried group lands in that same partition and folds into the same
    /// accumulator, so each group is accumulated exactly once, in input order — no partial-state merge
    /// exists and floating-point accumulation order is unchanged.</para>
    ///
    /// <para>The <see cref="SpillScope"/> is disposed in a <c>finally</c> block so spill files are
    /// cleaned up on completion, cancellation, and exception.</para>
    ///
    /// <para><b>Bounded by group count and by bytes.</b> A new group is admitted only while the group
    /// count is under the row cap and the estimated group bytes (<see cref="AggregateLayout.GroupBytes"/>:
    /// the key plus the group's state) are under <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/>,
    /// so a wide group key spills earlier than a narrow one. The recursive level applies the same two
    /// caps. The carried accumulators stay in memory until their partition runs, and a state that grows
    /// inside one group is charged at its starting size only.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregateGroupedWithPossibleSpill(
        IReadOnlyList<NodeAst> groupBy,
        AggregateLayout layout,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        StatisticsManager? stats,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = context.Options.SpillEffectiveThreshold;
        long byteThreshold = context.Options.SpillEffectiveThresholdBytes;
        long groupBytes = 0;
        int K = context.Options.SpillMergeFanIn;

        SpillScope? scope = null;
        SpillWriteStream[]? writers = null;
        string[]? paths = null;
        List<KeyValuePair<CompositeColumnValue, GroupAccumulator>>[]? carried = null;

        try
        {
            GroupKeyBuilder builder = new();
            ColumnValue[] scratch = new ColumnValue[groupBy.Count];
            Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
            var lookup = groups.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

            await foreach (QueryResultRow row in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                if (scope is not null)
                {
                    WriteToGroupPartition(row, groupBy, ticket, K, writers!, builder, scratch);
                    continue;
                }

                ReadOnlySpan<ColumnValue> key = builder.BuildInto(groupBy, row.Row, ticket, scratch);

                if (lookup.TryGetValue(key, out GroupAccumulator? accumulator))
                {
                    accumulator.AddRow(row.Row, ticket);
                    continue;
                }

                // A new distinct group: the thresholds are checked before it is admitted, so the group
                // count never exceeds the row cap and the group bytes pass the byte cap by at most one
                // group — the same admission rule the partition level applies.
                if (groups.Count < threshold && groupBytes < byteThreshold)
                {
                    groupBytes += layout.GroupBytes(key);
                    accumulator = new GroupAccumulator(layout);
                    lookup[key] = accumulator;
                    accumulator.AddRow(row.Row, ticket);
                    continue;
                }

                context.Probe?.NoteSpill();
                scope = SpillFileManager.CreateScope(context.SpillDirectory, context.Options);
                paths = new string[K];
                writers = new SpillWriteStream[K];
                for (int i = 0; i < K; i++)
                    paths[i] = scope.OpenWriter(out writers[i]);

                // The accumulated groups hold rows that exist nowhere else, so they are handed to the
                // partition their key hashes to instead of being dropped. Every later row of such a
                // group hashes to that same partition and folds into this same accumulator.
                carried = PartitionAccumulators(groups, K, seed: 0);
                groups.Clear();

                WriteToGroupPartition(row, groupBy, ticket, K, writers, builder, scratch);
            }

            if (scope is null)
            {
                // Never spilled: every group fits, so the accumulators are the answer.
                foreach (GroupAccumulator accumulator in groups.Values)
                    yield return accumulator.ToResultRow(ticket);
            }
            else
            {
                for (int i = 0; i < K; i++)
                {
                    await writers![i].FlushAsync(ct).ConfigureAwait(false);
                    writers[i].Close();
                }

                for (int i = 0; i < K; i++)
                {
                    await foreach (QueryResultRow row in AggregatePartitionAsync(
                        paths![i], carried![i], layout, groupBy, ticket, scope!, depth: 0, seed: 0, stats, context, ct).ConfigureAwait(false))
                        yield return row;
                }
            }
        }
        finally
        {
            if (scope is not null)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Routes each in-flight accumulator to the partition bucket its group key hashes to, under
    /// <paramref name="seed"/>. Used whenever a level starts spilling: the accumulators already hold
    /// rows that are no longer available anywhere, so they travel with the partition that will receive
    /// the rest of their group.
    /// </summary>
    private static List<KeyValuePair<CompositeColumnValue, GroupAccumulator>>[] PartitionAccumulators(
        Dictionary<CompositeColumnValue, GroupAccumulator> groups,
        int K,
        int seed)
    {
        List<KeyValuePair<CompositeColumnValue, GroupAccumulator>>[] buckets =
            new List<KeyValuePair<CompositeColumnValue, GroupAccumulator>>[K];

        for (int i = 0; i < K; i++)
            buckets[i] = [];

        foreach (KeyValuePair<CompositeColumnValue, GroupAccumulator> group in groups)
            buckets[GroupPartitionIndex(group.Key, K, seed)].Add(group);

        return buckets;
    }

    /// <summary>
    /// Hashes <paramref name="row"/>'s group key and writes it to the matching partition writer.
    /// </summary>
    private static void WriteToGroupPartition(
        QueryResultRow row,
        IReadOnlyList<NodeAst> groupBy,
        QueryTicket ticket,
        int K,
        SpillWriteStream[] writers,
        GroupKeyBuilder builder,
        ColumnValue[] scratch,
        int seed = 0)
    {
        ReadOnlySpan<ColumnValue> key = builder.BuildInto(groupBy, row.Row, ticket, scratch);
        int p = GroupPartitionIndex(key, K, seed);
        SpillRowCodec.EncodeToStream(writers[p], row);
    }

    /// <summary>
    /// Maps a group key to a partition bucket in [0, <paramref name="K"/>). A murmur-style
    /// finalizer is applied after the hash so the distribution is independent of the exact hash
    /// bit pattern (important for power-of-two K values where a raw mod would cluster into
    /// the low-order bits). <paramref name="seed"/> is mixed in before the finalizer so that
    /// recursive repartitioning (see <see cref="AggregatePartitionAsync"/>) uses a different
    /// mapping at each level, spreading group keys that collided at the parent level across
    /// different sub-partitions.
    /// </summary>
    internal static int GroupPartitionIndex(CompositeColumnValue key, int K, int seed = 0)
        => PartitionFromHash(GroupKeyComparer.Instance.GetHashCode(key), K, seed);

    /// <summary>
    /// Span overload of <see cref="GroupPartitionIndex(CompositeColumnValue,int,int)"/>. Routes a
    /// just-computed key span to its partition bucket without allocating a <see cref="CompositeColumnValue"/>.
    /// Uses the same hash and finalizer as the object form, so a row routes identically whether the
    /// key is a span or a materialized composite.
    /// </summary>
    internal static int GroupPartitionIndex(ReadOnlySpan<ColumnValue> key, int K, int seed = 0)
        => PartitionFromHash(GroupKeyComparer.Instance.GetHashCode(key), K, seed);

    /// <summary>
    /// Maps a hash code to a partition bucket in [0, <paramref name="K"/>) under
    /// <paramref name="seed"/>. Shared by GROUP BY and DISTINCT spill partitioning, so both use the
    /// same finalizer and the same per-level seed rule.
    /// </summary>
    internal static int PartitionFromHash(int hashCode, int K, int seed)
    {
        uint h = (uint)hashCode;
        h ^= (uint)seed * 0x9E3779B9u;
        h *= 0x85EBCA6Bu; h ^= h >> 13;
        h *= 0xC2B2AE35u; h ^= h >> 16;
        return (int)(h % (uint)K);
    }

    // Maximum recursion depth for per-partition GROUP BY repartitioning. Beyond this depth
    // the partition is aggregated in memory regardless of size (graceful degradation: the
    // hash function cannot split the remaining keys further, so memory growth is bounded by
    // the number of truly distinct group keys, not the raw row count).
    private const int MaxGroupByRecursionDepth = 3;

    /// <summary>
    /// Aggregates one partition: the accumulators carried in from the level that produced this
    /// partition, plus every row in its spill file. Because <see cref="GroupPartitionIndex"/> is
    /// deterministic, all rows sharing a group key are in this one partition, so its result is the
    /// final result for those groups regardless of which level produced the file.
    ///
    /// <para>
    /// <paramref name="carriedGroups"/> are accumulators that already folded the rows their groups saw
    /// before spilling began. They are seeded into the group table first, so a file row with the same
    /// key continues the same accumulator rather than starting a second one.
    /// </para>
    ///
    /// <para><b>Overflow works exactly as it does at level 0.</b> When admitting a new group would push
    /// the group count past <see cref="context.Options.SpillEffectiveThreshold"/>, or the group bytes
    /// (carried groups included) are already at <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/>, and
    /// <paramref name="depth"/> is below <see cref="MaxGroupByRecursionDepth"/>, this level starts its
    /// own spill: the accumulators built so far are routed to sub-partitions under
    /// <paramref name="seed"/>+1 (so keys that collided here are separated), and only the rows from
    /// this point onward are written to those sub-partitions. The file is never re-read and no row is
    /// folded twice. Beyond the depth cap the remaining rows aggregate in memory regardless of count:
    /// the hash cannot separate truly identical keys, so the table is bounded by distinct keys, not by
    /// raw rows.</para>
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregatePartitionAsync(
        string path,
        IReadOnlyList<KeyValuePair<CompositeColumnValue, GroupAccumulator>> carriedGroups,
        AggregateLayout layout,
        IReadOnlyList<NodeAst> groupBy,
        QueryTicket ticket,
        SpillScope scope,
        int depth,
        int seed,
        StatisticsManager? stats,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = context.Options.SpillEffectiveThreshold;
        long byteThreshold = context.Options.SpillEffectiveThresholdBytes;
        long groupBytes = 0;

        Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);

        for (int i = 0; i < carriedGroups.Count; i++)
        {
            groups[carriedGroups[i].Key] = carriedGroups[i].Value;
            groupBytes += layout.GroupBytes(carriedGroups[i].Key.Values);
        }

        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, context.Options.SpillMaxFrameBytes, ct: ct).ConfigureAwait(false);

        if (reader is null)
        {
            // No file for this partition: only the carried accumulators contribute.
            foreach (GroupAccumulator carried in groups.Values)
                yield return carried.ToResultRow(ticket);

            yield break;
        }

        var partitionLookup = groups.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        GroupKeyBuilder partitionBuilder = new();
        ColumnValue[] partitionScratch = new ColumnValue[groupBy.Count];

        int K = Math.Max(2, context.Options.SpillMergeFanIn);
        int newSeed = seed + 1;

        string[]? subPaths = null;
        SpillWriteStream[]? subWriters = null;
        List<KeyValuePair<CompositeColumnValue, GroupAccumulator>>[]? subCarried = null;

        await using (reader)
        {
            do
            {
                QueryResultRow row = reader.Current;

                if (subWriters is not null)
                {
                    WriteToGroupPartition(row, groupBy, ticket, K, subWriters, partitionBuilder, partitionScratch, newSeed);
                    continue;
                }

                ReadOnlySpan<ColumnValue> key = partitionBuilder.BuildInto(groupBy, row.Row, ticket, partitionScratch);

                if (partitionLookup.TryGetValue(key, out GroupAccumulator? accumulator))
                {
                    accumulator.AddRow(row.Row, ticket);
                    continue;
                }

                if (depth >= MaxGroupByRecursionDepth || (groups.Count < threshold && groupBytes < byteThreshold))
                {
                    groupBytes += layout.GroupBytes(key);
                    accumulator = new GroupAccumulator(layout);
                    partitionLookup[key] = accumulator;
                    accumulator.AddRow(row.Row, ticket);
                    continue;
                }

                // Overflow: this partition still holds more distinct groups than the threshold. Split
                // the remaining input into sub-partitions under a fresh seed, and send the accumulators
                // built so far to the sub-partition their key now hashes to.
                if (stats is not null)
                    stats.GroupByPartitionRecursionCount++;

                subPaths = new string[K];
                subWriters = new SpillWriteStream[K];
                for (int i = 0; i < K; i++)
                    subPaths[i] = scope.OpenWriter(out subWriters[i]);

                subCarried = PartitionAccumulators(groups, K, newSeed);
                groups.Clear();

                WriteToGroupPartition(row, groupBy, ticket, K, subWriters, partitionBuilder, partitionScratch, newSeed);
            }
            while (await reader.AdvanceAsync(ct).ConfigureAwait(false));
        }

        if (subWriters is null)
        {
            foreach (GroupAccumulator accumulator in groups.Values)
                yield return accumulator.ToResultRow(ticket);

            yield break;
        }

        for (int i = 0; i < K; i++)
        {
            await subWriters[i].FlushAsync(ct).ConfigureAwait(false);
            subWriters[i].Close();
        }

        for (int i = 0; i < K; i++)
        {
            await foreach (QueryResultRow row in AggregatePartitionAsync(
                subPaths![i], subCarried![i], layout, groupBy, ticket, scope, depth + 1, newSeed, stats, context, ct).ConfigureAwait(false))
                yield return row;
        }
    }

    /// <summary>
    /// Resolves GROUP BY key values for one row, caching ordinals from the first
    /// <see cref="QueryRow"/>'s <see cref="RowLayout"/> to avoid per-row dictionary lookups.
    /// Mirrors the ordinal-cache pattern used by <see cref="QuerySorter"/> and
    /// <see cref="QueryDistincter"/>. Only simple unqualified identifier expressions are cached
    /// (ordinal = -1 for computed/alias expressions, which fall back to <see cref="SqlExecutor.EvalExpr"/>).
    /// </summary>
    private sealed class GroupKeyBuilder
    {
        private int[]? _ordinals;
        private RowLayout? _resolvedLayout;

        /// <summary>
        /// Resolves the group-key values for one row into the caller-owned <paramref name="scratch"/>
        /// buffer and returns them as a <see cref="ReadOnlySpan{T}"/> view — no per-row key array or
        /// <see cref="CompositeColumnValue"/> wrapper is allocated. The caller probes the group table
        /// with this span (via <c>Dictionary.GetAlternateLookup</c>) and only materializes an owned
        /// <see cref="CompositeColumnValue"/> when a <b>new</b> group is inserted. <paramref name="scratch"/>
        /// must be at least <c>groupBy.Count</c> long and is reused across rows, so the returned span is
        /// only valid until the next call — never store it in the group table (the alternate comparer's
        /// <c>Create</c> copies it on insert).
        /// </summary>
        internal ReadOnlySpan<ColumnValue> BuildInto(
            IReadOnlyList<NodeAst> groupBy,
            IReadOnlyDictionary<string, ColumnValue> row,
            QueryTicket ticket,
            ColumnValue[] scratch)
        {
            if (row is QueryRow qr)
            {
                // Lazy ordinal resolution from the first QueryRow's layout.
                if (_ordinals is null)
                {
                    _resolvedLayout = qr.Layout;
                    _ordinals = ResolveOrdinals(groupBy, _resolvedLayout);
                }

                // Fast path: same layout → use pre-resolved ordinals for identifier expressions.
                if (ReferenceEquals(qr.Layout, _resolvedLayout))
                {
                    for (int i = 0; i < groupBy.Count; i++)
                    {
                        int ord = _ordinals[i];
                        // Per-cell (GetColumnValue), not whole-row Values: only the group-key columns of
                        // a slot-backed row materialize, never the aggregated or unreferenced columns.
                        scratch[i] = ord >= 0
                            ? qr.GetColumnValue(ord)
                            : SqlExecutor.EvalExpr(groupBy[i], qr, ticket.Parameters, ticket.RowNameResolver);
                    }
                    return scratch.AsSpan(0, groupBy.Count);
                }

                // Layout mismatch — degrade to per-row EvalExpr.
                for (int i = 0; i < groupBy.Count; i++)
                    scratch[i] = SqlExecutor.EvalExpr(groupBy[i], qr, ticket.Parameters, ticket.RowNameResolver);
                return scratch.AsSpan(0, groupBy.Count);
            }

            // Non-QueryRow (e.g. spill-decoded plain dictionary) — always use EvalExpr.
            for (int i = 0; i < groupBy.Count; i++)
                scratch[i] = SqlExecutor.EvalExpr(groupBy[i], row, ticket.Parameters, ticket.RowNameResolver);
            return scratch.AsSpan(0, groupBy.Count);
        }

        /// <summary>
        /// Materializes an owned <see cref="CompositeColumnValue"/> group key. Used by the streaming
        /// (sorted-input, adjacent-groups) path, which is an async iterator and therefore cannot hold a
        /// <see cref="ReadOnlySpan{T}"/> across its <c>yield</c>; its memory is O(1) regardless, so the
        /// per-row wrapper here is not the hash-probe garbage <see cref="BuildInto"/> removes.
        /// </summary>
        internal CompositeColumnValue Build(
            IReadOnlyList<NodeAst> groupBy,
            IReadOnlyDictionary<string, ColumnValue> row,
            QueryTicket ticket)
        {
            ColumnValue[] values = new ColumnValue[groupBy.Count];
            BuildInto(groupBy, row, ticket, values);
            return new CompositeColumnValue(values);
        }

        private static int[] ResolveOrdinals(IReadOnlyList<NodeAst> groupBy, RowLayout layout)
        {
            int[] ordinals = new int[groupBy.Count];
            for (int i = 0; i < groupBy.Count; i++)
            {
                // Strip alias wrapper before resolving (e.g. "col AS alias" GROUP BY).
                NodeAst expr = groupBy[i].nodeType == NodeType.ExprAlias ? groupBy[i].leftAst! : groupBy[i];
                ordinals[i] = expr.nodeType == NodeType.Identifier && expr.yytext is not null
                    ? layout.IndexOf(expr.yytext)
                    : -1;
            }
            return ordinals;
        }
    }

    /// <summary>
    /// Classifies each select-list item. An item's <c>OutputName</c> is its <b>row key</b>
    /// (<see cref="QueryTicket.ProjectionRowKeys"/>), not its display name: the workspace row holds one
    /// cell per key, so two items that share a name (<c>SELECT a.id, b.id … GROUP BY a.id, b.id</c>)
    /// must not share a cell. The projector reads the cells back by the same keys.
    /// </summary>
    private static List<AnalyzedProjection> AnalyzeProjections(QueryTicket ticket)
    {
        List<NodeAst> projection = ticket.Projection!;
        string[] rowKeys = ticket.ProjectionRowKeys;
        List<AnalyzedProjection> analyzed = new(projection.Count);

        for (int i = 0; i < projection.Count; i++)
        {
            NodeAst expression = projection[i];
            bool isAggregate = QueryExpressionClassifier.IsAggregateProjection(expression);
            bool isCompound = !isAggregate && QueryExpressionClassifier.IsCompoundAggregateProjection(expression);
            NodeAst? funcCall = isAggregate ? GetAggregateFuncCall(expression) : null;

            IReadOnlyList<(NodeAst AggregateNode, string Placeholder)>? compoundAggregates = null;
            NodeAst? rewrittenExpression = null;

            if (isCompound)
            {
                // Unwrap alias before extraction so RewrittenExpression has no alias wrapper:
                // EvalExpr cannot evaluate ExprAlias nodes. The alias is already captured in
                // OutputName and is re-applied by the caller that stores the result.
                NodeAst inner = QueryExpressionClassifier.UnwrapAlias(expression);
                var (aggregates, rewritten) = QueryAggregateExtractor.Extract(inner);
                compoundAggregates = aggregates;
                rewrittenExpression = rewritten;
            }

            analyzed.Add(new AnalyzedProjection(
                expression,
                rowKeys[i],
                isAggregate,
                funcCall,
                isCompound,
                compoundAggregates,
                rewrittenExpression));
        }

        return analyzed;
    }

    private static List<AnalyzedProjection> AnalyzeGroupedWorkspace(QueryTicket ticket)
    {
        List<AnalyzedProjection> projections = AnalyzeProjections(ticket);
        HashSet<string> outputNames = new(StringComparer.Ordinal);

        foreach (AnalyzedProjection projection in projections)
            outputNames.Add(projection.OutputName);

        if (ticket.OrderBy is not null)
        {
            foreach (QueryOrderBy orderClause in ticket.OrderBy)
            {
                if (outputNames.Contains(orderClause.ColumnName))
                    continue;

                NodeAst expression = ResolveHiddenSortExpression(ticket, orderClause.ColumnName);
                projections.Add(new AnalyzedProjection(expression, orderClause.ColumnName, false, null));
                outputNames.Add(orderClause.ColumnName);
            }
        }

        if (ticket.Having is not null)
            QueryHavingWorkspace.AddHiddenProjections(ticket.Having, ticket, projections, outputNames);

        return projections;
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalWorkspace(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        List<AnalyzedProjection> projections = AnalyzeProjections(ticket);
        HashSet<string> outputNames = new(StringComparer.Ordinal);

        foreach (AnalyzedProjection projection in projections)
            outputNames.Add(projection.OutputName);

        if (ticket.Having is not null)
            QueryHavingWorkspace.AddHiddenProjections(ticket.Having, ticket, projections, outputNames);

        GroupAccumulator accumulator = new(AggregateLayout.Build(projections));

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
            accumulator.AddRow(resultRow.Row, ticket);

        yield return accumulator.ToResultRow(ticket);
    }

    private static NodeAst ResolveHiddenSortExpression(QueryTicket ticket, string columnName)
    {
        IReadOnlyList<NodeAst> groupBy = ticket.GroupBy
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Grouped sort requires GROUP BY");

        for (int i = 0; i < groupBy.Count; i++)
        {
            if (string.Equals(QueryProjectionResolver.GetGroupByOutputName(groupBy[i], i), columnName, StringComparison.Ordinal))
                return groupBy[i];
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Sort column '{columnName}' has no grouped expression");
    }

    internal static NodeAst GetAggregateFuncCall(NodeAst expression)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        if (target.nodeType != NodeType.ExprFuncCall)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Invalid aggregation projection");
        }

        return target;
    }

    private static bool HasCompoundProjection(List<NodeAst>? projection)
    {
        if (projection is null)
            return false;
        foreach (NodeAst p in projection)
            if (QueryExpressionClassifier.IsCompoundAggregateProjection(p))
                return true;
        return false;
    }

    private static NodeAst GetSingleAggregationFuncCall(List<NodeAst> projection)
    {
        NodeAst nodeAst = projection[0];

        return nodeAst.nodeType switch
        {
            NodeType.ExprFuncCall => nodeAst,
            NodeType.ExprAlias when nodeAst.leftAst?.nodeType == NodeType.ExprFuncCall => nodeAst.leftAst,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid aggregation projection"),
        };
    }

    private static QueryAggregationType GetAggregationType(NodeAst funcCall) =>
        AggregateFunctionCatalog.GetAggregationType(funcCall.leftAst!.yytext!);

    private static bool IsCountAll(NodeAst funcCall)
    {
        if (funcCall.rightAst is null)
            return true;

        return funcCall.rightAst.nodeType == NodeType.ExprAllFields;
    }

    private static bool TryGetAggregationValue(
        NodeAst funcCall,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        out ColumnValue? value)
    {
        if (IsCountAll(funcCall))
        {
            value = null;
            return false;
        }

        NodeAst? argument = funcCall.rightAst;

        if (argument is null)
        {
            value = null;
            return false;
        }

        value = row is QueryRow qr
            ? SqlExecutor.EvalExpr(argument, qr, ticket.Parameters, ticket.RowNameResolver)
            : SqlExecutor.EvalExpr(argument, row, ticket.Parameters, ticket.RowNameResolver);
        return true;
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalCount(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (IsCountAll(funcCall))
            {
                count++;
                continue;
            }

            if (TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value)
                && value!.Type != ColumnType.Null)
            {
                count++;
            }
        }

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), new ColumnValue(ColumnType.Integer64, count) } });
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalSum(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        SumAccumulator sum = new();

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            sum.Add(value);
        }

        ColumnValue result = sum.Result();

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), result } });
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalAverage(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        AverageAccumulator average = new();

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            average.Add(value);
        }

        ColumnValue result = average.Result();

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), result } });
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalMin(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        ColumnValue? min = null;

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            min = min is null || value.CompareTo(min) < 0 ? value : min;
        }

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), min ?? ColumnValue.Null } });
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalMax(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        ColumnValue? max = null;

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            max = max is null || value.CompareTo(max) > 0 ? value : max;
        }

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), max ?? ColumnValue.Null } });
    }

    private static string GetGlobalAggregateOutputName(QueryTicket ticket, int index)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            return index.ToString();

        return QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[index], index);
    }

    /// <summary>
    /// Describes one projection column after analysis. Bare aggregates populate
    /// <see cref="IsAggregate"/>/<see cref="FuncCall"/>. Compound aggregate expressions
    /// (e.g. <c>COALESCE(SUM(x),0)</c>, <c>SUM(a)/SUM(b)</c>) populate
    /// <see cref="IsCompound"/>, <see cref="CompoundAggregates"/>, and
    /// <see cref="RewrittenExpression"/>. Plain (non-aggregate) projections leave all
    /// aggregate fields null/false.
    /// </summary>
    internal readonly record struct AnalyzedProjection(
        NodeAst Expression,
        string OutputName,
        bool IsAggregate,
        NodeAst? FuncCall,
        bool IsCompound,
        IReadOnlyList<(NodeAst AggregateNode, string Placeholder)>? CompoundAggregates,
        NodeAst? RewrittenExpression)
    {
        /// <summary>Backward-compatible constructor for plain and bare-aggregate projections.</summary>
        public AnalyzedProjection(NodeAst expression, string outputName, bool isAggregate, NodeAst? funcCall)
            : this(expression, outputName, isAggregate, funcCall, false, null, null) { }
    }

    /// <summary>
    /// What one aggregate call is: its node, its kind, and whether it is <c>COUNT(*)</c>. Decided
    /// once per query by <see cref="AggregateLayout"/>, not once per group.
    /// </summary>
    private sealed record AggregateDescriptor(NodeAst FuncCall, QueryAggregationType Type, bool IsCountAll)
    {
        /// <summary>The resolved extension aggregate and its argument expressions, for <see cref="QueryAggregationType.Extension"/>.</summary>
        public ExtensionAggregateCall? Extension { get; init; }
    }

    /// <summary>
    /// One call to an extension aggregate, resolved once per query: the aggregate, the name the call
    /// used (for error messages), and the argument expressions in call order. The arity is checked
    /// here, so a wrong call fails before any row is read.
    /// </summary>
    private sealed class ExtensionAggregateCall
    {
        public ExtensionAggregateCall(NodeAst funcCall)
        {
            CalledName = funcCall.leftAst!.yytext!;
            Aggregate = AggregateFunctionCatalog.ResolveExtension(CalledName);

            List<NodeAst> arguments = [];

            if (funcCall.rightAst is { } argumentAst)
            {
                if (argumentAst.nodeType == NodeType.ExprAllFields)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Aggregate '{CalledName}' does not accept *; pass its arguments");

                if (argumentAst.nodeType == NodeType.ExprArgumentList)
                    ExpressionChains.Flatten(argumentAst, NodeType.ExprArgumentList, arguments);
                else
                    arguments.Add(argumentAst);
            }

            Functions.ScalarFunctionArguments.ValidateArity(CalledName, Aggregate.MinArity, Aggregate.MaxArity, arguments.Count);
            Arguments = [.. arguments];
        }

        public string CalledName { get; }

        public Extensions.ExtensionAggregate Aggregate { get; }

        public NodeAst[] Arguments { get; }
    }

    /// <summary>
    /// The part of grouped aggregation that is the same for every group, analyzed once per query:
    /// the distinct aggregate calls, which one each projection reads, the placeholders of each
    /// compound projection, and the plain columns a compound projection reads.
    ///
    /// <para>Before it existed, every new group repeated this work: it compared every aggregate
    /// call with every other (<see cref="QueryAstComparer.AreEquivalent"/>) to share states,
    /// resolved each call's kind from its name, and walked each compound expression for its free
    /// columns. With many groups that work dominated the per-group cost, and none of it depends
    /// on the group. A group now allocates only its mutable states and the values it captures.</para>
    ///
    /// <para>Immutable after <see cref="Build"/>, so the spill path, which carries accumulators
    /// between partitions, and every group of one query share one instance.</para>
    /// </summary>
    private sealed class AggregateLayout
    {
        /// <summary>One entry of the group table (hash, next, key and value references) plus its bucket.</summary>
        private const int GroupTableEntryBytes = 32;

        /// <summary>A <see cref="GroupAccumulator"/> object and its fields.</summary>
        private const int AccumulatorBytes = 48;

        /// <summary>The accumulator's captured-value dictionary before entries.</summary>
        private const int CapturedDictionaryBytes = 128;

        /// <summary>One captured-value dictionary entry.</summary>
        private const int CapturedEntryBytes = 32;

        /// <summary>One <see cref="AggregateMetricState"/> with its inline sum and average accumulators.</summary>
        private const int AggregateStateBytes = 256;

        public required List<AnalyzedProjection> Projections { get; init; }

        /// <summary>The distinct aggregate calls; structurally equal calls share one entry.</summary>
        public required AggregateDescriptor[] Aggregates { get; init; }

        /// <summary>For each projection, the aggregate a bare-aggregate projection reads, else -1.</summary>
        public required int[] ProjectionAggregate { get; init; }

        /// <summary>For each projection, the (placeholder, aggregate) pairs of a compound projection; empty otherwise.</summary>
        public required (string Placeholder, int Aggregate)[][] CompoundSlots { get; init; }

        /// <summary>For each projection, the plain column references of a compound projection; empty otherwise.</summary>
        public required NodeAst[][] CompoundFreeColumns { get; init; }

        /// <summary>
        /// Estimated heap bytes of one group's state, without its key: the group-table entry, the
        /// accumulator with its captured-value dictionary, and one state per distinct aggregate. A
        /// spilling GROUP BY charges this plus the key (<see cref="GroupBytes"/>) when it admits a
        /// group. A state that grows with its rows (a MIN or MAX over strings) is charged at its
        /// starting size only.
        /// </summary>
        public required long GroupStateBytes { get; init; }

        /// <summary>Estimated heap bytes of one admitted group with key <paramref name="key"/>.</summary>
        public long GroupBytes(ReadOnlySpan<ColumnValue> key) => RowMemoryEstimator.EstimateKey(key) + GroupStateBytes;

        public static AggregateLayout Build(List<AnalyzedProjection> projections)
        {
            List<AggregateDescriptor> aggregates = [];
            int[] projectionAggregate = new int[projections.Count];
            var compoundSlots = new (string, int)[projections.Count][];
            var freeColumns = new NodeAst[projections.Count][];

            for (int i = 0; i < projections.Count; i++)
            {
                AnalyzedProjection projection = projections[i];
                projectionAggregate[i] = -1;
                compoundSlots[i] = [];
                freeColumns[i] = [];

                if (projection.IsAggregate)
                {
                    projectionAggregate[i] = IndexOf(projection.FuncCall!, aggregates);
                }
                else if (projection.IsCompound)
                {
                    var slots = new (string, int)[projection.CompoundAggregates!.Count];
                    for (int j = 0; j < slots.Length; j++)
                    {
                        (NodeAst aggregateNode, string placeholder) = projection.CompoundAggregates[j];
                        slots[j] = (placeholder, IndexOf(aggregateNode, aggregates));
                    }

                    compoundSlots[i] = slots;

                    List<NodeAst> columns = [];
                    QueryExpressionClassifier.CollectNonAggregateColumnRefs(projection.Expression, columns);
                    freeColumns[i] = [.. columns];
                }
            }

            int capturedValues = 0;
            for (int i = 0; i < projections.Count; i++)
            {
                if (!projections[i].IsAggregate)
                    capturedValues += projections[i].IsCompound ? freeColumns[i].Length : 1;
            }

            return new AggregateLayout
            {
                Projections = projections,
                Aggregates = [.. aggregates],
                ProjectionAggregate = projectionAggregate,
                CompoundSlots = compoundSlots,
                CompoundFreeColumns = freeColumns,
                GroupStateBytes = GroupTableEntryBytes
                    + AccumulatorBytes
                    + CapturedDictionaryBytes + (long)capturedValues * (CapturedEntryBytes + RowMemoryEstimator.ColumnValueBytes)
                    + RowMemoryEstimator.ArrayHeaderBytes + (long)aggregates.Count * (RowMemoryEstimator.ReferenceBytes + AggregateStateBytes)
                    + ExtensionStateBytes(aggregates),
            };
        }

        /// <summary>
        /// The bytes charged for the state objects of the extension aggregates: the engine cannot
        /// measure a user state, so each one costs its declared hint (or the default), plus the
        /// argument scratch array the state keeps.
        /// </summary>
        private static long ExtensionStateBytes(List<AggregateDescriptor> aggregates)
        {
            long bytes = 0;

            foreach (AggregateDescriptor aggregate in aggregates)
            {
                if (aggregate.Extension is { } call)
                    bytes += call.Aggregate.StateSizeBytes + RowMemoryEstimator.ArrayHeaderBytes
                             + (long)call.Arguments.Length * RowMemoryEstimator.ReferenceBytes;
            }

            return bytes;
        }

        /// <summary>
        /// The index of the aggregate structurally equal to <paramref name="aggregateNode"/>, added
        /// when there is none. O(n) in the aggregate count, which is small, and paid once per query.
        /// </summary>
        private static int IndexOf(NodeAst aggregateNode, List<AggregateDescriptor> aggregates)
        {
            for (int i = 0; i < aggregates.Count; i++)
            {
                if (QueryAstComparer.AreEquivalent(aggregates[i].FuncCall, aggregateNode))
                    return i;
            }

            QueryAggregationType type = GetAggregationType(aggregateNode);

            aggregates.Add(new AggregateDescriptor(aggregateNode, type, type != QueryAggregationType.Extension && IsCountAll(aggregateNode))
            {
                Extension = type == QueryAggregationType.Extension ? new ExtensionAggregateCall(aggregateNode) : null,
            });

            return aggregates.Count - 1;
        }
    }

    /// <summary>
    /// Accumulates rows for one group (or the global no-GROUP-BY aggregate). Handles three
    /// projection kinds: plain (group-key columns, captured once), bare aggregate (one
    /// <see cref="AggregateMetricState"/> per distinct aggregate), and compound aggregate
    /// (one <see cref="AggregateMetricState"/> per extracted sub-aggregate; the rewritten
    /// scalar expression is evaluated at finalize time against the resolved placeholder values).
    ///
    /// Identical aggregate sub-expressions across projections share one <see cref="AggregateMetricState"/>
    /// (the shared <see cref="AggregateLayout"/> decided which ones once per query), so each
    /// distinct aggregate is accumulated exactly once per row regardless of how many projections
    /// reference it.
    /// </summary>
    private sealed class GroupAccumulator
    {
        private readonly AggregateLayout layout;
        private readonly Dictionary<string, ColumnValue> outputValues = new(StringComparer.OrdinalIgnoreCase);
        private readonly AggregateMetricState[] states;
        private bool capturedGroupValues;

        public GroupAccumulator(AggregateLayout layout)
        {
            this.layout = layout;
            states = new AggregateMetricState[layout.Aggregates.Length];

            for (int i = 0; i < states.Length; i++)
                states[i] = new AggregateMetricState(layout.Aggregates[i]);
        }

        public void AddRow(IReadOnlyDictionary<string, ColumnValue> row, QueryTicket ticket)
        {
            if (!capturedGroupValues)
            {
                List<AnalyzedProjection> projections = layout.Projections;
                QueryRow? qr = row as QueryRow;
                for (int i = 0; i < projections.Count; i++)
                {
                    if (projections[i].IsAggregate)
                        continue;

                    if (projections[i].IsCompound)
                    {
                        // Capture values for each non-aggregate column reference in the compound
                        // expression. These are the free variables the rewritten expression will
                        // look up by name in the synthetic row built by ToResultRow.
                        foreach (NodeAst colRef in layout.CompoundFreeColumns[i])
                        {
                            string colName = colRef.yytext!;
                            if (outputValues.ContainsKey(colName))
                                continue;
                            outputValues[colName] = qr is not null
                                ? SqlExecutor.EvalExpr(colRef, qr, ticket.Parameters, ticket.RowNameResolver)
                                : SqlExecutor.EvalExpr(colRef, row, ticket.Parameters, ticket.RowNameResolver);
                        }
                        continue;
                    }

                    NodeAst expression = QueryExpressionClassifier.UnwrapAlias(projections[i].Expression);
                    outputValues[projections[i].OutputName] = qr is not null
                        ? SqlExecutor.EvalExpr(expression, qr, ticket.Parameters, ticket.RowNameResolver)
                        : SqlExecutor.EvalExpr(expression, row, ticket.Parameters, ticket.RowNameResolver);
                }

                capturedGroupValues = true;
            }

            // Each distinct aggregate is fed exactly once, even when several projections read it.
            foreach (AggregateMetricState state in states)
                state.AddRow(row, ticket);
        }

        /// <summary>
        /// Finalizes all aggregate states, evaluates compound projections' rewritten
        /// scalar expressions against the resolved placeholder values, and returns
        /// the complete output row. An aggregate that several projections read is finalized once.
        ///
        /// The synthetic row is evaluated with <c>rowNameResolver: null</c> because the
        /// placeholder names (null-char prefix) would cause <see cref="QueryRowNameResolver"/>
        /// to throw <c>UnknownColumn</c>. Non-aggregate column references (e.g. <c>bucket</c>
        /// in <c>bucket + SUM(amount)</c>) are pre-captured into <see cref="outputValues"/> by
        /// <see cref="AddRow"/> on the first row of each group, keyed by their unqualified
        /// identifier name — the same key the rewritten expression looks up at evaluation time.
        /// </summary>
        public QueryResultRow ToResultRow(QueryTicket ticket)
        {
            List<AnalyzedProjection> projections = layout.Projections;
            Dictionary<string, ColumnValue> row = new(outputValues, StringComparer.OrdinalIgnoreCase);
            ColumnValue?[] finalized = new ColumnValue?[states.Length];

            for (int i = 0; i < projections.Count; i++)
            {
                if (projections[i].IsAggregate)
                {
                    row[projections[i].OutputName] = Finalized(layout.ProjectionAggregate[i]);
                }
                else if (projections[i].IsCompound)
                {
                    // Build a synthetic row that merges the already-captured group-key values
                    // with the finalized placeholder values for this projection's sub-aggregates.
                    Dictionary<string, ColumnValue> syntheticRow = new(row, StringComparer.OrdinalIgnoreCase);
                    foreach ((string placeholder, int aggregate) in layout.CompoundSlots[i])
                        syntheticRow[placeholder] = Finalized(aggregate);

                    row[projections[i].OutputName] = SqlExecutor.EvalExpr(
                        projections[i].RewrittenExpression!,
                        syntheticRow,
                        ticket.Parameters,
                        rowNameResolver: null);
                }
            }

            return new QueryResultRow(default(ObjectIdValue), row);

            ColumnValue Finalized(int aggregate) => finalized[aggregate] ??= states[aggregate].FinalizeValue();
        }
    }

    /// <summary>
    /// The running state of one <c>SUM</c>, shared by the global and the grouped paths so both apply
    /// one rule. The result type follows the values: any Float64 gives Float64; otherwise any NUMERIC
    /// gives NUMERIC, exact, with Integer64 values scaled in; otherwise Integer64 (which wraps on
    /// overflow, as it always has). A NUMERIC total past the range is
    /// <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/>, as Spanner reports it. Only the final
    /// total is checked: the running total is an <see cref="ExactTotal"/>, which can pass the range on
    /// the way and come back. A mutable struct: hold it in a field or a local and call it in place.
    /// </summary>
    private struct SumAccumulator
    {
        private long intSum;            // Integer64 values, wrapping (the Integer64 result)
        private ExactTotal exactIntSum; // the same values, exact (scaled into a NUMERIC result)
        private double floatSum;        // Integer64 and Float64 values as doubles (the Float64 result)
        private ExactTotal numericSum;  // NUMERIC values, unscaled
        private bool hasValue;
        private bool hasFloat;
        private bool hasNumeric;

        public void Add(ColumnValue value)
        {
            hasValue = true;

            switch (value.Type)
            {
                case ColumnType.Integer64:
                    intSum += value.LongValue;
                    exactIntSum.Add(value.LongValue);
                    floatSum += value.LongValue;
                    break;

                case ColumnType.Float64:
                    hasFloat = true;
                    floatSum += value.FloatValue;
                    break;

                case ColumnType.Numeric:
                    hasNumeric = true;
                    numericSum.Add(value.NumericUnscaled);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"SUM requires a numeric column, got {value.Type}");
            }
        }

        public readonly ColumnValue Result()
        {
            if (!hasValue)
                return ColumnValue.Null;

            if (hasFloat)
                return new ColumnValue(ColumnType.Float64, hasNumeric ? floatSum + numericSum.ToDouble() : floatSum);

            if (hasNumeric)
                return ColumnValue.FromNumeric(ExactTotal.Combine(numericSum, exactIntSum).ToNumeric("SUM"));

            return new ColumnValue(ColumnType.Integer64, intSum);
        }
    }

    /// <summary>
    /// The running state of one <c>AVG</c>, shared by the global and the grouped paths. An average of
    /// NUMERIC values (with or without Integer64 ones) is NUMERIC: the exact total divided by the count,
    /// rounded half away from zero. The total may pass the NUMERIC range while the average does not
    /// (the average of two maximum values is the maximum), so only the average is range-checked. Any
    /// Float64 value, or only Integer64 values, gives Float64, as before. A mutable struct: call it in
    /// place.
    /// </summary>
    private struct AverageAccumulator
    {
        private double sum;             // Integer64 and Float64 values as doubles
        private ExactTotal exactIntSum; // Integer64 values, exact
        private ExactTotal numericSum;  // NUMERIC values, unscaled
        private long count;
        private bool hasFloat;
        private bool hasNumeric;

        public void Add(ColumnValue value)
        {
            switch (value.Type)
            {
                case ColumnType.Integer64:
                    sum += value.LongValue;
                    exactIntSum.Add(value.LongValue);
                    break;

                case ColumnType.Float64:
                    hasFloat = true;
                    sum += value.FloatValue;
                    break;

                case ColumnType.Numeric:
                    hasNumeric = true;
                    numericSum.Add(value.NumericUnscaled);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"AVG requires a numeric column, got {value.Type}");
            }

            count++;
        }

        public readonly ColumnValue Result()
        {
            if (count == 0)
                return ColumnValue.Null;

            if (hasNumeric && !hasFloat)
                return ColumnValue.FromNumeric(ExactTotal.Combine(numericSum, exactIntSum).AverageToNumeric(count, "AVG"));

            double total = hasNumeric ? sum + numericSum.ToDouble() : sum;
            return new ColumnValue(ColumnType.Float64, total / count);
        }
    }

    /// <summary>
    /// An exact running total of Int128 values that never overflows: it stays an <see cref="Int128"/>
    /// (no allocation) until a sum leaves that range, and only then moves to a
    /// <see cref="BigInteger"/>. Used for NUMERIC unscaled totals and exact integer totals, where a
    /// partial total may legitimately pass the NUMERIC range (or even 128 bits) before the final
    /// result comes back inside it. A mutable struct: call it in place.
    /// </summary>
    private struct ExactTotal
    {
        private Int128 narrow;
        private BigInteger? wide;

        public void Add(Int128 value)
        {
            if (wide is { } current)
            {
                wide = current + (BigInteger)value;
                return;
            }

            Int128 next = narrow + value;

            // Signed overflow happened when both operands share a sign and the result does not.
            if ((narrow >= 0) == (value >= 0) && (next >= 0) != (narrow >= 0))
                wide = (BigInteger)narrow + (BigInteger)value;
            else
                narrow = next;
        }

        private readonly BigInteger AsBig => wide ?? (BigInteger)narrow;

        /// <summary><paramref name="numeric"/> (unscaled) plus <paramref name="integers"/> scaled by 10⁹.</summary>
        public static ExactTotal Combine(ExactTotal numeric, ExactTotal integers)
        {
            if (numeric.wide is null && integers.wide is null && integers.narrow == 0)
                return numeric;

            return new ExactTotal { wide = numeric.AsBig + integers.AsBig * NumericMath.ScaleFactor };
        }

        public readonly double ToDouble() =>
            wide is { } big ? (double)big / NumericMath.ScaleFactor : NumericMath.ToDouble(narrow);

        /// <summary>The total as an in-range unscaled NUMERIC; anything else is a NUMERIC overflow.</summary>
        public readonly Int128 ToNumeric(string operation)
        {
            if (wide is null)
                return NumericMath.IsInRange(narrow) ? narrow : throw NumericMath.OutOfRange(operation);

            BigInteger big = wide.Value;
            return big >= (BigInteger)NumericMath.MinUnscaled && big <= (BigInteger)NumericMath.MaxUnscaled
                ? (Int128)big
                : throw NumericMath.OutOfRange(operation);
        }

        /// <summary>The total divided by <paramref name="count"/>, rounded half away from zero, as an in-range unscaled NUMERIC.</summary>
        public readonly Int128 AverageToNumeric(long count, string operation)
        {
            if (wide is null)
            {
                // The common case, with no allocation: |remainder| < count ≤ long.MaxValue, so doubling
                // it cannot overflow Int128.
                Int128 narrowQuotient = narrow / count;
                Int128 narrowRemainder = narrow % count;

                if (Int128.Abs(narrowRemainder) * 2 >= count)
                    narrowQuotient += Int128.Sign(narrowRemainder);

                return NumericMath.IsInRange(narrowQuotient) ? narrowQuotient : throw NumericMath.OutOfRange(operation);
            }

            BigInteger quotient = BigInteger.DivRem(AsBig, count, out BigInteger remainder);

            if (BigInteger.Abs(remainder) * 2 >= count)
                quotient += remainder.Sign;

            return new ExactTotal { wide = quotient }.ToNumeric(operation);
        }
    }

    private sealed class AggregateMetricState
    {
        private readonly NodeAst funcCall;
        private readonly QueryAggregationType aggregationType;
        private readonly bool isCountAll;
        private long countAll;
        private long countNonNull;
        private SumAccumulator sum;
        private AverageAccumulator average;
        private ColumnValue? min;
        private ColumnValue? max;

        // An extension aggregate: the call, its user state, and the argument scratch reused per row
        // (a state is fed by one thread, and the adapter converts the values before the user sees them).
        private readonly ExtensionAggregateCall? extension;
        private readonly object? extensionState;
        private readonly ColumnValue[]? extensionArguments;

        /// <param name="descriptor">The call, decided once per query by <see cref="AggregateLayout"/>.</param>
        public AggregateMetricState(AggregateDescriptor descriptor)
        {
            funcCall = descriptor.FuncCall;
            aggregationType = descriptor.Type;
            isCountAll = descriptor.IsCountAll;

            // The state exists from the first moment of the group, so a global aggregate over zero
            // rows still finishes a created state, as the contract says.
            if (descriptor.Extension is { } call)
            {
                extension = call;
                extensionState = call.Aggregate.CreateState(call.CalledName);
                extensionArguments = new ColumnValue[call.Arguments.Length];
            }
        }

        public void AddRow(IReadOnlyDictionary<string, ColumnValue> row, QueryTicket ticket)
        {
            switch (aggregationType)
            {
                case QueryAggregationType.Count:
                    if (isCountAll)
                    {
                        countAll++;
                        return;
                    }

                    if (TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? countValue)
                        && countValue!.Type != ColumnType.Null)
                    {
                        countNonNull++;
                    }

                    return;

                case QueryAggregationType.Sum:
                    if (!TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? sumValue) || sumValue!.Type == ColumnType.Null)
                        return;

                    sum.Add(sumValue);
                    return;

                case QueryAggregationType.Average:
                    if (!TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? avgValue) || avgValue!.Type == ColumnType.Null)
                        return;

                    average.Add(avgValue);
                    return;

                case QueryAggregationType.Min:
                    if (!TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? minValue) || minValue!.Type == ColumnType.Null)
                        return;

                    min = min is null || minValue.CompareTo(min) < 0 ? minValue : min;
                    return;

                case QueryAggregationType.Max:
                    if (!TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? maxValue) || maxValue!.Type == ColumnType.Null)
                        return;

                    max = max is null || maxValue.CompareTo(max) > 0 ? maxValue : max;
                    return;

                case QueryAggregationType.Extension:
                {
                    NodeAst[] arguments = extension!.Arguments;
                    ColumnValue[] values = extensionArguments!;
                    QueryRow? qr = row as QueryRow;

                    for (int i = 0; i < arguments.Length; i++)
                        values[i] = qr is not null
                            ? SqlExecutor.EvalExpr(arguments[i], qr, ticket.Parameters, ticket.RowNameResolver)
                            : SqlExecutor.EvalExpr(arguments[i], row, ticket.Parameters, ticket.RowNameResolver);

                    extension.Aggregate.Accumulate(extensionState!, extension.CalledName, values);
                    return;
                }

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "Unsupported grouped aggregation type");
            }
        }

        public ColumnValue FinalizeValue()
        {
            return aggregationType switch
            {
                QueryAggregationType.Count => new ColumnValue(
                    ColumnType.Integer64,
                    isCountAll ? countAll : countNonNull),
                QueryAggregationType.Sum => sum.Result(),
                QueryAggregationType.Average => average.Result(),
                QueryAggregationType.Min => min ?? ColumnValue.Null,
                QueryAggregationType.Max => max ?? ColumnValue.Null,
                QueryAggregationType.Extension => extension!.Aggregate.Finish(extensionState!, extension.CalledName),
                _ => throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Unsupported grouped aggregation type"),
            };
        }
    }

    /// <summary>
    /// Equality/hash for GROUP BY keys. Implements the span alternate
    /// (<see cref="IAlternateEqualityComparer{TAlternate,T}"/>) so the group table can be probed with a
    /// <see cref="ReadOnlySpan{T}"/> of just-computed key values — no per-row <see cref="CompositeColumnValue"/>
    /// is allocated on a probe. An owned key is materialized only when a new group is inserted, via
    /// <see cref="Create"/>. The span hash and equality mirror the object forms exactly (same field
    /// mixing; equality via <see cref="ColumnValue.CompareTo"/>), so a span probe and a stored key agree.
    /// </summary>
    private sealed class GroupKeyComparer
        : IEqualityComparer<CompositeColumnValue>,
          IAlternateEqualityComparer<ReadOnlySpan<ColumnValue>, CompositeColumnValue>
    {
        public static GroupKeyComparer Instance { get; } = new();

        public bool Equals(CompositeColumnValue? x, CompositeColumnValue? y)
        {
            if (ReferenceEquals(x, y))
                return true;

            if (x is null || y is null)
                return false;

            return x.CompareTo(y) == 0;
        }

        public int GetHashCode(CompositeColumnValue obj) => HashValues(obj.Values);

        // ── Span alternate: probe the group table without allocating a composite key ──

        /// <summary>Materializes an owned key from the probe span. Called only on new-group insert.</summary>
        public CompositeColumnValue Create(ReadOnlySpan<ColumnValue> alternate) =>
            new(alternate.ToArray());

        public bool Equals(ReadOnlySpan<ColumnValue> alternate, CompositeColumnValue other)
        {
            ColumnValue[] values = other.Values;
            if (alternate.Length != values.Length)
                return false;

            for (int i = 0; i < alternate.Length; i++)
                if (alternate[i].CompareTo(values[i]) != 0)
                    return false;

            return true;
        }

        public int GetHashCode(ReadOnlySpan<ColumnValue> alternate) => HashValues(alternate);

        private static int HashValues(ReadOnlySpan<ColumnValue> values)
        {
            HashCode hash = new();
            hash.Add(values.Length);

            foreach (ColumnValue value in values)
                ColumnValueHash.Add(ref hash, value);

            return hash.ToHashCode();
        }
    }
}
