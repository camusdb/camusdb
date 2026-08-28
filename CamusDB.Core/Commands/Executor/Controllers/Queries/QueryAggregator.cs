
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
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

        if (QueryHavingWorkspace.NeedsExpandedGlobalAggregate(ticket) || HasCompoundProjection(ticket.Projection))
            return AggregateGlobalWorkspace(ticket, dataCursor);

        NodeAst funcCall = GetSingleAggregationFuncCall(ticket.Projection);
        QueryAggregationType aggregationType = GetAggregationType(funcCall);

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
    /// original implementation. When spilling is enabled the path buffers input rows until the
    /// buffer reaches <see cref="context.Options.SpillEffectiveThreshold"/>; if overflow occurs
    /// the rows are partitioned into <see cref="context.Options.SpillMergeFanIn"/> spill files by
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
        List<AnalyzedProjection> projections = AnalyzeGroupedWorkspace(ticket);

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
                    accumulator = new GroupAccumulator(projections);
                    groupLookup[groupKey] = accumulator;
                }
                accumulator.AddRow(resultRow.Row, ticket);
            }
            foreach (GroupAccumulator accumulator in groups.Values)
                yield return accumulator.ToResultRow(ticket);
            yield break;
        }

        await foreach (QueryResultRow row in AggregateGroupedWithPossibleSpill(
            groupBy, projections, ticket, dataCursor, stats, context, ct).ConfigureAwait(false))
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
        List<AnalyzedProjection> projections = AnalyzeGroupedWorkspace(ticket);
        return StreamingAggregateRows(groupBy, projections, ticket, dataCursor, context, context.CancellationToken);
    }

    private static async IAsyncEnumerable<QueryResultRow> StreamingAggregateRows(
        IReadOnlyList<NodeAst> groupBy,
        List<AnalyzedProjection> projections,
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
                currentAccumulator = new GroupAccumulator(projections);
            }

            currentAccumulator!.AddRow(resultRow.Row, ticket);
        }

        if (currentAccumulator is not null)
            yield return currentAccumulator.ToResultRow(ticket);
    }

    /// <summary>
    /// Spill-aware GROUP BY aggregation. Buffers rows until the buffer count reaches
    /// <see cref="context.Options.SpillEffectiveThreshold"/>. If the threshold is never reached
    /// the buffered rows are aggregated in memory. Otherwise all buffered rows plus the
    /// remaining input are written to <see cref="context.Options.SpillMergeFanIn"/> partition
    /// files by <see cref="GroupPartitionIndex"/>, and each partition file is aggregated by
    /// <see cref="AggregatePartitionAsync"/>. The <see cref="SpillScope"/> is disposed in a
    /// <c>finally</c> block so spill files are cleaned up on completion, cancellation, and
    /// exception.
    ///
    /// If a first-level partition still holds more than <see cref="context.Options.SpillEffectiveThreshold"/>
    /// distinct groups, <see cref="AggregatePartitionAsync"/> recursively re-partitions it with a
    /// fresh hash seed until the partition fits in memory or the recursion depth cap is reached.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregateGroupedWithPossibleSpill(
        IReadOnlyList<NodeAst> groupBy,
        List<AnalyzedProjection> projections,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        StatisticsManager? stats,
        QueryExecutionContext context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = context.Options.SpillEffectiveThreshold;
        int K = context.Options.SpillMergeFanIn;

        List<QueryResultRow> buffer = new();
        SpillScope? scope = null;
        FileStream[]? writers = null;
        string[]? paths = null;

        try
        {
            GroupKeyBuilder writeBuilder = new();
            ColumnValue[] writeScratch = new ColumnValue[groupBy.Count];
            await foreach (QueryResultRow row in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                if (scope is null)
                {
                    buffer.Add(row);
                    if (buffer.Count >= threshold)
                    {
                        context.Probe?.NoteSpill();
                        scope = SpillFileManager.CreateScope(context.SpillDirectory);
                        paths   = new string[K];
                        writers = new FileStream[K];
                        for (int i = 0; i < K; i++)
                            paths[i] = scope.OpenWriter(out writers[i]);

                        foreach (QueryResultRow buffered in buffer)
                            WriteToGroupPartition(buffered, groupBy, ticket, K, writers, writeBuilder, writeScratch);
                        buffer.Clear();
                    }
                }
                else
                {
                    WriteToGroupPartition(row, groupBy, ticket, K, writers!, writeBuilder, writeScratch);
                }
            }

            if (scope is null)
            {
                // All rows fit in the buffer — aggregate in-memory.
                GroupKeyBuilder memBuilder = new();
                Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
                var memLookup = groups.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
                ColumnValue[] memScratch = new ColumnValue[groupBy.Count];
                foreach (QueryResultRow row in buffer)
                {
                    ReadOnlySpan<ColumnValue> key = memBuilder.BuildInto(groupBy, row.Row, ticket, memScratch);
                    if (!memLookup.TryGetValue(key, out GroupAccumulator? acc))
                    {
                        acc = new GroupAccumulator(projections);
                        memLookup[key] = acc;
                    }
                    acc.AddRow(row.Row, ticket);
                }
                foreach (GroupAccumulator acc in groups.Values)
                    yield return acc.ToResultRow(ticket);
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
                        paths![i], projections, groupBy, ticket, scope!, depth: 0, seed: 0, stats, context, ct).ConfigureAwait(false))
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
    /// Hashes <paramref name="row"/>'s group key and writes it to the matching partition writer.
    /// </summary>
    private static void WriteToGroupPartition(
        QueryResultRow row,
        IReadOnlyList<NodeAst> groupBy,
        QueryTicket ticket,
        int K,
        FileStream[] writers,
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

    private static int PartitionFromHash(int hashCode, int K, int seed)
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
    /// Reads all rows from a single partition spill file and aggregates them in-memory.
    /// Because <see cref="GroupPartitionIndex"/> is deterministic, all rows sharing a group key
    /// are guaranteed to be in the same partition, so the result is correct independently of
    /// which level produced this partition file.
    ///
    /// When the number of distinct groups in this partition would exceed
    /// <see cref="context.Options.SpillEffectiveThreshold"/> and <paramref name="depth"/> is
    /// below <see cref="MaxGroupByRecursionDepth"/>, the partition is re-read and split into
    /// <see cref="context.Options.SpillMergeFanIn"/> sub-partitions using <paramref name="seed"/>+1
    /// so that group keys that collided at this level land in different sub-partitions. Each
    /// sub-partition is then aggregated recursively. Beyond the depth cap the remaining rows
    /// are aggregated in-memory regardless of count: the hash function cannot separate truly
    /// identical keys, and the in-memory dictionary is bounded by the number of distinct group
    /// values, not the raw row count.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregatePartitionAsync(
        string path,
        List<AnalyzedProjection> projections,
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

        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, context.Options.SpillMaxFrameBytes, ct: ct).ConfigureAwait(false);
        if (reader is null) yield break;

        GroupKeyBuilder partitionBuilder = new();
        Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
        var partitionLookup = groups.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        ColumnValue[] partitionScratch = new ColumnValue[groupBy.Count];
        bool overflow = false;

        await using (reader)
        {
            do
            {
                QueryResultRow row = reader.Current;
                ReadOnlySpan<ColumnValue> key = partitionBuilder.BuildInto(groupBy, row.Row, ticket, partitionScratch);
                if (!partitionLookup.TryGetValue(key, out GroupAccumulator? acc))
                {
                    // A new distinct group: check threshold before adding. Beyond the recursion
                    // depth cap we accept unbounded growth here because the dictionary is bounded
                    // by truly distinct keys, not raw row count.
                    if (depth < MaxGroupByRecursionDepth && groups.Count >= threshold)
                    {
                        overflow = true;
                        break;
                    }
                    acc = new GroupAccumulator(projections);
                    partitionLookup[key] = acc;
                }
                acc.AddRow(row.Row, ticket);
            }
            while (await reader.AdvanceAsync(ct).ConfigureAwait(false));
        }

        if (!overflow)
        {
            foreach (GroupAccumulator acc in groups.Values)
                yield return acc.ToResultRow(ticket);
            yield break;
        }

        // Overflow: the partition still holds more distinct groups than the threshold.
        // Re-read the file and split into K sub-partitions with a new seed so colliding
        // group keys at this level redistribute across different sub-partitions.
        if (stats is not null)
            stats.GroupByPartitionRecursionCount++;

        int K = Math.Max(2, context.Options.SpillMergeFanIn);
        int newSeed = seed + 1;
        string[] subPaths = new string[K];
        FileStream[] subWriters = new FileStream[K];
        for (int i = 0; i < K; i++)
            subPaths[i] = scope.OpenWriter(out subWriters[i]);

        SpillRunReader? rdr2 = await SpillRunReader.OpenAsync(path, context.Options.SpillMaxFrameBytes, ct: ct).ConfigureAwait(false);
        if (rdr2 is not null)
        {
            GroupKeyBuilder rePartitionBuilder = new();
            ColumnValue[] rePartitionScratch = new ColumnValue[groupBy.Count];
            await using (rdr2)
            {
                do
                {
                    WriteToGroupPartition(rdr2.Current, groupBy, ticket, K, subWriters, rePartitionBuilder, rePartitionScratch, newSeed);
                }
                while (await rdr2.AdvanceAsync(ct).ConfigureAwait(false));
            }
        }

        for (int i = 0; i < K; i++)
        {
            await subWriters[i].FlushAsync(ct).ConfigureAwait(false);
            subWriters[i].Close();
        }

        for (int i = 0; i < K; i++)
        {
            await foreach (QueryResultRow row in AggregatePartitionAsync(
                subPaths[i], projections, groupBy, ticket, scope, depth + 1, newSeed, stats, context, ct).ConfigureAwait(false))
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

    private static List<AnalyzedProjection> AnalyzeProjections(List<NodeAst> projection)
    {
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
                GetProjectionOutputName(expression, i),
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
        List<AnalyzedProjection> projections = AnalyzeProjections(ticket.Projection!);
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
        List<AnalyzedProjection> projections = AnalyzeProjections(ticket.Projection!);
        HashSet<string> outputNames = new(StringComparer.Ordinal);

        foreach (AnalyzedProjection projection in projections)
            outputNames.Add(projection.OutputName);

        if (ticket.Having is not null)
            QueryHavingWorkspace.AddHiddenProjections(ticket.Having, ticket, projections, outputNames);

        GroupAccumulator accumulator = new(projections);

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

    private static string GetProjectionOutputName(NodeAst expression, int index)
    {
        return QueryProjectionResolver.GetOutputNameFromProjectionExpression(expression, index);
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

    private static QueryAggregationType GetAggregationType(NodeAst funcCall)
    {
        return funcCall.leftAst!.yytext!.ToLowerInvariant() switch
        {
            "count" => QueryAggregationType.Count,
            "sum" => QueryAggregationType.Sum,
            "avg" => QueryAggregationType.Average,
            "min" => QueryAggregationType.Min,
            "max" => QueryAggregationType.Max,
            _ => QueryAggregationType.None,
        };
    }

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
        double sum = 0;
        long intSum = 0;
        bool hasValue = false;
        bool allInteger = true;

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            hasValue = true;

            switch (value.Type)
            {
                case ColumnType.Integer64:
                    intSum += value.LongValue;
                    sum += value.LongValue;
                    break;

                case ColumnType.Float64:
                    allInteger = false;
                    sum += value.FloatValue;
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"SUM requires a numeric column, got {value.Type}");
            }
        }

        ColumnValue result = !hasValue
            ? ColumnValue.Null
            : allInteger
                ? new ColumnValue(ColumnType.Integer64, intSum)
                : new ColumnValue(ColumnType.Float64, sum);

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase) { { GetGlobalAggregateOutputName(ticket, 0), result } });
    }

    private static async IAsyncEnumerable<QueryResultRow> AggregateGlobalAverage(
        NodeAst funcCall,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        double sum = 0;
        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, ticket, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            switch (value.Type)
            {
                case ColumnType.Integer64:
                    sum += value.LongValue;
                    count++;
                    break;

                case ColumnType.Float64:
                    sum += value.FloatValue;
                    count++;
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"AVG requires a numeric column, got {value.Type}");
            }
        }

        ColumnValue result = count == 0
            ? ColumnValue.Null
            : new ColumnValue(ColumnType.Float64, sum / count);

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
    /// Accumulates rows for one group (or the global no-GROUP-BY aggregate). Handles three
    /// projection kinds: plain (group-key columns, captured once), bare aggregate (one
    /// <see cref="AggregateMetricState"/> per projection), and compound aggregate
    /// (one <see cref="AggregateMetricState"/> per extracted sub-aggregate; the rewritten
    /// scalar expression is evaluated at finalize time against the resolved placeholder values).
    ///
    /// Identical aggregate sub-expressions across projections share one <see cref="AggregateMetricState"/>
    /// instance (compared structurally via <see cref="QueryAstComparer.AreEquivalent"/>), so each
    /// distinct aggregate is accumulated exactly once per row regardless of how many projections
    /// reference it.
    /// </summary>
    private sealed class GroupAccumulator
    {
        private readonly List<AnalyzedProjection> projections;
        private readonly Dictionary<string, ColumnValue> outputValues = new(StringComparer.OrdinalIgnoreCase);
        private readonly AggregateMetricState?[] aggregateStates;
        // Flat list: (projectionIndex, placeholder, state) for each sub-aggregate in each compound projection.
        private readonly (int ProjIdx, string Placeholder, AggregateMetricState State)[] compoundStates;
        // Deduplicated list of every unique state — each is fed exactly once per row in AddRow.
        private readonly AggregateMetricState[] allUniqueStates;
        private bool capturedGroupValues;

        public GroupAccumulator(List<AnalyzedProjection> projections)
        {
            this.projections = projections;
            aggregateStates = new AggregateMetricState?[projections.Count];

            // Shared pool: when two projections reference structurally identical aggregates
            // they share one AggregateMetricState so each row is accumulated only once.
            List<(NodeAst Node, AggregateMetricState State)> sharedPool = [];
            List<(int, string, AggregateMetricState)> compoundList = [];

            for (int i = 0; i < projections.Count; i++)
            {
                if (projections[i].IsAggregate)
                    aggregateStates[i] = GetOrCreateState(projections[i].FuncCall!, sharedPool);
                else if (projections[i].IsCompound)
                {
                    foreach (var (aggregateNode, placeholder) in projections[i].CompoundAggregates!)
                        compoundList.Add((i, placeholder, GetOrCreateState(aggregateNode, sharedPool)));
                }
            }

            compoundStates = [.. compoundList];

            allUniqueStates = new AggregateMetricState[sharedPool.Count];
            for (int i = 0; i < sharedPool.Count; i++)
                allUniqueStates[i] = sharedPool[i].State;
        }

        /// <summary>
        /// Returns the existing <see cref="AggregateMetricState"/> from <paramref name="pool"/>
        /// whose aggregate node is structurally equivalent to <paramref name="aggregateNode"/>,
        /// or creates and registers a new one if none matches. O(n) in pool size; n is
        /// typically 1-3 aggregates per group accumulator.
        /// </summary>
        private static AggregateMetricState GetOrCreateState(
            NodeAst aggregateNode,
            List<(NodeAst Node, AggregateMetricState State)> pool)
        {
            foreach (var (node, state) in pool)
            {
                if (QueryAstComparer.AreEquivalent(node, aggregateNode))
                    return state;
            }
            AggregateMetricState newState = new(aggregateNode);
            pool.Add((aggregateNode, newState));
            return newState;
        }

        public void AddRow(IReadOnlyDictionary<string, ColumnValue> row, QueryTicket ticket)
        {
            if (!capturedGroupValues)
            {
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
                        List<NodeAst> colRefs = [];
                        QueryExpressionClassifier.CollectNonAggregateColumnRefs(projections[i].Expression, colRefs);
                        foreach (NodeAst colRef in colRefs)
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

            // Each unique state is fed exactly once, even when shared across projections.
            foreach (AggregateMetricState state in allUniqueStates)
                state.AddRow(row, ticket);
        }

        /// <summary>
        /// Finalizes all aggregate states, evaluates compound projections' rewritten
        /// scalar expressions against the resolved placeholder values, and returns
        /// the complete output row.
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
            Dictionary<string, ColumnValue> row = new(outputValues, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < projections.Count; i++)
            {
                if (projections[i].IsAggregate)
                {
                    row[projections[i].OutputName] = aggregateStates[i]!.FinalizeValue();
                }
                else if (projections[i].IsCompound)
                {
                    // Build a synthetic row that merges the already-captured group-key values
                    // with the finalized placeholder values for this projection's sub-aggregates.
                    Dictionary<string, ColumnValue> syntheticRow = new(row, StringComparer.OrdinalIgnoreCase);
                    foreach (var (projIdx, placeholder, state) in compoundStates)
                    {
                        if (projIdx == i)
                            syntheticRow[placeholder] = state.FinalizeValue();
                    }

                    row[projections[i].OutputName] = SqlExecutor.EvalExpr(
                        projections[i].RewrittenExpression!,
                        syntheticRow,
                        ticket.Parameters,
                        rowNameResolver: null);
                }
            }

            return new QueryResultRow(default(ObjectIdValue), row);
        }
    }

    private sealed class AggregateMetricState
    {
        private readonly NodeAst funcCall;
        private readonly QueryAggregationType aggregationType;
        private long countAll;
        private long countNonNull;
        private long intSum;
        private double floatSum;
        private bool hasSum;
        private bool allInteger = true;
        private double avgSum;
        private long avgCount;
        private ColumnValue? min;
        private ColumnValue? max;

        public AggregateMetricState(NodeAst funcCall)
        {
            this.funcCall = funcCall;
            aggregationType = GetAggregationType(funcCall);
        }

        public void AddRow(IReadOnlyDictionary<string, ColumnValue> row, QueryTicket ticket)
        {
            switch (aggregationType)
            {
                case QueryAggregationType.Count:
                    if (IsCountAll(funcCall))
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

                    hasSum = true;

                    switch (sumValue.Type)
                    {
                        case ColumnType.Integer64:
                            intSum += sumValue.LongValue;
                            floatSum += sumValue.LongValue;
                            break;

                        case ColumnType.Float64:
                            allInteger = false;
                            floatSum += sumValue.FloatValue;
                            break;

                        default:
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"SUM requires a numeric column, got {sumValue.Type}");
                    }

                    return;

                case QueryAggregationType.Average:
                    if (!TryGetAggregationValue(funcCall, row, ticket, out ColumnValue? avgValue) || avgValue!.Type == ColumnType.Null)
                        return;

                    switch (avgValue.Type)
                    {
                        case ColumnType.Integer64:
                            avgSum += avgValue.LongValue;
                            avgCount++;
                            break;

                        case ColumnType.Float64:
                            avgSum += avgValue.FloatValue;
                            avgCount++;
                            break;

                        default:
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"AVG requires a numeric column, got {avgValue.Type}");
                    }

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
                    IsCountAll(funcCall) ? countAll : countNonNull),
                QueryAggregationType.Sum => !hasSum
                    ? ColumnValue.Null
                    : allInteger
                        ? new ColumnValue(ColumnType.Integer64, intSum)
                        : new ColumnValue(ColumnType.Float64, floatSum),
                QueryAggregationType.Average => avgCount == 0
                    ? ColumnValue.Null
                    : new ColumnValue(ColumnType.Float64, avgSum / avgCount),
                QueryAggregationType.Min => min ?? ColumnValue.Null,
                QueryAggregationType.Max => max ?? ColumnValue.Null,
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
            {
                hash.Add(value.Type);
                hash.Add(value.StrValue);
                hash.Add(value.LongValue);
                hash.Add(value.FloatValue);
                hash.Add(value.BoolValue);
            }

            return hash.ToHashCode();
        }
    }
}
