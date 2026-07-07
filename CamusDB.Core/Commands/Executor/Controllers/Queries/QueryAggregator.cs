
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

    internal IAsyncEnumerable<QueryResultRow> AggregateResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be aggregated");

        if (ticket.GroupBy is { Count: > 0 })
            return AggregateGrouped(ticket, dataCursor, _stats);

        if (QueryHavingWorkspace.NeedsExpandedGlobalAggregate(ticket))
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
    /// Aggregates grouped rows. When <see cref="CamusDBConfig.SpillEnabled"/> is <c>false</c>
    /// (the default), all groups are accumulated in a single in-memory dictionary — same as the
    /// original implementation. When spilling is enabled the path buffers input rows until the
    /// buffer reaches <see cref="CamusDBConfig.SpillEffectiveThreshold"/>; if overflow occurs
    /// the rows are partitioned into <see cref="CamusDBConfig.SpillMergeFanIn"/> spill files by
    /// group-key hash, and each partition is then aggregated in-memory independently. Because
    /// the hash is deterministic, all rows that share a group key land in the same partition and
    /// the per-partition result is identical to the global result.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregateGrouped(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        StatisticsManager? stats,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        IReadOnlyList<NodeAst> groupBy = ticket.GroupBy!;
        List<AnalyzedProjection> projections = AnalyzeGroupedWorkspace(ticket);

        if (!CamusDBConfig.SpillEnabled)
        {
            Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
            await foreach (QueryResultRow resultRow in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                CompositeColumnValue groupKey = BuildGroupKey(groupBy, resultRow.Row, ticket);
                if (!groups.TryGetValue(groupKey, out GroupAccumulator? accumulator))
                {
                    accumulator = new GroupAccumulator(projections);
                    groups.Add(groupKey, accumulator);
                }
                accumulator.AddRow(resultRow.Row, ticket);
            }
            foreach (GroupAccumulator accumulator in groups.Values)
                yield return accumulator.ToResultRow();
            yield break;
        }

        await foreach (QueryResultRow row in AggregateGroupedWithPossibleSpill(
            groupBy, projections, ticket, dataCursor, stats, ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Spill-aware GROUP BY aggregation. Buffers rows until the buffer count reaches
    /// <see cref="CamusDBConfig.SpillEffectiveThreshold"/>. If the threshold is never reached
    /// the buffered rows are aggregated in memory. Otherwise all buffered rows plus the
    /// remaining input are written to <see cref="CamusDBConfig.SpillMergeFanIn"/> partition
    /// files by <see cref="GroupPartitionIndex"/>, and each partition file is aggregated by
    /// <see cref="AggregatePartitionAsync"/>. The <see cref="SpillScope"/> is disposed in a
    /// <c>finally</c> block so spill files are cleaned up on completion, cancellation, and
    /// exception.
    ///
    /// If a first-level partition still holds more than <see cref="CamusDBConfig.SpillEffectiveThreshold"/>
    /// distinct groups, <see cref="AggregatePartitionAsync"/> recursively re-partitions it with a
    /// fresh hash seed until the partition fits in memory or the recursion depth cap is reached.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> AggregateGroupedWithPossibleSpill(
        IReadOnlyList<NodeAst> groupBy,
        List<AnalyzedProjection> projections,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        StatisticsManager? stats,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = CamusDBConfig.SpillEffectiveThreshold;
        int K = CamusDBConfig.SpillMergeFanIn;

        List<QueryResultRow> buffer = new();
        SpillScope? scope = null;
        FileStream[]? writers = null;
        string[]? paths = null;

        try
        {
            await foreach (QueryResultRow row in dataCursor.WithCancellation(ct).ConfigureAwait(false))
            {
                if (scope is null)
                {
                    buffer.Add(row);
                    if (buffer.Count >= threshold)
                    {
                        scope = SpillFileManager.CreateScope(CamusDBConfig.DataDirectory);
                        paths   = new string[K];
                        writers = new FileStream[K];
                        for (int i = 0; i < K; i++)
                            paths[i] = scope.OpenWriter(out writers[i]);

                        foreach (QueryResultRow buffered in buffer)
                            WriteToGroupPartition(buffered, groupBy, ticket, K, writers);
                        buffer.Clear();
                    }
                }
                else
                {
                    WriteToGroupPartition(row, groupBy, ticket, K, writers!);
                }
            }

            if (scope is null)
            {
                // All rows fit in the buffer — aggregate in-memory.
                Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
                foreach (QueryResultRow row in buffer)
                {
                    CompositeColumnValue key = BuildGroupKey(groupBy, row.Row, ticket);
                    if (!groups.TryGetValue(key, out GroupAccumulator? acc))
                    {
                        acc = new GroupAccumulator(projections);
                        groups.Add(key, acc);
                    }
                    acc.AddRow(row.Row, ticket);
                }
                foreach (GroupAccumulator acc in groups.Values)
                    yield return acc.ToResultRow();
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
                        paths![i], projections, groupBy, ticket, scope!, depth: 0, seed: 0, stats, ct).ConfigureAwait(false))
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
        int seed = 0)
    {
        CompositeColumnValue key = BuildGroupKey(groupBy, row.Row, ticket);
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
    {
        uint h = (uint)GroupKeyComparer.Instance.GetHashCode(key);
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
    /// <see cref="CamusDBConfig.SpillEffectiveThreshold"/> and <paramref name="depth"/> is
    /// below <see cref="MaxGroupByRecursionDepth"/>, the partition is re-read and split into
    /// <see cref="CamusDBConfig.SpillMergeFanIn"/> sub-partitions using <paramref name="seed"/>+1
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
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = CamusDBConfig.SpillEffectiveThreshold;

        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, ct).ConfigureAwait(false);
        if (reader is null) yield break;

        Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);
        bool overflow = false;

        await using (reader)
        {
            do
            {
                QueryResultRow row = reader.Current;
                CompositeColumnValue key = BuildGroupKey(groupBy, row.Row, ticket);
                if (!groups.TryGetValue(key, out GroupAccumulator? acc))
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
                    groups.Add(key, acc);
                }
                acc.AddRow(row.Row, ticket);
            }
            while (await reader.AdvanceAsync(ct).ConfigureAwait(false));
        }

        if (!overflow)
        {
            foreach (GroupAccumulator acc in groups.Values)
                yield return acc.ToResultRow();
            yield break;
        }

        // Overflow: the partition still holds more distinct groups than the threshold.
        // Re-read the file and split into K sub-partitions with a new seed so colliding
        // group keys at this level redistribute across different sub-partitions.
        if (stats is not null)
            stats.GroupByPartitionRecursionCount++;

        int K = Math.Max(2, CamusDBConfig.SpillMergeFanIn);
        int newSeed = seed + 1;
        string[] subPaths = new string[K];
        FileStream[] subWriters = new FileStream[K];
        for (int i = 0; i < K; i++)
            subPaths[i] = scope.OpenWriter(out subWriters[i]);

        SpillRunReader? rdr2 = await SpillRunReader.OpenAsync(path, ct).ConfigureAwait(false);
        if (rdr2 is not null)
        {
            await using (rdr2)
            {
                do
                {
                    WriteToGroupPartition(rdr2.Current, groupBy, ticket, K, subWriters, newSeed);
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
                subPaths[i], projections, groupBy, ticket, scope, depth + 1, newSeed, stats, ct).ConfigureAwait(false))
                yield return row;
        }
    }

    private static CompositeColumnValue BuildGroupKey(
        IReadOnlyList<NodeAst> groupBy,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue[] values = new ColumnValue[groupBy.Count];

        for (int i = 0; i < groupBy.Count; i++)
        {
            values[i] = SqlExecutor.EvalExpr(
                groupBy[i],
                row,
                ticket.Parameters,
                ticket.RowNameResolver);
        }

        return new CompositeColumnValue(values);
    }

    private static List<AnalyzedProjection> AnalyzeProjections(List<NodeAst> projection)
    {
        List<AnalyzedProjection> analyzed = new(projection.Count);

        for (int i = 0; i < projection.Count; i++)
        {
            NodeAst expression = projection[i];
            bool isAggregate = QueryExpressionClassifier.IsAggregateProjection(expression);
            NodeAst? funcCall = isAggregate ? GetAggregateFuncCall(expression) : null;

            analyzed.Add(new AnalyzedProjection(
                expression,
                GetProjectionOutputName(expression, i),
                isAggregate,
                funcCall));
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

        yield return accumulator.ToResultRow();
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

        value = SqlExecutor.EvalExpr(argument, row, ticket.Parameters, ticket.RowNameResolver);
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
            new Dictionary<string, ColumnValue> { { GetGlobalAggregateOutputName(ticket, 0), new ColumnValue(ColumnType.Integer64, count) } });
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
            new Dictionary<string, ColumnValue> { { GetGlobalAggregateOutputName(ticket, 0), result } });
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
            new Dictionary<string, ColumnValue> { { GetGlobalAggregateOutputName(ticket, 0), result } });
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
            new Dictionary<string, ColumnValue> { { GetGlobalAggregateOutputName(ticket, 0), min ?? ColumnValue.Null } });
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
            new Dictionary<string, ColumnValue> { { GetGlobalAggregateOutputName(ticket, 0), max ?? ColumnValue.Null } });
    }

    private static string GetGlobalAggregateOutputName(QueryTicket ticket, int index)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            return index.ToString();

        return QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[index], index);
    }

    internal readonly record struct AnalyzedProjection(
        NodeAst Expression,
        string OutputName,
        bool IsAggregate,
        NodeAst? FuncCall);

    private sealed class GroupAccumulator
    {
        private readonly List<AnalyzedProjection> projections;
        private readonly Dictionary<string, ColumnValue> outputValues = new();
        private readonly AggregateMetricState[] aggregateStates;
        private bool capturedGroupValues;

        public GroupAccumulator(List<AnalyzedProjection> projections)
        {
            this.projections = projections;
            aggregateStates = new AggregateMetricState[projections.Count];

            for (int i = 0; i < projections.Count; i++)
            {
                if (projections[i].IsAggregate)
                    aggregateStates[i] = new AggregateMetricState(projections[i].FuncCall!);
            }
        }

        public void AddRow(IReadOnlyDictionary<string, ColumnValue> row, QueryTicket ticket)
        {
            if (!capturedGroupValues)
            {
                for (int i = 0; i < projections.Count; i++)
                {
                    if (projections[i].IsAggregate)
                        continue;

                    NodeAst expression = QueryExpressionClassifier.UnwrapAlias(projections[i].Expression);
                    outputValues[projections[i].OutputName] = SqlExecutor.EvalExpr(
                        expression,
                        row,
                        ticket.Parameters,
                        ticket.RowNameResolver);
                }

                capturedGroupValues = true;
            }

            for (int i = 0; i < projections.Count; i++)
            {
                if (!projections[i].IsAggregate)
                    continue;

                aggregateStates[i].AddRow(row, ticket);
            }
        }

        public QueryResultRow ToResultRow()
        {
            Dictionary<string, ColumnValue> row = new(outputValues);

            for (int i = 0; i < projections.Count; i++)
            {
                if (!projections[i].IsAggregate)
                    continue;

                row[projections[i].OutputName] = aggregateStates[i].FinalizeValue();
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

    private sealed class GroupKeyComparer : IEqualityComparer<CompositeColumnValue>
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

        public int GetHashCode(CompositeColumnValue obj)
        {
            HashCode hash = new();
            hash.Add(obj.Values.Length);

            foreach (ColumnValue value in obj.Values)
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
