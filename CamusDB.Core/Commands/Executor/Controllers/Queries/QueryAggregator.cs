
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryAggregator
{
    internal IAsyncEnumerable<QueryResultRow> AggregateResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be aggregated");

        if (ticket.GroupBy is { Count: > 0 })
            return AggregateGrouped(ticket, dataCursor);

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

    private static async IAsyncEnumerable<QueryResultRow> AggregateGrouped(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        IReadOnlyList<NodeAst> groupBy = ticket.GroupBy!;
        List<AnalyzedProjection> projections = AnalyzeGroupedOutput(ticket);
        Dictionary<CompositeColumnValue, GroupAccumulator> groups = new(GroupKeyComparer.Instance);

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
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
    }

    private static CompositeColumnValue BuildGroupKey(
        IReadOnlyList<NodeAst> groupBy,
        Dictionary<string, ColumnValue> row,
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

    private static List<AnalyzedProjection> AnalyzeGroupedOutput(QueryTicket ticket)
    {
        List<AnalyzedProjection> projections = AnalyzeProjections(ticket.Projection!);
        HashSet<string> outputNames = new(StringComparer.Ordinal);

        foreach (AnalyzedProjection projection in projections)
            outputNames.Add(projection.OutputName);

        if (ticket.OrderBy is null)
            return projections;

        foreach (QueryOrderBy orderClause in ticket.OrderBy)
        {
            if (outputNames.Contains(orderClause.ColumnName))
                continue;

            NodeAst expression = ResolveHiddenSortExpression(ticket, orderClause.ColumnName);
            projections.Add(new AnalyzedProjection(expression, orderClause.ColumnName, false, null));
            outputNames.Add(orderClause.ColumnName);
        }

        return projections;
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

    private static NodeAst GetAggregateFuncCall(NodeAst expression)
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
        Dictionary<string, ColumnValue> row,
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
            new() { { "0", new ColumnValue(ColumnType.Integer64, count) } });
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
            ? new ColumnValue(ColumnType.Null, 0)
            : allInteger
                ? new ColumnValue(ColumnType.Integer64, intSum)
                : new ColumnValue(ColumnType.Float64, sum);

        yield return new QueryResultRow(default(ObjectIdValue), new() { { "0", result } });
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
            ? new ColumnValue(ColumnType.Null, 0)
            : new ColumnValue(ColumnType.Float64, sum / count);

        yield return new QueryResultRow(default(ObjectIdValue), new() { { "0", result } });
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
            new() { { "0", min ?? new ColumnValue(ColumnType.Null, 0) } });
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
            new() { { "0", max ?? new ColumnValue(ColumnType.Null, 0) } });
    }

    private readonly record struct AnalyzedProjection(
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

        public void AddRow(Dictionary<string, ColumnValue> row, QueryTicket ticket)
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

        public void AddRow(Dictionary<string, ColumnValue> row, QueryTicket ticket)
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
                    ? new ColumnValue(ColumnType.Null, 0)
                    : allInteger
                        ? new ColumnValue(ColumnType.Integer64, intSum)
                        : new ColumnValue(ColumnType.Float64, floatSum),
                QueryAggregationType.Average => avgCount == 0
                    ? new ColumnValue(ColumnType.Null, 0)
                    : new ColumnValue(ColumnType.Float64, avgSum / avgCount),
                QueryAggregationType.Min => min ?? new ColumnValue(ColumnType.Null, 0),
                QueryAggregationType.Max => max ?? new ColumnValue(ColumnType.Null, 0),
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
