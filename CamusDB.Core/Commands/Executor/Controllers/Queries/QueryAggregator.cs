
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
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

        NodeAst funcCall = GetAggregationFuncCall(ticket.Projection);
        QueryAggregationType aggregationType = GetAggregationType(funcCall);

        return aggregationType switch
        {
            QueryAggregationType.Count => AggregateCount(dataCursor),
            QueryAggregationType.Sum => AggregateSum(funcCall, dataCursor),
            QueryAggregationType.Average => AggregateAverage(funcCall, dataCursor),
            QueryAggregationType.Min => AggregateMin(funcCall, dataCursor),
            QueryAggregationType.Max => AggregateMax(funcCall, dataCursor),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This aggregation type is not supported"),
        };
    }

    private async IAsyncEnumerable<QueryResultRow> AggregateCount(IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        long count = 0;

        await foreach (QueryResultRow _ in dataCursor)
            count++;

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new() { { "0", new ColumnValue(ColumnType.Integer64, count) } }
        );
    }

    private async IAsyncEnumerable<QueryResultRow> AggregateSum(NodeAst funcCall, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        double sum = 0;
        long intSum = 0;
        bool hasValue = false;
        bool allInteger = true;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, out ColumnValue? value) || value!.Type == ColumnType.Null)
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

    private async IAsyncEnumerable<QueryResultRow> AggregateAverage(NodeAst funcCall, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        double sum = 0;
        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, out ColumnValue? value) || value!.Type == ColumnType.Null)
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

    private async IAsyncEnumerable<QueryResultRow> AggregateMin(NodeAst funcCall, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        ColumnValue? min = null;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            min = min is null || value.CompareTo(min) < 0 ? value : min;
        }

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new() { { "0", min ?? new ColumnValue(ColumnType.Null, 0) } }
        );
    }

    private async IAsyncEnumerable<QueryResultRow> AggregateMax(NodeAst funcCall, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        ColumnValue? max = null;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (!TryGetAggregationValue(funcCall, resultRow.Row, out ColumnValue? value) || value!.Type == ColumnType.Null)
                continue;

            max = max is null || value.CompareTo(max) > 0 ? value : max;
        }

        yield return new QueryResultRow(
            default(ObjectIdValue),
            new() { { "0", max ?? new ColumnValue(ColumnType.Null, 0) } }
        );
    }

    private static NodeAst GetAggregationFuncCall(List<NodeAst> projection)
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

    private static bool TryGetAggregationValue(NodeAst funcCall, Dictionary<string, ColumnValue> row, out ColumnValue? value)
    {
        NodeAst? argument = funcCall.rightAst;

        if (argument is null)
        {
            value = null;
            return false;
        }

        if (argument.nodeType == NodeType.Identifier)
        {
            if (!row.TryGetValue(argument.yytext!, out ColumnValue? columnValue))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown column: " + argument.yytext);

            value = columnValue;
            return true;
        }

        value = null;
        return false;
    }
}
