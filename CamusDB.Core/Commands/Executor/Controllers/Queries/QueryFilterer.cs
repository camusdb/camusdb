
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryFilterer
{
    private readonly ExistsSubqueryExecutor existsSubqueryExecutor;

    public QueryFilterer(ExistsSubqueryExecutor existsSubqueryExecutor)
    {
        this.existsSubqueryExecutor = existsSubqueryExecutor;
    }

    internal async ValueTask<bool> MeetPlanFilterAsync(
        QueryPlan plan,
        Dictionary<string, ColumnValue> row)
    {
        NodeAst? filter = plan.ExecutionFilter;

        if (filter is null)
            return true;

        return await MeetWhereAsync(filter, row, plan.Ticket, plan.Database).ConfigureAwait(false);
    }

    internal async ValueTask<bool> MeetHavingAsync(
        NodeAst having,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue evaluatedExpr = await EvaluateHavingAsync(having, row, ticket).ConfigureAwait(false);
        return ToPredicateResult(evaluatedExpr);
    }

    internal async ValueTask<bool> MeetWhereAsync(
        NodeAst where,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        ColumnValue evaluatedExpr = await EvaluatePredicateAsync(where, row, ticket, database).ConfigureAwait(false);

        return ToPredicateResult(evaluatedExpr);
    }

    internal async IAsyncEnumerable<QueryResultRow> FilterHavingResultset(
        DatabaseDescriptor database,
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Having is null)
        {
            await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
                yield return resultRow;

            yield break;
        }

        await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
        {
            if (await MeetHavingAsync(ticket.Having, resultRow.Row, ticket).ConfigureAwait(false))
                yield return resultRow;
        }
    }

    private ValueTask<ColumnValue> EvaluateHavingAsync(
        NodeAst expr,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        if (expr.nodeType is NodeType.ExprAnd)
        {
            return EvaluateHavingAndAsync(expr, row, ticket);
        }

        if (expr.nodeType is NodeType.ExprOr)
        {
            return EvaluateHavingOrAsync(expr, row, ticket);
        }

        return ValueTask.FromResult(QueryHavingEvaluator.Evaluate(expr, row, ticket, ticket.Parameters));
    }

    private async ValueTask<ColumnValue> EvaluateHavingAndAsync(
        NodeAst expr,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue leftValue = QueryHavingEvaluator.Evaluate(expr.leftAst!, row, ticket, ticket.Parameters);

        if (!ToPredicateResult(leftValue))
            return ColumnValue.False;

        return await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
    }

    private async ValueTask<ColumnValue> EvaluateHavingOrAsync(
        NodeAst expr,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue leftValue = QueryHavingEvaluator.Evaluate(expr.leftAst!, row, ticket, ticket.Parameters);

        if (ToPredicateResult(leftValue))
            return ColumnValue.True;

        return await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
    }

    private async ValueTask<ColumnValue> EvaluatePredicateAsync(
        NodeAst expr,
        Dictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        switch (expr.nodeType)
        {
            case NodeType.ExprExistsCorrelated:
            {
                if (ticket.ExistsSubqueries is null || !ticket.ExistsSubqueries.TryGet(expr, out PreparedExistsSubquery prepared))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "Correlated EXISTS subquery was not prepared");
                }

                bool exists = await existsSubqueryExecutor.ExecuteCorrelatedAsync(
                    database,
                    prepared,
                    row,
                    ticket.TxnState,
                    ticket.Parameters).ConfigureAwait(false);

                return ColumnValue.FromBool(exists);
            }

            case NodeType.ExprAnd:
            {
                ColumnValue leftValue = await EvaluatePredicateAsync(expr.leftAst!, row, ticket, database).ConfigureAwait(false);

                if (!ToPredicateResult(leftValue))
                    return ColumnValue.False;

                return await EvaluatePredicateAsync(expr.rightAst!, row, ticket, database).ConfigureAwait(false);
            }

            case NodeType.ExprOr:
            {
                ColumnValue leftValue = await EvaluatePredicateAsync(expr.leftAst!, row, ticket, database).ConfigureAwait(false);

                if (ToPredicateResult(leftValue))
                    return ColumnValue.True;

                return await EvaluatePredicateAsync(expr.rightAst!, row, ticket, database).ConfigureAwait(false);
            }

            default:
                return SqlExecutor.EvalExpr(expr, row, ticket.Parameters, ticket.RowNameResolver);
        }
    }

    private static bool ToPredicateResult(ColumnValue evaluatedExpr) =>
        evaluatedExpr.Type switch
        {
            ColumnType.Null => false,
            ColumnType.Bool => evaluatedExpr.BoolValue,
            ColumnType.Float64 => evaluatedExpr.FloatValue != 0,
            ColumnType.Integer64 => evaluatedExpr.LongValue != 0,
            _ => false,
        };
}
