
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryFilterer
{
    private readonly ExistsSubqueryExecutor existsSubqueryExecutor;

    // Memoizes, per predicate AST, whether evaluating it can suspend (contains a correlated EXISTS).
    // Keyed by AST identity via a weak table so cached decisions never keep a parsed query alive.
    private readonly ConditionalWeakTable<NodeAst, object> predicateNeedsAsyncCache = new();

    public QueryFilterer(ExistsSubqueryExecutor existsSubqueryExecutor)
    {
        this.existsSubqueryExecutor = existsSubqueryExecutor;
    }

    /// <summary>
    /// Evaluates a plan's execution filter against a row. Returns a <b>completed</b>
    /// <see cref="ValueTask{T}"/> synchronously for the common case — a predicate with no correlated
    /// EXISTS subquery evaluates through the fully synchronous <see cref="EvaluatePredicate"/>, so the
    /// per-row hot path allocates no async state machine. Only a predicate that can suspend (a correlated
    /// EXISTS reaches out to the KV store per row) falls through to the async path.
    /// </summary>
    internal ValueTask<bool> MeetPlanFilterAsync(
        QueryPlan plan,
        IReadOnlyDictionary<string, ColumnValue> row)
    {
        NodeAst? filter = plan.ExecutionFilter;

        if (filter is null)
            return new ValueTask<bool>(true);

        if (!PredicateNeedsAsync(filter))
            return new ValueTask<bool>(ToPredicateResult(EvaluatePredicate(filter, row, plan.Ticket)));

        return MeetWhereAsyncCore(filter, row, plan.Ticket, plan.Database);
    }

    internal async ValueTask<bool> MeetHavingAsync(
        NodeAst having,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue evaluatedExpr = await EvaluateHavingAsync(having, row, ticket).ConfigureAwait(false);
        return ToPredicateResult(evaluatedExpr);
    }

    /// <summary>
    /// Evaluates a WHERE / join predicate against a row. Synchronous (completed <see cref="ValueTask{T}"/>,
    /// no state-machine allocation) unless the predicate contains a correlated EXISTS subquery — see
    /// <see cref="MeetPlanFilterAsync"/>.
    /// </summary>
    internal ValueTask<bool> MeetWhereAsync(
        NodeAst where,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        if (!PredicateNeedsAsync(where))
            return new ValueTask<bool>(ToPredicateResult(EvaluatePredicate(where, row, ticket)));

        return MeetWhereAsyncCore(where, row, ticket, database);
    }

    private async ValueTask<bool> MeetWhereAsyncCore(
        NodeAst where,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        ColumnValue evaluatedExpr = await EvaluatePredicateAsync(where, row, ticket, database).ConfigureAwait(false);

        return ToPredicateResult(evaluatedExpr);
    }

    /// <summary>
    /// True when <paramref name="predicate"/> can suspend during evaluation — i.e. it contains a
    /// correlated EXISTS anywhere in its tree (the only node the filter evaluates asynchronously). The
    /// result is a property of the AST, so it is memoized per predicate rather than recomputed per row.
    /// </summary>
    private bool PredicateNeedsAsync(NodeAst predicate)
    {
        if (predicateNeedsAsyncCache.TryGetValue(predicate, out object? cached))
            return (bool)cached;

        bool needsAsync = ContainsCorrelatedExists(predicate);
        // AddOrUpdate: a benign race just recomputes and stores the same value.
        predicateNeedsAsyncCache.AddOrUpdate(predicate, needsAsync);
        return needsAsync;
    }

    private static bool ContainsCorrelatedExists(NodeAst node)
    {
        if (node.nodeType == NodeType.ExprExistsCorrelated)
            return true;

        return (node.leftAst is not null && ContainsCorrelatedExists(node.leftAst))
            || (node.rightAst is not null && ContainsCorrelatedExists(node.rightAst))
            || (node.extendedOne is not null && ContainsCorrelatedExists(node.extendedOne))
            || (node.extendedTwo is not null && ContainsCorrelatedExists(node.extendedTwo))
            || (node.extendedThree is not null && ContainsCorrelatedExists(node.extendedThree))
            || (node.extendedFour is not null && ContainsCorrelatedExists(node.extendedFour))
            || (node.extendedFive is not null && ContainsCorrelatedExists(node.extendedFive));
    }

    /// <summary>
    /// Synchronous twin of <see cref="EvaluatePredicateAsync"/> for predicates that cannot suspend
    /// (guaranteed by <see cref="PredicateNeedsAsync"/> returning false). Mirrors it exactly — AND/OR
    /// short-circuit, prepared IN-set membership, and the ordinal-fast-path delegation to
    /// <see cref="SqlExecutor.EvalExpr"/> — but as a plain recursive call with no async machinery.
    /// </summary>
    private ColumnValue EvaluatePredicate(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        switch (expr.nodeType)
        {
            case NodeType.ExprInMembership:
            {
                if (ticket.PreparedInSets is not null && ticket.PreparedInSets.TryGetValue(expr, out PreparedInSet? prepared))
                {
                    ColumnValue lhs = row is QueryRow qrIn
                        ? SqlExecutor.EvalExpr(expr.leftAst!, qrIn, ticket.Parameters, ticket.RowNameResolver)
                        : SqlExecutor.EvalExpr(expr.leftAst!, row, ticket.Parameters, ticket.RowNameResolver);
                    return ColumnValue.FromBool(prepared.Contains(lhs));
                }
                goto default;
            }

            case NodeType.ExprAnd:
            {
                ColumnValue leftValue = EvaluatePredicate(expr.leftAst!, row, ticket);

                if (!ToPredicateResult(leftValue))
                    return ColumnValue.False;

                return EvaluatePredicate(expr.rightAst!, row, ticket);
            }

            case NodeType.ExprOr:
            {
                ColumnValue leftValue = EvaluatePredicate(expr.leftAst!, row, ticket);

                if (ToPredicateResult(leftValue))
                    return ColumnValue.True;

                return EvaluatePredicate(expr.rightAst!, row, ticket);
            }

            default:
                return row is QueryRow qr
                    ? SqlExecutor.EvalExpr(expr, qr, ticket.Parameters, ticket.RowNameResolver)
                    : SqlExecutor.EvalExpr(expr, row, ticket.Parameters, ticket.RowNameResolver);
        }
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
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        if (expr.nodeType is NodeType.ExprAnd)        
            return EvaluateHavingAndAsync(expr, row, ticket);        

        if (expr.nodeType is NodeType.ExprOr)        
            return EvaluateHavingOrAsync(expr, row, ticket);        

        return ValueTask.FromResult(QueryHavingEvaluator.Evaluate(expr, row, ticket, ticket.Parameters));
    }

    private async ValueTask<ColumnValue> EvaluateHavingAndAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue leftValue = QueryHavingEvaluator.Evaluate(expr.leftAst!, row, ticket, ticket.Parameters);

        if (!ToPredicateResult(leftValue))
            return ColumnValue.False;

        return await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
    }

    private async ValueTask<ColumnValue> EvaluateHavingOrAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue leftValue = QueryHavingEvaluator.Evaluate(expr.leftAst!, row, ticket, ticket.Parameters);

        if (ToPredicateResult(leftValue))
            return ColumnValue.True;

        return await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
    }

    private async ValueTask<ColumnValue> EvaluatePredicateAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
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

            case NodeType.ExprInMembership:
            {
                if (ticket.PreparedInSets is not null && ticket.PreparedInSets.TryGetValue(expr, out PreparedInSet? prepared))
                {
                    ColumnValue lhs = row is QueryRow qrIn
                        ? SqlExecutor.EvalExpr(expr.leftAst!, qrIn, ticket.Parameters, ticket.RowNameResolver)
                        : SqlExecutor.EvalExpr(expr.leftAst!, row, ticket.Parameters, ticket.RowNameResolver);
                    return ColumnValue.FromBool(prepared.Contains(lhs));
                }
                goto default;
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
                // Use ordinal fast path when the row is a QueryRow (emitted by the scanner
                // via RowEncoder.DecodeToQueryRowAsync). Falls back to the dictionary overload
                // for intermediate rows produced by joins or aggregation.
                return row is QueryRow qr
                    ? SqlExecutor.EvalExpr(expr, qr, ticket.Parameters, ticket.RowNameResolver)
                    : SqlExecutor.EvalExpr(expr, row, ticket.Parameters, ticket.RowNameResolver);
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
