
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
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
            return new ValueTask<bool>(ToPredicateResult(EvaluatePredicate(filter, row, plan.Ticket), "WHERE"));

        return MeetWhereAsyncCore(filter, row, plan.Ticket, plan.Database);
    }

    internal async ValueTask<bool> MeetHavingAsync(
        NodeAst having,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue evaluatedExpr = await EvaluateHavingAsync(having, row, ticket).ConfigureAwait(false);
        return ToPredicateResult(evaluatedExpr, "HAVING");
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
            return new ValueTask<bool>(ToPredicateResult(EvaluatePredicate(where, row, ticket), "WHERE or ON"));

        return MeetWhereAsyncCore(where, row, ticket, database);
    }

    private async ValueTask<bool> MeetWhereAsyncCore(
        NodeAst where,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        ColumnValue evaluatedExpr = await EvaluatePredicateAsync(where, row, ticket, database).ConfigureAwait(false);

        return ToPredicateResult(evaluatedExpr, "WHERE or ON");
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

    private static bool ContainsCorrelatedExists(NodeAst node) =>
        SubqueryNodeScan.Contains(node, SubqueryNodeKinds.ExistsCorrelated);

    /// <summary>
    /// Synchronous twin of <see cref="EvaluatePredicateAsync"/> for predicates that cannot suspend
    /// (guaranteed by <see cref="PredicateNeedsAsync"/> returning false). Mirrors it exactly — AND/OR
    /// short-circuit, NOT negation, prepared IN-set membership, and the ordinal-fast-path delegation
    /// to <see cref="SqlExecutor.EvalExpr"/> — but as a plain recursive call with no async machinery.
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
                    return prepared.Evaluate(lhs);
                }
                goto default;
            }

            case NodeType.ExprAnd:
            {
                // Three-valued, exactly as the async twin: false short-circuits, UNKNOWN propagates.
                ColumnValue leftValue = EvaluatePredicate(expr.leftAst!, row, ticket);

                if (leftValue.Type == ColumnType.Bool && !leftValue.BoolValue)
                    return ColumnValue.False;

                return SQLExecutorBaseCreator.EvalAnd(leftValue, EvaluatePredicate(expr.rightAst!, row, ticket));
            }

            case NodeType.ExprOr:
            {
                ColumnValue leftValue = EvaluatePredicate(expr.leftAst!, row, ticket);

                if (leftValue.Type == ColumnType.Bool && leftValue.BoolValue)
                    return ColumnValue.True;

                return SQLExecutorBaseCreator.EvalOr(leftValue, EvaluatePredicate(expr.rightAst!, row, ticket));
            }

            case NodeType.ExprNot:
            {
                ColumnValue value = EvaluatePredicate(expr.leftAst!, row, ticket);

                return NegatePredicateValue(value);
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
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return new ValueTask<ColumnValue>(EvaluateHavingOnFreshStack(expr, row, ticket));

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

        if (leftValue.Type == ColumnType.Bool && !leftValue.BoolValue)
            return ColumnValue.False;

        ColumnValue rightValue = await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
        return SQLExecutorBaseCreator.EvalAnd(leftValue, rightValue);
    }

    private async ValueTask<ColumnValue> EvaluateHavingOrAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket)
    {
        ColumnValue leftValue = QueryHavingEvaluator.Evaluate(expr.leftAst!, row, ticket, ticket.Parameters);

        if (leftValue.Type == ColumnType.Bool && leftValue.BoolValue)
            return ColumnValue.True;

        ColumnValue rightValue = await EvaluateHavingAsync(expr.rightAst!, row, ticket).ConfigureAwait(false);
        return SQLExecutorBaseCreator.EvalOr(leftValue, rightValue);
    }

    /// <summary>Runs <see cref="EvaluateHavingAsync"/> on a fresh thread-pool stack.</summary>
    private Task<ColumnValue> EvaluateHavingOnFreshStack(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket) =>
        Task.Run(() => EvaluateHavingAsync(expr, row, ticket).AsTask());

    /// <summary>Runs <see cref="EvaluatePredicateAsync"/> on a fresh thread-pool stack.</summary>
    private Task<ColumnValue> EvaluatePredicateOnFreshStack(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database) =>
        Task.Run(() => EvaluatePredicateAsync(expr, row, ticket, database).AsTask());

    private async ValueTask<ColumnValue> EvaluatePredicateAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await EvaluatePredicateOnFreshStack(expr, row, ticket, database).ConfigureAwait(false);

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
                    ticket.Parameters,
                    ticket.CancellationToken).ConfigureAwait(false);

                return ColumnValue.FromBool(exists);
            }

            case NodeType.ExprInMembership:
            {
                if (ticket.PreparedInSets is not null && ticket.PreparedInSets.TryGetValue(expr, out PreparedInSet? prepared))
                {
                    ColumnValue lhs = row is QueryRow qrIn
                        ? SqlExecutor.EvalExpr(expr.leftAst!, qrIn, ticket.Parameters, ticket.RowNameResolver)
                        : SqlExecutor.EvalExpr(expr.leftAst!, row, ticket.Parameters, ticket.RowNameResolver);
                    return prepared.Evaluate(lhs);
                }
                goto default;
            }

            case NodeType.ExprAnd:
            {
                // Three-valued, matching SQLExecutorBaseCreator.EvalAnd: a false side decides without visiting the
                // other (short-circuit keeps a correlated EXISTS on the right unexecuted), otherwise an
                // UNKNOWN side makes the conjunction UNKNOWN so NOT (NULL AND TRUE) stays UNKNOWN.
                ColumnValue leftValue = await EvaluatePredicateAsync(expr.leftAst!, row, ticket, database).ConfigureAwait(false);

                if (leftValue.Type == ColumnType.Bool && !leftValue.BoolValue)
                    return ColumnValue.False;

                ColumnValue rightValue = await EvaluatePredicateAsync(expr.rightAst!, row, ticket, database).ConfigureAwait(false);

                return SQLExecutorBaseCreator.EvalAnd(leftValue, rightValue);
            }

            case NodeType.ExprOr:
            {
                ColumnValue leftValue = await EvaluatePredicateAsync(expr.leftAst!, row, ticket, database).ConfigureAwait(false);

                if (leftValue.Type == ColumnType.Bool && leftValue.BoolValue)
                    return ColumnValue.True;

                ColumnValue rightValue = await EvaluatePredicateAsync(expr.rightAst!, row, ticket, database).ConfigureAwait(false);

                return SQLExecutorBaseCreator.EvalOr(leftValue, rightValue);
            }

            case NodeType.ExprNot:
            {
                // Recurse through the filterer rather than delegating the whole subtree to EvalExpr,
                // so a correlated EXISTS under the NOT is executed against its prepared registry
                // entry instead of hitting EvalExpr's "must be evaluated by the query filter" throw.
                ColumnValue value = await EvaluatePredicateAsync(expr.leftAst!, row, ticket, database).ConfigureAwait(false);

                return NegatePredicateValue(value);
            }

            default:
                // A correlated EXISTS below a node that is not AND, OR or NOT (a CASE branch, a
                // comparison operand, an IS NULL test, a BETWEEN bound) cannot be run by the
                // synchronous evaluator. Each one is answered for this row first, and the answers
                // take their places in a copy of the subtree the evaluator then sees.
                if (PredicateNeedsAsync(expr))
                    expr = await ResolveCorrelatedExistsAsync(expr, row, ticket, database).ConfigureAwait(false);

                // Use ordinal fast path when the row is a QueryRow (emitted by the scanner
                // via RowEncoder.DecodeToQueryRowAsync). Falls back to the dictionary overload
                // for intermediate rows produced by joins or aggregation.
                return row is QueryRow qr
                    ? SqlExecutor.EvalExpr(expr, qr, ticket.Parameters, ticket.RowNameResolver)
                    : SqlExecutor.EvalExpr(expr, row, ticket.Parameters, ticket.RowNameResolver);
        }
    }

    /// <summary>
    /// Returns <paramref name="expr"/> with every correlated EXISTS below it replaced by the literal
    /// it evaluates to for <paramref name="row"/>. Only the nodes on a path to a correlated EXISTS are
    /// copied; a subtree without one is shared with the original predicate, so the copy costs a few
    /// nodes per row, and only for the rare predicate that puts an EXISTS under a non-boolean node.
    /// The original AST is never modified: it is shared across executions of the same SQL text.
    /// </summary>
    private async ValueTask<NodeAst> ResolveCorrelatedExistsAsync(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await Task.Run(() => ResolveCorrelatedExistsAsync(expr, row, ticket, database).AsTask()).ConfigureAwait(false);

        if (expr.nodeType == NodeType.ExprExistsCorrelated)
        {
            ColumnValue exists = await EvaluatePredicateAsync(expr, row, ticket, database).ConfigureAwait(false);
            return exists.BoolValue ? NodeAst.True : NodeAst.False;
        }

        if (!PredicateNeedsAsync(expr))
            return expr;

        NodeAst? left = await ResolveChildAsync(expr.leftAst, row, ticket, database).ConfigureAwait(false);
        NodeAst? right = await ResolveChildAsync(expr.rightAst, row, ticket, database).ConfigureAwait(false);
        NodeAst? one = await ResolveChildAsync(expr.extendedOne, row, ticket, database).ConfigureAwait(false);
        NodeAst? two = await ResolveChildAsync(expr.extendedTwo, row, ticket, database).ConfigureAwait(false);
        NodeAst? three = await ResolveChildAsync(expr.extendedThree, row, ticket, database).ConfigureAwait(false);
        NodeAst? four = await ResolveChildAsync(expr.extendedFour, row, ticket, database).ConfigureAwait(false);
        NodeAst? five = await ResolveChildAsync(expr.extendedFive, row, ticket, database).ConfigureAwait(false);

        return new NodeAst(expr.nodeType, left, right, one, two, three, four, five, expr.yytext, expr.extendedSix, expr.extendedSeven);
    }

    private ValueTask<NodeAst?> ResolveChildAsync(
        NodeAst? child,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
    {
        if (child is null || !PredicateNeedsAsync(child))
            return new ValueTask<NodeAst?>(child);

        return ResolveChildCoreAsync(child, row, ticket, database);
    }

    private async ValueTask<NodeAst?> ResolveChildCoreAsync(
        NodeAst child,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database)
        => await ResolveCorrelatedExistsAsync(child, row, ticket, database).ConfigureAwait(false);

    /// <summary>
    /// Negates a predicate operand using SQL three-valued logic, matching
    /// <see cref="SqlExecutor.EvalExpr"/>'s <c>ExprNot</c> case exactly: NOT NULL is NULL (unknown,
    /// which <see cref="ToPredicateResult"/> treats as non-matching), NOT of a boolean is its
    /// complement, and any other operand type is a user error rather than a truthiness coercion —
    /// so <c>NOT &lt;non-boolean&gt;</c> fails identically whether or not the enclosing predicate
    /// happens to contain a correlated EXISTS (which is what decides between the async walker here
    /// and the synchronous delegation to <see cref="SqlExecutor.EvalExpr"/>).
    /// </summary>
    private static ColumnValue NegatePredicateValue(ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (value.Type != ColumnType.Bool)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator NOT for argument type: {value.Type}");
        }

        return ColumnValue.FromBool(!value.BoolValue);
    }

    /// <summary>
    /// Decides whether a row passes a condition: TRUE keeps it, FALSE and NULL drop it. Any other type is
    /// an error and never a truth value, as in PostgreSQL, because <c>UPDATE t SET a = 0 WHERE 1</c> would
    /// otherwise write every row, and <c>WHERE 'x'</c> would return nothing with no error. The binding
    /// refuses a condition whose type the statement shows
    /// (<see cref="ConditionalResultTypes.CheckCondition"/>), so this check is for a condition whose type
    /// only its value shows: a parameter or a function call.
    /// </summary>
    /// <param name="clause">The clause name the error uses.</param>
    internal static bool ToPredicateResult(ColumnValue evaluatedExpr, string clause) =>
        evaluatedExpr.Type switch
        {
            ColumnType.Bool => evaluatedExpr.BoolValue,
            ColumnType.Null => false,
            _ => throw StaticOperandRules.ConditionNotBoolean(clause, evaluatedExpr.Type.ToString()),
        };
}
