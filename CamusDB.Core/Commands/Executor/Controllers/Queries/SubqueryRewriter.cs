
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Resolves uncorrelated scalar, IN, and NOT IN subqueries before binding and planning.
/// EXISTS subqueries are handled by <see cref="ExistsSubqueryPreparer"/>.
/// </summary>
internal sealed class SubqueryRewriter
{
    private readonly ScalarSubqueryExecutor scalarExecutor;
    private readonly InSubqueryExecutor inExecutor;
    private readonly ExistsSubqueryExecutor existsExecutor;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public SubqueryRewriter(
        ScalarSubqueryExecutor scalarExecutor,
        InSubqueryExecutor inExecutor,
        ExistsSubqueryExecutor existsExecutor)
    {
        this.scalarExecutor = scalarExecutor;
        this.inExecutor = inExecutor;
        this.existsExecutor = existsExecutor;
    }

    /// <summary>
    /// True when the expression tree contains a node this rewriter would act on. The rewrite walks
    /// recurse through left/right children only, so this scan mirrors exactly that traversal: an
    /// expression this scan clears is one the walk would have returned unchanged, node for node.
    /// Checked before rewriting so the common no-subquery predicate — every point read and point
    /// update — skips the recursive async walk, which allocated a <c>Task</c> and a state-machine
    /// box per AST node per statement execution only to return the same references.
    /// <paramref name="includeExists"/> selects the projection rewriter's wider trigger set (it also
    /// resolves EXISTS); the WHERE rewriter leaves EXISTS to <see cref="ExistsSubqueryPreparer"/>.
    /// </summary>
    private static bool ContainsSubqueryToRewrite(NodeAst expr, bool includeExists)
    {
        if (expr.nodeType is NodeType.ExprScalarSubquery or NodeType.ExprInSubquery or NodeType.ExprNotInSubquery)
            return true;

        if (includeExists && expr.nodeType is NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated)
            return true;

        if (expr.leftAst is not null && ContainsSubqueryToRewrite(expr.leftAst, includeExists))
            return true;

        return expr.rightAst is not null && ContainsSubqueryToRewrite(expr.rightAst, includeExists);
    }

    /// <summary>
    /// Rewrites an expression that appears in a projection position by pre-materializing any
    /// uncorrelated subquery it contains (scalar, <c>IN</c>/<c>NOT IN</c>, and <c>EXISTS</c>) into a
    /// literal, so the synchronous <c>SqlExecutor.EvalExpr</c> can finish the projection. Used by
    /// FROM-less <c>SELECT</c> (there is no outer row, so every projection subquery is uncorrelated).
    /// Unlike the WHERE rewriter (<see cref="RewriteSelectQueryAsync"/>), this also resolves EXISTS,
    /// because a FROM-less projection has no per-row EXISTS-preparer stage to fall back on.
    /// </summary>
    public async ValueTask<NodeAst> RewriteProjectionExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket)
    {
        // Nothing to rewrite anywhere below this node: return it unchanged without the recursive
        // walk. Running the scan at every recursion level is deliberate — a subquery-free subtree
        // exits here immediately, so the walk only descends where a rewrite can actually happen.
        if (!ContainsSubqueryToRewrite(expr, includeExists: true))
            return expr;

        switch (expr.nodeType)
        {
            case NodeType.ExprExistsSubquery:
            case NodeType.ExprExistsCorrelated:
            {
                if (expr.leftAst is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid EXISTS subquery expression");

                bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters,
                    ticket.CancellationToken).ConfigureAwait(false);

                return exists ? NodeAst.True : NodeAst.False;
            }

            case NodeType.ExprScalarSubquery:
            {
                if (expr.leftAst is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid scalar subquery expression");

                ColumnValue value = await scalarExecutor.ExecuteAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters).ConfigureAwait(false);

                return ColumnValueAstBuilder.FromColumnValue(value);
            }

            case NodeType.ExprInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, negated: false).ConfigureAwait(false);

            case NodeType.ExprNotInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, negated: true).ConfigureAwait(false);
        }

        NodeAst? left = expr.leftAst is not null
            ? await RewriteProjectionExpressionAsync(database, expr.leftAst, ticket).ConfigureAwait(false)
            : null;

        NodeAst? right = expr.rightAst is not null
            ? await RewriteProjectionExpressionAsync(database, expr.rightAst, ticket).ConfigureAwait(false)
            : null;

        if (ReferenceEquals(left, expr.leftAst) && ReferenceEquals(right, expr.rightAst))
            return expr;

        return new NodeAst(
            expr.nodeType, left, right,
            expr.extendedOne, expr.extendedTwo, expr.extendedThree,
            expr.extendedFour, expr.extendedFive, expr.yytext, expr.extendedSix);
    }

    /// <summary>
    /// Rewrites a predicate expression (a <c>WHERE</c> clause, or an <c>UPDATE ... SET</c> value)
    /// by pre-materializing any uncorrelated scalar / <c>IN</c> / <c>NOT IN</c> subquery it
    /// contains into a literal, so the synchronous <c>SqlExecutor.EvalExpr</c> can evaluate it per
    /// row. This is the same contract as the <c>WHERE</c> handling of
    /// <see cref="RewriteSelectQueryAsync"/> and is used by <c>DELETE</c>/<c>UPDATE</c>, which have
    /// no per-row EXISTS-preparer stage. <c>EXISTS</c> is intentionally left intact: materializing
    /// a correlated EXISTS as if it were uncorrelated (as the projection rewriter does for FROM-less
    /// SELECT) would silently return wrong rows, so it instead surfaces the explicit
    /// "must be resolved" guard. Returns the same node reference when nothing was rewritten.
    /// </summary>
    public ValueTask<NodeAst> RewriteWhereExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket)
        => ContainsSubqueryToRewrite(expr, includeExists: false)
            ? new ValueTask<NodeAst>(RewriteExpressionAsync(database, expr, ticket))
            : new ValueTask<NodeAst>(expr);

    public async ValueTask<SelectQuery> RewriteSelectQueryAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        BoundPredicate? where = query.Where;

        // No subquery in the WHERE (or no WHERE): the walk would return the same expression, so the
        // query record itself is also unchanged — skip the walk and the record copy entirely.
        if (where is null || !ContainsSubqueryToRewrite(where.Expression, includeExists: false))
            return query;

        NodeAst rewritten = await RewriteExpressionAsync(database, where.Expression, ticket).ConfigureAwait(false);
        return query with { Where = new BoundPredicate(rewritten) };
    }

    private async Task<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket)
    {
        if (expr.nodeType == NodeType.ExprScalarSubquery)
        {
            if (expr.leftAst is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Invalid scalar subquery expression");
            }

            ColumnValue value = await scalarExecutor.ExecuteAsync(
                database,
                expr.leftAst,
                ticket.TxnState,
                ticket.Parameters).ConfigureAwait(false);

            return ColumnValueAstBuilder.FromColumnValue(value);
        }

        if (expr.nodeType == NodeType.ExprInSubquery)
        {
            return await RewriteInSubqueryAsync(database, expr, ticket, negated: false).ConfigureAwait(false);
        }

        if (expr.nodeType == NodeType.ExprNotInSubquery)
        {
            return await RewriteInSubqueryAsync(database, expr, ticket, negated: true).ConfigureAwait(false);
        }

        NodeAst? left = expr.leftAst is not null
            ? await RewriteExpressionAsync(database, expr.leftAst, ticket).ConfigureAwait(false)
            : null;

        NodeAst? right = expr.rightAst is not null
            ? await RewriteExpressionAsync(database, expr.rightAst, ticket).ConfigureAwait(false)
            : null;

        if (ReferenceEquals(left, expr.leftAst) && ReferenceEquals(right, expr.rightAst))
            return expr;

        return new NodeAst(
            expr.nodeType,
            left,
            right,
            expr.extendedOne,
            expr.extendedTwo,
            expr.extendedThree,
            expr.extendedFour,
            expr.extendedFive,
            expr.yytext);
    }

    private async Task<NodeAst> RewriteInSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        bool negated)
    {
        if (expr.leftAst is null || expr.rightAst is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                negated ? "Invalid NOT IN subquery expression" : "Invalid IN subquery expression");
        }

        InSubqueryAnalyzer.EnsureUncorrelated(expr.rightAst, selectQueryCreator);

        NodeAst lhs = await RewriteExpressionAsync(database, expr.leftAst, ticket).ConfigureAwait(false);

        await using InSubqueryMaterialization materialization = await inExecutor.MaterializeAsync(
            database,
            expr.rightAst,
            ticket.TxnState,
            ticket.Parameters,
            ticket.CancellationToken).ConfigureAwait(false);

        return negated
            ? await SubqueryValueListAst.BuildNotInMembershipAsync(lhs, materialization).ConfigureAwait(false)
            : await SubqueryValueListAst.BuildInMembershipAsync(lhs, materialization).ConfigureAwait(false);
    }
}
