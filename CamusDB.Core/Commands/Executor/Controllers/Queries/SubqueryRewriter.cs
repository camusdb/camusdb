
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
    /// Rewrites an expression that appears in a projection position by pre-materializing any
    /// uncorrelated subquery it contains (scalar, <c>IN</c>/<c>NOT IN</c>, and <c>EXISTS</c>) into a
    /// literal, so the synchronous <c>SqlExecutor.EvalExpr</c> can finish the projection. Used by
    /// FROM-less <c>SELECT</c> (there is no outer row, so every projection subquery is uncorrelated).
    /// Unlike the WHERE rewriter (<see cref="RewriteSelectQueryAsync"/>), this also resolves EXISTS,
    /// because a FROM-less projection has no per-row EXISTS-preparer stage to fall back on.
    /// </summary>
    public async Task<NodeAst> RewriteProjectionExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket)
    {
        switch (expr.nodeType)
        {
            case NodeType.ExprExistsSubquery:
            case NodeType.ExprExistsCorrelated:
            {
                if (expr.leftAst is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid EXISTS subquery expression");

                bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters).ConfigureAwait(false);

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

    public async Task<SelectQuery> RewriteSelectQueryAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        BoundPredicate? where = query.Where;

        if (where is not null)
        {
            NodeAst rewritten = await RewriteExpressionAsync(database, where.Expression, ticket).ConfigureAwait(false);
            where = new BoundPredicate(rewritten);
        }

        return query with { Where = where };
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
            ticket.Parameters).ConfigureAwait(false);

        return negated
            ? await SubqueryValueListAst.BuildNotInMembershipAsync(lhs, materialization).ConfigureAwait(false)
            : await SubqueryValueListAst.BuildInMembershipAsync(lhs, materialization).ConfigureAwait(false);
    }
}
