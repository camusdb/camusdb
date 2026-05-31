
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
/// Resolves uncorrelated scalar, IN, and NOT IN subqueries before binding and planning (QP5.2–QP5.3a).
/// EXISTS subqueries are handled by <see cref="ExistsSubqueryPreparer"/> (QP5.4).
/// </summary>
internal sealed class SubqueryRewriter
{
    private readonly ScalarSubqueryExecutor scalarExecutor;
    private readonly InSubqueryExecutor inExecutor;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public SubqueryRewriter(ScalarSubqueryExecutor scalarExecutor, InSubqueryExecutor inExecutor)
    {
        this.scalarExecutor = scalarExecutor;
        this.inExecutor = inExecutor;
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

        InSubqueryMaterialization materialization = await inExecutor.MaterializeAsync(
            database,
            expr.rightAst,
            ticket.TxnState,
            ticket.Parameters).ConfigureAwait(false);

        return negated
            ? SubqueryValueListAst.BuildNotInMembership(lhs, materialization)
            : SubqueryValueListAst.BuildInMembership(lhs, materialization);
    }
}
