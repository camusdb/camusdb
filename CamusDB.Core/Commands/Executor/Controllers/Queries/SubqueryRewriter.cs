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
/// Replaces every uncorrelated subquery of an expression with the value it returns, before the
/// expression is bound and evaluated: a scalar subquery becomes a literal, <c>IN</c> / <c>NOT IN</c>
/// and a quantified comparison become a membership test over the rows the inner query returned, and
/// an <c>EXISTS</c> becomes <c>true</c> or <c>false</c>. The per-row evaluator is synchronous and
/// cannot run a query, so a subquery node that reaches it is an engine defect
/// (<see cref="CamusDBErrorCodes.InvalidInternalOperation"/>); this rewriter is what keeps that from
/// happening in every expression position: a SELECT's WHERE, select list, HAVING and JOIN ON
/// predicates, the WHERE and SET values of UPDATE and DELETE, INSERT VALUES, and ON CONFLICT DO
/// UPDATE.
///
/// <para>The one subquery that is evaluated per row is a <b>correlated</b> <c>EXISTS</c>, and only in
/// the WHERE of a SELECT: there the rewriter leaves <c>EXISTS</c> alone and
/// <see cref="ExistsSubqueryPreparer"/> binds it against the outer sources. In every other position
/// a correlated <c>EXISTS</c> is refused with <see cref="CamusDBErrorCodes.FeatureNotSupported"/>,
/// because folding it as if it were uncorrelated would return wrong rows. A correlated <c>IN</c> is
/// refused the same way by <see cref="InSubqueryAnalyzer"/>.</para>
///
/// <para>The walk visits every child slot of a node, not only the left and right ones: a CASE keeps
/// its ELSE result in <c>extendedOne</c> and a BETWEEN keeps its bounds in <c>extendedOne</c> and
/// <c>extendedTwo</c>, and a subquery there was once missed and reached the evaluator.</para>
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
    /// True when the expression tree contains a node this rewriter would act on. Checked before
    /// rewriting so the common no-subquery expression — every point read and point update — skips the
    /// recursive async walk, which allocated a <c>Task</c> and a state-machine box per AST node per
    /// statement execution only to return the same references. The scan is iterative
    /// (<see cref="SubqueryNodeScan"/>): it also runs over the rows of a multi-row INSERT, whose
    /// depth the parser does not bound. <paramref name="includeExists"/> is false for the WHERE of a
    /// SELECT, where <c>EXISTS</c> belongs to <see cref="ExistsSubqueryPreparer"/>.
    /// </summary>
    private static bool ContainsSubqueryToRewrite(NodeAst expr, bool includeExists) =>
        SubqueryNodeScan.Contains(expr, includeExists ? SubqueryNodeKinds.Rewritable | SubqueryNodeKinds.Exists : SubqueryNodeKinds.Rewritable);

    /// <summary>
    /// Replaces every uncorrelated subquery of <paramref name="expr"/>, <c>EXISTS</c> included, with
    /// its value. Used for every expression position that has no per-row EXISTS stage: a FROM-less
    /// select list, the WHERE and SET values of UPDATE and DELETE, INSERT VALUES, and the select
    /// list, HAVING and JOIN ON of a SELECT. A correlated <c>EXISTS</c> is refused with
    /// <see cref="CamusDBErrorCodes.FeatureNotSupported"/>, and <paramref name="position"/> names
    /// the clause in that message ("the select list", "the WHERE of a DELETE"). Returns the same node
    /// reference when nothing was rewritten.
    /// </summary>
    public ValueTask<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string position)
        => ContainsSubqueryToRewrite(expr, includeExists: true)
            ? new ValueTask<NodeAst>(RewriteCoreAsync(database, expr, ticket, position))
            : new ValueTask<NodeAst>(expr);

    /// <summary>
    /// Rewrites the subqueries of one SELECT. Its WHERE keeps <c>EXISTS</c> for
    /// <see cref="ExistsSubqueryPreparer"/>, which runs after binding and handles the correlated form.
    /// Its select list, HAVING and JOIN ON predicates have no per-row EXISTS stage, so every subquery
    /// there is folded now, and a correlated <c>EXISTS</c> is refused. The derived tables of the
    /// source tree are not entered: each nested SELECT runs this stage for itself when the binder
    /// binds it through the pipeline. Returns the same query instance when nothing was rewritten;
    /// callers that cache a binding rely on that.
    /// </summary>
    public async ValueTask<SelectQuery> RewriteSelectQueryAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        BoundPredicate? where = query.Where;
        if (where is not null && ContainsSubqueryToRewrite(where.Expression, includeExists: false))
        {
            NodeAst rewritten = await RewriteCoreAsync(database, where.Expression, ticket, position: null).ConfigureAwait(false);
            where = new BoundPredicate(rewritten);
        }

        IReadOnlyList<ProjectionItem> projections = await RewriteProjectionsAsync(database, query.Projections, ticket).ConfigureAwait(false);

        BoundPredicate? having = query.Having;
        if (having is not null && ContainsSubqueryToRewrite(having.Expression, includeExists: true))
        {
            NodeAst rewritten = await RewriteCoreAsync(database, having.Expression, ticket, "HAVING").ConfigureAwait(false);
            having = new BoundPredicate(rewritten);
        }

        QuerySource source = await RewriteJoinPredicatesAsync(database, query.Source, ticket).ConfigureAwait(false);

        if (ReferenceEquals(where, query.Where)
            && ReferenceEquals(projections, query.Projections)
            && ReferenceEquals(having, query.Having)
            && ReferenceEquals(source, query.Source))
        {
            return query;
        }

        return query with { Where = where, Projections = projections, Having = having, Source = source };
    }

    /// <summary>
    /// Rewrites the subqueries of the select list. A projection subquery runs once for the whole
    /// statement, not once per row, which is exactly the meaning of an uncorrelated subquery. The
    /// list instance is kept when no item changed.
    /// </summary>
    private async ValueTask<IReadOnlyList<ProjectionItem>> RewriteProjectionsAsync(
        DatabaseDescriptor database,
        IReadOnlyList<ProjectionItem> projections,
        ExecuteSQLTicket ticket)
    {
        List<ProjectionItem>? rewritten = null;

        for (int i = 0; i < projections.Count; i++)
        {
            ProjectionItem item = projections[i];

            if (!ContainsSubqueryToRewrite(item.Expression, includeExists: true))
            {
                rewritten?.Add(item);
                continue;
            }

            NodeAst expression = await RewriteCoreAsync(database, item.Expression, ticket, "the select list").ConfigureAwait(false);

            if (rewritten is null)
            {
                rewritten = new List<ProjectionItem>(projections.Count);
                for (int j = 0; j < i; j++)
                    rewritten.Add(projections[j]);
            }

            rewritten.Add(item with { Expression = expression });
        }

        return rewritten ?? projections;
    }

    /// <summary>
    /// Rewrites the subqueries of every JOIN ON predicate of a source tree. A join tree is left-deep
    /// and at most as deep as the number of tables, so the recursion is bounded. A derived table is
    /// a leaf here: its inner SELECT is rewritten by its own pass through the pipeline.
    /// </summary>
    private async ValueTask<QuerySource> RewriteJoinPredicatesAsync(
        DatabaseDescriptor database,
        QuerySource source,
        ExecuteSQLTicket ticket)
    {
        if (source is not JoinSource join)
            return source;

        QuerySource left = await RewriteJoinPredicatesAsync(database, join.Left, ticket).ConfigureAwait(false);
        QuerySource right = await RewriteJoinPredicatesAsync(database, join.Right, ticket).ConfigureAwait(false);
        NodeAst on = await RewriteExpressionAsync(database, join.OnPredicate, ticket, "a JOIN ON clause").ConfigureAwait(false);

        if (ReferenceEquals(left, join.Left) && ReferenceEquals(right, join.Right) && ReferenceEquals(on, join.OnPredicate))
            return join;

        return join with { Left = left, Right = right, OnPredicate = on };
    }

    /// <summary>
    /// The recursive walk. <paramref name="position"/> is null for the WHERE of a SELECT, where an
    /// <c>EXISTS</c> is left for the preparer; otherwise it names the clause, an uncorrelated
    /// <c>EXISTS</c> is folded and a correlated one is refused.
    /// </summary>
    private async Task<NodeAst> RewriteCoreAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string? position)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await Task.Run(() => RewriteCoreAsync(database, expr, ticket, position)).ConfigureAwait(false);

        switch (expr.nodeType)
        {
            case NodeType.ExprScalarSubquery:
            {
                if (expr.leftAst is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid scalar subquery expression");

                ColumnValue value = await scalarExecutor.ExecuteAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters).ConfigureAwait(false);

                return ColumnValueAstBuilder.FromColumnValue(value);
            }

            case NodeType.ExprInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, negated: false).ConfigureAwait(false);

            case NodeType.ExprNotInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, negated: true).ConfigureAwait(false);

            case NodeType.ExprQuantifiedComparison when expr.rightAst?.nodeType == NodeType.ExprScalarSubquery:
                return await RewriteQuantifiedSubqueryAsync(database, expr, ticket, position).ConfigureAwait(false);

            case NodeType.ExprExistsSubquery when position is not null:
                return await RewriteExistsAsync(database, expr, ticket, position).ConfigureAwait(false);
        }

        NodeAst? left = expr.leftAst is not null
            ? await RewriteCoreAsync(database, expr.leftAst, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? right = expr.rightAst is not null
            ? await RewriteCoreAsync(database, expr.rightAst, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? one = expr.extendedOne is not null
            ? await RewriteCoreAsync(database, expr.extendedOne, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? two = expr.extendedTwo is not null
            ? await RewriteCoreAsync(database, expr.extendedTwo, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? three = expr.extendedThree is not null
            ? await RewriteCoreAsync(database, expr.extendedThree, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? four = expr.extendedFour is not null
            ? await RewriteCoreAsync(database, expr.extendedFour, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? five = expr.extendedFive is not null
            ? await RewriteCoreAsync(database, expr.extendedFive, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? six = expr.extendedSix is not null
            ? await RewriteCoreAsync(database, expr.extendedSix, ticket, position).ConfigureAwait(false)
            : null;

        NodeAst? seven = expr.extendedSeven is not null
            ? await RewriteCoreAsync(database, expr.extendedSeven, ticket, position).ConfigureAwait(false)
            : null;

        if (ReferenceEquals(left, expr.leftAst)
            && ReferenceEquals(right, expr.rightAst)
            && ReferenceEquals(one, expr.extendedOne)
            && ReferenceEquals(two, expr.extendedTwo)
            && ReferenceEquals(three, expr.extendedThree)
            && ReferenceEquals(four, expr.extendedFour)
            && ReferenceEquals(five, expr.extendedFive)
            && ReferenceEquals(six, expr.extendedSix)
            && ReferenceEquals(seven, expr.extendedSeven))
        {
            return expr;
        }

        return new NodeAst(expr.nodeType, left, right, one, two, three, four, five, expr.yytext, six, seven);
    }

    /// <summary>
    /// Folds an <c>EXISTS</c> that sits outside the WHERE of a SELECT. Only the uncorrelated form can
    /// be folded: it runs once and its answer holds for every row. A correlated form would need the
    /// outer row, which this position does not have, so it is refused rather than answered wrongly.
    /// </summary>
    private async Task<NodeAst> RewriteExistsAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string position)
    {
        if (expr.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid EXISTS subquery expression");

        SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(expr.leftAst);

        if (ExistsSubqueryAnalyzer.IsCorrelated(innerQuery))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"A correlated EXISTS subquery is not supported in {position}; it is supported in the WHERE clause of a SELECT");
        }

        bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
            database, expr.leftAst, ticket.TxnState, ticket.Parameters, ticket.CancellationToken).ConfigureAwait(false);

        return exists ? NodeAst.True : NodeAst.False;
    }

    private async Task<NodeAst> RewriteInSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string? position,
        bool negated)
    {
        if (expr.leftAst is null || expr.rightAst is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                negated ? "Invalid NOT IN subquery expression" : "Invalid IN subquery expression");
        }

        InSubqueryAnalyzer.EnsureUncorrelated(expr.rightAst, selectQueryCreator);

        // The tested value is rewritten with the rule of its enclosing clause, so an EXISTS on the left
        // of an IN keeps the treatment of its position.
        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position).ConfigureAwait(false);

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

    /// <summary>
    /// Replaces the subquery of an ordered quantified comparison (<c>x &lt; ANY (SELECT …)</c>,
    /// <c>x = ALL (SELECT …)</c>, …) with the <c>ARRAY[…]</c> of the rows it returned, so the
    /// synchronous per-row evaluator can fold over it. The operator marker and the quantifier word
    /// are carried over unchanged; only the right operand changes.
    ///
    /// <para>The subquery must be uncorrelated, for the reason <c>IN</c> requires it: it is executed
    /// once, before the outer scan starts, so a reference to an outer column has no value to read.
    /// The same analyzer reports it, so both forms reject the same statements.</para>
    /// </summary>
    private async Task<NodeAst> RewriteQuantifiedSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string? position)
    {
        NodeAst? select = expr.rightAst?.leftAst;

        if (expr.leftAst is null || select is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Invalid quantified comparison subquery expression");
        }

        InSubqueryAnalyzer.EnsureUncorrelated(select, selectQueryCreator);

        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position).ConfigureAwait(false);

        await using InSubqueryMaterialization materialization = await inExecutor.MaterializeAsync(
            database,
            select,
            ticket.TxnState,
            ticket.Parameters,
            ticket.CancellationToken).ConfigureAwait(false);

        NodeAst elements = await SubqueryValueListAst.BuildValueSetAsync(
            materialization,
            ticket.CancellationToken).ConfigureAwait(false);

        return new NodeAst(
            expr.nodeType,
            lhs,
            elements,
            expr.extendedOne,
            expr.extendedTwo,
            expr.extendedThree,
            expr.extendedFour,
            expr.extendedFive,
            expr.yytext,
            expr.extendedSix,
            expr.extendedSeven);
    }
}
