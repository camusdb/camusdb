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
/// because folding it as if it were uncorrelated would return wrong rows. A correlated scalar,
/// <c>IN</c> or quantified subquery is refused the same way, in every position.</para>
///
/// <para>A subquery is correlated when <see cref="SubqueryScopeAnalyzer"/> finds a name in it that
/// resolves to the query around it: the SELECT whose clauses are rewritten, or the table of an UPDATE or
/// DELETE. A name resolves to the nearest query first, so a column of the subquery's own FROM never
/// makes it correlated, and a bare name that only the outer query has does.</para>
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

    private readonly SubqueryScopeAnalyzer scopeAnalyzer;

    private readonly SelectQueryCreator selectQueryCreator = new();

    public SubqueryRewriter(
        ScalarSubqueryExecutor scalarExecutor,
        InSubqueryExecutor inExecutor,
        ExistsSubqueryExecutor existsExecutor,
        SubqueryScopeAnalyzer scopeAnalyzer)
    {
        this.scalarExecutor = scalarExecutor;
        this.inExecutor = inExecutor;
        this.existsExecutor = existsExecutor;
        this.scopeAnalyzer = scopeAnalyzer;
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
    /// reference when nothing was rewritten. <paramref name="outerSource"/> is the table that a subquery
    /// of the expression can read, such as the table of an UPDATE or DELETE, or null when there is none.
    /// </summary>
    public ValueTask<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string position,
        QuerySource? outerSource = null)
        => ContainsSubqueryToRewrite(expr, includeExists: true)
            ? new ValueTask<NodeAst>(RewriteCoreAsync(database, expr, ticket, position, ScopeOf(database, outerSource)))
            : new ValueTask<NodeAst>(expr);

    /// <summary>The names a subquery of a clause over <paramref name="source"/> can read.</summary>
    private OuterNameScope ScopeOf(DatabaseDescriptor database, QuerySource? source) =>
        source is null ? OuterNameScope.None : scopeAnalyzer.DeferredScope(database, source);

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
        // The FROM of this query is what its subqueries can read. It binds for its names only when a
        // subquery has a name that its own FROM does not have.
        OuterNameScope outer = scopeAnalyzer.DeferredScope(database, query.Source);

        BoundPredicate? where = query.Where;
        if (where is not null && ContainsSubqueryToRewrite(where.Expression, includeExists: false))
        {
            NodeAst rewritten = await RewriteCoreAsync(database, where.Expression, ticket, position: null, outer).ConfigureAwait(false);
            where = new BoundPredicate(rewritten);
        }

        IReadOnlyList<ProjectionItem> projections = await RewriteProjectionsAsync(database, query.Projections, ticket, outer).ConfigureAwait(false);

        BoundPredicate? having = query.Having;
        if (having is not null && ContainsSubqueryToRewrite(having.Expression, includeExists: true))
        {
            NodeAst rewritten = await RewriteCoreAsync(database, having.Expression, ticket, "HAVING", outer).ConfigureAwait(false);
            having = new BoundPredicate(rewritten);
        }

        QuerySource source = await RewriteJoinPredicatesAsync(database, query.Source, ticket, outer).ConfigureAwait(false);

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
        ExecuteSQLTicket ticket,
        OuterNameScope outer)
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

            NodeAst expression = await RewriteCoreAsync(database, item.Expression, ticket, "the select list", outer).ConfigureAwait(false);

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
        ExecuteSQLTicket ticket,
        OuterNameScope outer)
    {
        if (source is not JoinSource join)
            return source;

        QuerySource left = await RewriteJoinPredicatesAsync(database, join.Left, ticket, outer).ConfigureAwait(false);

        QuerySource right = await RewriteJoinPredicatesAsync(database, join.Right, ticket, outer).ConfigureAwait(false);

        NodeAst on = ContainsSubqueryToRewrite(join.OnPredicate, includeExists: true)
            ? await RewriteCoreAsync(database, join.OnPredicate, ticket, "a JOIN ON clause", outer).ConfigureAwait(false)
            : join.OnPredicate;

        if (ReferenceEquals(left, join.Left) && ReferenceEquals(right, join.Right) && ReferenceEquals(on, join.OnPredicate))
            return join;

        return join with { Left = left, Right = right, OnPredicate = on };
    }

    /// <summary>
    /// The recursive walk. <paramref name="position"/> is null for the WHERE of a SELECT, where an
    /// <c>EXISTS</c> is left for the preparer; otherwise it names the clause, an uncorrelated
    /// <c>EXISTS</c> is folded and a correlated one is refused. <paramref name="outer"/> holds the
    /// names that a subquery of the expression can read.
    /// </summary>
    private async Task<NodeAst> RewriteCoreAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string? position,
        OuterNameScope outer)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await Task.Run(() => RewriteCoreAsync(database, expr, ticket, position, outer)).ConfigureAwait(false);

        switch (expr.nodeType)
        {
            case NodeType.ExprScalarSubquery:
            {
                if (expr.leftAst is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid scalar subquery expression");

                await EnsureUncorrelatedAsync(database, expr.leftAst, outer, "A correlated scalar subquery").ConfigureAwait(false);

                SubqueryOutputType outputType = new();

                ColumnValue value = await scalarExecutor.ExecuteAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters, outputType).ConfigureAwait(false);

                return value.Type == ColumnType.Null
                    ? SubqueryResultTypes.NullOf(outputType.Type)
                    : ColumnValueAstBuilder.FromColumnValue(value);
            }

            case NodeType.ExprInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, outer, negated: false).ConfigureAwait(false);

            case NodeType.ExprNotInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, outer, negated: true).ConfigureAwait(false);

            case NodeType.ExprQuantifiedComparison when expr.rightAst?.nodeType == NodeType.ExprScalarSubquery:
                return await RewriteQuantifiedSubqueryAsync(database, expr, ticket, position, outer).ConfigureAwait(false);

            case NodeType.ExprExistsSubquery when position is not null:
                return await RewriteExistsAsync(database, expr, ticket, position, outer).ConfigureAwait(false);
        }

        NodeAst? left = expr.leftAst is not null
            ? await RewriteCoreAsync(database, expr.leftAst, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? right = expr.rightAst is not null
            ? await RewriteCoreAsync(database, expr.rightAst, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? one = expr.extendedOne is not null
            ? await RewriteCoreAsync(database, expr.extendedOne, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? two = expr.extendedTwo is not null
            ? await RewriteCoreAsync(database, expr.extendedTwo, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? three = expr.extendedThree is not null
            ? await RewriteCoreAsync(database, expr.extendedThree, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? four = expr.extendedFour is not null
            ? await RewriteCoreAsync(database, expr.extendedFour, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? five = expr.extendedFive is not null
            ? await RewriteCoreAsync(database, expr.extendedFive, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? six = expr.extendedSix is not null
            ? await RewriteCoreAsync(database, expr.extendedSix, ticket, position, outer).ConfigureAwait(false)
            : null;

        NodeAst? seven = expr.extendedSeven is not null
            ? await RewriteCoreAsync(database, expr.extendedSeven, ticket, position, outer).ConfigureAwait(false)
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
        string position,
        OuterNameScope outer)
    {
        if (expr.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid EXISTS subquery expression");

        SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(expr.leftAst);

        if ((await scopeAnalyzer.FindOuterReferencesAsync(database, innerQuery, outer).ConfigureAwait(false)).Count > 0)
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
        OuterNameScope outer,
        bool negated)
    {
        if (expr.leftAst is null || expr.rightAst is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                negated ? "Invalid NOT IN subquery expression" : "Invalid IN subquery expression");
        }

        if (expr.rightAst.nodeType != NodeType.Select)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "IN subquery must be a SELECT statement");

        await EnsureUncorrelatedAsync(database, expr.rightAst, outer, "A correlated IN or NOT IN subquery").ConfigureAwait(false);

        // The tested value is rewritten with the rule of its enclosing clause, so an EXISTS on the left
        // of an IN keeps the treatment of its position.
        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position, outer).ConfigureAwait(false);

        await using InSubqueryMaterialization materialization = await inExecutor.MaterializeAsync(
            database,
            expr.rightAst,
            ticket.TxnState,
            ticket.Parameters,
            ticket.CancellationToken).ConfigureAwait(false);

        NodeAst membership = negated
            ? await SubqueryValueListAst.BuildNotInMembershipAsync(lhs, materialization).ConfigureAwait(false)
            : await SubqueryValueListAst.BuildInMembershipAsync(lhs, materialization).ConfigureAwait(false);

        return SubqueryResultTypes.Remember(membership, materialization.OutputType);
    }

    /// <summary>
    /// Replaces the subquery of an ordered quantified comparison (<c>x &lt; ANY (SELECT …)</c>,
    /// <c>x = ALL (SELECT …)</c>, …) with the <c>ARRAY[…]</c> of the rows it returned, so the
    /// synchronous per-row evaluator can fold over it. The operator marker and the quantifier word
    /// are carried over unchanged; only the right operand changes.
    ///
    /// <para>The subquery must be uncorrelated, for the reason <c>IN</c> requires it: it is executed
    /// once, before the outer scan starts, so a reference to an outer column has no value to read.
    /// The same check reports it, so both forms reject the same statements.</para>
    /// </summary>
    private async Task<NodeAst> RewriteQuantifiedSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string? position,
        OuterNameScope outer)
    {
        NodeAst? select = expr.rightAst?.leftAst;

        if (expr.leftAst is null || select is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Invalid quantified comparison subquery expression");
        }

        if (select.nodeType != NodeType.Select)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "IN subquery must be a SELECT statement");

        await EnsureUncorrelatedAsync(database, select, outer, "A correlated subquery in an ANY, SOME or ALL comparison").ConfigureAwait(false);

        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position, outer).ConfigureAwait(false);

        await using InSubqueryMaterialization materialization = await inExecutor.MaterializeAsync(
            database,
            select,
            ticket.TxnState,
            ticket.Parameters,
            ticket.CancellationToken).ConfigureAwait(false);

        NodeAst elements = SubqueryResultTypes.Remember(
            await SubqueryValueListAst.BuildValueSetAsync(materialization, ticket.CancellationToken).ConfigureAwait(false),
            materialization.OutputType);

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

    /// <summary>
    /// Refuses a subquery that reads the query around it, because this stage runs it once for the
    /// whole statement and an outer column has no value then. The message names the first outer name,
    /// so the reader can see why a name that the subquery does not have is not an unknown column.
    /// </summary>
    private async ValueTask EnsureUncorrelatedAsync(
        DatabaseDescriptor database,
        NodeAst select,
        OuterNameScope outer,
        string form)
    {
        SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(select);
        IReadOnlyList<NodeAst> outerReferences = await scopeAnalyzer
            .FindOuterReferencesAsync(database, innerQuery, outer)
            .ConfigureAwait(false);

        if (outerReferences.Count > 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"{form} is not supported yet: it reads '{outerReferences[0].yytext}' of the outer query");
        }
    }
}
