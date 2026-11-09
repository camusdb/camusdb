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
/// <para>A <b>correlated</b> subquery is evaluated for each outer row instead, and only in some clauses:
/// the WHERE and the select list of a SELECT, the WHERE of an UPDATE or a DELETE, and the SET values of an
/// UPDATE. The rewriter leaves each one there for <see cref="ExistsSubqueryPreparer"/>, which binds it against
/// the outer sources (<see cref="RewriteKeepingCorrelatedAsync"/>). In the WHERE of a SELECT it also leaves
/// every uncorrelated <c>EXISTS</c>, which the preparer folds. A select list of a query that groups, aggregates
/// or has a window function, the HAVING, a JOIN ON, ON CONFLICT DO UPDATE and every other clause refuse a
/// correlated subquery with <see cref="CamusDBErrorCodes.FeatureNotSupported"/>, because folding it as if it
/// were uncorrelated would return wrong rows.</para>
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
    /// its value. Used for every expression position that has no per-row stage: a FROM-less select list,
    /// INSERT VALUES, and ON CONFLICT DO UPDATE. A correlated subquery is refused with
    /// <see cref="CamusDBErrorCodes.FeatureNotSupported"/>, and <paramref name="position"/> names
    /// the clause in that message ("a SET value of ON CONFLICT DO UPDATE"). Returns the same node
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
            ? new ValueTask<NodeAst>(RewriteCoreAsync(database, expr, ticket, new ClausePosition(position), ScopeOf(database, outerSource)))
            : new ValueTask<NodeAst>(expr);

    /// <summary>
    /// Rewrites <paramref name="expr"/> as <see cref="RewriteExpressionAsync"/> does, an uncorrelated
    /// <c>EXISTS</c> included, but leaves each correlated subquery for <see cref="ExistsSubqueryPreparer"/>.
    /// Used for the WHERE and the SET values of an UPDATE and the WHERE of a DELETE, whose locate scan
    /// evaluates a correlated subquery for each row of <paramref name="outerSource"/>.
    /// </summary>
    public ValueTask<NodeAst> RewriteKeepingCorrelatedAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        string position,
        QuerySource outerSource)
    {
        return ContainsSubqueryToRewrite(expr, includeExists: true)
            ? new ValueTask<NodeAst>(RewriteCoreAsync(database, expr, ticket, new ClausePosition(position, KeepCorrelated: true), ScopeOf(database, outerSource)))
            : new ValueTask<NodeAst>(expr);
    }

    /// <summary>
    /// How a clause treats a subquery. Every clause folds an uncorrelated subquery, except that
    /// <see cref="LeaveExists"/> (the WHERE of a SELECT) leaves every <c>EXISTS</c> for
    /// <see cref="ExistsSubqueryPreparer"/>. <see cref="KeepCorrelated"/> leaves a correlated subquery for the
    /// preparer; without it a correlated subquery is refused, with <see cref="Name"/> in the message.
    /// </summary>
    private readonly record struct ClausePosition(string Name, bool KeepCorrelated = false, bool LeaveExists = false);

    /// <summary>The WHERE of a SELECT.</summary>
    private static readonly ClausePosition SelectWhere = new("the WHERE clause", KeepCorrelated: true, LeaveExists: true);

    /// <summary>The names a subquery of a clause over <paramref name="source"/> can read.</summary>
    private OuterNameScope ScopeOf(DatabaseDescriptor database, QuerySource? source) =>
        source is null ? OuterNameScope.None : scopeAnalyzer.DeferredScope(database, source);

    /// <summary>
    /// Rewrites the subqueries of one SELECT. Its WHERE keeps <c>EXISTS</c> and each correlated subquery for
    /// <see cref="ExistsSubqueryPreparer"/>, which runs after binding. Its select list keeps a correlated
    /// subquery too, unless the query groups, aggregates or has a window function (see
    /// <see cref="SelectListPosition"/>). Its HAVING and JOIN ON predicates have no per-row stage, so a
    /// correlated subquery there is refused. Every uncorrelated subquery is folded now. The derived tables of the
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
            NodeAst rewritten = await RewriteCoreAsync(database, where.Expression, ticket, SelectWhere, outer).ConfigureAwait(false);
            where = new BoundPredicate(rewritten);
        }

        IReadOnlyList<ProjectionItem> projections = await RewriteProjectionsAsync(database, query.Projections, ticket, SelectListPosition(query), outer).ConfigureAwait(false);

        BoundPredicate? having = query.Having;
        if (having is not null && ContainsSubqueryToRewrite(having.Expression, includeExists: true))
        {
            NodeAst rewritten = await RewriteCoreAsync(database, having.Expression, ticket, new ClausePosition("HAVING"), outer).ConfigureAwait(false);
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
    /// How the select list of <paramref name="query"/> treats a correlated subquery. The projector runs one
    /// for each row of a plain SELECT. A query that groups or aggregates computes its select list in the
    /// aggregator, and a query with a window function reads its rows from the derived table that the window
    /// rewrite built, so neither has a per-row stage for it, and both refuse it.
    /// </summary>
    private static ClausePosition SelectListPosition(SelectQuery query)
    {
        if (query.Source is DerivedTableSource { Windows: not null })
            return new ClausePosition("the select list of a query with a window function");

        if (query.GroupBy is { Count: > 0 })
            return new ClausePosition("the select list of a query that groups or aggregates");

        foreach (ProjectionItem item in query.Projections)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(item.Expression)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(item.Expression))
            {
                return new ClausePosition("the select list of a query that groups or aggregates");
            }
        }

        return new ClausePosition("the select list", KeepCorrelated: true);
    }

    /// <summary>
    /// Rewrites the subqueries of the select list. An uncorrelated projection subquery runs once for the
    /// whole statement, not once per row, which is exactly its meaning. A correlated one is left for the
    /// preparer when <paramref name="position"/> keeps it. The list instance is kept when no item changed.
    /// </summary>
    private async ValueTask<IReadOnlyList<ProjectionItem>> RewriteProjectionsAsync(
        DatabaseDescriptor database,
        IReadOnlyList<ProjectionItem> projections,
        ExecuteSQLTicket ticket,
        ClausePosition position,
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

            NodeAst expression = await RewriteCoreAsync(database, item.Expression, ticket, position, outer).ConfigureAwait(false);

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
            ? await RewriteCoreAsync(database, join.OnPredicate, ticket, new ClausePosition("a JOIN ON clause"), outer).ConfigureAwait(false)
            : join.OnPredicate;

        if (ReferenceEquals(left, join.Left) && ReferenceEquals(right, join.Right) && ReferenceEquals(on, join.OnPredicate))
            return join;

        return join with { Left = left, Right = right, OnPredicate = on };
    }

    /// <summary>
    /// The recursive walk. <paramref name="position"/> says how the clause treats an <c>EXISTS</c>, see
    /// <see cref="ClausePosition"/>. <paramref name="outer"/> holds the names that a subquery of the
    /// expression can read.
    /// </summary>
    private async Task<NodeAst> RewriteCoreAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        ClausePosition position,
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

                if (await IsCorrelatedAsync(database, expr.leftAst, outer, position, "A correlated scalar subquery").ConfigureAwait(false))
                    return expr;

                SubqueryOutputType outputType = new();

                ColumnValue value = await scalarExecutor.ExecuteAsync(
                    database, expr.leftAst, ticket.TxnState, ticket.Parameters, outputType).ConfigureAwait(false);

                return value.Type == ColumnType.Null
                    ? SubqueryResultTypes.NullOf(outputType.Type, outputType.DeclaredType)
                    : ColumnValueAstBuilder.FromColumnValue(value);
            }

            case NodeType.ExprInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, outer, negated: false).ConfigureAwait(false);

            case NodeType.ExprNotInSubquery:
                return await RewriteInSubqueryAsync(database, expr, ticket, position, outer, negated: true).ConfigureAwait(false);

            case NodeType.ExprQuantifiedComparison when expr.rightAst?.nodeType == NodeType.ExprScalarSubquery:
                return await RewriteQuantifiedSubqueryAsync(database, expr, ticket, position, outer).ConfigureAwait(false);

            case NodeType.ExprExistsSubquery when !position.LeaveExists:
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
    /// be folded: it runs once and its answer holds for every row. A correlated form needs the outer
    /// row. Where the clause keeps it, it stays for the preparer; in any other position it is refused
    /// rather than answered wrongly.
    /// </summary>
    private async Task<NodeAst> RewriteExistsAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        ClausePosition position,
        OuterNameScope outer)
    {
        if (expr.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid EXISTS subquery expression");

        if (await IsCorrelatedAsync(database, expr.leftAst, outer, position, "A correlated EXISTS subquery").ConfigureAwait(false))
            return expr;

        bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
            database, expr.leftAst, ticket.TxnState, ticket.Parameters, ticket.CancellationToken).ConfigureAwait(false);

        return exists ? NodeAst.True : NodeAst.False;
    }

    private async Task<NodeAst> RewriteInSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        ClausePosition position,
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

        bool correlated = await IsCorrelatedAsync(database, expr.rightAst, outer, position, "A correlated IN or NOT IN subquery").ConfigureAwait(false);

        // The tested value is rewritten with the rule of its enclosing clause, so an EXISTS on the left
        // of an IN keeps the treatment of its position.
        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position, outer).ConfigureAwait(false);

        // The preparer runs a correlated subquery for each row; the tested value is part of the test.
        if (correlated)
            return ReferenceEquals(lhs, expr.leftAst) ? expr : WithLeft(expr, lhs);

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
    /// <para>A correlated subquery is left for the preparer where the clause keeps it, as an <c>IN</c> is,
    /// and refused elsewhere: this stage runs a subquery once, before the outer scan starts, so a
    /// reference to an outer column has no value to read here.</para>
    /// </summary>
    private async Task<NodeAst> RewriteQuantifiedSubqueryAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        ClausePosition position,
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

        bool correlated = await IsCorrelatedAsync(database, select, outer, position, "A correlated subquery in an ANY, SOME or ALL comparison").ConfigureAwait(false);

        NodeAst lhs = await RewriteCoreAsync(database, expr.leftAst, ticket, position, outer).ConfigureAwait(false);

        if (correlated)
            return ReferenceEquals(lhs, expr.leftAst) ? expr : WithLeft(expr, lhs);

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
    /// True when <paramref name="select"/> reads the query around it and <paramref name="position"/> keeps such
    /// a subquery for the preparer. A subquery that reads the outer query in any other clause is refused,
    /// because this stage runs it once for the whole statement and an outer column has no value then. The
    /// message names the clause and the first outer name, so the reader can see why a name that the
    /// subquery does not have is not an unknown column.
    /// </summary>
    private async ValueTask<bool> IsCorrelatedAsync(
        DatabaseDescriptor database,
        NodeAst select,
        OuterNameScope outer,
        ClausePosition position,
        string form)
    {
        IReadOnlyList<NodeAst> outerReferences = await scopeAnalyzer
            .FindOuterReferencesAsync(database, select, outer)
            .ConfigureAwait(false);

        if (outerReferences.Count == 0)
            return false;

        if (position.KeepCorrelated)
            return true;

        throw new CamusDBException(
            CamusDBErrorCodes.FeatureNotSupported,
            $"{form} is not supported in {position.Name} yet: it reads '{outerReferences[0].yytext}' of the outer query");
    }

    /// <summary>A copy of <paramref name="expr"/> with <paramref name="left"/> as its tested value.</summary>
    private static NodeAst WithLeft(NodeAst expr, NodeAst left) =>
        new(expr.nodeType, left, expr.rightAst, expr.extendedOne, expr.extendedTwo, expr.extendedThree,
            expr.extendedFour, expr.extendedFive, expr.yytext, expr.extendedSix, expr.extendedSeven);
}
