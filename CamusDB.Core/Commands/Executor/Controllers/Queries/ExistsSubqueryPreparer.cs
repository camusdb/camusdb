
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
/// Prepares the correlated subqueries of a clause before ticket creation, and folds an uncorrelated
/// EXISTS of the WHERE of a SELECT to a literal. A correlated subquery is bound against the outer sources
/// and registered in an <see cref="ExistsSubqueryRegistry"/>; the per-row stages run it for each outer row,
/// see <see cref="CorrelatedSubqueryValues"/>. Its inner query binds with the <see cref="NestedSelectBinder"/>
/// the pipeline passes in, so a derived table in its FROM resolves its own subqueries like one anywhere else.
///
/// <para>A correlated EXISTS whose body reads one table and needs only its FROM and WHERE gets the fast
/// form, <see cref="CorrelatedExistsScan"/>. Every other EXISTS body (a join, a derived table or a view, no
/// FROM, an aggregate, HAVING, OFFSET, a LIMIT that is not a positive constant, a window function, a subquery
/// in its WHERE), and the body of every correlated scalar, <c>IN</c>, <c>NOT IN</c> and quantified subquery,
/// becomes a <see cref="CorrelatedSubqueryBody"/>, which runs through the whole pipeline for each outer
/// row.</para>
///
/// <para>The clauses are the WHERE and the select list of a SELECT, and the WHERE and the SET values of an
/// UPDATE or the WHERE of a DELETE, whose outer query is the target table. <see cref="SubqueryRewriter"/>
/// runs first and folds each uncorrelated scalar, <c>IN</c> and quantified subquery, so each one left here is
/// correlated. It refuses a correlated subquery in every other clause.</para>
///
/// <para>Each prepared node is a new node with a hidden name, so the parsed tree, which the parser cache
/// shares, is never changed and never registered. A scalar node records the type of its column
/// (<see cref="SubqueryResultTypes"/>), and the caller runs the statement checks again over the prepared
/// clauses, so a type mistake fails with no outer row, as in PostgreSQL.</para>
///
/// <para>A body is correlated when <see cref="SubqueryScopeAnalyzer"/> finds a name in it that resolves to
/// the outer query. That includes a bare name that only the outer query has, such as <c>v</c> in
/// <c>EXISTS (SELECT 1 FROM r2 WHERE w = v)</c>. A name that both queries have is the inner column.</para>
/// </summary>
internal sealed class ExistsSubqueryPreparer
{
    private readonly ExistsSubqueryExecutor existsExecutor;

    private readonly SubqueryQueryExecutor subqueryExecutor;

    private readonly QueryBinder queryBinder;

    private readonly SubqueryScopeAnalyzer scopeAnalyzer;

    private readonly SelectQueryCreator selectQueryCreator = new();

    public ExistsSubqueryPreparer(
        ExistsSubqueryExecutor existsExecutor,
        SubqueryQueryExecutor subqueryExecutor,
        QueryBinder queryBinder,
        SubqueryScopeAnalyzer scopeAnalyzer)
    {
        this.existsExecutor = existsExecutor;
        this.subqueryExecutor = subqueryExecutor;
        this.queryBinder = queryBinder;
        this.scopeAnalyzer = scopeAnalyzer;
    }

    /// <summary>
    /// Prepares the WHERE and the select list of <paramref name="query"/>, whose FROM bound to
    /// <paramref name="outerTableSources"/> and <paramref name="outerDerivedSources"/>. Returns the same
    /// query instance and no registry when neither clause holds a subquery to prepare.
    /// </summary>
    public async ValueTask<(SelectQuery Query, ExistsSubqueryRegistry? Registry)> PrepareAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        NestedSelectBinder nested)
    {
        // No subquery to prepare: the rewrite walk would traverse every node — allocating a Task and a
        // state-machine box per node per statement execution — only to return the same references. The
        // scan mirrors the walk's own traversal of every child slot, so an expression it clears is one the
        // walk would have left untouched.
        bool where = query.Where is not null && ContainsSubqueryToPrepare(query.Where.Expression);
        bool projections = false;

        foreach (ProjectionItem item in query.Projections)
            projections |= ContainsSubqueryToPrepare(item.Expression);

        if (!where && !projections)
            return (query, null);

        CorrelatedPreparation preparation = new(nested, outerTableSources, outerDerivedSources);
        BoundPredicate? preparedWhere = query.Where;

        if (where)
        {
            NodeAst rewritten = await PrepareExpressionAsync(database, query.Where!.Expression, preparation, ticket).ConfigureAwait(false);

            if (!ReferenceEquals(rewritten, query.Where!.Expression))
                preparedWhere = new BoundPredicate(rewritten);
        }

        IReadOnlyList<ProjectionItem> preparedProjections = query.Projections;

        if (projections)
        {
            List<ProjectionItem> items = new(query.Projections.Count);

            foreach (ProjectionItem item in query.Projections)
            {
                items.Add(ContainsSubqueryToPrepare(item.Expression)
                    ? item with { Expression = await PrepareExpressionAsync(database, item.Expression, preparation, ticket).ConfigureAwait(false) }
                    : item);
            }

            preparedProjections = items;
        }

        if (ReferenceEquals(preparedWhere, query.Where) && ReferenceEquals(preparedProjections, query.Projections) && preparation.Registry is null)
            return (query, null);

        return (query with { Where = preparedWhere, Projections = preparedProjections }, preparation.Registry);
    }

    /// <summary>
    /// Returns true when <paramref name="expr"/> or any descendant is a subquery this stage prepares. Every
    /// child slot is visited: a CASE keeps its ELSE result in <c>extendedOne</c>, and a subquery there must be
    /// prepared like one under a comparison.
    /// </summary>
    internal static bool ContainsSubqueryToPrepare(NodeAst expr) =>
        SubqueryNodeScan.Contains(expr, SubqueryNodeKinds.Exists | SubqueryNodeKinds.Rewritable);

    /// <summary>
    /// Prepares each correlated subquery of <paramref name="expr"/> against the outer sources of
    /// <paramref name="preparation"/>, and folds each uncorrelated EXISTS to a literal. Every clause of one
    /// query level goes through one <paramref name="preparation"/>, so its registry holds them all; the caller
    /// evaluates each clause with a query ticket that carries that registry. Returns the same node when
    /// nothing changed.
    /// </summary>
    internal async ValueTask<NodeAst> PrepareExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        CorrelatedPreparation preparation,
        ExecuteSQLTicket ticket)
    {
        if (!ContainsSubqueryToPrepare(expr))
            return expr;

        return await RewriteExpressionAsync(database, expr, ticket, preparation).ConfigureAwait(false);
    }

    private async Task<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await Task.Run(() => RewriteExpressionAsync(database, expr, ticket, context)).ConfigureAwait(false);

        IReadOnlyList<BoundTableSource> outerTableSources = context.OuterTables;
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources = context.OuterDerived;

        switch (expr.nodeType)
        {
            case NodeType.ExprScalarSubquery when expr.leftAst is not null:
                return await PrepareScalarAsync(database, expr.leftAst, ticket, context).ConfigureAwait(false);

            case NodeType.ExprInSubquery or NodeType.ExprNotInSubquery when expr.leftAst is not null && expr.rightAst is not null:
                return await PrepareInAsync(database, expr, ticket, context).ConfigureAwait(false);

            case NodeType.ExprQuantifiedComparison when expr.leftAst is not null && expr.rightAst is { nodeType: NodeType.ExprScalarSubquery, leftAst: not null }:
                return await PrepareQuantifiedAsync(database, expr, ticket, context).ConfigureAwait(false);
        }

        if (expr.nodeType == NodeType.ExprExistsSubquery)
        {
            if (expr.leftAst is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Invalid EXISTS subquery expression");
            }

            IReadOnlyList<NodeAst> outerReferences = await scopeAnalyzer
                .FindOuterReferencesAsync(database, expr.leftAst, context.OuterScope)
                .ConfigureAwait(false);

            if (outerReferences.Count == 0)
            {
                bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
                    database,
                    expr.leftAst,
                    ticket.TxnState,
                    ticket.Parameters,
                    ticket.CancellationToken).ConfigureAwait(false);

                return ColumnValueAstBuilder.FromColumnValue(ColumnValue.FromBool(exists));
            }

            SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(expr.leftAst);
            ExistsBodyForm form = ExistsBody.Classify(innerQuery, ticket.Parameters);

            PreparedExistsSubquery prepared = await PrepareCorrelatedAsync(
                database,
                expr.leftAst,
                innerQuery,
                form,
                outerReferences,
                outerTableSources,
                outerDerivedSources,
                ticket,
                context.Nested).ConfigureAwait(false);

            if (form == ExistsBodyForm.NoRows)
                return ColumnValueAstBuilder.FromColumnValue(ColumnValue.FromBool(false));

            ExistsSubqueryRegistry registry = context.Registry ??= new ExistsSubqueryRegistry();

            NodeAst correlatedNode = new(
                NodeType.ExprExistsCorrelated,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: registry.NextHiddenName());

            registry.Register(correlatedNode, prepared);
            return correlatedNode;
        }

        NodeAst? left = await RewriteChildAsync(database, expr.leftAst, ticket, context).ConfigureAwait(false);
        NodeAst? right = await RewriteChildAsync(database, expr.rightAst, ticket, context).ConfigureAwait(false);
        NodeAst? one = await RewriteChildAsync(database, expr.extendedOne, ticket, context).ConfigureAwait(false);
        NodeAst? two = await RewriteChildAsync(database, expr.extendedTwo, ticket, context).ConfigureAwait(false);
        NodeAst? three = await RewriteChildAsync(database, expr.extendedThree, ticket, context).ConfigureAwait(false);
        NodeAst? four = await RewriteChildAsync(database, expr.extendedFour, ticket, context).ConfigureAwait(false);
        NodeAst? five = await RewriteChildAsync(database, expr.extendedFive, ticket, context).ConfigureAwait(false);
        NodeAst? six = await RewriteChildAsync(database, expr.extendedSix, ticket, context).ConfigureAwait(false);
        NodeAst? seven = await RewriteChildAsync(database, expr.extendedSeven, ticket, context).ConfigureAwait(false);

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

    /// <summary>Rewrites one child slot; a null slot stays null without a walk.</summary>
    private ValueTask<NodeAst?> RewriteChildAsync(
        DatabaseDescriptor database,
        NodeAst? child,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        if (child is null)
            return new ValueTask<NodeAst?>((NodeAst?)null);

        return RewriteChildCoreAsync(database, child, ticket, context);
    }

    private async ValueTask<NodeAst?> RewriteChildCoreAsync(
        DatabaseDescriptor database,
        NodeAst child,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
        => await RewriteExpressionAsync(database, child, ticket, context).ConfigureAwait(false);

    /// <summary>
    /// A new scalar subquery node for the correlated body <paramref name="select"/>, with a hidden name and the
    /// type of its column. The rewriter folded every uncorrelated scalar subquery, so this one reads the
    /// outer query.
    /// </summary>
    private async Task<NodeAst> PrepareScalarAsync(
        DatabaseDescriptor database,
        NodeAst select,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        (ExistsSubqueryRegistry registry, PreparedValueSubquery prepared) = await PrepareValueBodyAsync(database, select, ticket, context).ConfigureAwait(false);

        NodeAst node = SubqueryResultTypes.Remember(
            new NodeAst(NodeType.ExprScalarSubquery, select, null, null, null, null, null, null, registry.NextHiddenName()),
            prepared.Body.OutputType);

        registry.RegisterValue(node, prepared);
        return node;
    }

    /// <summary>
    /// A new <c>IN</c> or <c>NOT IN</c> node for a correlated body, with a hidden name and the type of the body's
    /// column, which the statement checks compare with the tested value. A subquery in the tested value is
    /// prepared too.
    /// </summary>
    private async Task<NodeAst> PrepareInAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        NodeAst tested = await RewriteExpressionAsync(database, expr.leftAst!, ticket, context).ConfigureAwait(false);

        (ExistsSubqueryRegistry registry, PreparedValueSubquery prepared) = await PrepareValueBodyAsync(database, expr.rightAst!, ticket, context).ConfigureAwait(false);

        NodeAst node = SubqueryResultTypes.Remember(
            new NodeAst(expr.nodeType, tested, expr.rightAst, null, null, null, null, null, registry.NextHiddenName()),
            prepared.Body.OutputType);

        registry.RegisterValue(node, prepared);
        return node;
    }

    /// <summary>
    /// A new quantified comparison (<c>x &lt; ALL (SELECT …)</c>) for a correlated body. The comparison keeps its
    /// quantifier word in its own text, so the hidden name and the type of the body's column go on a new
    /// subquery node in its right operand.
    /// </summary>
    private async Task<NodeAst> PrepareQuantifiedAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        NodeAst tested = await RewriteExpressionAsync(database, expr.leftAst!, ticket, context).ConfigureAwait(false);
        NodeAst select = expr.rightAst!.leftAst!;

        (ExistsSubqueryRegistry registry, PreparedValueSubquery prepared) = await PrepareValueBodyAsync(database, select, ticket, context).ConfigureAwait(false);

        NodeAst operand = SubqueryResultTypes.Remember(
            new NodeAst(NodeType.ExprScalarSubquery, select, null, null, null, null, null, null, registry.NextHiddenName()),
            prepared.Body.OutputType);

        NodeAst node = new(
            expr.nodeType,
            tested,
            operand,
            expr.extendedOne,
            expr.extendedTwo,
            expr.extendedThree,
            expr.extendedFour,
            expr.extendedFive,
            expr.yytext,
            expr.extendedSix,
            expr.extendedSeven);

        registry.RegisterValue(node, prepared);
        return node;
    }

    /// <summary>
    /// Prepares the one-column body <paramref name="select"/> of a correlated scalar, <c>IN</c> or quantified
    /// subquery. It runs through the whole pipeline for each outer row, so every form of SELECT can be the body.
    /// </summary>
    private async Task<(ExistsSubqueryRegistry Registry, PreparedValueSubquery Prepared)> PrepareValueBodyAsync(
        DatabaseDescriptor database,
        NodeAst select,
        ExecuteSQLTicket ticket,
        CorrelatedPreparation context)
    {
        if (select.nodeType != NodeType.Select)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Subquery must be a SELECT statement");

        IReadOnlyList<NodeAst> outerReferences = await scopeAnalyzer
            .FindOuterReferencesAsync(database, select, context.OuterScope)
            .ConfigureAwait(false);

        // The rewriter runs before this stage and folds every subquery that does not read the outer query.
        if (outerReferences.Count == 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "An uncorrelated subquery reached the correlated subquery preparer");
        }

        SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(select);

        CorrelatedSubqueryBody body = await CorrelatedSubqueryBody.PrepareAsync(
            subqueryExecutor,
            database,
            select,
            innerQuery,
            outerReferences,
            context.OuterTables,
            context.OuterDerived,
            ticket,
            isExistsBody: false).ConfigureAwait(false);

        // A body that binds again for each row was checked in a flow that does not refuse a constant division.
        ConstantDivisionCheck.CheckSelect(innerQuery);

        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);
        foreach (NodeAst reference in outerReferences)
            names.Add(reference.yytext!);

        CorrelatedResultCache<KeptSubqueryResult>? results = body.ResultsCanBeKept
            ? new CorrelatedResultCache<KeptSubqueryResult>(body.Outer)
            : null;

        return (context.Registry ??= new ExistsSubqueryRegistry(), new PreparedValueSubquery([.. names], body, results));
    }

    /// <summary>
    /// Prepares a correlated body in the fast form when it fits, and as a <see cref="CorrelatedSubqueryBody"/>
    /// otherwise. Both forms check the body here, so an error does not depend on the outer rows.
    /// </summary>
    private async Task<PreparedExistsSubquery> PrepareCorrelatedAsync(
        DatabaseDescriptor database,
        NodeAst select,
        SelectQuery innerQuery,
        ExistsBodyForm form,
        IReadOnlyList<NodeAst> outerReferenceNodes,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        NestedSelectBinder nested)
    {
        HashSet<string> outerReferences = new(StringComparer.OrdinalIgnoreCase);
        foreach (NodeAst node in outerReferenceNodes)
            outerReferences.Add(node.yytext!);

        CorrelatedExistsScan? scan = await TryPrepareScanAsync(
            database,
            innerQuery,
            form,
            outerReferences,
            outerTableSources,
            outerDerivedSources,
            nested).ConfigureAwait(false);

        if (scan is not null)
            return new PreparedExistsSubquery([.. outerReferences], scan, Body: null, ScanResults(scan, outerReferenceNodes));

        CorrelatedSubqueryBody body = await CorrelatedSubqueryBody.PrepareAsync(
            subqueryExecutor,
            database,
            select,
            innerQuery,
            outerReferenceNodes,
            outerTableSources,
            outerDerivedSources,
            ticket,
            isExistsBody: true).ConfigureAwait(false);

        // A body that binds again for each row was checked in a flow that does not refuse a constant
        // division, so the check runs here, by the rule of the fast form below.
        ConstantDivisionCheck.CheckSelect(form == ExistsBodyForm.OutputDiscarded ? ExistsBody.DiscardOutput(innerQuery) : innerQuery);

        return new PreparedExistsSubquery(
            [.. outerReferences],
            Scan: null,
            body,
            body.ResultsCanBeKept ? new CorrelatedResultCache<bool>(body.Outer) : null);
    }

    /// <summary>
    /// The result cache of a fast-form EXISTS, or null. A form with an index seek reads a few inner rows for
    /// each outer row, so a kept answer would save little and the key would cost a read of the outer row; only
    /// the form that scans the whole inner table for each outer row gets a cache. See
    /// <see cref="CorrelatedResultCache{T}"/> for the other rules.
    /// </summary>
    private static CorrelatedResultCache<bool>? ScanResults(CorrelatedExistsScan scan, IReadOnlyList<NodeAst> outerReferenceNodes)
    {
        if (scan.SeekPlan is not null || !CorrelatedResultCache<bool>.CanKeepResultsOf(scan.InnerWhere, EmptyPlaceholders))
            return null;

        // The key reads each outer name as the body form does: through a resolver of the outer sources only.
        QueryRowNameResolver outerRowNames = new(scan.OuterSources, scan.OuterDerivedSources);

        return new CorrelatedResultCache<bool>(
            CorrelatedOuterValues.Create(outerReferenceNodes, scan.OuterSources, scan.OuterDerivedSources, outerRowNames));
    }

    private static readonly IReadOnlySet<NodeAst> EmptyPlaceholders = new HashSet<NodeAst>(ReferenceEqualityComparer.Instance);

    /// <summary>
    /// The fast form, or null when the body needs more than the FROM and WHERE of one table: an aggregate,
    /// HAVING, OFFSET or a LIMIT that the form must run, a source that is not one table (a view binds to a
    /// derived table), or a subquery in the WHERE.
    /// </summary>
    private async Task<CorrelatedExistsScan?> TryPrepareScanAsync(
        DatabaseDescriptor database,
        SelectQuery innerQuery,
        ExistsBodyForm form,
        IReadOnlySet<string> outerReferences,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        NestedSelectBinder nested)
    {
        if (form == ExistsBodyForm.MustRun || innerQuery.Source is not TableSource)
            return null;

        if (innerQuery.Where is not null && SubqueryNodeScan.Contains(innerQuery.Where.Expression, SubqueryNodeKinds.All))
            return null;

        // The output can read the outer row, so it is not part of the shell and has its own check.
        SelectQuery shell = ExistsBody.DiscardOutput(innerQuery);

        BoundSelectQuery innerBound = await queryBinder.BindSubqueryAsync(database, shell, nested).ConfigureAwait(false);

        if (innerBound.Sources.Count != 1 || innerBound.DerivedSources.Count != 0)
            return null;

        if (ExistsBody.OutputNeedsCheck(innerQuery))
            await queryBinder.CheckDiscardedOutputAsync(database, innerQuery, outerTableSources, outerDerivedSources).ConfigureAwait(false);

        // This shell skips the pipeline, so the check of its stage runs here. PostgreSQL keeps the whole
        // body under LIMIT 0, so a constant division in its output fails there.
        ConstantDivisionCheck.CheckSelect(form == ExistsBodyForm.NoRows ? innerQuery : shell);

        List<BoundTableSource> combinedTableSources = new(innerBound.Sources.Count + outerTableSources.Count);
        combinedTableSources.AddRange(innerBound.Sources);
        combinedTableSources.AddRange(outerTableSources);

        List<BoundDerivedTableSource> combinedDerivedSources = new(innerBound.DerivedSources.Count + outerDerivedSources.Count);
        combinedDerivedSources.AddRange(innerBound.DerivedSources);
        combinedDerivedSources.AddRange(outerDerivedSources);

        // A name resolves to the inner query first, so a column that both queries have is not ambiguous.
        QueryRowNameResolver combinedResolver = new(
            combinedTableSources,
            combinedDerivedSources,
            innerBound.Sources.Count,
            innerBound.DerivedSources.Count);

        if (innerQuery.Where is not null)
            ValidateExpression(innerQuery.Where.Expression, combinedResolver);

        // Each outer row evaluates the predicate with this one resolver, so it must not be written again.
        combinedResolver.FreezeForSharedReuse();

        // Choose the inner access path once here rather than per outer row: it depends only on the
        // inner predicate and the inner table's indexes. Null keeps the full-scan fallback.
        CorrelatedExistsSeekPlan? seekPlan = CorrelatedExistsSeekPlanner.TryPlan(
            innerBound.Sources[0].Table,
            innerQuery.Where?.Expression,
            innerBound.Sources[0].Alias,
            ExistsSubqueryAnalyzer.CollectSourceAliases(innerQuery.Source),
            outerReferences);

        return new CorrelatedExistsScan(
            innerQuery.Where?.Expression,
            innerBound,
            outerTableSources,
            outerDerivedSources,
            combinedResolver,
            seekPlan);
    }

    private static void ValidateExpression(NodeAst expression, QueryRowNameResolver rowNames)
    {
        HashSet<string> identifiers = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(expression, identifiers);

        foreach (string identifier in identifiers)
            rowNames.ValidateColumnReference(identifier);
    }
}
