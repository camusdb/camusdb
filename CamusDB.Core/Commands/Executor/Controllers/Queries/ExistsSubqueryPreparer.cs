
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
/// Rewrites EXISTS subqueries before ticket creation: an uncorrelated EXISTS runs once and folds to
/// a literal, a correlated one is bound against the outer sources and registered in an
/// <see cref="ExistsSubqueryRegistry"/> for the per-row evaluator. A correlated EXISTS's inner query
/// binds with the <see cref="NestedSelectBinder"/> the pipeline passes in, so a derived table in its
/// FROM resolves its own subqueries like one anywhere else.
///
/// <para>A body is correlated when <see cref="SubqueryScopeAnalyzer"/> finds a name in it that resolves to
/// the outer query. That includes a bare name that only the outer query has, such as <c>v</c> in
/// <c>EXISTS (SELECT 1 FROM r2 WHERE w = v)</c>. A name that both queries have is the inner column.</para>
/// </summary>
internal sealed class ExistsSubqueryPreparer
{
    private readonly ExistsSubqueryExecutor existsExecutor;
    private readonly QueryBinder queryBinder;
    private readonly SubqueryScopeAnalyzer scopeAnalyzer;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public ExistsSubqueryPreparer(ExistsSubqueryExecutor existsExecutor, QueryBinder queryBinder, SubqueryScopeAnalyzer scopeAnalyzer)
    {
        this.existsExecutor = existsExecutor;
        this.queryBinder = queryBinder;
        this.scopeAnalyzer = scopeAnalyzer;
    }

    public ValueTask<(SelectQuery Query, ExistsSubqueryRegistry? Registry)> PrepareAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        NestedSelectBinder nested)
    {
        // No EXISTS anywhere in the WHERE (or no WHERE): the rewrite walk would traverse every node
        // — allocating a Task and a state-machine box per node per statement execution — only to
        // return the same references. The scan mirrors the walk's own traversal of every child slot,
        // so an expression it clears is one the walk would have left untouched.
        if (query.Where is null || !ContainsExistsSubquery(query.Where.Expression))
            return new ValueTask<(SelectQuery, ExistsSubqueryRegistry?)>((query, null));

        return PrepareCoreAsync(database, query, outerTableSources, outerDerivedSources, ticket, nested);
    }

    /// <summary>
    /// Returns true when <paramref name="expr"/> or any descendant is an EXISTS subquery. Every child
    /// slot is visited: a CASE keeps its ELSE result in <c>extendedOne</c>, and an EXISTS there must
    /// be prepared like one under a comparison.
    /// </summary>
    private static bool ContainsExistsSubquery(NodeAst expr) => SubqueryNodeScan.Contains(expr, SubqueryNodeKinds.Exists);

    private async ValueTask<(SelectQuery Query, ExistsSubqueryRegistry? Registry)> PrepareCoreAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        NestedSelectBinder nested)
    {
        ExistsRewriteContext context = new(nested, OuterNameScope.Of(outerTableSources, outerDerivedSources));
        NodeAst rewritten = await RewriteExpressionAsync(
            database,
            query.Where!.Expression,
            outerTableSources,
            outerDerivedSources,
            ticket,
            context).ConfigureAwait(false);

        if (ReferenceEquals(rewritten, query.Where!.Expression) && context.Registry is null)
            return (query, null);

        return (query with { Where = new BoundPredicate(rewritten) }, context.Registry);
    }

    private sealed class ExistsRewriteContext
    {
        public ExistsRewriteContext(NestedSelectBinder nested, OuterNameScope outerScope)
        {
            Nested = nested;
            OuterScope = outerScope;
        }

        public ExistsSubqueryRegistry? Registry { get; set; }

        /// <summary>Binds the derived tables of a correlated EXISTS's inner FROM through the pipeline.</summary>
        public NestedSelectBinder Nested { get; }

        /// <summary>The names of the outer query, which a body can read.</summary>
        public OuterNameScope OuterScope { get; }
    }

    private async Task<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        ExistsRewriteContext context)
    {
        // An async frame is large, and a deep predicate would overflow the stack; continue on a
        // fresh thread-pool stack instead. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
            return await Task.Run(() => RewriteExpressionAsync(database, expr, outerTableSources, outerDerivedSources, ticket, context)).ConfigureAwait(false);

        if (expr.nodeType == NodeType.ExprExistsSubquery)
        {
            if (expr.leftAst is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Invalid EXISTS subquery expression");
            }

            SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(expr.leftAst);

            IReadOnlyList<NodeAst> outerReferences = await scopeAnalyzer
                .FindOuterReferencesAsync(database, innerQuery, context.OuterScope)
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

            // The correlated evaluator reads only FROM and WHERE, so it cannot run a body that needs
            // more. It once ignored those clauses, which gave a wrong answer with no error.
            ExistsBodyForm form = ExistsBody.Classify(innerQuery, ticket.Parameters);

            if (form == ExistsBodyForm.MustRun)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.FeatureNotSupported,
                    "A correlated EXISTS subquery with an aggregate, HAVING, OFFSET or a LIMIT that is not a positive constant or zero is not supported");
            }

            // The correlated evaluator reads the WHERE row by row and cannot run a subquery in it.
            if (innerQuery.Where is not null && SubqueryNodeScan.Contains(innerQuery.Where.Expression, SubqueryNodeKinds.Rewritable | SubqueryNodeKinds.Exists))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.FeatureNotSupported,
                    "A correlated EXISTS subquery with a subquery in its WHERE is not supported yet");
            }

            PreparedExistsSubquery prepared = await PrepareCorrelatedAsync(
                database,
                innerQuery,
                form,
                outerReferences,
                outerTableSources,
                outerDerivedSources,
                context.Nested).ConfigureAwait(false);

            if (form == ExistsBodyForm.NoRows)
                return ColumnValueAstBuilder.FromColumnValue(ColumnValue.FromBool(false));

            NodeAst correlatedNode = new(
                NodeType.ExprExistsCorrelated,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: null);

            context.Registry ??= new ExistsSubqueryRegistry();
            context.Registry.Register(correlatedNode, prepared);
            return correlatedNode;
        }

        NodeAst? left = await RewriteChildAsync(database, expr.leftAst, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? right = await RewriteChildAsync(database, expr.rightAst, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? one = await RewriteChildAsync(database, expr.extendedOne, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? two = await RewriteChildAsync(database, expr.extendedTwo, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? three = await RewriteChildAsync(database, expr.extendedThree, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? four = await RewriteChildAsync(database, expr.extendedFour, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? five = await RewriteChildAsync(database, expr.extendedFive, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? six = await RewriteChildAsync(database, expr.extendedSix, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);
        NodeAst? seven = await RewriteChildAsync(database, expr.extendedSeven, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);

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
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        ExistsRewriteContext context)
    {
        if (child is null)
            return new ValueTask<NodeAst?>((NodeAst?)null);

        return RewriteChildCoreAsync(database, child, outerTableSources, outerDerivedSources, ticket, context);
    }

    private async ValueTask<NodeAst?> RewriteChildCoreAsync(
        DatabaseDescriptor database,
        NodeAst child,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        ExistsRewriteContext context)
        => await RewriteExpressionAsync(database, child, outerTableSources, outerDerivedSources, ticket, context).ConfigureAwait(false);

    private async Task<PreparedExistsSubquery> PrepareCorrelatedAsync(
        DatabaseDescriptor database,
        SelectQuery innerQuery,
        ExistsBodyForm form,
        IReadOnlyList<NodeAst> outerReferenceNodes,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        NestedSelectBinder nested)
    {
        // The output can read the outer row, so it is not part of the shell and has its own check.
        SelectQuery shell = ExistsBody.DiscardOutput(innerQuery);

        BoundSelectQuery innerBound = await queryBinder.BindSubqueryAsync(database, shell, nested).ConfigureAwait(false);

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

        HashSet<string> outerReferences = new(StringComparer.OrdinalIgnoreCase);
        foreach (NodeAst node in outerReferenceNodes)
            outerReferences.Add(node.yytext!);

        // Choose the inner access path once here rather than per outer row: it depends only on the
        // inner predicate and the inner table's indexes. Null keeps the full-scan fallback.
        CorrelatedExistsSeekPlan? seekPlan = innerBound.Sources.Count == 1
            ? CorrelatedExistsSeekPlanner.TryPlan(
                innerBound.Sources[0].Table,
                innerQuery.Where?.Expression,
                innerBound.Sources[0].Alias,
                ExistsSubqueryAnalyzer.CollectSourceAliases(innerQuery.Source),
                outerReferences)
            : null;

        return new PreparedExistsSubquery(
            innerQuery.Where?.Expression,
            innerBound,
            outerTableSources,
            outerDerivedSources,
            combinedResolver,
            [.. outerReferences],
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
