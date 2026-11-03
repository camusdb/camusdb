
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
/// </summary>
internal sealed class ExistsSubqueryPreparer
{
    private readonly ExistsSubqueryExecutor existsExecutor;
    private readonly QueryBinder queryBinder;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public ExistsSubqueryPreparer(ExistsSubqueryExecutor existsExecutor, QueryBinder queryBinder)
    {
        this.existsExecutor = existsExecutor;
        this.queryBinder = queryBinder;
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
        ExistsRewriteContext context = new(nested);
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
        public ExistsRewriteContext(NestedSelectBinder nested)
        {
            Nested = nested;
        }

        public ExistsSubqueryRegistry? Registry { get; set; }

        /// <summary>Binds the derived tables of a correlated EXISTS's inner FROM through the pipeline.</summary>
        public NestedSelectBinder Nested { get; }
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

            if (!ExistsSubqueryAnalyzer.IsCorrelated(innerQuery))
            {
                bool exists = await existsExecutor.ExecuteUncorrelatedAsync(
                    database,
                    expr.leftAst,
                    ticket.TxnState,
                    ticket.Parameters,
                    ticket.CancellationToken).ConfigureAwait(false);

                return ColumnValueAstBuilder.FromColumnValue(ColumnValue.FromBool(exists));
            }

            PreparedExistsSubquery prepared = await PrepareCorrelatedAsync(
                database,
                innerQuery,
                outerTableSources,
                outerDerivedSources,
                context.Nested).ConfigureAwait(false);

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
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        NestedSelectBinder nested)
    {
        BoundSelectQuery innerBound = await queryBinder.BindSubqueryAsync(database, innerQuery, nested).ConfigureAwait(false);

        List<BoundTableSource> combinedTableSources = new(innerBound.Sources.Count + outerTableSources.Count);
        combinedTableSources.AddRange(innerBound.Sources);
        combinedTableSources.AddRange(outerTableSources);

        List<BoundDerivedTableSource> combinedDerivedSources = new(innerBound.DerivedSources.Count + outerDerivedSources.Count);
        combinedDerivedSources.AddRange(innerBound.DerivedSources);
        combinedDerivedSources.AddRange(outerDerivedSources);

        QueryRowNameResolver combinedResolver = new(combinedTableSources, combinedDerivedSources);

        if (innerQuery.Where is not null)
            ValidateExpression(innerQuery.Where.Expression, combinedResolver);

        // Choose the inner access path once here rather than per outer row: it depends only on the
        // inner predicate and the inner table's indexes. Null keeps the full-scan fallback.
        CorrelatedExistsSeekPlan? seekPlan = innerBound.Sources.Count == 1
            ? CorrelatedExistsSeekPlanner.TryPlan(
                innerBound.Sources[0].Table,
                innerQuery.Where?.Expression,
                innerBound.Sources[0].Alias,
                ExistsSubqueryAnalyzer.CollectSourceAliases(innerQuery.Source))
            : null;

        return new PreparedExistsSubquery(
            innerQuery.Where?.Expression,
            innerBound,
            outerTableSources,
            outerDerivedSources,
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
