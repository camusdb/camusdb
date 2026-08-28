
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
/// Rewrites EXISTS subqueries before ticket creation.
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
        ExecuteSQLTicket ticket)
    {
        // No EXISTS anywhere in the WHERE (or no WHERE): the rewrite walk would traverse every node
        // — allocating a Task and a state-machine box per node per statement execution — only to
        // return the same references. The scan mirrors the walk's own left/right traversal, so an
        // expression it clears is one the walk would have left untouched.
        if (query.Where is null || !ContainsExistsSubquery(query.Where.Expression))
            return new ValueTask<(SelectQuery, ExistsSubqueryRegistry?)>((query, null));

        return PrepareCoreAsync(database, query, outerTableSources, outerDerivedSources, ticket);
    }

    /// <summary>Returns true when <paramref name="expr"/> or a left/right descendant is an EXISTS subquery.</summary>
    private static bool ContainsExistsSubquery(NodeAst expr)
    {
        if (expr.nodeType == NodeType.ExprExistsSubquery)
            return true;

        if (expr.leftAst is not null && ContainsExistsSubquery(expr.leftAst))
            return true;

        return expr.rightAst is not null && ContainsExistsSubquery(expr.rightAst);
    }

    private async ValueTask<(SelectQuery Query, ExistsSubqueryRegistry? Registry)> PrepareCoreAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket)
    {
        ExistsRewriteContext context = new();
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
        public ExistsSubqueryRegistry? Registry { get; set; }
    }

    private async Task<NodeAst> RewriteExpressionAsync(
        DatabaseDescriptor database,
        NodeAst expr,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        ExistsRewriteContext context)
    {
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
                outerDerivedSources).ConfigureAwait(false);

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

        NodeAst? left = expr.leftAst is not null
            ? await RewriteExpressionAsync(database, expr.leftAst, outerTableSources, outerDerivedSources, ticket, context)
                .ConfigureAwait(false)
            : null;

        NodeAst? right = expr.rightAst is not null
            ? await RewriteExpressionAsync(database, expr.rightAst, outerTableSources, outerDerivedSources, ticket, context)
                .ConfigureAwait(false)
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

    private async Task<PreparedExistsSubquery> PrepareCorrelatedAsync(
        DatabaseDescriptor database,
        SelectQuery innerQuery,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        BoundSelectQuery innerBound = await queryBinder.BindSubqueryAsync(database, innerQuery).ConfigureAwait(false);

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
