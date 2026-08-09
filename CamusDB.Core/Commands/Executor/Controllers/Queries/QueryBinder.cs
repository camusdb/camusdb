
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Binds a logical <see cref="SelectQuery"/> to catalog objects and validates column references.
/// Opens all table sources (including both sides of inner joins) in left-to-right order for
/// deterministic nested-loop execution.
/// </summary>
internal sealed class QueryBinder
{
    private readonly TableOpener tableOpener;

    /// <summary>
    /// Resolves a view owner (name plus immutable id) to the principal its body runs as, or null when
    /// that owner can no longer be established. Null on an engine built without authentication.
    /// </summary>
    private readonly Func<string, string, Task<Principal?>>? resolveOwner;

    public QueryBinder(TableOpener tableOpener, Func<string, string, Task<Principal?>>? resolveOwner = null)
    {
        this.tableOpener = tableOpener;
        this.resolveOwner = resolveOwner;
    }

    public async Task<BoundSelectQuery> BindAsync(DatabaseDescriptor database, SelectQuery query)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];

        // Case-insensitive: aliases resolve case-insensitively downstream, so "t a JOIN u A"
        // is a genuine duplicate and must be rejected here with a clear error.
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources, derivedSources);

        QueryRowNameResolver rowNames = new(sources, derivedSources);
        ValidateQuery(query, rowNames);

        return new BoundSelectQuery(query, sources, rowNames, derivedSources);
    }

    /// <summary>
    /// Binds a subquery shell for correlated EXISTS execution. Inner WHERE may reference outer
    /// columns and is validated separately by the caller.
    /// </summary>
    internal async Task<BoundSelectQuery> BindSubqueryAsync(DatabaseDescriptor database, SelectQuery query)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources, derivedSources);

        QueryRowNameResolver rowNames = new(sources, derivedSources);

        foreach (ProjectionItem projection in query.Projections)
            ValidateExpression(projection.Expression, rowNames);

        if (query.GroupBy is not null)
        {
            foreach (NodeAst groupExpr in query.GroupBy)
            {
                ValidateNoAggregateInPreAggregateScope(groupExpr, "GROUP BY");
                ValidateExpression(groupExpr, rowNames);
            }
        }

        ValidateOrderBy(query, rowNames);
        ValidateProjectionAndGrouping(query);

        return new BoundSelectQuery(query, sources, rowNames, derivedSources);
    }

    private async Task CollectBoundSourcesAsync(
        DatabaseDescriptor database,
        QuerySource source,
        List<BoundTableSource> sources,
        List<BoundDerivedTableSource> derivedSources,
        HashSet<string> aliases)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                string alias = tableSource.Alias ?? tableSource.TableName;

                if (!aliases.Add(alias))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Duplicate alias '{alias}'");
                }

                TableDescriptor table = await tableOpener.Open(database, tableSource.TableName).ConfigureAwait(false);

                // Every read resolves its sources here — top-level, join, subquery and semi-join
                // alike — which is why the unpopulated check belongs at this point rather than at the
                // table open: opening is also what DESCRIBE, CREATE INDEX and ANALYZE do, and all
                // three are legitimate on a materialized view that holds no rows yet.
                MaterializedViewAccessGuard.RequireReadable(table);

                sources.Add(new BoundTableSource(tableSource, table, alias));
                return;
            }

            case DerivedTableSource derivedSource:
            {
                if (!aliases.Add(derivedSource.Alias))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Duplicate alias '{derivedSource.Alias}'");
                }

                // An expanded view's body binds as its OWNER, not as the caller. That is what a view
                // is for: the caller was already checked on the view itself, and the tables underneath
                // it are checked against whoever created it — so a view can expose a slice of a table
                // the caller has no access to, and nothing else.
                //
                // The swap is scoped to this subtree rather than to the statement, which is the part
                // that has to be right. A query may name the same table both through a view and
                // directly; only the reference that came through the view runs with the owner's
                // rights, and the direct one is still checked against the caller.
                // Two steps, and the split is load-bearing. Resolving the owner is asynchronous, but an
                // AsyncLocal written *inside* an async method is invisible to its caller — the same trap
                // the request entry points document. So the lookup happens in an awaited call and the
                // scope is entered by a synchronous constructor here, where the write does flow into the
                // BindAsync below.
                Principal? owner = await ResolveOwnerAsync(derivedSource).ConfigureAwait(false);

                using OwnerScope _ = new(owner);

                BoundSelectQuery innerBound = await BindAsync(database, derivedSource.Query).ConfigureAwait(false);
                IReadOnlyList<DerivedColumnSchema> columns =
                    DerivedTableSchemaBuilder.Build(derivedSource.Query, innerBound);

                derivedSources.Add(new BoundDerivedTableSource(
                    derivedSource,
                    derivedSource.Alias,
                    columns,
                    innerBound));
                return;
            }

            case JoinSource joinSource:
                await CollectBoundSourcesAsync(database, joinSource.Left, sources, derivedSources, aliases)
                    .ConfigureAwait(false);
                await CollectBoundSourcesAsync(database, joinSource.Right, sources, derivedSources, aliases)
                    .ConfigureAwait(false);
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    /// <summary>
    /// Swaps the ambient principal to a view's owner for the duration of binding that view's body.
    /// Returns a no-op scope for a derived table the user wrote themselves, and on an engine with
    /// authentication disabled.
    /// </summary>
    /// <remarks>
    /// Fails closed. An owner that no longer resolves — the user was dropped, or the view was created
    /// before owners were recorded — refuses the read rather than falling back to the caller's own
    /// rights, because that fallback would silently convert a definer's-rights view into an
    /// invoker's-rights one and hand out access the view was written to withhold.
    /// </remarks>
    private async Task<Principal?> ResolveOwnerAsync(DerivedTableSource derivedSource)
    {
        if (resolveOwner is null || derivedSource.OwnerId is not { Length: > 0 } ownerId)
            return null;

        // Nothing to swap on an unauthenticated flow: there is no principal, and the per-table check
        // is skipped anyway.
        if (AuthorizationContext.Current.Principal is null)
            return null;

        Principal? owner = await resolveOwner(derivedSource.OwnerName ?? "", ownerId).ConfigureAwait(false);

        if (owner is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"The owner of view '{derivedSource.Alias}' no longer exists, so the view cannot be read. " +
                "Recreate it under a current user, or transfer it with ALTER VIEW ... OWNER TO.");

        return owner;
    }

    /// <summary>
    /// Restores the previous authorization scope when disposed, on every path including an exception
    /// thrown while binding the view's body — a body that throws must not leave the owner's principal
    /// ambient for the rest of the statement.
    /// </summary>
    private readonly struct OwnerScope : IDisposable
    {
        private readonly AuthorizationScope previous;
        private readonly bool entered;

        internal OwnerScope(Principal? owner)
        {
            if (owner is null)
            {
                previous = default;
                entered = false;
                return;
            }

            previous = AuthorizationContext.Current;
            entered = true;
            AuthorizationContext.Current = previous with { Principal = owner };
        }

        public void Dispose()
        {
            if (entered)
                AuthorizationContext.Current = previous;
        }
    }

    private static void ValidateJoinPredicates(
        QuerySource source,
        IReadOnlyList<BoundTableSource> allSources,
        IReadOnlyList<BoundDerivedTableSource> allDerivedSources)
    {
        switch (source)
        {
            case JoinSource joinSource:
                ValidateJoinPredicates(joinSource.Left, allSources, allDerivedSources);

                List<BoundTableSource> inScopeTables = new();
                List<BoundDerivedTableSource> inScopeDerived = new();
                CollectBoundSourcesFromSubtree(joinSource.Left, allSources, allDerivedSources, inScopeTables, inScopeDerived);
                CollectBoundSourcesFromSubtree(joinSource.Right, allSources, allDerivedSources, inScopeTables, inScopeDerived);
                ValidateNoAggregateInPreAggregateScope(joinSource.OnPredicate, "JOIN ON");
                ValidateExpression(joinSource.OnPredicate, new QueryRowNameResolver(inScopeTables, inScopeDerived));
                return;

            case TableSource:
            case DerivedTableSource:
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static void CollectBoundSourcesFromSubtree(
        QuerySource source,
        IReadOnlyList<BoundTableSource> allSources,
        IReadOnlyList<BoundDerivedTableSource> allDerivedSources,
        List<BoundTableSource> tableOutput,
        List<BoundDerivedTableSource> derivedOutput)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                string alias = tableSource.Alias ?? tableSource.TableName;
                tableOutput.Add(FindBoundTableSource(allSources, tableSource.TableName, alias));
                return;
            }

            case DerivedTableSource derivedSource:
                derivedOutput.Add(FindBoundDerivedSource(allDerivedSources, derivedSource.Alias));
                return;

            case JoinSource joinSource:
                CollectBoundSourcesFromSubtree(joinSource.Left, allSources, allDerivedSources, tableOutput, derivedOutput);
                CollectBoundSourcesFromSubtree(joinSource.Right, allSources, allDerivedSources, tableOutput, derivedOutput);
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static BoundTableSource FindBoundTableSource(
        IReadOnlyList<BoundTableSource> sources,
        string tableName,
        string alias)
    {
        foreach (BoundTableSource source in sources)
        {
            if (source.Source.TableName == tableName && source.Alias == alias)
                return source;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Bound source not found for table '{tableName}' alias '{alias}'");
    }

    private static BoundDerivedTableSource FindBoundDerivedSource(
        IReadOnlyList<BoundDerivedTableSource> sources,
        string alias)
    {
        foreach (BoundDerivedTableSource source in sources)
        {
            if (source.Alias == alias)
                return source;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Bound derived source not found for alias '{alias}'");
    }

    private static void ValidateQuery(SelectQuery query, QueryRowNameResolver rowNames)
    {
        foreach (ProjectionItem projection in query.Projections)
        {
            QueryExpressionClassifier.ValidateNoNestedAggregate(projection.Expression);
            ValidateExpression(projection.Expression, rowNames);
        }

        if (query.Where is not null)
        {
            ValidateNoAggregateInPreAggregateScope(query.Where.Expression, "WHERE");
            ValidateExpression(query.Where.Expression, rowNames);
        }

        if (query.GroupBy is not null)
        {
            foreach (NodeAst groupExpr in query.GroupBy)
            {
                ValidateNoAggregateInPreAggregateScope(groupExpr, "GROUP BY");
                ValidateExpression(groupExpr, rowNames);
            }
        }

        ValidateOrderBy(query, rowNames);

        if (query.Having is not null)
            QueryExpressionClassifier.ValidateNoNestedAggregate(query.Having.Expression);

        QueryPostAggregateScopeValidator.ValidateHaving(query, rowNames);

        ValidateDistinct(query);
        ValidateProjectionAndGrouping(query);
    }

    private static void ValidateDistinct(SelectQuery query)
    {
        if (!query.IsDistinct)
            return;

        if (query.GroupBy is { Count: > 0 })
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "SELECT DISTINCT with GROUP BY is not supported");
        }

        foreach (ProjectionItem projection in query.Projections)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(projection.Expression))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SELECT DISTINCT cannot be used with aggregate projections");
            }
        }
    }

    private static void ValidateOrderBy(SelectQuery query, QueryRowNameResolver rowNames)
    {
        if (query.OrderBy is null)
            return;

        bool sortAfterProjection = query.GroupBy is { Count: > 0 }
            || (query.IsDistinct && !IsFullProjection(query.Projections));

        foreach (OrderByItem orderBy in query.OrderBy)
        {
            QueryExpressionClassifier.ValidateNoNestedAggregate(orderBy.Expression);

            if (sortAfterProjection
                && QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
                    orderBy.Expression,
                    query.Projections,
                    query.GroupBy,
                    out _))
            {
                continue;
            }

            if (sortAfterProjection)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "ORDER BY must reference a SELECT projection or GROUP BY expression");
            }

            ValidateExpression(orderBy.Expression, rowNames);
        }
    }

    private static bool IsFullProjection(IReadOnlyList<ProjectionItem> projections) =>
        projections is [{ Expression.nodeType: NodeType.ExprAllFields }];

    private static void ValidateProjectionAndGrouping(SelectQuery query)
    {
        bool hasGroupBy = query.GroupBy is { Count: > 0 };
        bool hasAggregation = false;
        bool hasNonAggregateProjection = false;

        foreach (ProjectionItem projection in query.Projections)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(projection.Expression))
                hasAggregation = true;
            else
                hasNonAggregateProjection = true;
        }

        if (hasGroupBy)
        {
            foreach (ProjectionItem projection in query.Projections)
            {
                if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression))
                    continue;

                if (QueryExpressionClassifier.IsCompoundAggregateProjection(projection.Expression))
                {
                    // Validate that every non-aggregate column reference in the compound
                    // expression is listed in GROUP BY. Column refs inside aggregate calls are
                    // exempt — they are consumed by the accumulator, not by the outer evaluator.
                    List<NodeAst> colRefs = [];
                    QueryExpressionClassifier.CollectNonAggregateColumnRefs(projection.Expression, colRefs);
                    foreach (NodeAst colRef in colRefs)
                    {
                        if (!IsExpressionListedInGroupBy(colRef, query.GroupBy!))
                        {
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Column '{colRef.yytext}' must appear in the GROUP BY clause or be used in an aggregate function");
                        }
                    }
                    continue;
                }

                NodeAst projectionExpr = QueryExpressionClassifier.UnwrapAlias(projection.Expression);

                if (!IsExpressionListedInGroupBy(projectionExpr, query.GroupBy!))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        "Non-aggregate projection must appear in GROUP BY");
                }
            }

            return;
        }

        if (!hasAggregation)
            return;

        // No GROUP BY but aggregation is present. A compound expression that references a bare
        // column is invalid: without GROUP BY there is no group key to bind the column to.
        foreach (ProjectionItem projection in query.Projections)
        {
            if (!QueryExpressionClassifier.IsCompoundAggregateProjection(projection.Expression))
                continue;

            List<NodeAst> colRefs = [];
            QueryExpressionClassifier.CollectNonAggregateColumnRefs(projection.Expression, colRefs);
            if (colRefs.Count > 0)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{colRefs[0].yytext}' cannot appear in a compound aggregate expression without GROUP BY");
            }
        }

        if (hasNonAggregateProjection)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Aggregations cannot be accompanied by other projections or expressions.");
        }

        if (query.Projections.Count > 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Aggregations cannot be accompanied by other projections or expressions.");
        }
    }

    private static bool IsExpressionListedInGroupBy(
        NodeAst projectionExpression,
        IReadOnlyList<NodeAst> groupBy)
    {
        foreach (NodeAst groupExpression in groupBy)
        {
            if (QueryAstComparer.AreEquivalent(projectionExpression, groupExpression))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// if <paramref name="expression"/> contains an aggregate function call. Used to guard
    /// pre-aggregate scopes (WHERE, JOIN ON, GROUP BY) where aggregate calls are illegal.
    /// HAVING and post-aggregate ORDER BY are intentionally excluded from this check.
    /// </summary>
    private static void ValidateNoAggregateInPreAggregateScope(NodeAst expression, string scope)
    {
        if (!QueryExpressionClassifier.ContainsAggregate(expression))
            return;

        string funcName = FindFirstAggregateName(expression) ?? "aggregate";
        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Aggregate function '{funcName}' is not allowed in {scope}");
    }

    private static string? FindFirstAggregateName(NodeAst node)
    {
        if (node.nodeType == NodeType.ExprFuncCall
            && node.leftAst?.yytext is string name
            && QueryExpressionClassifier.IsAggregateName(name))
            return name;

        string? found = node.leftAst is not null ? FindFirstAggregateName(node.leftAst) : null;
        if (found is not null) return found;
        found = node.rightAst is not null ? FindFirstAggregateName(node.rightAst) : null;
        if (found is not null) return found;
        found = node.extendedOne is not null ? FindFirstAggregateName(node.extendedOne) : null;
        if (found is not null) return found;
        return node.extendedTwo is not null ? FindFirstAggregateName(node.extendedTwo) : null;
    }

    private static void ValidateExpression(NodeAst expression, QueryRowNameResolver rowNames)
    {
        if (expression.nodeType == NodeType.ExprExistsSubquery)
        {
            ExistsSubqueryValidator.ValidateOuterReferences(expression, rowNames);
            return;
        }

        HashSet<string> identifiers = new(StringComparer.Ordinal);

        QueryExpressionWalker.CollectColumnReferences(expression, identifiers);

        foreach (string identifier in identifiers)
            rowNames.ValidateColumnReference(identifier);
    }
}
