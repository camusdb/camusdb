
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
/// deterministic nested-loop execution in QP4.3.
/// </summary>
internal sealed class QueryBinder
{
    private readonly TableOpener tableOpener;

    public QueryBinder(TableOpener tableOpener)
    {
        this.tableOpener = tableOpener;
    }

    public async Task<BoundSelectQuery> BindAsync(DatabaseDescriptor database, SelectQuery query)
    {
        List<BoundTableSource> sources = new();
        List<BoundDerivedTableSource> derivedSources = new();
        HashSet<string> aliases = new(StringComparer.Ordinal);

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
        List<BoundTableSource> sources = new();
        List<BoundDerivedTableSource> derivedSources = new();
        HashSet<string> aliases = new(StringComparer.Ordinal);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources, derivedSources);

        QueryRowNameResolver rowNames = new(sources, derivedSources);

        foreach (ProjectionItem projection in query.Projections)
            ValidateExpression(projection.Expression, rowNames);

        if (query.GroupBy is not null)
        {
            foreach (NodeAst groupExpr in query.GroupBy)
                ValidateExpression(groupExpr, rowNames);
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
            ValidateExpression(projection.Expression, rowNames);

        if (query.Where is not null)
            ValidateExpression(query.Where.Expression, rowNames);

        if (query.GroupBy is not null)
        {
            foreach (NodeAst groupExpr in query.GroupBy)
                ValidateExpression(groupExpr, rowNames);
        }

        ValidateOrderBy(query, rowNames);
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
            if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression))
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
            if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression))
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
