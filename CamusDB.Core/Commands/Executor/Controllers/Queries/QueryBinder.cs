
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
        HashSet<string> aliases = new(StringComparer.Ordinal);

        await CollectBoundSourcesAsync(database, query.Source, sources, aliases).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources);

        QueryRowNameResolver rowNames = new(sources);
        ValidateQuery(query, rowNames);

        return new BoundSelectQuery(query, sources, rowNames);
    }

    private async Task CollectBoundSourcesAsync(
        DatabaseDescriptor database,
        QuerySource source,
        List<BoundTableSource> sources,
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

            case JoinSource joinSource:
                await CollectBoundSourcesAsync(database, joinSource.Left, sources, aliases).ConfigureAwait(false);
                await CollectBoundSourcesAsync(database, joinSource.Right, sources, aliases).ConfigureAwait(false);
                return;

            case DerivedTableSource:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Derived table sources are not supported yet");

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static void ValidateJoinPredicates(QuerySource source, IReadOnlyList<BoundTableSource> allSources)
    {
        switch (source)
        {
            case JoinSource joinSource:
                ValidateJoinPredicates(joinSource.Left, allSources);

                List<BoundTableSource> inScope = new();
                CollectBoundSourcesFromSubtree(joinSource.Left, allSources, inScope);
                CollectBoundSourcesFromSubtree(joinSource.Right, allSources, inScope);
                ValidateExpression(joinSource.OnPredicate, new QueryRowNameResolver(inScope));
                return;

            case TableSource:
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
        List<BoundTableSource> output)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                string alias = tableSource.Alias ?? tableSource.TableName;
                output.Add(FindBoundSource(allSources, tableSource.TableName, alias));
                return;
            }

            case JoinSource joinSource:
                CollectBoundSourcesFromSubtree(joinSource.Left, allSources, output);
                CollectBoundSourcesFromSubtree(joinSource.Right, allSources, output);
                return;

            case DerivedTableSource:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Derived table sources are not supported yet");

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    private static BoundTableSource FindBoundSource(
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

        ValidateProjectionAndGrouping(query);
    }

    private static void ValidateOrderBy(SelectQuery query, QueryRowNameResolver rowNames)
    {
        if (query.OrderBy is null)
            return;

        bool sortAfterProjection = query.GroupBy is { Count: > 0 };

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
        HashSet<string> identifiers = new(StringComparer.Ordinal);

        QueryExpressionWalker.CollectColumnReferences(expression, identifiers);

        foreach (string identifier in identifiers)
            rowNames.ValidateColumnReference(identifier);
    }
}
