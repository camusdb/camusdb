
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Temporary bridge from logical <see cref="SelectQuery"/> back to legacy <see cref="QueryTicket"/>.
/// </summary>
internal static class QueryTicketAdapter
{
    public static QueryTicket ToQueryTicket(SelectQuery query, ExecuteSQLTicket ticket) =>
        ToQueryTicketInternal(query, ticket, rowNameResolver: null);

    public static QueryTicket ToQueryTicket(BoundSelectQuery bound, ExecuteSQLTicket ticket) =>
        ToQueryTicket(bound, ticket, existsSubqueries: null);

    /// <param name="exclusivePredicateLocks">
    /// True when the scan drives writes (the rows it reads decide what is written), so its predicate
    /// range locks must be exclusive and its reads must fold into the commit-time read set. Set by
    /// <c>INSERT … SELECT</c> for the same reason the UPDATE/DELETE locate scan sets it.
    /// </param>
    /// <param name="suppressCacheHint">
    /// True to drop any <c>{cache=name}</c> hint the source query carries. A write statement's source
    /// must execute live: serving it from a result cache would copy rows the cache captured at some
    /// other point in time.
    /// </param>
    public static QueryTicket ToQueryTicket(
        BoundSelectQuery bound,
        ExecuteSQLTicket ticket,
        ExistsSubqueryRegistry? existsSubqueries,
        IReadOnlyList<SemiJoinSpec>? semiJoinSpecs = null,
        bool exclusivePredicateLocks = false,
        bool suppressCacheHint = false) =>
        ToQueryTicketInternal(
            bound.Query, ticket, bound.RowNames, existsSubqueries, semiJoinSpecs,
            exclusivePredicateLocks, suppressCacheHint);

    private static QueryTicket ToQueryTicketInternal(
        SelectQuery query,
        ExecuteSQLTicket ticket,
        QueryRowNameResolver? rowNameResolver,
        ExistsSubqueryRegistry? existsSubqueries = null,
        IReadOnlyList<SemiJoinSpec>? semiJoinSpecs = null,
        bool exclusivePredicateLocks = false,
        bool suppressCacheHint = false)
    {
        TableSource tableSource = GetPrimaryTableSource(query.Source);
        NodeAst? where = query.Where?.Expression;
        PredicateAnalysis analyzedWhere = PredicateAnalyzer.Analyze(where, ticket.Parameters);

        IReadOnlyDictionary<NodeAst, PreparedInSet>? preparedInSets = BuildPreparedInSets(analyzedWhere);

        return new QueryTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: tableSource.TableName,
            index: tableSource.ForcedIndexName,
            projection: query.Projections.Select(item => item.Expression).ToList(),
            filters: null,
            where: where,
            orderBy: ToQueryOrderBy(
                query.OrderBy,
                query.Projections,
                query.GroupBy,
                SortAfterProjection(query)),
            limit: query.Limit,
            offset: query.Offset,
            parameters: ticket.Parameters,
            groupBy: query.GroupBy,
            having: query.Having?.Expression,
            rowNameResolver: rowNameResolver,
            analyzedWhere: analyzedWhere,
            existsSubqueries: existsSubqueries,
            isDistinct: query.IsDistinct,
            selectQuery: query,
            semiJoinSpecs: semiJoinSpecs,
            preparedInSets: preparedInSets,
            exclusivePredicateLocks: exclusivePredicateLocks,
            cacheHint: suppressCacheHint ? null : query.CacheHint);
    }

    private static IReadOnlyDictionary<NodeAst, PreparedInSet>? BuildPreparedInSets(PredicateAnalysis analysis)
    {
        if (analysis.InListComparisons.Count == 0)
            return null;

        // Key by node reference — the same NodeAst object appears in both the WHERE tree
        // (traversed by QueryFilterer) and in AnalyzedInList.Conjunct.
        Dictionary<NodeAst, PreparedInSet> sets = new(
            analysis.InListComparisons.Count,
            ReferenceEqualityComparer.Instance);

        foreach (AnalyzedInList inList in analysis.InListComparisons)
            sets[inList.Conjunct] = new PreparedInSet(inList.Values);

        return sets;
    }

    /// <summary>
    /// Resolves legacy ticket table metadata for single-table scans and multi-source queries.
    /// Derived sources use the first inner base table when available, otherwise the derived alias.
    /// </summary>
    private static TableSource GetPrimaryTableSource(QuerySource source)
    {
        return source switch
        {
            TableSource tableSource => tableSource,
            DerivedTableSource derivedSource => GetPrimaryTableSourceFromDerived(derivedSource),
            JoinSource joinSource => GetPrimaryTableSource(joinSource.Left),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput,$"Unsupported query source: {source.GetType().Name}"),
        };
    }

    private static TableSource GetPrimaryTableSourceFromDerived(DerivedTableSource derived)
    {
        TableSource? innerTable = TryGetFirstTableSource(derived.Query.Source);

        if (innerTable is not null)
            return innerTable;

        return new TableSource(derived.Alias, Alias: derived.Alias);
    }

    private static TableSource? TryGetFirstTableSource(QuerySource source)
    {
        switch (source)
        {
            case TableSource tableSource:
                return tableSource;

            case DerivedTableSource derivedSource:
                return TryGetFirstTableSource(derivedSource.Query.Source);

            case JoinSource joinSource:
                return TryGetFirstTableSource(joinSource.Left) ?? TryGetFirstTableSource(joinSource.Right);

            default:
                return null;
        }
    }

    private static List<QueryOrderBy>? ToQueryOrderBy(
        IReadOnlyList<OrderByItem>? orderBy,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        bool sortAfterProjection)
    {
        if (orderBy is null || orderBy.Count == 0)
            return null;

        List<QueryOrderBy> clauses = new(orderBy.Count);

        foreach (OrderByItem item in orderBy)
            clauses.Add(ResolveOrderClause(item, projections, groupBy, sortAfterProjection));

        return clauses;
    }

    /// <summary>
    /// Binds one ORDER BY item to either a row key or a per-row expression.
    ///
    /// <para>A grouped or aggregate query sorts <em>after</em> projection, so its keys resolve to
    /// output column names exactly as before. A plain SELECT sorts <em>before</em> projection, so an
    /// explicit alias is resolved back to the expression it names — the same precedence, reached from
    /// the other side. When the result is a bare column the clause stays a row key, which keeps the
    /// ordinal-resolving fast path in the comparer for every ordinary sort.</para>
    ///
    /// <para>Anything else becomes an expression clause. It used to raise
    /// <c>InvalidInternalOperation</c> here, which reported a caller's valid-looking SQL as an engine
    /// fault.</para>
    /// </summary>
    private static QueryOrderBy ResolveOrderClause(
        OrderByItem item,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        bool sortAfterProjection)
    {
        NodeAst expression = item.Expression;

        if (sortAfterProjection
            && QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
                expression,
                projections,
                groupBy,
                out string outputName))
        {
            return new QueryOrderBy(outputName, item.Direction);
        }

        if (!sortAfterProjection
            && QueryProjectionResolver.TryResolveProjectionAliasTarget(expression, projections, out NodeAst aliased))
        {
            expression = aliased;
        }

        if (expression.nodeType == NodeType.Identifier)
            return new QueryOrderBy(expression.yytext ?? "", item.Direction);

        if (sortAfterProjection)
        {
            // Rows here have already been reduced to grouped output, so a base column the expression
            // names is simply gone by the time the sort runs. There is nothing to compute it from.
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "ORDER BY expression must appear in the select list or GROUP BY clause when the query groups or aggregates");
        }

        RequirePerRowOrderExpression(expression);

        return new QueryOrderBy(DescribeOrderExpression(expression), item.Direction, expression);
    }

    /// <summary>
    /// Rejects ordering expressions that cannot be evaluated against a single row.
    ///
    /// <para>An aggregate is legal in a grouped query's ORDER BY, but that form never reaches here:
    /// it resolves to an output column on the post-projection path above. Reaching this method means
    /// the aggregate has no GROUP BY to belong to, so it is a caller error rather than a shape the
    /// sorter should attempt.</para>
    ///
    /// <para>These raise <c>InvalidInput</c> so an unsupported query reads as a rejected statement,
    /// never as an internal fault.</para>
    /// </summary>
    private static void RequirePerRowOrderExpression(NodeAst expression)
    {
        if (ContainsSubquery(expression))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "ORDER BY does not support subqueries");

        if (QueryExpressionClassifier.IsAggregateProjection(expression)
            || QueryExpressionClassifier.IsCompoundAggregateProjection(expression))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "ORDER BY cannot use an aggregate function without GROUP BY");
        }
    }

    private static bool ContainsSubquery(NodeAst? expression)
    {
        if (expression is null)
            return false;

        if (expression.nodeType is NodeType.ExprScalarSubquery
            or NodeType.ExprInSubquery
            or NodeType.ExprNotInSubquery
            or NodeType.ExprExistsSubquery
            or NodeType.ExprExistsCorrelated)
        {
            return true;
        }

        return ContainsSubquery(expression.leftAst)
            || ContainsSubquery(expression.rightAst)
            || ContainsSubquery(expression.extendedOne)
            || ContainsSubquery(expression.extendedTwo)
            || ContainsSubquery(expression.extendedThree)
            || ContainsSubquery(expression.extendedFour)
            || ContainsSubquery(expression.extendedFive);
    }

    /// <summary>
    /// Short label for an expression key. It exists for error messages and plan output only — an
    /// expression clause is never looked up by name — so a function call reports its own name rather
    /// than a rendering of its arguments.
    /// </summary>
    private static string DescribeOrderExpression(NodeAst expression)
    {
        if (expression.nodeType == NodeType.ExprFuncCall && expression.leftAst?.yytext is { } functionName)
            return $"{functionName}(…)";

        return expression.nodeType.ToString();
    }

    private static bool SortAfterProjection(SelectQuery query) =>
        query.GroupBy is { Count: > 0 }
        || (query.IsDistinct && !IsFullProjection(query.Projections));

    private static bool IsFullProjection(IReadOnlyList<ProjectionItem> projections) =>
        projections is [{ Expression.nodeType: NodeType.ExprAllFields }];
}
