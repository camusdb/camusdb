
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
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

    private readonly SelectQueryCreator selectQueryCreator = new();

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

    /// <summary>
    /// Binds the query's sources to catalog objects and validates every column reference, for the
    /// schema of the query only: a SELECT nested in the source tree binds through this binder alone,
    /// with its subquery nodes left in place, so the result must not run. Every executed statement
    /// binds through <see cref="BindAsync(DatabaseDescriptor, SelectQuery, NestedSelectBinder?)"/>.
    /// <para><b>Parameter independence (audited):</b> the result is a function of the query and the
    /// catalog only. Nothing here reads a parameter value or transaction state, so a binding is
    /// reusable across executions with different parameters — which the bound-query cache does for
    /// single-table statements, guarded by the source table's schema stamp. Two caveats the cache
    /// must respect (and does, by excluding derived sources): binding a derived source consults the
    /// ambient <see cref="AuthorizationContext"/> to run a view body as its owner, and table opens
    /// enforce the caller's privileges — the latter stays per-execution because a cache hit re-opens
    /// the table through the same chokepoint. A change that makes the single-table path read
    /// per-execution state must also make that shape ineligible for the cache.</para>
    /// </summary>
    public Task<BoundSelectQuery> BindAsync(DatabaseDescriptor database, SelectQuery query) =>
        BindAsync(database, query, nested: null);

    /// <summary>
    /// Binds the query as the overload above does, and binds every SELECT nested in its source tree
    /// — the inner query of a derived table, each operand of a set operation — through
    /// <paramref name="nested"/> when one is given, so those SELECTs resolve their own subqueries and
    /// can run. With a nested binder the result is no longer parameter independent: the nested
    /// subquery stages read storage and parameter values. The bound-query cache is unaffected, since
    /// it already excludes every shape that has a derived source.
    /// </summary>
    public async Task<BoundSelectQuery> BindAsync(DatabaseDescriptor database, SelectQuery query, NestedSelectBinder? nested)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];

        // Case-insensitive: aliases resolve case-insensitively downstream, so "t a JOIN u A"
        // is a genuine duplicate and must be rejected here with a clear error.
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases, nested).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources, derivedSources);

        QueryRowNameResolver rowNames = new(sources, derivedSources);
        BoundSelectQuery bound = new(query, sources, rowNames, derivedSources);
        ValidateQuery(query, rowNames, ResolveOrderByPositions(bound));

        // The statement checks run here, before any row is read, so an unknown function, a known type
        // mismatch, a non-boolean condition, or a CASE, COALESCE, IFNULL or IF with no common type fails
        // the statement over an empty table and under LIMIT 0 too.
        ConditionalResultTypes.CheckQuery(bound);

        return bound;
    }

    /// <summary>
    /// Binds a subquery shell for correlated EXISTS execution. Inner WHERE may reference outer
    /// columns and is validated separately by the caller. A SELECT nested in the shell's source tree
    /// binds through <paramref name="nested"/>, as in the main overload.
    /// </summary>
    internal async Task<BoundSelectQuery> BindSubqueryAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        NestedSelectBinder? nested)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases, nested).ConfigureAwait(false);
        ValidateJoinPredicates(query.Source, sources, derivedSources);

        QueryRowNameResolver rowNames = new(sources, derivedSources);

        QueryLimiter.RefuseNegativeLiteral(query);

        foreach (ProjectionItem projection in query.Projections)
            ValidateExpression(projection.Expression, rowNames);

        if (query.GroupBy is not null)
        {
            foreach (NodeAst groupExpr in query.GroupBy)
            {
                // A group key is matched to the select list by its shape, and the subquery rewrite
                // does not visit it, so a subquery there would reach the evaluator unresolved.
                if (QueryTicketAdapter.ContainsSubquery(groupExpr))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "GROUP BY does not support subqueries");

                ValidateNoAggregateInPreAggregateScope(groupExpr, "GROUP BY");
                ValidateExpression(groupExpr, rowNames);
            }
        }

        BoundSelectQuery bound = new(query, sources, rowNames, derivedSources);

        ValidateOrderBy(query, rowNames, ResolveOrderByPositions(bound));
        ValidateProjectionAndGrouping(query);

        ConditionalResultTypes.CheckQuery(bound);

        return bound;
    }

    /// <summary>
    /// The names that <paramref name="source"/> declares, for <see cref="SubqueryScopeAnalyzer"/>. The
    /// sources bind for their schema only, as a view definition does: a table opens and a derived
    /// table binds with no nested binder, so nothing runs and no row is read.
    ///
    /// <para>A derived table inside a subquery can read the query around the subquery, which this bind
    /// does not have, so its bind alone can fail with an unknown alias. Then its names come from its select
    /// list (<see cref="DeclaredNamesAsync"/>). The bind of the subquery itself checks it later, with the
    /// outer names in scope.</para>
    /// </summary>
    internal async Task<NameScope> BindNameScopeAsync(DatabaseDescriptor database, QuerySource source)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);
        List<(string Alias, IReadOnlyCollection<string> Columns)> declared = [];

        Stack<QuerySource> pending = new();
        pending.Push(source);

        while (pending.TryPop(out QuerySource? current))
        {
            if (current is JoinSource join)
            {
                pending.Push(join.Right);
                pending.Push(join.Left);
                continue;
            }

            if (current is not DerivedTableSource { Function: null } derived)
            {
                await CollectBoundSourcesAsync(database, current, sources, derivedSources, aliases, nested: null).ConfigureAwait(false);
                continue;
            }

            try
            {
                await CollectBoundSourcesAsync(database, derived, sources, derivedSources, aliases, nested: null).ConfigureAwait(false);
            }
            catch (CamusDBException)
            {
                declared.Add((derived.Alias, await DeclaredNamesAsync(database, derived.Query).ConfigureAwait(false)));
            }
        }

        return new NameScope(sources, derivedSources, declared);
    }

    /// <summary>
    /// The output names of <paramref name="query"/>, read from its select list: an alias, or the column part of
    /// a name, and for a <c>*</c> every column that its FROM declares. A set operation takes the names of its
    /// first operand. An expression with no alias has no name that a statement can write.
    ///
    /// <para>Used only where the query cannot bind alone. The names can be more than the query gives (a
    /// <c>t.*</c> adds the columns of every source), never fewer: a name that the subquery has must not be read
    /// as a name of the outer query, because that would give another answer. A name too many in the scope only
    /// makes a mistake fail as an unknown column.</para>
    /// </summary>
    private async Task<IReadOnlyCollection<string>> DeclaredNamesAsync(DatabaseDescriptor database, SelectQuery query)
    {
        while (query.Source is SetOperationSource setOperation)
            query = setOperation.Left;

        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

        foreach (ProjectionItem item in query.Projections)
        {
            if (QueryExpressionClassifier.UnwrapAlias(item.Expression).nodeType == NodeType.ExprAllFields)
            {
                NameScope inner = await BindNameScopeAsync(database, query.Source).ConfigureAwait(false);
                names.UnionWith(inner.ColumnNames);
                continue;
            }

            if (item.OutputName is { } name)
                names.Add(name[(name.LastIndexOf('.') + 1)..]);
        }

        return names;
    }

    /// <summary>
    /// Binds <paramref name="source"/> for its schema, as <see cref="BindNameScopeAsync"/> does, and returns
    /// the bound sources. Used for the target table of an UPDATE or DELETE, which is the outer query of a
    /// correlated EXISTS in its WHERE.
    /// </summary>
    internal async Task<(IReadOnlyList<BoundTableSource> Sources, IReadOnlyList<BoundDerivedTableSource> DerivedSources)> BindSourcesAsync(
        DatabaseDescriptor database,
        QuerySource source)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, source, sources, derivedSources, aliases, nested: null).ConfigureAwait(false);

        return (sources, derivedSources);
    }

    /// <summary>
    /// Finds the statement mistakes in the output of an EXISTS body whose output
    /// <see cref="ExistsBody"/> discards. The same checks run as for any SELECT, except that a select
    /// list needs only grouped columns, not the exact group key: PostgreSQL accepts
    /// <c>EXISTS (SELECT v + 1 FROM r GROUP BY v)</c>.
    ///
    /// <para>This reads no row and evaluates nothing, and a subquery in the output does not run. That is
    /// why it binds with no nested binder, as CREATE VIEW does, and walks the subqueries itself.</para>
    /// </summary>
    /// <param name="query">The body as the statement wrote it.</param>
    /// <param name="outerSources">The sources of the outer query, which a correlated body can read.</param>
    /// <param name="outerDerivedSources">The derived tables of the outer query.</param>
    internal async Task CheckDiscardedOutputAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        List<BoundTableSource> sources = [];
        List<BoundDerivedTableSource> derivedSources = [];
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

        await CollectBoundSourcesAsync(database, query.Source, sources, derivedSources, aliases, nested: null).ConfigureAwait(false);

        // A * reads the sources of the body only, so its positions resolve before the outer sources join.
        IReadOnlyList<OrderByItem>? orderBy = ResolveOrderByPositions(
            new BoundSelectQuery(query, [.. sources], new QueryRowNameResolver(sources, derivedSources), [.. derivedSources]));

        int nestedSourceCount = sources.Count;
        int nestedDerivedCount = derivedSources.Count;

        sources.AddRange(outerSources);
        derivedSources.AddRange(outerDerivedSources);

        // An uncorrelated body has no outer source here, so it reads its own row keys as usual.
        QueryRowNameResolver rowNames = outerSources.Count + outerDerivedSources.Count == 0
            ? new QueryRowNameResolver(sources, derivedSources)
            : new QueryRowNameResolver(sources, derivedSources, nestedSourceCount, nestedDerivedCount);

        ValidateQuery(query, rowNames, orderBy, outputIsDiscarded: true);
        ConditionalResultTypes.CheckQuery(new BoundSelectQuery(query, sources, rowNames, derivedSources));

        await CheckSubqueriesOfOutputAsync(database, query).ConfigureAwait(false);
    }

    /// <summary>
    /// Binds each subquery in the output of <paramref name="query"/>. The subquery rewrite would run them,
    /// so without this walk a discarded subquery could hold an unknown column with no error.
    /// </summary>
    private async Task CheckSubqueriesOfOutputAsync(DatabaseDescriptor database, SelectQuery query)
    {
        foreach (ProjectionItem projection in query.Projections)
            await CheckSubqueriesAsync(database, projection.Expression).ConfigureAwait(false);

        if (query.OrderBy is not null)
            foreach (OrderByItem item in query.OrderBy)
                await CheckSubqueriesAsync(database, item.Expression).ConfigureAwait(false);
    }

    private async Task CheckSubqueriesAsync(DatabaseDescriptor database, NodeAst expression)
    {
        // An async frame is large, and subqueries can nest. See StatementDepthGuard.HasStackHeadroom.
        if (!StatementDepthGuard.HasStackHeadroom())
        {
            await Task.Run(() => CheckSubqueriesAsync(database, expression)).ConfigureAwait(false);
            return;
        }

        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out NodeAst? node))
        {
            if (node.nodeType == NodeType.Select)
            {
                SelectQuery subquery = selectQueryCreator.CreateSelectQuery(node);
                await BindAsync(database, subquery, nested: null).ConfigureAwait(false);
                await CheckSubqueriesOfOutputAsync(database, subquery).ConfigureAwait(false);
                continue;
            }

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null)
                    pending.Push(child);
            }
        }
    }

    /// <summary>
    /// The grouping rule of a discarded select list: every column it reads must be grouped, or the whole
    /// expression must be a group key.
    /// </summary>
    private static void ValidateDiscardedGrouping(SelectQuery query, QueryRowNameResolver rowNames)
    {
        if (query.GroupBy is not { Count: > 0 } groupBy)
            return;

        foreach (ProjectionItem projection in query.Projections)
        {
            NodeAst expression = QueryExpressionClassifier.UnwrapAlias(projection.Expression);

            if (expression.nodeType == NodeType.ExprAllFields)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Non-aggregate projection must appear in GROUP BY");

            if (IsExpressionListedInGroupBy(expression, groupBy))
                continue;

            List<NodeAst> columns = [];
            QueryExpressionClassifier.CollectNonAggregateColumnRefs(expression, columns);

            foreach (NodeAst column in columns)
            {
                if (!IsColumnGrouped(column, groupBy, rowNames))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{column.yytext}' must appear in the GROUP BY clause or be used in an aggregate function");
                }
            }
        }
    }

    /// <summary>Compares resolved names, because <c>v</c> and <c>r.v</c> can be one column.</summary>
    private static bool IsColumnGrouped(NodeAst column, IReadOnlyList<NodeAst> groupBy, QueryRowNameResolver rowNames)
    {
        string key = rowNames.ResolveRowLookupKey(column.yytext!);

        foreach (NodeAst groupExpression in groupBy)
        {
            if (groupExpression.nodeType == NodeType.Identifier
                && string.Equals(rowNames.ResolveRowLookupKey(groupExpression.yytext!), key, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private async Task CollectBoundSourcesAsync(
        DatabaseDescriptor database,
        QuerySource source,
        List<BoundTableSource> sources,
        List<BoundDerivedTableSource> derivedSources,
        HashSet<string> aliases,
        NestedSelectBinder? nested)
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

                // A function in FROM has no inner query to bind: its columns and types come from the
                // function, and it opens no table, so there is nothing to authorize.
                if (derivedSource.Function is { } call)
                {
                    TableFunction function = TableFunctionRegistry.Resolve(call.FunctionName, call.Arguments.Count);

                    // The arguments run only when the source is read, and a LIMIT 0 reads nothing.
                    foreach (NodeAst argument in call.Arguments)
                        ConditionalResultTypes.Check(argument, lookup: null, recorder: null, allowAggregates: false);

                    List<DerivedColumnSchema> functionColumns = new(call.ColumnNames.Count);
                    if (function.OutputColumns is { } outputs)
                    {
                        for (int i = 0; i < outputs.Count; i++)
                            functionColumns.Add(new DerivedColumnSchema(call.ColumnNames[i], outputs[i].Type));
                    }
                    else
                    {
                        functionColumns.Add(new DerivedColumnSchema(call.ColumnNames[0], function.OutputType) { ElementType = function.OutputElementType });
                    }

                    derivedSources.Add(new BoundDerivedTableSource(
                        derivedSource,
                        derivedSource.Alias,
                        functionColumns,
                        new BoundSelectQuery(derivedSource.Query, [], new QueryRowNameResolver([]))));
                    return;
                }

                // A set operation has no inner query to bind either: its two operands are bound
                // instead, each one as a complete query of its own, so each one opens its own tables
                // and so needs SELECT on them. The shell around them has no sources.
                if (derivedSource.SetOperation is { } setOperation)
                {
                    BoundSetOperation bound = await BindSetOperationAsync(database, setOperation, nested).ConfigureAwait(false);

                    derivedSources.Add(new BoundDerivedTableSource(
                        derivedSource,
                        derivedSource.Alias,
                        bound.Columns,
                        new BoundSelectQuery(derivedSource.Query, [], new QueryRowNameResolver([])))
                    {
                        SetOperation = bound,
                    });
                    return;
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

                // The whole inner bind runs inside the scope, the subquery stages included: an IN or
                // EXISTS in a view body reads its table as the view's owner, like the body's FROM.
                (BoundSelectQuery innerBound, SubqueryBindProducts innerSubqueries) =
                    await BindNestedAsync(database, derivedSource.Query, nested).ConfigureAwait(false);

                // The columns come from the bound query, which the derived-table executor runs: there the
                // pipeline folded each uncorrelated subquery into its value, or into a NULL that keeps the
                // subquery's column type. The query as written still holds the subquery node, which has
                // no type, so its column fell back to String and a CTAS refused the Int64 value.
                IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.WithPlainRowKeys(
                    DerivedTableSchemaBuilder.Build(innerBound.Query, innerBound),
                    out (string InnerKey, string RowKey)[]? innerRowKeys);

                // A windowed SELECT's derived table also holds one result column per window call,
                // which the window operator adds to each row after the inner query ran.
                if (derivedSource.Windows is { } windows)
                    columns = WindowResultSchema.Build(columns, windows);

                derivedSources.Add(new BoundDerivedTableSource(
                    derivedSource,
                    derivedSource.Alias,
                    columns,
                    innerBound)
                {
                    InnerSubqueries = innerSubqueries,
                    InnerRowKeys = innerRowKeys,
                });
                return;
            }

            case JoinSource joinSource:
                await CollectBoundSourcesAsync(database, joinSource.Left, sources, derivedSources, aliases, nested)
                    .ConfigureAwait(false);
                await CollectBoundSourcesAsync(database, joinSource.Right, sources, derivedSources, aliases, nested)
                    .ConfigureAwait(false);
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported query source: {source.GetType().Name}");
        }
    }

    /// <summary>
    /// Binds a SELECT met inside a source tree. Through <paramref name="nested"/> it runs the whole
    /// bind pipeline, so its subqueries are resolved and the products its ticket needs come back with
    /// it. Without one it binds for its schema only, and the products are default.
    /// </summary>
    private async Task<(BoundSelectQuery Bound, SubqueryBindProducts Subqueries)> BindNestedAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        NestedSelectBinder? nested)
    {
        if (nested is { } binder)
        {
            SelectBindResult result = await binder.BindAsync(database, query).ConfigureAwait(false);
            return (result.Bound, result.Subqueries);
        }

        return (await BindAsync(database, query, nested: null).ConfigureAwait(false), default);
    }

    /// <summary>
    /// Binds both operands of a set operation and resolves its output columns
    /// (<see cref="SetOperationSchema"/>). The operands bind in the authorization scope that is
    /// current here: the caller's for a set operation the statement writes, the view owner's for
    /// one that is a view's body, because the derived-table case above enters that scope before it
    /// binds the body. An operand that is itself an expanded view swaps to that view's owner for its
    /// own subtree only, exactly as it would in a plain FROM.
    /// </summary>
    private async Task<BoundSetOperation> BindSetOperationAsync(
        DatabaseDescriptor database,
        SetOperationSource setOperation,
        NestedSelectBinder? nested)
    {
        (BoundSelectQuery left, SubqueryBindProducts leftSubqueries) =
            await BindNestedAsync(database, setOperation.Left, nested).ConfigureAwait(false);
        (BoundSelectQuery right, SubqueryBindProducts rightSubqueries) =
            await BindNestedAsync(database, setOperation.Right, nested).ConfigureAwait(false);

        // The columns come from each bound operand, as for a derived table above: there each
        // uncorrelated subquery is folded into its value, or into a NULL that keeps its column type.
        IReadOnlyList<DerivedColumnSchema> leftColumns = DerivedTableSchemaBuilder.Build(left.Query, left);
        IReadOnlyList<DerivedColumnSchema> rightColumns = DerivedTableSchemaBuilder.Build(right.Query, right);

        (IReadOnlyList<DerivedColumnSchema> columns, IReadOnlyList<bool> untyped) =
            SetOperationSchema.Resolve(setOperation, left, leftColumns, right, rightColumns);

        return new BoundSetOperation(setOperation, left, right, leftColumns, rightColumns, columns, untyped)
        {
            LeftSubqueries = leftSubqueries,
            RightSubqueries = rightSubqueries,
        };
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

    /// <summary>
    /// Validates projections, WHERE, GROUP BY, ORDER BY, HAVING, and DISTINCT against the resolved
    /// sources.
    /// <para><b>Parameter independence (audited):</b> validation walks identifiers and expression
    /// structure only; placeholder nodes are never evaluated. It is also what populates the
    /// resolver's lookup memo with every identifier the statement references, which is the
    /// precondition for freezing that resolver when the binding is shared across concurrent
    /// executions (<see cref="QueryRowNameResolver.FreezeForSharedReuse"/>).</para>
    /// </summary>
    /// <param name="orderBy">The ORDER BY of <paramref name="query"/> with its positions resolved
    /// (<see cref="ResolveOrderByPositions"/>).</param>
    private static void ValidateQuery(
        SelectQuery query,
        QueryRowNameResolver rowNames,
        IReadOnlyList<OrderByItem>? orderBy,
        bool outputIsDiscarded = false)
    {
        QueryLimiter.RefuseNegativeLiteral(query);

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
                // A group key is matched to the select list by its shape, and the subquery rewrite
                // does not visit it, so a subquery there would reach the evaluator unresolved.
                if (QueryTicketAdapter.ContainsSubquery(groupExpr))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "GROUP BY does not support subqueries");

                ValidateNoAggregateInPreAggregateScope(groupExpr, "GROUP BY");
                ValidateExpression(groupExpr, rowNames);
            }
        }

        ValidateOrderBy(query, rowNames, orderBy);

        if (query.Having is not null)
            QueryExpressionClassifier.ValidateNoNestedAggregate(query.Having.Expression);

        QueryPostAggregateScopeValidator.ValidateHaving(query, rowNames);

        ValidateDistinct(query);

        if (outputIsDiscarded)
            ValidateDiscardedGrouping(query, rowNames);
        else
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

    private static void ValidateOrderBy(SelectQuery query, QueryRowNameResolver rowNames, IReadOnlyList<OrderByItem>? resolvedOrderBy)
    {
        if (resolvedOrderBy is null)
            return;

        bool sortAfterProjection = query.GroupBy is { Count: > 0 }
            || (query.IsDistinct && !IsFullProjection(query.Projections));

        bool ungroupedAggregate = QueryTicketAdapter.IsUngroupedAggregate(query);

        foreach (OrderByItem item in resolvedOrderBy)
        {
            // A column of a *, named by its position: its cell is known, so nothing is left to check.
            if (item.RowKey is not null)
                continue;

            OrderByItem orderBy = item;

            QueryProjectionResolver.RejectAmbiguousOrderName(
                orderBy.Expression,
                query.Projections,
                (first, other) => SameOrderTarget(first, other, rowNames),
                name => StarColumnOf(name, rowNames));

            QueryExpressionClassifier.ValidateNoNestedAggregate(orderBy.Expression);

            if (ungroupedAggregate)
            {
                ValidateUngroupedAggregateOrderKey(orderBy.Expression, query, rowNames);
                continue;
            }

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

            // A plain SELECT sorts before it projects, so an explicit select-list alias is resolved
            // back to the expression it names and that expression is what gets validated. Validating
            // the alias itself would reject `SELECT name AS n … ORDER BY n` as an unknown column,
            // since no row carries a column called "n". Same precedence as the grouped path above —
            // an alias outranks a base column of the same name — reached from the other side.
            NodeAst validated =
                QueryProjectionResolver.TryResolveProjectionAliasTarget(orderBy.Expression, query.Projections, out NodeAst aliased)
                    ? aliased
                    : orderBy.Expression;

            ValidateExpression(validated, rowNames);
        }
    }

    /// <summary>
    /// Resolves each ORDER BY position at or after a <c>*</c> (see <see cref="OrderByItem.OutputPosition"/>)
    /// against the output columns, as PostgreSQL counts them: a <c>*</c> counts once per column it puts out.
    /// A position that names a column of a <c>*</c> gets the row key of that column's cell. A position that
    /// names an item after the <c>*</c> becomes that item's expression, as a position before the <c>*</c>
    /// already did in the query creator.
    ///
    /// <para><b>Why the query keeps its positions.</b> The stages around the binder keep copies of the query
    /// as it was before the bind (the bind pipeline rebuilds the bound query from its own copy after the
    /// EXISTS preparation), so a rewrite of the bound query would be lost. The binder therefore resolves the
    /// positions only to check them, and the ticket resolves them again from the bound sources
    /// (<c>QueryTicketAdapter.ToQueryTicket</c>). Both reach the same columns, because both read the same
    /// sources.</para>
    /// </summary>
    /// <returns>The ORDER BY of <paramref name="bound"/>, the same list when it holds no such position, or null
    /// when the query has no ORDER BY.</returns>
    internal static IReadOnlyList<OrderByItem>? ResolveOrderByPositions(BoundSelectQuery bound)
    {
        SelectQuery query = bound.Query;

        if (query.OrderBy is not { } orderBy || !HasUnresolvedPosition(orderBy))
            return query.OrderBy;

        List<DerivedColumnSchema> starColumns = DerivedTableSchemaBuilder.ExpandStar(bound);
        List<OrderByItem> resolved = new(orderBy.Count);

        foreach (OrderByItem item in orderBy)
        {
            resolved.Add(item.OutputPosition is int position && item.RowKey is null
                ? ResolveOutputPosition(item, position, query.Projections, starColumns)
                : item);
        }

        return resolved;
    }

    private static bool HasUnresolvedPosition(IReadOnlyList<OrderByItem> orderBy)
    {
        foreach (OrderByItem item in orderBy)
        {
            if (item.OutputPosition is not null && item.RowKey is null)
                return true;
        }

        return false;
    }

    private static OrderByItem ResolveOutputPosition(
        OrderByItem item,
        int position,
        IReadOnlyList<ProjectionItem> projections,
        List<DerivedColumnSchema> starColumns)
    {
        int remaining = position;

        foreach (ProjectionItem projection in projections)
        {
            NodeAst target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);
            bool isStar = target.nodeType == NodeType.ExprAllFields;
            int width = isStar ? starColumns.Count : 1;

            if (remaining <= width)
            {
                return isStar
                    ? item with { RowKey = starColumns[remaining - 1].RowKey }
                    : new OrderByItem(target, item.Direction);
            }

            remaining -= width;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"ORDER BY position {position} is out of range");
    }

    /// <summary>
    /// True when two select-list expressions that an ORDER BY name matches are one meaning: the same tree,
    /// or two column references to the same input column (<c>r.id</c> and <c>id</c> over one table). A
    /// reference that does not resolve is never the same as another one; its own error comes later.
    /// </summary>
    private static bool SameOrderTarget(NodeAst first, NodeAst other, QueryRowNameResolver rowNames)
    {
        if (QueryAstComparer.AreEquivalent(first, other))
            return true;

        if (first.nodeType != NodeType.Identifier || other.nodeType != NodeType.Identifier
            || first.yytext is null || other.yytext is null)
            return false;

        return TryLookupKey(first.yytext, rowNames) is { } firstKey
            && string.Equals(firstKey, TryLookupKey(other.yytext, rowNames), StringComparison.Ordinal);
    }

    /// <summary>
    /// The input column that a <c>*</c> item puts out under <paramref name="name"/>, as a column reference,
    /// or null when no input column has the name or the name is ambiguous among the inputs (its own error
    /// comes later).
    /// </summary>
    private static NodeAst? StarColumnOf(string name, QueryRowNameResolver rowNames) =>
        TryLookupKey(name, rowNames) is null
            ? null
            : new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, name);

    /// <summary>
    /// The row key of a column reference, or null when it does not resolve. Only the ORDER BY ambiguity
    /// check calls it, at bind time, for a name that two select-list items have or that a <c>*</c> item
    /// may put out, so the exception path costs nothing on the execution of a bound query.
    /// </summary>
    private static string? TryLookupKey(string identifier, QueryRowNameResolver rowNames)
    {
        try
        {
            return rowNames.ResolveRowLookupKey(identifier);
        }
        catch (CamusDBException)
        {
            return null;
        }
    }

    /// <summary>
    /// Checks one ORDER BY key of an aggregate query with no GROUP BY, by the PostgreSQL rule. The query
    /// has one output row, so the key orders nothing and the sort never runs
    /// (<see cref="QueryTicketAdapter.IsUngroupedAggregate"/>), but the statement must still make sense.
    /// An output alias, an aggregate and a constant are valid keys. A column outside an aggregate is not,
    /// because that row has no one value for the column. The error does not depend on LIMIT or on the
    /// table contents, because this runs at bind time.
    /// </summary>
    private static void ValidateUngroupedAggregateOrderKey(NodeAst key, SelectQuery query, QueryRowNameResolver rowNames)
    {
        if (QueryTicketAdapter.ContainsSubquery(key))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ORDER BY does not support subqueries");

        if (QueryProjectionResolver.TryResolvePostAggregateOrderColumn(key, query.Projections, null, out _))
            return;

        ValidateExpression(key, rowNames);

        List<NodeAst> columns = [];
        QueryExpressionClassifier.CollectNonAggregateColumnRefs(key, columns);

        if (columns.Count > 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Column '{columns[0].yytext}' must appear in the GROUP BY clause or be used in an aggregate function");
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

        // Several aggregates are fine: without GROUP BY they all reduce the same single group. A bare
        // column or other non-aggregate projection is not, because there is no group key to bind it to.
        if (hasNonAggregateProjection)
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
