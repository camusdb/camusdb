
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Turns a logical <see cref="SelectQuery"/> into a <see cref="BoundSelectQuery"/> the operators can
/// run, by running the four bind stages in their one correct order:
/// <list type="number">
/// <item><see cref="SemiJoinAnalyzer"/> lifts eligible top-level <c>IN</c>/<c>NOT IN</c> subqueries
/// into semi/anti-join specs. It must run first, because the next stage would otherwise
/// materialize those subqueries into value lists.</item>
/// <item><see cref="SubqueryRewriter"/> executes the remaining uncorrelated scalar / <c>IN</c> /
/// <c>NOT IN</c> subqueries and folds their results into the WHERE as literals.</item>
/// <item><see cref="QueryBinder"/> opens the sources and resolves names.</item>
/// <item><see cref="ExistsSubqueryPreparer"/> prepares EXISTS subqueries. It runs last because a
/// correlated EXISTS binds against the outer sources the previous stage produced.</item>
/// </list>
///
/// <para>
/// Every SELECT that reaches the operators must go through here: the top-level statement, EXPLAIN,
/// and — the case that motivated pulling the sequence into one class — a SELECT nested inside another
/// statement's subquery. The subquery executors used to bind the inner SELECT directly, skipping
/// stages 1, 2 and 4, so an <c>IN (SELECT …)</c> whose own WHERE contained another
/// <c>IN (SELECT …)</c> reached the per-row evaluator unresolved and failed with "IN subquery must
/// be resolved before expression evaluation". Routing the inner SELECT through the same pipeline
/// makes nesting depth irrelevant: each level resolves its own subqueries before it is bound.
/// </para>
///
/// <para>
/// Stages 1, 2 and 4 read storage under the ticket's transaction, so this is not a pure function of
/// the AST; callers that cache a binding check the result's <see cref="SelectBindResult.Bound"/>
/// query instance against the one they passed in (see <c>SelectStatementExecutor</c>).
/// </para>
/// </summary>
internal sealed class SelectBindPipeline
{
    private readonly SemiJoinAnalyzer semiJoinAnalyzer;
    private readonly SubqueryRewriter subqueryRewriter;
    private readonly QueryBinder queryBinder;
    private readonly ExistsSubqueryPreparer existsSubqueryPreparer;

    public SelectBindPipeline(
        SemiJoinAnalyzer semiJoinAnalyzer,
        SubqueryRewriter subqueryRewriter,
        QueryBinder queryBinder,
        ExistsSubqueryPreparer existsSubqueryPreparer)
    {
        this.semiJoinAnalyzer = semiJoinAnalyzer;
        this.subqueryRewriter = subqueryRewriter;
        this.queryBinder = queryBinder;
        this.existsSubqueryPreparer = existsSubqueryPreparer;
    }

    /// <summary>
    /// Runs the bind stages over <paramref name="selectQuery"/>. Subquery stages execute against
    /// storage under <paramref name="ticket"/>'s transaction and with its parameters.
    /// </summary>
    public async ValueTask<SelectBindResult> BindAsync(
        DatabaseDescriptor database,
        SelectQuery selectQuery,
        ExecuteSQLTicket ticket)
    {
        selectQuery = await PrepareSetOperationOperandsAsync(database, selectQuery, ticket).ConfigureAwait(false);

        (selectQuery, List<SemiJoinSpec> semiJoinSpecs) = await semiJoinAnalyzer
            .AnalyzeAsync(database, selectQuery, ticket)
            .ConfigureAwait(false);

        selectQuery = await subqueryRewriter
            .RewriteSelectQueryAsync(database, selectQuery, ticket)
            .ConfigureAwait(false);

        BoundSelectQuery boundQuery = await queryBinder.BindAsync(database, selectQuery).ConfigureAwait(false);

        (selectQuery, ExistsSubqueryRegistry? existsRegistry) = await existsSubqueryPreparer
            .PrepareAsync(
                database,
                selectQuery,
                boundQuery.Sources,
                boundQuery.DerivedSources,
                ticket)
            .ConfigureAwait(false);

        if (!ReferenceEquals(selectQuery, boundQuery.Query))
        {
            boundQuery = new BoundSelectQuery(
                selectQuery,
                boundQuery.Sources,
                boundQuery.RowNames,
                boundQuery.DerivedSources);
        }

        return new SelectBindResult(boundQuery, existsRegistry, semiJoinSpecs);
    }

    /// <summary>
    /// Resolves the uncorrelated scalar, <c>IN</c> and <c>NOT IN</c> subqueries in the WHERE of every
    /// set-operation operand under <paramref name="query"/>, as stage 2 does for the query itself.
    ///
    /// <para>An operand is bound by <see cref="QueryBinder"/> as part of its parent's source, not
    /// through this pipeline, and runs with a ticket of its own. Without this step
    /// <c>SELECT a FROM t WHERE a IN (SELECT …) UNION …</c> would reach the per-row evaluator with
    /// the subquery unresolved. The walk goes through joins and derived tables to find the set
    /// operations, and rewrites only their operands. An operand's EXISTS stays as it is: it has no
    /// EXISTS-preparer stage, the same as the inner query of any derived table.</para>
    /// </summary>
    private async ValueTask<SelectQuery> PrepareSetOperationOperandsAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        QuerySource source = await PrepareSourceAsync(database, query.Source, ticket).ConfigureAwait(false);
        return ReferenceEquals(source, query.Source) ? query : query with { Source = source };
    }

    private async ValueTask<QuerySource> PrepareSourceAsync(
        DatabaseDescriptor database,
        QuerySource source,
        ExecuteSQLTicket ticket)
    {
        switch (source)
        {
            case DerivedTableSource { SetOperation: { } setOperation } derived:
            {
                SelectQuery left = await PrepareOperandAsync(database, setOperation.Left, ticket).ConfigureAwait(false);
                SelectQuery right = await PrepareOperandAsync(database, setOperation.Right, ticket).ConfigureAwait(false);

                if (ReferenceEquals(left, setOperation.Left) && ReferenceEquals(right, setOperation.Right))
                    return derived;

                return derived with { Query = derived.Query with { Source = setOperation with { Left = left, Right = right } } };
            }

            case DerivedTableSource derived:
            {
                SelectQuery inner = await PrepareSetOperationOperandsAsync(database, derived.Query, ticket).ConfigureAwait(false);
                return ReferenceEquals(inner, derived.Query) ? derived : derived with { Query = inner };
            }

            case JoinSource join:
            {
                QuerySource left = await PrepareSourceAsync(database, join.Left, ticket).ConfigureAwait(false);
                QuerySource right = await PrepareSourceAsync(database, join.Right, ticket).ConfigureAwait(false);

                return ReferenceEquals(left, join.Left) && ReferenceEquals(right, join.Right)
                    ? join
                    : join with { Left = left, Right = right };
            }

            default:
                return source;
        }
    }

    private async ValueTask<SelectQuery> PrepareOperandAsync(
        DatabaseDescriptor database,
        SelectQuery operand,
        ExecuteSQLTicket ticket)
    {
        operand = await PrepareSetOperationOperandsAsync(database, operand, ticket).ConfigureAwait(false);
        return await subqueryRewriter.RewriteSelectQueryAsync(database, operand, ticket).ConfigureAwait(false);
    }
}

/// <summary>
/// What <see cref="SelectBindPipeline.BindAsync"/> produces: the bound query plus the two side
/// products the query ticket must carry so the operators can finish what the bind stages started.
/// </summary>
/// <param name="Bound">The bound query. Its <see cref="BoundSelectQuery.Query"/> is the same
/// instance the caller passed in only when no stage changed the statement.</param>
/// <param name="ExistsRegistry">Prepared EXISTS subqueries, or null when the WHERE has none.</param>
/// <param name="SemiJoinSpecs">Semi/anti-join specs lifted out of the WHERE; empty when none.</param>
internal readonly record struct SelectBindResult(
    BoundSelectQuery Bound,
    ExistsSubqueryRegistry? ExistsRegistry,
    List<SemiJoinSpec> SemiJoinSpecs)
{
    /// <summary>The specs in the null-when-empty shape the query ticket adapter expects.</summary>
    public IReadOnlyList<SemiJoinSpec>? SemiJoinSpecsOrNull => SemiJoinSpecs.Count > 0 ? SemiJoinSpecs : null;
}
