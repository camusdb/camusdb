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
/// a SELECT nested inside another statement's subquery, the inner query of a derived table, and
/// each operand of a set operation. The subquery executors used to bind the inner SELECT directly,
/// skipping stages 1, 2 and 4, so an <c>IN (SELECT …)</c> whose own WHERE contained another
/// <c>IN (SELECT …)</c> reached the per-row evaluator unresolved and failed with "IN subquery must
/// be resolved before expression evaluation". The binder then had the same gap for the SELECTs it
/// meets in a source tree — a derived table's inner query and a set operation's operands bound
/// through <see cref="QueryBinder.BindAsync(DatabaseDescriptor, SelectQuery)"/> alone, so a
/// <c>FROM (SELECT … WHERE EXISTS (…)) d</c> failed the same way. Stage 3 therefore receives a
/// <see cref="NestedSelectBinder"/>, and binds every nested SELECT through this pipeline again, so
/// each level resolves its own subqueries before it is bound, whatever the nesting.
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
    /// storage under <paramref name="ticket"/>'s transaction and with its parameters. A nested SELECT
    /// in the source tree runs the same stages with the same ticket, inside the authorization scope
    /// the binder gives it: an expanded view's body, and so every subquery in it, runs as the view's
    /// owner, and a hand-written derived table runs as the caller.
    /// </summary>
    public async ValueTask<SelectBindResult> BindAsync(
        DatabaseDescriptor database,
        SelectQuery selectQuery,
        ExecuteSQLTicket ticket)
    {
        NestedSelectBinder nested = new(this, ticket);

        (selectQuery, List<SemiJoinSpec> semiJoinSpecs) = await semiJoinAnalyzer
            .AnalyzeAsync(database, selectQuery, ticket)
            .ConfigureAwait(false);

        selectQuery = await subqueryRewriter
            .RewriteSelectQueryAsync(database, selectQuery, ticket)
            .ConfigureAwait(false);

        BoundSelectQuery boundQuery = await queryBinder.BindAsync(database, selectQuery, nested).ConfigureAwait(false);

        (selectQuery, ExistsSubqueryRegistry? existsRegistry) = await existsSubqueryPreparer
            .PrepareAsync(
                database,
                selectQuery,
                boundQuery.Sources,
                boundQuery.DerivedSources,
                ticket,
                nested)
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
}

/// <summary>
/// What the binder uses to bind a SELECT it meets inside a source tree — the inner query of a
/// derived table, or an operand of a set operation — through the whole
/// <see cref="SelectBindPipeline"/> with the statement's ticket, instead of through the binder
/// alone. The pipeline hands one to the binder on every bind, so the two need no construction-time
/// reference to each other.
///
/// <para>A bind without one (a view definition validated at CREATE VIEW, a RETURNING list) resolves
/// names and schema only. The nested SELECT it produces still carries its subquery nodes, so it must
/// never reach the operators: see <see cref="BoundDerivedTableSource.InnerSubqueries"/>.</para>
/// </summary>
internal readonly struct NestedSelectBinder
{
    private readonly SelectBindPipeline pipeline;
    private readonly ExecuteSQLTicket ticket;

    public NestedSelectBinder(SelectBindPipeline pipeline, ExecuteSQLTicket ticket)
    {
        this.pipeline = pipeline;
        this.ticket = ticket;
    }

    /// <summary>Runs the pipeline over <paramref name="query"/> under the statement's ticket.</summary>
    public ValueTask<SelectBindResult> BindAsync(DatabaseDescriptor database, SelectQuery query) =>
        pipeline.BindAsync(database, query, ticket);
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

    /// <summary>The side products alone, in the shape a bound source keeps for its own ticket.</summary>
    public SubqueryBindProducts Subqueries => new(ExistsRegistry, SemiJoinSpecsOrNull);
}

/// <summary>
/// The side products of binding a nested SELECT through the pipeline, kept on the bound source that
/// stands for it (<see cref="BoundDerivedTableSource.InnerSubqueries"/>,
/// <see cref="BoundSetOperation.LeftSubqueries"/>) until the executor builds that SELECT's query
/// ticket. The ticket is built when the source runs, not when it is bound, because it also carries
/// the transaction and parameters of that run.
///
/// <para>Both members are null for a SELECT bound without the pipeline. That is not "no subqueries":
/// such a SELECT may still hold unresolved subquery nodes, and the evaluator refuses them with
/// <c>InvalidInternalOperation</c> if it ever runs.</para>
/// </summary>
/// <param name="ExistsRegistry">The prepared correlated EXISTS subqueries of the SELECT's WHERE, or null.</param>
/// <param name="SemiJoinSpecs">The semi/anti-join specs lifted out of the SELECT's WHERE, or null.</param>
internal readonly record struct SubqueryBindProducts(
    ExistsSubqueryRegistry? ExistsRegistry,
    IReadOnlyList<SemiJoinSpec>? SemiJoinSpecs);
