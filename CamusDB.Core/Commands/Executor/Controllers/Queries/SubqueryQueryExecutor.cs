
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Shared uncorrelated subquery execution for scalar, IN and EXISTS subqueries.
///
/// <para>
/// Both entry points return an <see cref="IAsyncEnumerable{T}"/> rather than a materialised
/// <c>List</c>. Callers stream the rows and extract only what they need (e.g., a single
/// <see cref="ColumnValue"/> per row for IN lists, the first row for scalar subqueries, or
/// a row-presence check for EXISTS). This eliminates the <c>ToListAsync</c> unbounded
/// in-memory buffer at the subquery level.
/// </para>
///
/// <para>
/// The inner SELECT is bound through the same <see cref="SelectBindPipeline"/> as a top-level
/// SELECT, so a subquery may itself contain scalar / IN / NOT IN / EXISTS subqueries to any depth,
/// and an eligible inner IN becomes a semi-join exactly as it would at the top level. That pipeline
/// contains the <see cref="SubqueryRewriter"/>, which in turn owns this executor, so the pipeline
/// cannot be a constructor argument: <c>CommandExecutor</c> attaches it with
/// <see cref="AttachBindPipeline"/> right after both exist. A call before that attach is a wiring
/// bug and fails fast rather than silently binding the inner SELECT without its subquery stages.
/// </para>
/// </summary>
internal sealed class SubqueryQueryExecutor
{
    private readonly QueryExecutor queryExecutor;
    private readonly QueryJoinExecutor queryJoinExecutor;
    private readonly SelectQueryCreator selectQueryCreator = new();
    private SelectBindPipeline? bindPipeline;

    public SubqueryQueryExecutor(QueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
        queryJoinExecutor = new QueryJoinExecutor(queryExecutor, queryExecutor.Options);
    }

    /// <summary>
    /// Completes the wiring cycle described in the class summary. Called once, during engine
    /// construction, before any statement can run.
    /// </summary>
    public void AttachBindPipeline(SelectBindPipeline pipeline)
    {
        if (bindPipeline is not null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Subquery executor already has a bind pipeline attached");
        }

        bindPipeline = pipeline;
    }

    /// <summary>
    /// Executes a single-column subquery and returns a streaming cursor over the result rows.
    /// The caller is responsible for consuming the cursor entirely when needed.
    /// </summary>
    /// <param name="outputType">
    /// Receives the static type of the subquery's column when the bind of the first read completes,
    /// also for a subquery that returns no row. See <see cref="SubqueryResultTypes"/>.
    /// </param>
    public IAsyncEnumerable<QueryResultRow> ExecuteSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        CancellationToken cancellationToken = default,
        SubqueryOutputType? outputType = null) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: true, outputType, isExistsBody: false, cancellationToken);

    /// <summary>
    /// Executes a subquery for EXISTS semantics and returns a streaming cursor.
    /// Projection shape is not validated; callers check whether any row is present.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteExistsSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        CancellationToken cancellationToken = default) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: false, outputType: null, isExistsBody: true, cancellationToken);

    /// <summary>
    /// Binds <paramref name="selectAst"/> through the pipeline under <paramref name="ticket"/>, for a
    /// correlated body that then runs once for each outer row with <see cref="RunBoundAsync"/>. The caller
    /// must make sure that no stage of the bind reads a value that changes between those runs: the bind
    /// runs each uncorrelated subquery of the body, so a body with an outer value inside a nested subquery
    /// must bind again for each run. See <see cref="CorrelatedSubqueryBody"/>.
    /// </summary>
    internal async ValueTask<SelectBindResult> BindForRunsAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket,
        bool isExistsBody)
    {
        if (bindPipeline is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Subquery executor has no bind pipeline attached");
        }

        SelectQuery subquery = selectQueryCreator.CreateSelectQuery(selectAst);
        return await bindPipeline.BindAsync(database, subquery, ticket, isExistsBody).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs a body that <see cref="BindForRunsAsync"/> bound, with the values of one run in
    /// <paramref name="parameters"/>. Each run gets its own query ticket, because the ticket reads the
    /// parameter values (an index seek on a parameter, for example). The bound query is shared by the runs
    /// and is not frozen: each stage that evaluates a correlated subquery (the filter, the projector, the
    /// locate scan of an UPDATE) runs it for one outer row at a time, so two runs never use it at once.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> RunBoundAsync(
        DatabaseDescriptor database,
        SelectBindResult bound,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        CancellationToken cancellationToken)
    {
        ExecuteSQLTicket runTicket = new(
            txnState: txnState,
            database: database.Name,
            sql: "",
            parameters: parameters,
            cancellationToken: cancellationToken);

        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(
            bound.Bound, runTicket, bound.ExistsRegistry, bound.SemiJoinSpecsOrNull);

        SelectStatementExecutor.PinSchemaVersions(database, bound.Bound.Sources, txnState);

        return bound.Bound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, bound.Bound, queryTicket)
            : queryExecutor.Query(database, bound.Bound.PrimaryTable, queryTicket);
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteSelectInternalAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        bool requireSingleColumn,
        SubqueryOutputType? outputType,
        bool isExistsBody,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (selectAst.nodeType != NodeType.Select)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must be a SELECT statement");
        }

        if (bindPipeline is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Subquery executor has no bind pipeline attached");
        }

        SelectQuery subquery = selectQueryCreator.CreateSelectQuery(selectAst);

        // A set operation projects *, and its column count is known only once its operands are bound.
        bool isSetOperation = subquery.Source is DerivedTableSource { SetOperation: not null };

        if (requireSingleColumn && !isSetOperation)
            ValidateSingleColumnProjection(subquery);

        // No request-scoped ticket reaches this helper, so the descriptor's display name is the only
        // name available. That is safe because a rename refreshes it in place — but if this method
        // ever gains access to the outer ticket, prefer its DatabaseName.
        ExecuteSQLTicket executeTicket = new(
            txnState: txnState,
            database: database.Name,
            sql: "",
            parameters: parameters,
            cancellationToken: ct);

        SelectBindResult bound = await bindPipeline.BindAsync(database, subquery, executeTicket, isExistsBody).ConfigureAwait(false);

        if (requireSingleColumn && isSetOperation && bound.Bound.DerivedSources[0].Columns.Count != 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must return exactly one column");

        // A type that the static rules do not show (an aggregate such as MAX(v)) comes from the schema
        // builder, which types the column as CREATE TABLE AS does: the column of (SELECT MAX(v) FROM t) is
        // the type of v. A stored column needs it when no row gives a value, so that the NULL that replaces
        // the subquery keeps the column type, as in PostgreSQL. The builder falls back to String for a node
        // it cannot type (a parameter, for example), so that type is only the declared type, and the
        // statement checks do not compare it. The result must not depend on the rows.
        if (outputType is not null)
        {
            outputType.Type = SubqueryResultTypes.OutputTypeOf(bound.Bound);
            outputType.DeclaredType = outputType.Type ?? DeclaredColumnType(bound.Bound);

            // A stored definition reads no row, so no value shows the type, and its checks compare the
            // declared type, as they did before.
            if (StoredDefinitionBind.IsActive)
                outputType.Type ??= outputType.DeclaredType;
        }

        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(
            bound.Bound, executeTicket, bound.ExistsRegistry, bound.SemiJoinSpecsOrNull);

        // The subquery reads these tables under the caller's transaction, so the transaction must
        // carry the same schema pins a top-level SELECT of them would take.
        SelectStatementExecutor.PinSchemaVersions(database, bound.Bound.Sources, txnState);

        // A stored definition binds its subqueries and reads no row of them: its result must not
        // depend on the data. See StoredDefinitionBind.
        if (StoredDefinitionBind.IsActive)
            yield break;

        IAsyncEnumerable<QueryResultRow> cursor = bound.Bound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, bound.Bound, queryTicket)
            : queryExecutor.Query(database, bound.Bound.PrimaryTable, queryTicket);

        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// The type of the one output column of <paramref name="bound"/> as the schema builder derives it,
    /// or null when the builder gives NULL. Used when the static rules show no type for the column. See
    /// <see cref="SubqueryOutputType.DeclaredType"/>.
    /// </summary>
    private static ExpressionType? DeclaredColumnType(BoundSelectQuery bound)
    {
        IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.Build(bound.Query, bound);

        if (columns.Count != 1 || columns[0].Type == ColumnType.Null)
            return null;

        return new ExpressionType(columns[0].Type, columns[0].ElementType ?? ColumnType.Null);
    }

    internal static void ValidateSingleColumnProjection(SelectQuery subquery)
    {
        if (subquery.Projections.Count != 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must return exactly one column");
        }

        if (subquery.Projections[0].Expression.nodeType == NodeType.ExprAllFields)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must return exactly one column");
        }
    }

    internal static ColumnValue ExtractSingleColumnValue(QueryResultRow row)
    {
        if (row.Row.Count == 0)
            return ColumnValue.Null;

        if (row.Row.Count > 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must return exactly one column");
        }

        return row.Row.Values.First();
    }
}
