
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
    public IAsyncEnumerable<QueryResultRow> ExecuteSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        CancellationToken cancellationToken = default) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: true, cancellationToken);

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
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: false, cancellationToken);

    private async IAsyncEnumerable<QueryResultRow> ExecuteSelectInternalAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        bool requireSingleColumn,
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

        if (requireSingleColumn)
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

        SelectBindResult bound = await bindPipeline.BindAsync(database, subquery, executeTicket).ConfigureAwait(false);

        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(
            bound.Bound, executeTicket, bound.ExistsRegistry, bound.SemiJoinSpecsOrNull);

        // The subquery reads these tables under the caller's transaction, so the transaction must
        // carry the same schema pins a top-level SELECT of them would take.
        SelectStatementExecutor.PinSchemaVersions(database, bound.Bound.Sources, txnState);

        IAsyncEnumerable<QueryResultRow> cursor = bound.Bound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, bound.Bound, queryTicket)
            : queryExecutor.Query(database, bound.Bound.PrimaryTable, queryTicket);

        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
            yield return row;
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
