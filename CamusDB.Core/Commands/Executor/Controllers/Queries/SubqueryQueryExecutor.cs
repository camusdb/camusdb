
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
/// Shared uncorrelated subquery execution for scalar and IN subqueries.
///
/// <para>
/// Both entry points return an <see cref="IAsyncEnumerable{T}"/> rather than a materialised
/// <c>List</c>. Callers stream the rows and extract only what they need (e.g., a single
/// <see cref="ColumnValue"/> per row for IN lists, the first row for scalar subqueries, or
/// a row-presence check for EXISTS). This eliminates the <c>ToListAsync</c> unbounded
/// in-memory buffer at the subquery level.
/// </para>
/// </summary>
internal sealed class SubqueryQueryExecutor
{
    private readonly QueryBinder queryBinder;
    private readonly QueryExecutor queryExecutor;
    private readonly QueryJoinExecutor queryJoinExecutor;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public SubqueryQueryExecutor(QueryBinder queryBinder, QueryExecutor queryExecutor)
    {
        this.queryBinder = queryBinder;
        this.queryExecutor = queryExecutor;
        queryJoinExecutor = new QueryJoinExecutor(queryExecutor);
    }

    /// <summary>
    /// Executes a single-column subquery and returns a streaming cursor over the result rows.
    /// The caller is responsible for consuming the cursor entirely when needed.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: true);

    /// <summary>
    /// Executes a subquery for EXISTS semantics and returns a streaming cursor.
    /// Projection shape is not validated; callers check whether any row is present.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteExistsSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: false);

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

        SelectQuery subquery = selectQueryCreator.CreateSelectQuery(selectAst);

        if (requireSingleColumn)
            ValidateSingleColumnProjection(subquery);

        BoundSelectQuery bound = await queryBinder.BindAsync(database, subquery).ConfigureAwait(false);

        ExecuteSQLTicket executeTicket = new(
            txnState: txnState,
            database: database.Name,
            sql: "",
            parameters: parameters);

        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        IAsyncEnumerable<QueryResultRow> cursor = bound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, bound, queryTicket)
            : queryExecutor.Query(database, bound.PrimaryTable, queryTicket);

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
