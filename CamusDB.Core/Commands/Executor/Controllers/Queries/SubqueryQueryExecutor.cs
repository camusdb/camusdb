
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
/// Shared uncorrelated subquery execution for scalar and IN subqueries (QP5.2–QP5.3).
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

    public Task<List<QueryResultRow>> ExecuteSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: true);

    /// <summary>
    /// Executes a subquery for EXISTS semantics: projection shape is ignored; only row presence matters.
    /// </summary>
    public Task<List<QueryResultRow>> ExecuteExistsSelectAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters) =>
        ExecuteSelectInternalAsync(database, selectAst, txnState, parameters, requireSingleColumn: false);

    private async Task<List<QueryResultRow>> ExecuteSelectInternalAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        bool requireSingleColumn)
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

        return await cursor.ToListAsync().ConfigureAwait(false);
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
            return new ColumnValue(ColumnType.Null, 0);

        if (row.Row.Count > 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Subquery must return exactly one column");
        }

        return row.Row.Values.First();
    }
}
