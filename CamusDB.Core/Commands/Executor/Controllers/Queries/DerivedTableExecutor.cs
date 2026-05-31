
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes a bound derived table into an in-memory row stream (QP5.5).
/// </summary>
internal sealed class DerivedTableExecutor
{
    private readonly QueryExecutor queryExecutor;
    private readonly QueryJoinExecutor queryJoinExecutor;
    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    public DerivedTableExecutor(QueryExecutor queryExecutor, QueryJoinExecutor queryJoinExecutor)
    {
        this.queryExecutor = queryExecutor;
        this.queryJoinExecutor = queryJoinExecutor;
    }

    public async Task<List<Dictionary<string, ColumnValue>>> MaterializeAsync(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter)
    {
        BoundSelectQuery innerBound = source.InnerBound;

        ExecuteSQLTicket executeTicket = new(
            txnState: outerTicket.TxnState,
            database: database.Name,
            sql: "",
            parameters: outerTicket.Parameters);

        QueryTicket innerTicket = QueryTicketAdapter.ToQueryTicket(innerBound, executeTicket);

        IAsyncEnumerable<QueryResultRow> cursor = innerBound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, innerBound, innerTicket)
            : queryExecutor.Query(database, innerBound.PrimaryTable, innerTicket);

        List<Dictionary<string, ColumnValue>> rows = new();

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (executionFilter is not null)
            {
                Dictionary<string, ColumnValue> evalRow = outerTicket.RowNameResolver is { } resolver
                                                         && resolver.UsesQualifiedRowKeys()
                    ? QueryRowMerger.QualifyRow(row.Row, source.Alias)
                    : row.Row;

                if (!await queryFilterer
                        .MeetWhereAsync(executionFilter, evalRow, outerTicket, database)
                        .ConfigureAwait(false))
                {
                    continue;
                }
            }

            rows.Add(new Dictionary<string, ColumnValue>(row.Row));
        }

        return rows;
    }
}
