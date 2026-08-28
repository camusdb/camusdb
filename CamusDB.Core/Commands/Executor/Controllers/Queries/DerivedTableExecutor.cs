
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes a bound derived table into a <see cref="SpillableRowList"/>.
///
/// <para>
/// When <see cref="CamusDBOptions.SpillEnabled"/> is <c>false</c> or the row count stays
/// below the threshold, all rows remain in memory — byte-identical to a plain list. When
/// the threshold is exceeded, rows overflow to a spill file managed by
/// <see cref="SpillableRowList"/>. The caller is responsible for disposing the returned
/// list via <see cref="QueryPlan.DisposeMaterializationsAsync"/>.
/// </para>
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

    /// <summary>
    /// Executes the derived table query and materializes all passing rows into a
    /// <see cref="SpillableRowList"/>. The caller must await <see cref="SpillableRowList.SealAsync"/>
    /// before this method returns (it is called internally), so the returned list is ready to
    /// enumerate. Ownership of the list transfers to the caller.
    /// </summary>
    public async Task<SpillableRowList> MaterializeAsync(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter)
    {
        BoundSelectQuery innerBound = source.InnerBound;

        // The request-scoped name, not the descriptor's display name: the inner ticket can be
        // re-resolved by name downstream, and the descriptor's name is a cached value that a rename
        // refreshes rather than a value this request asked for.
        ExecuteSQLTicket executeTicket = new(
            txnState: outerTicket.TxnState,
            database: outerTicket.DatabaseName,
            sql: "",
            parameters: outerTicket.Parameters,
            cancellationToken: outerTicket.CancellationToken);

        QueryTicket innerTicket = QueryTicketAdapter.ToQueryTicket(innerBound, executeTicket);

        IAsyncEnumerable<QueryResultRow> cursor = innerBound.IsMultiSource
            ? queryJoinExecutor.ExecuteJoinQuery(database, innerBound, innerTicket)
            : queryExecutor.Query(database, innerBound.PrimaryTable, innerTicket);

        SpillableRowList rows = new(QueryExecutionContext.For(database, outerTicket.CancellationToken));

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (executionFilter is not null)
            {
                IReadOnlyDictionary<string, ColumnValue> evalRow = outerTicket.RowNameResolver is { } resolver
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

            await rows.AddAsync(row).ConfigureAwait(false);
        }

        await rows.SealAsync().ConfigureAwait(false);
        return rows;
    }
}
