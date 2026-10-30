
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
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
/// list via <see cref="QueryPlan.DisposeMaterializationsAsync"/>. When the fill throws, no list
/// is returned, so this executor disposes it and its spill file itself.
/// </para>
/// </summary>
internal sealed class DerivedTableExecutor
{
    private readonly QueryExecutor queryExecutor;
    private readonly QueryJoinExecutor queryJoinExecutor;
    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    private readonly QuerySorter querySorter = new();

    private readonly SetOperationExecutor setOperationExecutor;

    public DerivedTableExecutor(QueryExecutor queryExecutor, QueryJoinExecutor queryJoinExecutor)
    {
        this.queryExecutor = queryExecutor;
        this.queryJoinExecutor = queryJoinExecutor;
        setOperationExecutor = new SetOperationExecutor(queryExecutor, queryJoinExecutor);
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
        SpillableRowList rows = new(QueryExecutionContext.For(database, outerTicket));

        try
        {
            await foreach (QueryResultRow row in Stream(database, source, outerTicket, executionFilter).ConfigureAwait(false))
                await rows.AddAsync(row).ConfigureAwait(false);

            await rows.SealAsync().ConfigureAwait(false);
        }
        catch
        {
            // The caller gets no list to dispose, so the spill files it wrote are deleted here.
            await rows.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return rows;
    }

    /// <summary>
    /// Runs the derived table and streams the rows that pass <paramref name="executionFilter"/>,
    /// without keeping them. <see cref="MaterializeAsync"/> fills its list from here. A caller that
    /// reads the derived table exactly once, such as a set operation that is the whole query, reads
    /// this directly, so a <c>LIMIT</c> over <c>UNION ALL</c> stops the operands early and nothing is
    /// held in memory.
    /// </summary>
    /// <param name="stats">
    /// The per-operand row counts of a set operation, for EXPLAIN (ANALYZE); null otherwise and for
    /// any other derived table.
    /// </param>
    public async IAsyncEnumerable<QueryResultRow> Stream(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter,
        SetOperationStats? stats = null,
        WindowStats? windowStats = null)
    {
        IAsyncEnumerable<QueryResultRow> cursor;

        if (source.SetOperation is { } setOperation)
        {
            cursor = setOperationExecutor.Execute(database, setOperation, outerTicket, stats);
        }
        else
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
                cancellationToken: outerTicket.CancellationToken,
                probe: outerTicket.Probe);

            // The inner query bound through the whole pipeline, so its ticket carries what the
            // pipeline's stages left for it: the prepared EXISTS subqueries the per-row evaluator looks
            // up, and the semi-join specs the planner turns into index probes.
            QueryTicket innerTicket = QueryTicketAdapter.ToQueryTicket(
                innerBound, executeTicket, source.InnerSubqueries.ExistsRegistry, source.InnerSubqueries.SemiJoinSpecs);

            cursor = innerBound.IsMultiSource
                ? queryJoinExecutor.ExecuteJoinQuery(database, innerBound, innerTicket)
                : queryExecutor.Query(database, innerBound.PrimaryTable, innerTicket);

            // A windowed SELECT's derived table: the inner query computed every window input, and the
            // window operator adds the results. It runs before the filter below, so a filter on a
            // window result (WHERE rn <= 3 over this derived table) sees whole partitions.
            if (source.Source.Windows is { } windows)
                cursor = WindowOperator.Apply(
                    cursor,
                    windows,
                    source.Columns,
                    QueryExecutionContext.For(database, outerTicket),
                    querySorter,
                    outerTicket.Parameters,
                    windowStats,
                    outerTicket.CancellationToken);
        }

        // A cell can carry a narrower numeric type than its column declares (COALESCE(p, 0) gives an
        // INT64 zero for a NULL NUMERIC p). The outer join plans its key strategy from the declared
        // types, so every cell is widened to that type here, before any consumer reads it. A set
        // operation has already widened its cells, so for it this check finds nothing to change.
        (string RowKey, ColumnType Type)[] numericColumns = NumericWidening.NumericColumns(source.Columns);

        await foreach (QueryResultRow scanned in cursor.ConfigureAwait(false))
        {
            QueryResultRow row = numericColumns.Length == 0
                ? scanned
                : NumericWidening.WidenToDeclaredTypes(scanned, numericColumns);

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

            yield return row;
        }
    }
}
