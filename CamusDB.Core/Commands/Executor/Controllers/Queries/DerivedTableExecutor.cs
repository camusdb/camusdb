
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
    private readonly QueryFilterer queryFilterer = new();

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

        // A CTE that the statement computes once: the first reader keeps its rows, and every reader
        // reads them. A statement that opened no entry for it runs it here, as an inlined CTE.
        if (source.Source.ComputedOnce is { } marker
            && await ComputedOnceRows.GetOrComputeAsync(
                    outerTicket.TxnState, marker, () => KeepAsync(database, source, outerTicket)).ConfigureAwait(false) is { } kept)
        {
            cursor = kept.EnumerateAsync(outerTicket.CancellationToken);
        }
        else
        {
            cursor = Produce(database, source, outerTicket, stats, windowStats);
        }

        await foreach (QueryResultRow row in WidenAndFilter(database, source, outerTicket, executionFilter, cursor).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// The largest number of rows of one run of a <c>LATERAL</c> body that
    /// <see cref="BoundDerivedTableSource.LateralResults"/> keeps. A run with more rows runs again for the
    /// next left row with the same values, so a large result costs time, not memory.
    /// </summary>
    internal const int MaxKeptLateralRows = 256;

    /// <summary>
    /// Runs the <c>LATERAL</c> body of <paramref name="source"/> for one left row and streams the rows that
    /// pass <paramref name="executionFilter"/>. The nested-loop join calls it once for each left row, with
    /// the row it built for that left row, whose keys are qualified (<c>{alias}.{column}</c>).
    ///
    /// <para>The body reads the left values it names (see <see cref="CorrelatedOuterValues"/>). When the body
    /// can keep its results, a run whose rows all fit in <see cref="MaxKeptLateralRows"/> is kept under those
    /// values for the rest of the statement, and a later left row with the same values reads the kept rows.
    /// A run that a reader stops early (a <c>LIMIT</c>) keeps nothing.</para>
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> StreamLateral(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter,
        IReadOnlyDictionary<string, ColumnValue> leftRow)
    {
        CorrelatedSubqueryBody body = source.Lateral
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Derived table '{source.Alias}' is not LATERAL");

        ColumnValue[] outerValues = body.Outer.For(leftRow, outerTicket.Parameters, rowKeysAreQualified: true);

        // A body of expressions only: one row, keyed by the column row keys, with no subquery run.
        if (source.LateralExpressions is not null)
        {
            if (await EvaluateLateralExpressionsAsync(database, source, outerTicket, executionFilter, leftRow).ConfigureAwait(false) is { } row)
                yield return row;

            yield break;
        }

        CorrelatedResultCache<QueryResultRow[]>? results = source.LateralResults is { } cache
            && CorrelatedResultCache<QueryResultRow[]>.TryKey(outerValues)
                ? cache
                : null;

        IAsyncEnumerable<QueryResultRow> cursor;

        if (results is not null && results.TryGet(outerValues, out QueryResultRow[] keptRows))
        {
            CorrelatedRunProbe.CountHit();
            cursor = KeptRows(keptRows);
        }
        else
        {
            cursor = body.RunAsync(database, outerValues, outerTicket.TxnState, outerTicket.Parameters, outerTicket.CancellationToken);

            if (results is not null)
                cursor = KeepWhenSmall(cursor, results, outerValues);
        }

        // A * over a join in the body keys its cells as {alias}.{column}; the derived table reads them under
        // the plain keys its columns declare.
        if (source.InnerRowKeys is { } renames)
            cursor = RenameInnerRowKeys(cursor, renames);

        await foreach (QueryResultRow row in WidenAndFilter(database, source, outerTicket, executionFilter, cursor).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// The one row of a <c>LATERAL</c> body of expressions only (<see cref="BoundDerivedTableSource.LateralExpressions"/>)
    /// for one left row, or null when <paramref name="executionFilter"/> drops it. It widens the cells and
    /// applies the filter as <see cref="StreamLateral"/> does for the rows of a run, with no async iterator,
    /// because the nested-loop join calls it once for each left row.
    /// </summary>
    public async ValueTask<QueryResultRow?> EvaluateLateralExpressionsAsync(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter,
        IReadOnlyDictionary<string, ColumnValue> leftRow)
    {
        LateralExpressionBody expressions = source.LateralExpressions
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Derived table '{source.Alias}' is not a LATERAL body of expressions");

        ColumnValue[] outerValues = source.Lateral!.Outer.For(leftRow, outerTicket.Parameters, rowKeysAreQualified: true);
        QueryResultRow row = expressions.Evaluate(outerValues, outerTicket.Parameters);

        (string RowKey, ColumnType Type)[] numericColumns = expressions.NumericColumns;

        if (numericColumns.Length > 0)
            row = NumericWidening.WidenToDeclaredTypes(row, numericColumns);

        if (executionFilter is null)
            return row;

        IReadOnlyDictionary<string, ColumnValue> evalRow = outerTicket.RowNameResolver is { } resolver
                                                           && resolver.UsesQualifiedRowKeys()
            ? QueryRowMerger.QualifyRow(row.Row, source.Alias)
            : row.Row;

        return await queryFilterer.MeetWhereAsync(executionFilter, evalRow, outerTicket, database).ConfigureAwait(false)
            ? row
            : null;
    }

    private static async IAsyncEnumerable<QueryResultRow> KeptRows(QueryResultRow[] rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Streams <paramref name="cursor"/> and, when it ends with at most <see cref="MaxKeptLateralRows"/> rows,
    /// keeps them in <paramref name="results"/> under <paramref name="key"/>.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> KeepWhenSmall(
        IAsyncEnumerable<QueryResultRow> cursor,
        CorrelatedResultCache<QueryResultRow[]> results,
        ColumnValue[] key)
    {
        List<QueryResultRow>? kept = new();

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (kept is not null)
            {
                if (kept.Count < MaxKeptLateralRows)
                    kept.Add(row);
                else
                    kept = null;
            }

            yield return row;
        }

        if (kept is not null)
            results.Add(key, [.. kept], kept.Count);
    }

    /// <summary>
    /// The last steps of every read of a derived table: each numeric cell widened to its declared type, and
    /// the filter of the reader.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> WidenAndFilter(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        NodeAst? executionFilter,
        IAsyncEnumerable<QueryResultRow> cursor)
    {
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

    /// <summary>
    /// The rows of the derived table's query, before the numeric widening and the filter of the reader:
    /// a set operation, or the inner query with its key renames and its window operator.
    /// </summary>
    private IAsyncEnumerable<QueryResultRow> Produce(
        DatabaseDescriptor database,
        BoundDerivedTableSource source,
        QueryTicket outerTicket,
        SetOperationStats? stats,
        WindowStats? windowStats)
    {
        IAsyncEnumerable<QueryResultRow> cursor;

        // The work table of a recursive CTE: the rows of the step before, which the recursive UNION
        // keeps, read under the keys of the work table's columns.
        if (source.Source.WorkTable is { } workTable)
        {
            SpillableRowList rows = workTable.Table.Rows
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"The work table of recursive query '{workTable.Table.Name}' was read outside its recursion");

            cursor = rows.EnumerateAsync(outerTicket.CancellationToken);

            if (source.InnerRowKeys is { } workRenames)
                cursor = RenameInnerRowKeys(cursor, workRenames);

            return cursor;
        }

        if (source.SetOperation is { } setOperation)
        {
            cursor = setOperationExecutor.Execute(database, setOperation, outerTicket, stats);

            // A column-alias list over a set operation renames the cells of its rows.
            if (source.InnerRowKeys is { } setRenames)
                cursor = RenameInnerRowKeys(cursor, setRenames);
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

            // A * over a join keys its cells as {alias}.{column}; the derived table reads them under
            // the plain keys its columns declare, before the window operator and any filter run.
            if (source.InnerRowKeys is { } renames)
                cursor = RenameInnerRowKeys(cursor, renames);

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

        return cursor;
    }

    /// <summary>
    /// Runs the query of a CTE that the statement computes once into a list that
    /// <see cref="ComputedOnceRows"/> keeps for the statement. No reader's filter applies here: every
    /// reader filters the kept rows itself. When the run throws, the list and its spill file are
    /// disposed here.
    /// </summary>
    private async Task<SpillableRowList> KeepAsync(DatabaseDescriptor database, BoundDerivedTableSource source, QueryTicket outerTicket)
    {
        SpillableRowList rows = new(QueryExecutionContext.For(database, outerTicket));

        try
        {
            await foreach (QueryResultRow row in Produce(database, source, outerTicket, stats: null, windowStats: null)
                               .WithCancellation(outerTicket.CancellationToken).ConfigureAwait(false))
                await rows.AddAsync(row).ConfigureAwait(false);

            await rows.SealAsync().ConfigureAwait(false);
        }
        catch
        {
            await rows.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return rows;
    }

    /// <summary>
    /// Renames the cell keys of each inner row by <paramref name="renames"/> (see
    /// <see cref="BoundDerivedTableSource.InnerRowKeys"/>). A slot-backed row keeps its values and gets
    /// a renamed layout, built once for each distinct inner layout, so no cell is copied; any other row
    /// is copied into a dictionary under the new keys.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> RenameInnerRowKeys(
        IAsyncEnumerable<QueryResultRow> cursor,
        (string InnerKey, string RowKey)[] renames)
    {
        Dictionary<string, string> byInnerKey = new(renames.Length, StringComparer.OrdinalIgnoreCase);

        foreach ((string innerKey, string rowKey) in renames)
            byInnerKey[innerKey] = rowKey;

        RowLayout? innerLayout = null;
        RowLayout? renamedLayout = null;

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (row.Row is QueryRow queryRow)
            {
                if (!ReferenceEquals(queryRow.Layout, innerLayout))
                {
                    innerLayout = queryRow.Layout;
                    string[] names = innerLayout.OutputNames;
                    string[] renamedNames = new string[names.Length];

                    for (int i = 0; i < names.Length; i++)
                        renamedNames[i] = byInnerKey.TryGetValue(names[i], out string? rowKey) ? rowKey : names[i];

                    renamedLayout = RowLayout.ForColumns(renamedNames);
                }

                yield return new QueryResultRow(row.RowId, queryRow.WithLayout(renamedLayout!));
                continue;
            }

            Dictionary<string, ColumnValue> copy = new(row.Row.Count, StringComparer.OrdinalIgnoreCase);

            foreach (KeyValuePair<string, ColumnValue> cell in row.Row)
                copy[byInnerKey.TryGetValue(cell.Key, out string? rowKey) ? rowKey : cell.Key] = cell.Value;

            yield return new QueryResultRow(row.RowId, copy);
        }
    }
}
