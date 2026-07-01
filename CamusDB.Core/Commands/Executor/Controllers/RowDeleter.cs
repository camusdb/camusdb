
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.Diagnostics;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.Statistics;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeleter
{
    private readonly ILogger<ICamusDB> logger;
    private readonly StatisticsManager? _stats;

    public RowDeleter(ILogger<ICamusDB> logger, StatisticsManager? stats = null)
    {
        this.logger = logger;
        _stats = stats;
    }

    public async Task<int> Delete(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, DeleteTicket ticket)
    {
        DeleteFluxState state = new(
            queryExecutor: queryExecutor,
            database: database,
            table: table,
            ticket: ticket
        );

        FluxMachine<DeleteFluxSteps, DeleteFluxState> machine = new(state);

        return await DeleteInternal(machine, state).ConfigureAwait(false);
    }

    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames, ColumnValue? extraUniqueValue = null)
    {
        ColumnValue[] columnValues = new ColumnValue[extraUniqueValue is null ? columnNames.Length : columnNames.Length + 1];

        for (int i = 0; i < columnNames.Length; i++)
        {
            string name = columnNames[i];

            if (!rowValues.TryGetValue(name, out ColumnValue? columnValue))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field '" + name + "'"
                );

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new CompositeColumnValue(columnValues);
    }

    /// <summary>
    /// Returns true when any of the index's columns is absent from the row or holds a NULL value.
    /// Such a row is exempt from a unique index (NULLs are distinct) and carries no index entry.
    /// </summary>
    private static bool HasNullKeyColumn(Dictionary<string, ColumnValue> rowValues, string[] columnNames)
    {
        foreach (string name in columnNames)
        {
            if (!rowValues.TryGetValue(name, out ColumnValue? value) || value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    private async Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
    {
        DeleteTicket ticket = state.Ticket;

        // Decode only the columns the WHERE/filter needs during the locate scan.
        // The write phase calls LoadWritableRow which does a full decode per matched row.
        // Returns null when the WHERE contains subquery nodes — fall back to full decode.
        IReadOnlySet<string>? locateColumns = RequiredColumnAnalyzer.ComputeForLocate(
            ticket.Where, ticket.Filters, exprValues: null);

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            limit: ticket.Limit,
            offset: null,
            parameters: ticket.Parameters,
            locateColumns: locateColumns,
            exclusivePredicateLocks: true
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        SpillableRowList rowList = new();
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            await rowList.AddAsync(row).ConfigureAwait(false);
        await rowList.SealAsync().ConfigureAwait(false);
        state.RowsToDelete = rowList;

        return FluxAction.Continue;
    }

    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete.Count == 0)
        {
            logger.LogError("Invalid rows to delete");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        KvTransaction tx = state.Ticket.TxnState;

        // Drain RowsToDelete in bounded chunks so a DELETE over a huge matched set does not
        // hold an O(matched) RowDelete list on the heap between scan and the Kahuna round-trip.
        // The chunk size is tied to SpillEffectiveThreshold so force-spill tests drive both
        // the row-buffer spill and the mutation batch with one knob.
        int chunkSize = CamusDBConfig.SpillEffectiveThreshold;
        List<KvTableStore.RowDelete> batch = new(Math.Min(chunkSize, 64));

        await foreach (QueryResultRow row in state.RowsToDelete.EnumerateAsync().ConfigureAwait(false))
        {
            ObjectIdValue rowId = row.RowId;
            Dictionary<string, ColumnValue> writableRow = await LoadWritableRow(state.Database, table, tx, rowId).ConfigureAwait(false);

            KvTableStore.RowDelete rowDelete = new() { RowId = rowId };
            CollectIndexDeletes(table, rowId, writableRow, rowDelete);
            batch.Add(rowDelete);

            if (batch.Count >= chunkSize)
            {
                await FlushDeleteBatch(table, tx, batch, state).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await FlushDeleteBatch(table, tx, batch, state).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private async Task FlushDeleteBatch(
        TableDescriptor table,
        KvTransaction tx,
        List<KvTableStore.RowDelete> batch,
        DeleteFluxState state)
    {
        if (_stats is not null && batch.Count > _stats.DeleteBatchMaxChunkSeen)
            _stats.DeleteBatchMaxChunkSeen = batch.Count;

        await table.Store.DeleteRowsBatch(tx, batch).ConfigureAwait(false);
        state.DeletedRows += batch.Count;

        foreach (KvTableStore.RowDelete row in batch)
            Log.LogRowDeleted(logger, row.RowId);
    }

    private static async Task<Dictionary<string, ColumnValue>> LoadWritableRow(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId
    )
    {
        byte[]? data = await table.Store.GetRow(tx, rowId).ConfigureAwait(false);
        if (data is null || data.Length == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Row '{rowId}' disappeared before delete");

        return await RowEncoder.DecodeWritableAsync(
            table.Schema,
            tx.TransactionId,
            rowId,
            data,
            visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);
    }

    private static void CollectIndexDeletes(
        TableDescriptor table,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> row,
        KvTableStore.RowDelete rowDelete
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (!SchemaElementStateRules.IsWritableIndex(table.Schema, index))
                continue;

            if (index.Type == IndexType.Unique)
            {
                // NULLs are distinct: a row with a NULL (or absent) value in any indexed column never
                // had a unique index entry, so there is nothing to delete.
                if (HasNullKeyColumn(row, index.Columns))
                    continue;

                CompositeColumnValue key = GetColumnValue(row, index.Columns);
                rowDelete.IndexEntries.Add(new KvTableStore.IndexDelete(index.KvId, key, rowId, Unique: true));
            }
            else if (index.Type == IndexType.Multi)
            {
                CompositeColumnValue key = GetColumnValue(row, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));
                rowDelete.IndexEntries.Add(new KvTableStore.IndexDelete(index.KvId, key, rowId, Unique: false));
            }
        }
    }

    private async Task<int> DeleteInternal(FluxMachine<DeleteFluxSteps, DeleteFluxState> machine, DeleteFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.DeleteRowsAndIndexesFromDisk, DeleteRowsAndIndexesFromDisk);

        try
        {
            while (!machine.IsAborted)
                await machine.RunStep(machine.NextStep()).ConfigureAwait(false);
        }
        finally
        {
            await state.RowsToDelete.DisposeAsync().ConfigureAwait(false);
        }

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogRowsDeleted(logger, state.DeletedRows, timeTaken);

        return state.DeletedRows;
    }
}
