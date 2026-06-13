
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
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeleter
{
    private readonly ILogger<ICamusDB> logger;

    public RowDeleter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
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

    private async Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
    {
        DeleteTicket ticket = state.Ticket;

        // R16: decode only the columns the WHERE/filter needs during the locate scan.
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
            locateColumns: locateColumns
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        state.RowsToDelete = await cursor.ToListAsync().ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete is null || state.RowsToDelete.Count == 0)
        {
            logger.LogError("Invalid rows to delete");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        KvTransaction tx = state.Ticket.TxnState;

        List<KvTableStore.RowDelete> batch = new(state.RowsToDelete.Count);

        foreach (QueryResultRow row in state.RowsToDelete)
        {
            ObjectIdValue rowId = row.RowId;
            Dictionary<string, ColumnValue> writableRow = await LoadWritableRow(state.Database, table, tx, rowId).ConfigureAwait(false);

            KvTableStore.RowDelete rowDelete = new() { RowId = rowId };
            CollectIndexDeletes(table, rowId, writableRow, rowDelete);
            batch.Add(rowDelete);
        }

        await table.Store.DeleteRowsBatch(tx, batch).ConfigureAwait(false);

        state.DeletedRows += batch.Count;

        foreach (KvTableStore.RowDelete row in batch)
            logger.LogInformation("Row with rowid {RowId} deleted", row.RowId);

        return FluxAction.Continue;
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
                CompositeColumnValue key = GetColumnValue(row, index.Columns);
                rowDelete.IndexEntries.Add(new KvTableStore.IndexDelete(index.Name, key, rowId, Unique: true));
            }
            else if (index.Type == IndexType.Multi)
            {
                CompositeColumnValue key = GetColumnValue(row, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));
                rowDelete.IndexEntries.Add(new KvTableStore.IndexDelete(index.Name, key, rowId, Unique: false));
            }
        }
    }

    private async Task<int> DeleteInternal(FluxMachine<DeleteFluxSteps, DeleteFluxState> machine, DeleteFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.DeleteRowsAndIndexesFromDisk, DeleteRowsAndIndexesFromDisk);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
            "Deleted {Rows} rows, Time taken: {Time}",
            state.DeletedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.DeletedRows;
    }
}
