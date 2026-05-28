
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

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
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

        foreach (QueryResultRow row in state.RowsToDelete)
        {
            ObjectIdValue rowId = row.RowId;

            await table.Store.DeleteRow(tx, rowId).ConfigureAwait(false);

            await DeleteUniqueIndexEntries(table, tx, rowId, row.Row).ConfigureAwait(false);

            await DeleteMultiIndexEntries(table, tx, rowId, row.Row).ConfigureAwait(false);

            logger.LogInformation("Row with rowid {RowId} deleted", rowId);

            state.DeletedRows++;
        }

        return FluxAction.Continue;
    }

    private static async Task DeleteUniqueIndexEntries(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique)
                continue;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(row, index.Columns);

            await table.Store.DeleteIndexEntry(tx, index.Name, uniqueKeyValue, rowId, unique: true).ConfigureAwait(false);
        }
    }

    private static async Task DeleteMultiIndexEntries(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi)
                continue;

            CompositeColumnValue multiKeyValue = GetColumnValue(row, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));

            await table.Store.DeleteIndexEntry(tx, index.Name, multiKeyValue, rowId, unique: false).ConfigureAwait(false);
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
