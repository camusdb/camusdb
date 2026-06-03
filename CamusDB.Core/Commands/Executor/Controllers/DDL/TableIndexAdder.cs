
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexAdder
{
    private readonly ILogger<ICamusDB> logger;

    public TableIndexAdder(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.AddPrimaryKey && table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Primary key already exists on table '{table.Name}'"
            );

        if (table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Index '{ticket.IndexName}' already exists on table '{table.Name}'"
            );

        foreach (ColumnIndexInfo indexColumn in ticket.Columns)
        {
            TableColumnSchema? tableColumn = null;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name == indexColumn.Name)
                {
                    tableColumn = column;
                    break;
                }
            }

            if (tableColumn is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' does not exist on table '{table.Name}'"
                );

            if (!SchemaElementStateRules.IsReadable(tableColumn) || !SchemaElementStateRules.IsWritable(tableColumn))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' is not public on table '{table.Name}'"
                );
        }
    }

    private static ColumnValue? GetColumnValue(Dictionary<string, ColumnValue> columnValues, string name)
    {
        return columnValues.GetValueOrDefault(name);
    }

    internal async Task<int> AddIndex(
        CatalogsManager catalogs,
        KvTransaction tx,
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        Validate(table, ticket);

        AddIndexFluxState state = new(
            catalogs: catalogs,
            tx: tx,
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor
        );

        FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine = new(state);

        return await AlterIndexInternal(machine, state).ConfigureAwait(false);
    }

    private async Task<FluxAction> AddWriteOnlySystemObject(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;
        IndexType indexType = ticket.Operation is AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey
            ? IndexType.Unique
            : IndexType.Multi;

        string indexId = ObjectIdGenerator.Generate().ToString();

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            database.SystemSchema.Indexes.Add(
                indexId,
                new DatabaseIndexObject(
                    indexId,
                    ticket.IndexName,
                    table.Id,
                    GetColumnIds(table, ticket.Columns),
                    indexType,
                    startOffset: "",
                    state: SchemaElementState.WriteOnly
                )
            );

            await state.Catalogs.PersistMetaAsync(database, state.Tx).ConfigureAwait(false);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.Indexes.Add(
            ticket.IndexName,
            new TableIndexSchema(ticket.IndexName, ticket.Columns.Select(x => x.Name).ToArray(), indexType, SchemaElementState.WriteOnly)
        );

        state.IndexId = indexId;

        return FluxAction.Continue;
    }

    private async Task<FluxAction> BackfillIndex(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        KvTransaction tx = state.Tx;
        bool unique = ticket.Operation is AlterIndexOperation.AddPrimaryKey or AlterIndexOperation.AddUniqueIndex;
        string indexId = state.IndexId ?? FindIndexId(state.Database, table.Id, ticket.IndexName);
        ObjectIdValue? afterRowId = GetBackfillCheckpoint(state.Database, indexId);

        int rows = 0;

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(
            tx.TransactionId,
            afterRowId: afterRowId).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                table.Schema,
                tx.TransactionId,
                rowId,
                data,
                visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);

            int i = 0;
            ColumnValue[] columnValues = unique
                ? new ColumnValue[ticket.Columns.Length]
                : new ColumnValue[ticket.Columns.Length + 1];

            foreach (ColumnIndexInfo columnIndex in ticket.Columns)
            {
                ColumnValue? keyValue = GetColumnValue(row, columnIndex.Name);

                if (keyValue is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"A null value was found for key field '{columnIndex.Name}'"
                    );

                columnValues[i++] = keyValue;
            }

            if (!unique)
                columnValues[i] = new(ColumnType.Id, rowId.ToString());

            CompositeColumnValue compositeKey = new(columnValues);

            await table.Store.PutIndexEntry(tx, ticket.IndexName, compositeKey, rowId, unique).ConfigureAwait(false);
            state.LastBackfilledRowId = rowId.ToString();

            rows++;
        }

        state.ModifiedRows = rows;

        logger.LogInformation("Added {Rows} rows to index {IndexName}", rows, ticket.IndexName);

        return FluxAction.Continue;
    }

    private async Task<FluxAction> PublishIndex(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;
        string indexId = state.IndexId ?? FindIndexId(database, table.Id, ticket.IndexName);

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            DatabaseIndexObject index = GetIndexObject(database, indexId);
            string finalOffset = state.LastBackfilledRowId ?? index.StartOffset;
            database.SystemSchema.Indexes[indexId] = CopyIndexObject(index, finalOffset, SchemaElementState.Public);

            await state.Catalogs.PersistMetaAsync(database, state.Tx).ConfigureAwait(false);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        TableIndexSchema current = table.Indexes[ticket.IndexName];
        table.Indexes[ticket.IndexName] = new TableIndexSchema(current.Name, current.Columns, current.Type, SchemaElementState.Public);

        return FluxAction.Continue;
    }

    private static ObjectIdValue? GetBackfillCheckpoint(DatabaseDescriptor database, string indexId)
    {
        DatabaseIndexObject index = GetIndexObject(database, indexId);

        if (string.IsNullOrWhiteSpace(index.StartOffset))
            return null;

        return ObjectId.ToValue(index.StartOffset);
    }

    private static string FindIndexId(DatabaseDescriptor database, string tableId, string indexName)
    {
        foreach (KeyValuePair<string, DatabaseIndexObject> index in database.SystemSchema.Indexes)
        {
            if (index.Value.TableId == tableId && index.Value.Name == indexName)
                return index.Key;
        }

        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Index '{indexName}' was not found in system metadata");
    }

    private static DatabaseIndexObject GetIndexObject(DatabaseDescriptor database, string indexId)
    {
        if (!database.SystemSchema.Indexes.TryGetValue(indexId, out DatabaseIndexObject? index))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Index '{indexId}' was not found in system metadata");

        return index;
    }

    private static DatabaseIndexObject CopyIndexObject(DatabaseIndexObject index, string startOffset, SchemaElementState state) =>
        new(index.Id, index.Name, index.TableId, index.ColumnIds, index.Type, startOffset, state);

    private static string[] GetColumnIds(TableDescriptor table, ReadOnlySpan<ColumnIndexInfo> columns)
    {
        int i = 0;
        string[] columnsIds = new string[columns.Length];

        foreach (ColumnIndexInfo columnIndex in columns)
        {
            bool hasColumn = false;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name == columnIndex.Name)
                {
                    hasColumn = true;
                    columnsIds[i++] = column.Id;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Couldn't get column id for column '{columnIndex.Name}'");
        }

        return columnsIds;
    }

    private async Task<int> AlterIndexInternal(FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine, AddIndexFluxState state)
    {
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(AddIndexFluxSteps.AddWriteOnlySystemObject, AddWriteOnlySystemObject);
        machine.When(AddIndexFluxSteps.BackfillIndex, BackfillIndex);
        machine.When(AddIndexFluxSteps.PublishIndex, PublishIndex);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
            "Added index {IndexName} to {Name}, Time taken: {Time}",
            ticket.IndexName,
            table.Name,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
