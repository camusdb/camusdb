
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
            bool hasColumn = false;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name == indexColumn.Name)
                {
                    hasColumn = true;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' does not exist on table '{table.Name}'"
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

    private async Task<FluxAction> LocateTuplesToFeedTheIndex(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: state.Tx,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        state.RowsToFeed = await cursor.ToListAsync().ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private async Task<FluxAction> FeedTheIndex(AddIndexFluxState state)
    {
        if (state.RowsToFeed is null)
        {
            logger.LogWarning("Invalid rows to AlterIndex");
            return FluxAction.Abort;
        }

        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        KvTransaction tx = state.Tx;
        bool unique = ticket.Operation is AlterIndexOperation.AddPrimaryKey or AlterIndexOperation.AddUniqueIndex;

        int rows = 0;

        foreach (QueryResultRow row in state.RowsToFeed)
        {
            int i = 0;
            ColumnValue[] columnValues = unique
                ? new ColumnValue[ticket.Columns.Length]
                : new ColumnValue[ticket.Columns.Length + 1];

            foreach (ColumnIndexInfo columnIndex in ticket.Columns)
            {
                ColumnValue? keyValue = GetColumnValue(row.Row, columnIndex.Name);

                if (keyValue is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"A null value was found for key field '{columnIndex.Name}'"
                    );

                columnValues[i++] = keyValue;
            }

            if (!unique)
                columnValues[i] = new(ColumnType.Id, row.RowId.ToString());

            CompositeColumnValue compositeKey = new(columnValues);

            await table.Store.PutIndexEntry(tx, ticket.IndexName, compositeKey, row.RowId, unique).ConfigureAwait(false);

            rows++;
        }

        logger.LogInformation("Added {Rows} rows to index {IndexName}", rows, ticket.IndexName);

        return FluxAction.Continue;
    }

    private async Task<FluxAction> AddSystemObject(AddIndexFluxState state)
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
                    startOffset: ""
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
            new TableIndexSchema(ticket.IndexName, ticket.Columns.Select(x => x.Name).ToArray(), indexType)
        );

        return FluxAction.Continue;
    }

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

        machine.When(AddIndexFluxSteps.LocateTuplesToFeedTheIndex, LocateTuplesToFeedTheIndex);
        machine.When(AddIndexFluxSteps.FeedTheIndex, FeedTheIndex);
        machine.When(AddIndexFluxSteps.AddSystemObject, AddSystemObject);

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
