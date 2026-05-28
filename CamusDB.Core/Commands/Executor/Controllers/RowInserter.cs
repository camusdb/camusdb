
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using System.Diagnostics;
using CamusDB.Core.Util.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Inserts a single row into a table
/// </summary>
internal sealed class RowInserter
{
    private readonly ILogger<ICamusDB> logger;

    public RowInserter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, InsertTicket ticket)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            // Step #1. Check for unknown columns
            foreach (KeyValuePair<string, ColumnValue> columnValue in values)
            {
                bool hasColumn = false;

                for (int i = 0; i < columns.Count; i++)
                {
                    TableColumnSchema column = columns[i];
                    if (column.Name == columnValue.Key)
                    {
                        hasColumn = true;
                        break;
                    }
                }

                if (!hasColumn)
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownColumn,
                        $"Unknown column '{columnValue.Key}' in column list"
                    );
            }

            // Step #2. Check for not null violations
            foreach (TableColumnSchema columnSchema in columns)
            {
                if (!columnSchema.NotNull)
                    continue;

                if (!values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.NotNullViolation,
                        $"Column '{columnSchema.Name}' cannot be null"
                    );
                }

                if (columnValue.Type == ColumnType.Null)
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.NotNullViolation,
                        $"Column '{columnSchema.Name}' cannot be null"
                    );
                }
            }
        }
    }

    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames, ColumnValue? extraUniqueValue = null)
    {
        ColumnValue[] columnValues = new ColumnValue[extraUniqueValue is null ? columnNames.Length : columnNames.Length + 1];

        for (int i = 0; i < columnNames.Length; i++)
        {
            string name = columnNames[i];

            if (string.IsNullOrEmpty(name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column name is null"
                );

            if (!rowValues.TryGetValue(name, out ColumnValue? columnValue))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"A null value was found for unique key field '{name}'"
                );

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new CompositeColumnValue(columnValues);
    }

    public async Task<int> Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Validate(table, ticket);

        InsertFluxState state = new(
            database: database,
            table: table,
            ticket: ticket
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        return await InsertInternal(machine, state).ConfigureAwait(false);
    }

    private async Task<FluxAction> InsertRowsAndIndexes(InsertFluxState state)
    {
        TableDescriptor table = state.Table;
        InsertTicket ticket = state.Ticket;
        KvTransaction tx = ticket.TxnState;

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            ObjectIdValue rowId = ObjectIdGenerator.Generate();

            byte[] rowBuffer = RowEncoder.Encode(table.Schema, values, rowId);

            await table.Store.InsertRow(tx, rowId, rowBuffer).ConfigureAwait(false);

            await UpdateUniqueIndexes(table, tx, rowId, values).ConfigureAwait(false);

            await UpdateMultiIndexes(table, tx, rowId, values).ConfigureAwait(false);

            logger.LogInformation("Row with rowid {RowId} inserted", rowId);

            state.InsertedRows++;
        }

        return FluxAction.Continue;
    }

    private static async Task UpdateUniqueIndexes(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> values
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique)
                continue;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(values, index.Columns);

            await table.Store.PutIndexEntry(tx, index.Name, uniqueKeyValue, rowId, unique: true).ConfigureAwait(false);
        }
    }

    private static async Task UpdateMultiIndexes(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> values
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi)
                continue;

            CompositeColumnValue multiKeyValue = GetColumnValue(values, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));

            await table.Store.PutIndexEntry(tx, index.Name, multiKeyValue, rowId, unique: false).ConfigureAwait(false);
        }
    }

    private async Task<int> InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(InsertFluxSteps.InsertRowsAndIndexes, InsertRowsAndIndexes);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
             "Inserted {Rows} rows, Time taken: {Time}",
             state.InsertedRows,
             timeTaken.ToString(@"m\:ss\.fff")
         );

        return state.InsertedRows;
    }
}
