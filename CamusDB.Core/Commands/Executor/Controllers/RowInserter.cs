
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
                    if (column.Name == columnValue.Key && SchemaElementStateRules.IsWritable(column))
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
                if (!SchemaElementStateRules.IsWritable(columnSchema))
                    continue;

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

            // Step #3. Check string/bytes length bounds
            foreach (TableColumnSchema columnSchema in columns)
            {
                if (!SchemaElementStateRules.IsWritable(columnSchema))
                    continue;

                if (columnSchema.Type != ColumnType.String && columnSchema.Type != ColumnType.Bytes)
                    continue;

                if (!values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                    continue;

                if (columnValue.Type == ColumnType.Null)
                    continue;

                EnforceLengthBound(columnSchema, columnValue);
            }
        }
    }

    private static void EnforceLengthBound(TableColumnSchema column, ColumnValue value)
    {
        if (column.Type == ColumnType.String)
        {
            string s = value.StrValue ?? "";
            int max = column.MaxLength ?? CamusDBConfig.DefaultStringMaxLength;
            if (s.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {s.Length})");
        }
        else if (column.Type == ColumnType.Bytes)
        {
            byte[] b = value.BytesValue ?? [];
            int max = column.MaxLength ?? CamusDBConfig.DefaultBytesMaxLength;
            if (b.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {b.Length})");
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

        // Build every row + index entry for the whole ticket, then write them in a single
        // batched pass (one AcquireMany + one SetMany) instead of an acquire+set per key.
        List<KvTableStore.RowWrite> writes = new(ticket.Values.Count);

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            ObjectIdValue rowId = ObjectIdGenerator.Generate();

            KvTableStore.RowWrite write = new()
            {
                RowId = rowId,
                RowData = RowEncoder.Encode(table.Schema, values, rowId)
            };

            foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
            {
                TableIndexSchema index = kv.Value;
                if (!SchemaElementStateRules.IsWritableIndex(table.Schema, index))
                    continue;

                if (index.Type == IndexType.Unique)
                {
                    CompositeColumnValue uniqueKeyValue = GetColumnValue(values, index.Columns);
                    write.IndexEntries.Add(new(index.Name, uniqueKeyValue, Unique: true));
                }
                else if (index.Type == IndexType.Multi)
                {
                    CompositeColumnValue multiKeyValue = GetColumnValue(values, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));
                    write.IndexEntries.Add(new(index.Name, multiKeyValue, Unique: false));
                }
            }

            writes.Add(write);
        }

        await table.Store.WriteRowsBatch(tx, writes).ConfigureAwait(false);

        state.InsertedRows += writes.Count;

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Inserted {Count} row(s) in a batched write", writes.Count);

        return FluxAction.Continue;
    }

    private async Task<int> InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(InsertFluxSteps.InsertRowsAndIndexes, InsertRowsAndIndexes);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogRowsInserted(logger, state.InsertedRows, timeTaken);

        return state.InsertedRows;
    }
}
