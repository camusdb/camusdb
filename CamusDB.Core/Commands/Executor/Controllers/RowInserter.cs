
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

        // Build lookups once per statement rather than rescanning per row.
        // writableNames: O(1) membership test for step 1.
        // validationRules: single precomputed list of columns that need not-null or length checks.
        HashSet<string> writableNames = new(columns.Count, StringComparer.Ordinal);
        List<TableColumnSchema> validationRules = new(columns.Count);

        foreach (TableColumnSchema col in columns)
        {
            if (!SchemaElementStateRules.IsWritable(col))
                continue;
            writableNames.Add(col.Name);
            if (col.NotNull || col.Type == ColumnType.String || col.Type == ColumnType.Bytes)
                validationRules.Add(col);
        }

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            // Step 1: check that every supplied column name is a writable schema column.
            foreach (string key in values.Keys)
            {
                if (!writableNames.Contains(key))
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownColumn,
                        $"Unknown column '{key}' in column list"
                    );
            }

            // Steps 2+3 merged: not-null and length-bound checks in one pass.
            foreach (TableColumnSchema col in validationRules)
            {
                bool present = values.TryGetValue(col.Name, out ColumnValue? val);

                if (col.NotNull)
                {
                    if (!present || val!.Type == ColumnType.Null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.NotNullViolation,
                            $"Column '{col.Name}' cannot be null"
                        );
                }

                if (present && val!.Type != ColumnType.Null
                    && (col.Type == ColumnType.String || col.Type == ColumnType.Bytes))
                {
                    EnforceLengthBound(col, val);
                }
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
                    // NULLs are distinct: a row with a NULL (or absent) value in any indexed column is
                    // exempt from the unique constraint and carries no index entry, so multiple such
                    // rows can coexist (standard SQL / Postgres / MySQL semantics).
                    if (HasNullKeyColumn(values, index.Columns))
                        continue;

                    CompositeColumnValue uniqueKeyValue = GetColumnValue(values, index.Columns);
                    write.IndexEntries.Add(new(index.KvId, uniqueKeyValue, Unique: true));
                }
                else if (index.Type == IndexType.Multi)
                {
                    CompositeColumnValue multiKeyValue = GetColumnValue(values, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));
                    write.IndexEntries.Add(new(index.KvId, multiKeyValue, Unique: false));
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
