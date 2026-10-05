
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
/// Inserts the rows of one INSERT ticket into a table, with their index entries, and checks the
/// foreign keys the table owns once every row is written (see <see cref="ForeignKeyStatementChecker"/>).
/// </summary>
internal sealed class RowInserter
{
    private readonly ILogger<ICamusDB> logger;

    /// <summary>Opens the parent tables a foreign-key check reads.</summary>
    private readonly TableOpener tableOpener;

    public RowInserter(ILogger<ICamusDB> logger, TableOpener tableOpener)
    {
        ArgumentNullException.ThrowIfNull(tableOpener);

        this.logger = logger;
        this.tableOpener = tableOpener;
    }

    /// <summary>
    /// Builds the foreign-key checker for a statement that calls <see cref="Insert"/> more than once —
    /// <c>INSERT … SELECT</c> writes in pages. The caller passes it to every call and completes it once,
    /// after the last page, so a parent written in a later page still counts for a child in an earlier
    /// one. Call it before the first write.
    /// </summary>
    internal ValueTask<ForeignKeyStatementChecker> CreateStatementCheckerAsync(DatabaseDescriptor database, TableDescriptor table, KvTransaction tx) =>
        ForeignKeyStatementChecker.ForChildWritesAsync(database, table, tableOpener, tx);

    private static void Validate(TableDescriptor table, InsertTicket ticket)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        // Build lookups once per statement rather than rescanning per row.
        // writableNames: O(1) membership test for step 1.
        // validationRules: single precomputed list of columns that need not-null or length checks.
        HashSet<string> writableNames = new(columns.Count, StringComparer.OrdinalIgnoreCase);
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
                    && col.Type is ColumnType.String or ColumnType.Bytes)
                {
                    EnforceLengthBound(col, val);
                }
            }

            // Step 4: CHECK constraints — evaluated after all values (including defaults) are present.
            CheckEnforcer.EnforceOnRow(table, values);
        }
    }

    private static void EnforceLengthBound(TableColumnSchema column, ColumnValue value)
    {
        if (column.Type == ColumnType.String)
        {
            string s = value.StrValue ?? "";
            int max = column.MaxLength ?? CamusDBConstants.DefaultStringMaxLength;
            if (s.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {s.Length})");
        }
        else if (column.Type == ColumnType.Bytes)
        {
            byte[] b = value.BytesValue ?? [];
            int max = column.MaxLength ?? CamusDBConstants.DefaultBytesMaxLength;
            if (b.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {b.Length})");
        }
    }

    /// <summary>
    /// Builds an index key from the row's values. A unique key is built only when every column is
    /// present and non-NULL (see <see cref="HasNullKeyColumn"/>), so a missing column there is a bug.
    /// A non-unique key indexes every row: a column the INSERT left out holds NULL, and is indexed as
    /// NULL, exactly like an explicit NULL — pass <paramref name="absentIsNull"/> for that case.
    /// </summary>
    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames, ColumnValue? extraUniqueValue = null, bool absentIsNull = false)
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
            {
                if (absentIsNull)
                {
                    columnValues[i] = ColumnValue.Null;
                    continue;
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"A null value was found for unique key field '{name}'"
                );
            }

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new(columnValues);
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

    /// <summary>
    /// Materializes the stored/payload (INCLUDE) tuple for a covering index, or <see langword="null"/>
    /// for a plain index. Called only from the branch that actually emits the index entry, so a row
    /// that carries no entry (e.g. a NULL unique key) never pays the encode + byte-check cost.
    /// </summary>
    private static byte[]? BuildIncludeTuple(TableIndexSchema index, Dictionary<string, ColumnValue> values, CamusDBOptions options)
        => index.HasIncludeColumns
            ? IndexIncludeValueCodec.EncodeTupleChecked(index.IncludeColumns, values, index.Name, options)
            : null;

    /// <summary>
    /// Inserts the ticket's rows. With no <paramref name="statementChecker"/>, this call is the whole
    /// statement: it builds the foreign-key checker before the first write and completes it after the
    /// last. With one, the caller owns the statement and completes the checker itself.
    /// </summary>
    /// <param name="insertedRows">
    /// Receives one (row id, stored values) pair for each written row, in insert order, for an
    /// <c>INSERT … RETURNING</c>. Null when the statement returns no rows, which costs nothing.
    /// </param>
    public async Task<int> Insert(
        DatabaseDescriptor database,
        TableDescriptor table,
        InsertTicket ticket,
        ForeignKeyStatementChecker? statementChecker = null,
        List<QueryResultRow>? insertedRows = null)
    {
        MaterializedViewAccessGuard.RequireWritable(table);
        Validate(table, ticket);

        ForeignKeyStatementChecker checker = statementChecker
            ?? await ForeignKeyStatementChecker.ForChildWritesAsync(database, table, tableOpener, ticket.TxnState).ConfigureAwait(false);

        InsertFluxState state = new(
            database: database,
            table: table,
            ticket: ticket
        )
        {
            ForeignKeys = checker,
            ReturningRows = insertedRows
        };

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        int inserted = await InsertInternal(machine, state).ConfigureAwait(false);

        if (statementChecker is null && checker.HasWork)
            await checker.CompleteAsync(ticket.TxnState).ConfigureAwait(false);

        return inserted;
    }

    private async Task<FluxAction> InsertRowsAndIndexes(InsertFluxState state)
    {
        TableDescriptor table = state.Table;
        InsertTicket ticket = state.Ticket;
        KvTransaction tx = ticket.TxnState;

        // Compiled once per statement: every inserted row is written under the current schema version,
        // so its positional layout is fixed for the whole VALUES list.
        CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, table.Schema.Version).ConfigureAwait(false);
        List<TableColumnSchema> schemaColumns = table.Schema.Columns!;

        // The storage rules for large values are read once per statement from the current options
        // snapshot, so a published setting change governs the next statement.
        LargeValuePolicy largeValuePolicy = LargeValuePolicy.For(schemaColumns, state.Database.Options);

        // Index writability is fixed for the statement (the transaction pins the schema version),
        // so filter once here instead of re-evaluating per index per row.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);

        // Process rows in bounded chunks so a large VALUES list does not retain all serialized
        // row bytes on the heap before the first KV write. Chunk size is tied to
        // SpillEffectiveThreshold so tests that set ForceSpillThresholdRows drive both spill
        // behavior and insert chunking with one knob.
        // The shared KvTransaction spans all chunks, so a duplicate-unique-key error in a later
        // chunk rolls back all prior chunks' staged writes and locks on transaction abort.
        int chunkSize = state.Database.Options.SpillEffectiveThreshold;
        List<KvTableStore.RowWrite> chunk = new(Math.Min(chunkSize, 64));

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            ObjectIdValue rowId = ObjectIdGenerator.Generate();

            // Built only once the first index entry exists, so a no-index table allocates no per-row list.
            List<KvTableStore.IndexWrite>? indexEntries = null;

            foreach (TableIndexSchema index in writableIndexes)
            {
                if (index.Type == IndexType.Unique)
                {
                    // NULLs are distinct: a row with a NULL (or absent) value in any indexed column is
                    // exempt from the unique constraint and carries no index entry, so multiple such
                    // rows can coexist (standard SQL / Postgres / MySQL semantics).
                    if (HasNullKeyColumn(values, index.Columns))
                        continue;

                    CompositeColumnValue uniqueKeyValue = GetColumnValue(values, index.Columns);
                    // Serialize the INCLUDE payload only now that the entry is known to be written —
                    // a NULL-key row above returns without paying the encode + byte-check cost.
                    (indexEntries ??= []).Add(new(index.KvId, uniqueKeyValue, Unique: true, IncludeTuple: BuildIncludeTuple(index, values, state.Database.Options)));
                }
                else if (index.Type == IndexType.Multi)
                {
                    CompositeColumnValue multiKeyValue = GetColumnValue(values, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()), absentIsNull: true);
                    (indexEntries ??= []).Add(new(index.KvId, multiKeyValue, Unique: false, IncludeTuple: BuildIncludeTuple(index, values, state.Database.Options)));
                }
            }

            EncodedRow encoded = codec.EncodeStorageValue(RowSlotAdapter.FromRow(schemaColumns, values), largeValuePolicy);

            state.ForeignKeys.AddChildRow(values);
            state.ReturningRows?.Add(new QueryResultRow(rowId, values));

            chunk.Add(new KvTableStore.RowWrite
            {
                RowId = rowId,
                RowData = encoded.StorageValue,
                IndexEntries = indexEntries,
                LargeValues = encoded.OutOfLine,
            });

            if (chunk.Count >= chunkSize)
            {
                await table.Store.WriteRowsBatch(tx, chunk).ConfigureAwait(false);
                state.InsertedRows += chunk.Count;
                chunk.Clear();
            }
        }

        if (chunk.Count > 0)
        {
            await table.Store.WriteRowsBatch(tx, chunk).ConfigureAwait(false);
            state.InsertedRows += chunk.Count;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Inserted {Count} row(s) in chunked writes", state.InsertedRows);

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
