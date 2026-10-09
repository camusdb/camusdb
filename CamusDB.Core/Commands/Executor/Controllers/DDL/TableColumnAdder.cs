/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Adds a column to a table and fills it in every row that existed before the column did.
///
/// <para><b>Each existing row gets the value an INSERT would give it.</b> A constant default is
/// copied, a function default (<c>now()</c>, <c>gen_uuid_v7()</c>) is evaluated once for each row,
/// and a sequence default (<c>DEFAULT nextval('…')</c>, <c>SERIAL</c>, <c>GENERATED … AS
/// IDENTITY</c>) draws one value for each row. A NOT NULL column whose fill is NULL fails the
/// statement with <see cref="CamusDBErrorCodes.NotNullViolation"/>, so the catalog never states a
/// constraint that a stored row breaks.</para>
///
/// <para><b>Only a row that does not hold the column is filled.</b> The row's own schema version
/// tells whether its bytes carry the column. A row written after the column existed holds the value
/// its writer chose, which can be an explicit NULL, and a rewrite would replace that value with the
/// default. The decoder cannot tell the two cases apart: it gives a row that lacks the column the
/// column's constant default, which is NULL for a function or a sequence default.</para>
///
/// <para>Both modes use <see cref="FillAddedColumnAsync"/>. A standalone node calls it from
/// <see cref="AddColumn"/>, inside the DDL transaction. A cluster calls it from the schema-change
/// coordinator, at the <c>WriteOnly → Public</c> step, through
/// <c>SchemaDdlService.BackfillColumnDefaultsAsync</c>. The column definition always comes from the
/// live schema, not from the statement, so a leader that resumes the job fills the same values.</para>
/// </summary>
public sealed class TableColumnAdder
{
    private readonly ILogger<ICamusDB> logger;

    private readonly SequenceStatementBinder sequenceBinder;

    internal TableColumnAdder(ILogger<ICamusDB> logger, SequenceStatementBinder sequenceBinder)
    {
        ArgumentNullException.ThrowIfNull(sequenceBinder);

        this.logger = logger;
        this.sequenceBinder = sequenceBinder;
    }

    private static void Validate(TableDescriptor table, AlterColumnTicket ticket, CamusDBOptions options)
    {
        bool hasColumn = false;

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (string.Equals(column.Name, ticket.Column.Name, StringComparison.OrdinalIgnoreCase))
            {
                hasColumn = true;
                break;
            }
        }

        if (hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate column '{ticket.Column.Name}'");

        int maxCols = options.MaxColumnsPerTable;
        if (maxCols > 0)
        {
            int currentCount = table.Schema.Columns!.Count;
            if (currentCount + 1 > maxCols)
                throw new CamusDBException(
                    CamusDBErrorCodes.SchemaLimitExceeded,
                    $"Table '{table.Name}' would exceed the maximum of {maxCols} columns per table");
        }
    }

    /// <summary>
    /// The standalone ADD COLUMN: applies the schema change, waits for the commits that wrote the table
    /// before it, then fills the existing rows inside <paramref name="tx"/>.
    /// </summary>
    /// <remarks>
    /// <para><b>The schema change commits on its own.</b> It goes through the schema log, not through
    /// <paramref name="tx"/>, so a failed fill leaves the column in the schema. The caller removes it
    /// again (<c>SchemaDdlService.AddColumnStandaloneAsync</c>).</para>
    ///
    /// <para><b>The fence comes before the fill.</b> A write planned under the earlier schema cannot
    /// commit after the change, but one whose commit is already in flight can land after the fill has
    /// read past its row. That row would then lack the column and decode to the constant default, which
    /// is NULL for a function or sequence default and for a NOT NULL column without a default.</para>
    /// </remarks>
    internal async Task<int> AddColumn(
        CatalogsManager catalogs,
        KvTransaction tx,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterColumnTicket ticket
    )
    {
        Validate(table, ticket, database.Options);

        ValueStopwatch timer = ValueStopwatch.StartNew();

        await catalogs.AlterTable(database, ticket, tx).ConfigureAwait(false);

        await database.FenceWritersAndWaitAsync(table.Schema, database.Kahuna.SchemaAckWaitTimeout).ConfigureAwait(false);

        int modifiedRows = await FillAddedColumnAsync(database, table, ticket.Column.Name, tx).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogColumnAdded(logger, modifiedRows, timeTaken);

        return modifiedRows;
    }

    /// <summary>
    /// Writes the default of the newly added column <paramref name="columnName"/> into every row that
    /// does not hold the column yet, inside <paramref name="tx"/>. Returns the number of rows written.
    /// </summary>
    /// <remarks>
    /// <para>Safe to run again: a row that a previous run filled holds the column and is skipped. A
    /// sequence draws values for the rows this run fills only.</para>
    ///
    /// <para>Throws <see cref="CamusDBErrorCodes.NotNullViolation"/> when the column is NOT NULL and a
    /// row would get NULL. The rows already written in <paramref name="tx"/> are then rolled back with
    /// it; removing the column is the caller's job.</para>
    /// </remarks>
    internal async Task<int> FillAddedColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        string columnName,
        KvTransaction tx)
    {
        TableColumnSchema column = table.Schema.Columns?.Find(
            c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
            ?? throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Column '{columnName}' does not exist in table '{table.Name}', so its rows cannot be filled");

        string columnId = column.Id;
        bool fillsNull = !HasFillValue(column);

        // Rows written under one schema version all hold, or all lack, the column.
        Dictionary<int, bool> versionHoldsColumn = new();
        RowEncoder.DictionaryDecodeState decodeState = new();
        List<(ObjectIdValue, IReadOnlyDictionary<string, ColumnValue>)> pending = new(StoredRowRewriter.BatchRows);
        Dictionary<string, ColumnValue>? sequenceRun = column.DefaultSequenceId is null ? null : new(4);
        int modifiedRows = 0;

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(tx, afterRowId: null).ConfigureAwait(false))
        {
            int storedVersion = RowEncoder.ReadStoredSchemaVersion(data.Span);

            if (!versionHoldsColumn.TryGetValue(storedVersion, out bool holdsColumn))
            {
                TableSchemaHistory history = await table.Schema.GetSchemaHistoryAsync(tx.TransactionId, storedVersion).ConfigureAwait(false);
                holdsColumn = history.Columns?.Exists(c => string.Equals(c.Id, columnId, StringComparison.Ordinal)) == true;
                versionHoldsColumn[storedVersion] = holdsColumn;
            }

            if (holdsColumn)
                continue;

            if (fillsNull && column.NotNull)
                throw ContainsNullValues(table, column);

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, data,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: decodeState).ConfigureAwait(false);

            pending.Add((rowId, row));

            if (pending.Count >= StoredRowRewriter.BatchRows)
            {
                modifiedRows += await FillBatchAsync(database, table, column, tx, pending, sequenceRun).ConfigureAwait(false);
                pending.Clear();
            }
        }

        modifiedRows += await FillBatchAsync(database, table, column, tx, pending, sequenceRun).ConfigureAwait(false);

        Log.LogColumnBackfillComplete(logger, modifiedRows, column.Name);

        return modifiedRows;
    }

    /// <summary>
    /// True when the column gives a row a value other than NULL: a non-NULL constant, a function or a
    /// sequence. ADD COLUMN ... NOT NULL without one is refused on a table that has a row.
    /// </summary>
    internal static bool HasFillValue(ColumnInfo column) =>
        column.DefaultSequenceId is not null
        || column.DefaultSequenceName is not null
        || column.Identity is not null
        || column.DefaultFunction is not null
        || column.Default is { Type: not ColumnType.Null };

    private static bool HasFillValue(TableColumnSchema column) =>
        column.DefaultSequenceId is not null
        || column.DefaultFunction is not null
        || column.DefaultValue is { Type: not ColumnType.Null };

    /// <summary>
    /// The error for a NOT NULL column that an existing row would hold NULL in. The same code and text
    /// as <c>ALTER COLUMN ... SET NOT NULL</c> over a NULL, plus the ways around it.
    /// </summary>
    internal static CamusDBException ContainsNullValues(TableDescriptor table, ColumnInfo column) =>
        ContainsNullValues(table.Name, column.Name);

    private static CamusDBException ContainsNullValues(TableDescriptor table, TableColumnSchema column) =>
        ContainsNullValues(table.Name, column.Name);

    private static CamusDBException ContainsNullValues(string tableName, string columnName) =>
        new(
            CamusDBErrorCodes.NotNullViolation,
            $"column \"{columnName}\" of table \"{tableName}\" contains null values: the table has rows, and " +
            "ADD COLUMN ... NOT NULL gives them no value. Declare a DEFAULT, or add the column without NOT NULL, " +
            $"fill it, then run ALTER TABLE {tableName} ALTER COLUMN {columnName} SET NOT NULL");

    private async Task<int> FillBatchAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TableColumnSchema column,
        KvTransaction tx,
        List<(ObjectIdValue RowId, IReadOnlyDictionary<string, ColumnValue> Values)> pending,
        Dictionary<string, ColumnValue>? sequenceRun)
    {
        if (pending.Count == 0)
            return 0;

        // One reservation for the batch, sized to the rows that take a value, so the sequence skips
        // nothing for the rows that already hold the column.
        if (column.DefaultSequenceId is { } sequenceId)
        {
            SequenceSchema sequence = database.Schema.FindSequenceById(sequenceId)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.SequenceDoesntExist,
                    $"Column '{column.Name}' defaults from sequence '{sequenceId}', which no longer exists");

            await sequenceBinder.ReserveIntoAsync(database, sequence, pending.Count, sequenceRun!, tx, CancellationToken.None)
                .ConfigureAwait(false);
        }

        for (int i = 0; i < pending.Count; i++)
        {
            ColumnValue value = column.DefaultSequenceId is { } drawFrom
                ? SequenceDefaults.TakeNextValue(drawFrom, column.Name, sequenceRun)
                : column.DefaultFunction is { } function
                    ? ScalarFunctionEvaluator.EvaluateNullary(function)
                    : column.DefaultValue ?? ColumnValue.Null;

            if (column.NotNull && value.Type == ColumnType.Null)
                throw ContainsNullValues(table, column);

            // The decoded row is this method's own copy, so it is written in place.
            ((Dictionary<string, ColumnValue>)pending[i].Values)[column.Name] = value;
        }

        await StoredRowRewriter.RewriteAsync(database, table, tx, pending).ConfigureAwait(false);

        return pending.Count;
    }
}
