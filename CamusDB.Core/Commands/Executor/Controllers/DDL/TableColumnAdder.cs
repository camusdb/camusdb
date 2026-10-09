/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Fills a newly added column in every row that does not hold a value for it yet. The schema-change
/// coordinator calls it while the column is <c>WriteOnly</c>, before the column becomes readable, in
/// both standalone and cluster mode.
///
/// <para><b>Each such row gets the value an INSERT would give it.</b> A constant default is copied, a
/// function default (<c>now()</c>, <c>gen_uuid_v7()</c>) is evaluated once for each row, and a sequence
/// default (<c>DEFAULT nextval('…')</c>, <c>GENERATED … AS IDENTITY</c>) draws one value for each row.
/// A NOT NULL column whose fill is NULL fails with <see cref="CamusDBErrorCodes.NotNullViolation"/>, so
/// the column is never published over a NULL it forbids.</para>
///
/// <para><b>Which rows need a value.</b> The decision is made on the row's stored layout:</para>
/// <list type="bullet">
/// <item><description>The layout does not have the column: the row was written before the column existed.</description></item>
/// <item><description>The layout has the column in <c>DeleteOnly</c>: the writer could not write it, and stored a NULL placeholder.</description></item>
/// <item><description>The layout has the column writable, and it holds NULL where the column is NOT NULL or has a
/// function or sequence default. No statement can name the column before it is readable, so that NULL
/// was not chosen by a user. An UPDATE of an older row writes the decoder's injected default, which is
/// NULL for those defaults.</description></item>
/// </list>
/// <para>Any other row keeps its value: an INSERT during <c>WriteOnly</c> already evaluated the default.</para>
///
/// <para><b>Each batch locks its rows and reads them again before it writes.</b> The first read takes no
/// lock, so a concurrent UPDATE or DELETE can commit between it and the write. Writing the image from
/// the first read would lose that update, or bring a deleted row back. With the lock held, the read
/// returns the latest committed row, and a row that a DELETE removed comes back null and is skipped
/// (<see cref="KvTableStore.LockAndReadRowsForMutationAsync"/>).</para>
///
/// <para><b>Each batch commits on its own.</b> The work held by one transaction is bounded by the batch,
/// not by the table, and a row lock is held for one batch only. The column is not readable until the
/// fill completes, so a partly filled column is never visible. A run that stops part-way is safe to run
/// again: a filled row no longer needs a value and is skipped. A batch that loses a lock race is retried;
/// any other failure stops the fill, and the coordinator removes the column.</para>
/// </summary>
public sealed class TableColumnAdder
{
    /// <summary>Attempts for one batch that keeps losing lock races to concurrent writers.</summary>
    private const int BatchAttempts = 8;

    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Test-only hook: invoked after a batch's row ids were scanned without a lock, and before the batch
    /// locks and reads them. A test commits an UPDATE or a DELETE at that point. Null in production; a
    /// test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeBatchLock;

    private readonly SequenceStatementBinder sequenceBinder;

    internal TableColumnAdder(ILogger<ICamusDB> logger, SequenceStatementBinder sequenceBinder)
    {
        ArgumentNullException.ThrowIfNull(sequenceBinder);

        this.logger = logger;
        this.sequenceBinder = sequenceBinder;
    }

    /// <summary>
    /// Fills the newly added column <paramref name="columnName"/> of <paramref name="table"/>, one
    /// committed batch at a time. Returns the number of rows written. The column definition comes from
    /// the live schema, so a leader that resumes the job fills the same values.
    /// </summary>
    internal async Task<int> FillAddedColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        string columnName,
        CancellationToken cancellationToken = default)
    {
        TableColumnSchema column = table.Schema.Columns?.Find(
            c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
            ?? throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Column '{columnName}' does not exist in table '{table.Name}', so its rows cannot be filled");

        // A nullable column with no default reads NULL in every row already: a row without the column
        // decodes to NULL, and so does a DeleteOnly placeholder. Rewriting the table would change nothing.
        if (!column.NotNull && column.DefaultSequenceId is null && column.DefaultFunction is null
            && column.DefaultValue is null or { Type: ColumnType.Null })
            return 0;

        ColumnFill fill = new(column);
        ObjectIdValue? afterRowId = null;
        int modifiedRows = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<ObjectIdValue> rowIds = await ScanRowIdsAsync(database, table, afterRowId, cancellationToken).ConfigureAwait(false);
            if (rowIds.Count == 0)
                break;

            if (TestInterceptBeforeBatchLock is { } intercept)
                await intercept().ConfigureAwait(false);

            modifiedRows += await SerializableRetryHelper.ExecuteAutocommitAsync(
                _ => FillBatchAsync(database, table, fill, rowIds, cancellationToken),
                canRetry: static () => true,
                maxAttempts: BatchAttempts,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            if (rowIds.Count < StoredRowRewriter.BatchRows)
                break;

            afterRowId = rowIds[^1];
        }

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

    /// <summary>
    /// The error for a NOT NULL column that an existing row would hold NULL in. The same code and text
    /// as <c>ALTER COLUMN ... SET NOT NULL</c> over a NULL, plus the ways around it.
    /// </summary>
    internal static CamusDBException ContainsNullValues(string tableName, string columnName) =>
        new(
            CamusDBErrorCodes.NotNullViolation,
            $"column \"{columnName}\" of table \"{tableName}\" contains null values: the table has rows, and " +
            "ADD COLUMN ... NOT NULL gives them no value. Declare a DEFAULT, or add the column without NOT NULL, " +
            $"fill it, then run ALTER TABLE {tableName} ALTER COLUMN {columnName} SET NOT NULL");

    /// <summary>
    /// The next ids in row-id order after <paramref name="afterRowId"/>, at most one batch. Read without a
    /// lock: only the ids are kept, and the batch reads each row again under its lock.
    /// </summary>
    private static async Task<List<ObjectIdValue>> ScanRowIdsAsync(
        DatabaseDescriptor database, TableDescriptor table, ObjectIdValue? afterRowId, CancellationToken cancellationToken)
    {
        List<ObjectIdValue> rowIds = new(StoredRowRewriter.BatchRows);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx, afterRowId: afterRowId).ConfigureAwait(false))
            {
                rowIds.Add(rowId);
                if (rowIds.Count >= StoredRowRewriter.BatchRows)
                    break;

                cancellationToken.ThrowIfCancellationRequested();
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return rowIds;
    }

    /// <summary>
    /// One batch in its own transaction: locks the rows, reads them, fills those that need a value, writes
    /// them and commits. Returns the number of rows written.
    /// </summary>
    private async Task<int> FillBatchAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        ColumnFill fill,
        List<ObjectIdValue> rowIds,
        CancellationToken cancellationToken)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            ReadOnlyMemory<byte>?[] current = await table.Store.LockAndReadRowsForMutationAsync(tx, rowIds, cancellationToken).ConfigureAwait(false);

            RowEncoder.DictionaryDecodeState decodeState = new();
            List<(ObjectIdValue, IReadOnlyDictionary<string, ColumnValue>)> pending = new(rowIds.Count);

            for (int i = 0; i < rowIds.Count; i++)
            {
                // Deleted after the scan read its id.
                if (current[i] is not { } data)
                    continue;

                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema, tx.TransactionId, rowIds[i], data,
                    visibilitySchemaVersion: table.Schema.Version,
                    decodeState: decodeState).ConfigureAwait(false);

                if (!await fill.NeedsValueAsync(table.Schema, tx, data, row).ConfigureAwait(false))
                    continue;

                pending.Add((rowIds[i], row));
            }

            if (pending.Count > 0)
            {
                await AssignValuesAsync(database, table, fill.Column, tx, pending).ConfigureAwait(false);
                await StoredRowRewriter.RewriteAsync(database, table, tx, pending).ConfigureAwait(false);
            }

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

            return pending.Count;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Writes the column's value into each row of <paramref name="pending"/>. A sequence default draws
    /// one reservation for the batch, sized to the rows that take a value.
    /// </summary>
    private async Task AssignValuesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TableColumnSchema column,
        KvTransaction tx,
        List<(ObjectIdValue RowId, IReadOnlyDictionary<string, ColumnValue> Values)> pending)
    {
        Dictionary<string, ColumnValue>? sequenceRun = null;

        if (column.DefaultSequenceId is { } sequenceId)
        {
            SequenceSchema sequence = database.Schema.FindSequenceById(sequenceId)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.SequenceDoesntExist,
                    $"Column '{column.Name}' defaults from sequence '{sequenceId}', which no longer exists");

            sequenceRun = new(4);
            await sequenceBinder.ReserveIntoAsync(database, sequence, pending.Count, sequenceRun, tx, CancellationToken.None)
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
                throw ContainsNullValues(table.Name, column.Name);

            // The decoded row is this batch's own copy, so it is written in place.
            ((Dictionary<string, ColumnValue>)pending[i].Values)[column.Name] = value;
        }
    }

    /// <summary>
    /// The per-run state of one fill: the column, and whether each stored layout version holds the column
    /// writable. Rows written under one version all hold it, or all lack it.
    /// </summary>
    private sealed class ColumnFill
    {
        private readonly Dictionary<int, bool> layoutHoldsWritable = new();

        /// <summary>
        /// True when a NULL in a layout that holds the column writable still needs a value. See the class
        /// summary for why such a NULL was not chosen by a user.
        /// </summary>
        private readonly bool nullNeedsValue;

        internal TableColumnSchema Column { get; }

        internal ColumnFill(TableColumnSchema column)
        {
            Column = column;
            nullNeedsValue = column.NotNull || column.DefaultFunction is not null || column.DefaultSequenceId is not null;
        }

        internal async ValueTask<bool> NeedsValueAsync(
            TableSchema schema, KvTransaction tx, ReadOnlyMemory<byte> data, Dictionary<string, ColumnValue> row)
        {
            int storedVersion = RowEncoder.ReadStoredSchemaVersion(data.Span);

            if (!layoutHoldsWritable.TryGetValue(storedVersion, out bool holdsWritable))
            {
                TableSchemaHistory history = await schema.GetSchemaHistoryAsync(tx.TransactionId, storedVersion).ConfigureAwait(false);
                TableColumnSchema? stored = history.Columns?.Find(c => string.Equals(c.Id, Column.Id, StringComparison.Ordinal));
                holdsWritable = stored is not null && SchemaElementStateRules.IsWritable(stored);
                layoutHoldsWritable[storedVersion] = holdsWritable;
            }

            if (!holdsWritable)
                return true;

            return nullNeedsValue
                && (!row.TryGetValue(Column.Name, out ColumnValue? value) || value.Type == ColumnType.Null);
        }
    }
}
