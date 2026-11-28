/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Writes the entries of a newly added index for every row that exists, one committed batch at a
/// time. The schema-change coordinator runs it through
/// <see cref="SchemaDdlService.BackfillIndexEntriesAsync"/>, on a standalone node and in a cluster alike.
/// The index is <c>WriteOnly</c> while this runs: every statement that starts now maintains it, and this
/// fills in the rows written before that.
///
/// <para><b>Each batch locks its rows before it reads them.</b> A batch scans a run of row ids without
/// a lock, takes an exclusive lock on each row
/// (<see cref="KvTableStore.LockAndReadRowsForMutationAsync"/>), reads the rows under the lock,
/// writes the entries of the batch and commits. The lock refuses every other writer's write of the row
/// until the batch is done: a pessimistic writer at its own lock, an optimistic one at its row
/// write, which is the only write of its statement the lock meets. So an entry always matches a
/// committed row, provided a writer's other writes are ordered after its row write; see
/// <see cref="Storage.Kv.KvBatchWriter"/>, which stages the row write before the deletes of the
/// row's old entries for exactly this reason. An unlocked read would let an UPDATE commit between
/// the read and the entry write: the UPDATE removes the entry for its old value, which does not
/// exist yet, writes the entry for its new value, and the backfill then writes the old value's
/// entry, where it stays. Such a stale entry sends an equality lookup to a row that no longer
/// matches, and on a unique index it refuses the next row that takes the value. A row that a DELETE
/// removed between the scan and the lock reads back null and gets no entry.</para>
///
/// <para>The batch transaction is pessimistic whatever the engine default is. Under an optimistic
/// default the batch would take no lock; its reads would be validated when it commits instead, and
/// a writer that touched any of its rows first would abort the whole batch after all of its work.
/// Under the lock a batch loses at the lock only, before any work, and a writer that wants a locked
/// row waits or retries its own statement. A batch that loses its locks is retried without limit by
/// <see cref="BackfillBatchRunner"/>, so a table under sustained writes is still indexed.</para>
///
/// <para><b>The entries of a batch go out together.</b> The batch decodes its rows first and then locks
/// and sets the entries of all of them in two round trips
/// (<see cref="KvTableStore.PutIndexEntriesBatch"/>), and more only for an index with many entries per
/// row. The rows stay locked for that many round trips, whatever the size of the batch, so a writer that
/// wants one of them waits for the batch and not for one round trip per row of it.</para>
///
/// <para><b>A row's entry may already exist.</b> An UPDATE that committed while the index was
/// <c>WriteOnly</c> wrote the row's entry before this backfill reached the row. The write is therefore
/// the idempotent form of <see cref="KvTableStore.PutIndexEntriesBatch"/> (<c>backfillMode</c>): on a
/// unique index, an existing entry that names the same row is accepted, and one that names another row
/// is the duplicate it looks like. A resumed backfill re-runs at most its last batch for the same
/// reason.</para>
///
/// <para>After each full batch commits, the checkpoint callback receives the last row id, which the
/// coordinator persists so a resume skips the rows already indexed. The trailing
/// short batch does not checkpoint: the element reaching <c>Public</c> signals completion.</para>
/// </summary>
internal static class IndexBackfiller
{
    /// <summary>
    /// Rows scanned per backfill batch. It bounds the row locks one batch holds and the entries one
    /// transaction writes. It is also the resume-checkpoint granularity, so raising it makes a resume
    /// replay more work. It is a ceiling, not a fixed size: a batch that loses its locks is halved by
    /// <see cref="BackfillBatchRunner"/>.
    /// </summary>
    internal const int BatchRows = 500;

    /// <summary>
    /// Test-only seam: runs before each attempt of a batch locks its rows, after the batch's row ids
    /// were scanned without a lock. A test commits or holds a write at that point, or counts the
    /// attempts. Null in production; a test clears it after use.
    /// </summary>
    internal static Func<Task>? TestInterceptBeforeBatchLock;

    /// <summary>
    /// Test-only seam: runs after an attempt locked and read the rows of its batch and before it
    /// writes the first entry. A test starts a competing statement there, so the statement runs
    /// against rows the batch holds and is about to index. Null in production; a test clears it after
    /// use.
    /// </summary>
    internal static Func<Task>? TestInterceptAfterBatchRead;

    /// <summary>
    /// The index to build. <paramref name="KeyColumns"/> are the key columns in index order and
    /// <paramref name="IncludeColumns"/> the covering columns, both by name; null when the index
    /// carries none. <paramref name="Inverted"/> marks a GIN index: its one key column is a jsonb
    /// document, and the backfill writes one entry per distinct document entry
    /// (<see cref="JsonbGinIndexKeys"/>) instead of one key per row.
    /// </summary>
    internal readonly record struct Target(
        string IndexId,
        string IndexName,
        bool Unique,
        string[] KeyColumns,
        string[]? IncludeColumns,
        bool Inverted = false,
        BoundIndexExpression?[]? Expressions = null
    )
    {
        /// <summary>
        /// The key as the write paths see it: <see cref="KeyColumns"/> holds the hidden key name at
        /// each expression position, and <see cref="Expressions"/> the bound expression there, so
        /// <see cref="IndexKeyRows"/> fills a row the same way for a backfilled entry as for one an
        /// INSERT writes.
        /// </summary>
        public TableIndexSchema KeyShape => new(
            IndexName,
            KeyColumns,
            Unique ? IndexType.Unique : IndexType.Multi,
            id: IndexId,
            expressions: Expressions);
    }

    /// <summary>
    /// Builds <paramref name="target"/> over every row of <paramref name="table"/> after
    /// <paramref name="afterRowId"/>, as the class summary describes, and returns the rows it visited under
    /// their locks. <paramref name="priority"/> is the admission priority of each batch transaction, or null
    /// for the engine default.
    /// </summary>
    internal static async Task<int> BackfillAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Target target,
        ObjectIdValue? afterRowId,
        TransactionPriority? priority,
        Func<string, Task>? onCheckpoint,
        ILogger logger,
        CamusDBOptions options,
        CancellationToken cancellationToken)
    {
        // The backfill reads only the key and INCLUDE columns, so the per-row decode and the
        // out-of-line fetch are narrowed to them. The rows are never re-encoded, only turned into
        // entries, and the values of those columns are the same as in a full decode.
        // An expression position reads the columns of its expression, not its hidden key name.
        TableIndexSchema keyShape = target.KeyShape;
        HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase);
        IndexKeyRows.AddReadColumns(keyShape, requiredColumns);
        if (target.IncludeColumns is { } includeColumns)
        {
            foreach (string columnName in includeColumns)
                requiredColumns.Add(columnName);
        }

        LargeValueFetch? largeValues = LargeValueFetch.Columns(table.Schema, requiredColumns);
        string element = $"index '{target.IndexName}' of table '{table.Name}'";

        int totalRows = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<ObjectIdValue> rowIds = await BackfillBatchRunner.ScanRowIdsAsync(database, table, afterRowId, BatchRows, logger, element, cancellationToken).ConfigureAwait(false);
            if (rowIds.Count == 0)
                break;

            totalRows += await BackfillBatchRunner.RunAsync(
                rowIds,
                (ids, ct) => WriteEntriesAsync(database, table, target, keyShape, requiredColumns, largeValues, ids, priority, options, ct),
                logger,
                element,
                cancellationToken).ConfigureAwait(false);

            // A short batch means the scan reached the end of the table.
            if (rowIds.Count < BatchRows)
                break;

            // More rows may remain: advance the cursor, then checkpoint it. Every row up to the cursor
            // is committed, so a resume from the checkpoint re-runs nothing it must not.
            afterRowId = rowIds[^1];
            if (onCheckpoint is not null)
                await onCheckpoint(rowIds[^1].ToString()).ConfigureAwait(false);
        }

        return totalRows;
    }

    /// <summary>
    /// One attempt over one batch: locks the rows, reads them under the lock, writes the entries of the
    /// rows that still exist and commits. Returns the rows that still existed. A retryable refusal
    /// (a lock a writer holds, a commit the coordinator refused) propagates to the runner, which
    /// retries the batch; everything else ends the build.
    /// </summary>
    private static async Task<int> WriteEntriesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Target target,
        TableIndexSchema keyShape,
        HashSet<string> requiredColumns,
        LargeValueFetch? largeValues,
        IReadOnlyList<ObjectIdValue> rowIds,
        TransactionPriority? priority,
        CamusDBOptions options,
        CancellationToken cancellationToken)
    {
        if (TestInterceptBeforeBatchLock is { } intercept)
            await intercept().ConfigureAwait(false);

        // Pessimistic with no read-set validation, whatever the engine default: the row locks are
        // this batch's conflict detection (see the class summary). Unlimited mutations: a batch is
        // bounded by its row count, not by the per-statement limit.
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0,
            locking: KeyValueTransactionLocking.Pessimistic,
            readValidation: ReadValidation.None,
            priority: priority
        ).ConfigureAwait(false);

        try
        {
            ReadOnlyMemory<byte>?[] current = await table.Store.LockAndReadRowsForMutationAsync(tx, rowIds, cancellationToken, largeValues).ConfigureAwait(false);

            if (TestInterceptAfterBatchRead is { } afterRead)
                await afterRead().ConfigureAwait(false);

            // Per-batch decode-plan cache; the visibility version is re-read from the live schema
            // for each batch.
            RowEncoder.DictionaryDecodeState decodeState = new();
            List<KvTableStore.IndexEntryWrite> entries = new(rowIds.Count);
            int present = 0;

            for (int i = 0; i < rowIds.Count; i++)
            {
                // Deleted after the scan read its id.
                if (current[i] is not { } data)
                    continue;

                present++;
                ObjectIdValue rowId = rowIds[i];

                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema, tx.TransactionId, rowId, data,
                    requiredColumns: requiredColumns,
                    visibilitySchemaVersion: table.Schema.Version,
                    decodeState: decodeState).ConfigureAwait(false);

                AddRowEntries(entries, target, keyShape, rowId, row, tx.MutationLimit, options);

                if (entries.Count >= KvStoreConstants.IndexEntryBatchMaxEntries)
                    await FlushAsync(tx, table, target, entries, backfillMode: true, cancellationToken).ConfigureAwait(false);
            }

            await FlushAsync(tx, table, target, entries, backfillMode: true, cancellationToken).ConfigureAwait(false);

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

            return present;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Builds <paramref name="target"/> over <paramref name="rows"/> inside <paramref name="tx"/>, and returns
    /// the number of rows. For a table that a transaction with a private schema created: only that
    /// transaction can see the table before its commit, so <paramref name="rows"/> are its own rows, read
    /// through it, and no other writer can touch them. No batch, lock pass or fence is needed. The entries
    /// count against the limits of <paramref name="tx"/>, as the rows did.
    ///
    /// <para>The write is the plain form of <see cref="KvTableStore.PutIndexEntriesBatch"/>: a second row with
    /// the same unique key fails with <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>, in the same
    /// batch or against the transaction's own staged copy of the key, as a second INSERT of it would.</para>
    /// </summary>
    internal static async Task<int> BuildInTransactionAsync(
        KvTransaction tx,
        TableDescriptor table,
        Target target,
        IAsyncEnumerable<QueryResultRow> rows,
        CamusDBOptions options,
        CancellationToken cancellationToken)
    {
        TableIndexSchema keyShape = target.KeyShape;
        List<KvTableStore.IndexEntryWrite> entries = [];
        int count = 0;

        await foreach (QueryResultRow result in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // The decoder's rows match column names without case; a lazy query row is copied the same way.
            Dictionary<string, ColumnValue> row = result.Row as Dictionary<string, ColumnValue>
                ?? new(result.Row, StringComparer.OrdinalIgnoreCase);

            AddRowEntries(entries, target, keyShape, result.RowId, row, tx.MutationLimit, options);
            count++;

            if (entries.Count >= KvStoreConstants.IndexEntryBatchMaxEntries)
                await FlushAsync(tx, table, target, entries, backfillMode: false, cancellationToken).ConfigureAwait(false);
        }

        await FlushAsync(tx, table, target, entries, backfillMode: false, cancellationToken).ConfigureAwait(false);

        return count;
    }

    /// <summary>
    /// Writes the collected entries and empties <paramref name="entries"/> for the next rows.
    /// <paramref name="backfillMode"/> selects the idempotent write of a backfill that runs beside other
    /// writers; see the class summary.
    /// </summary>
    private static async Task FlushAsync(
        KvTransaction tx,
        TableDescriptor table,
        Target target,
        List<KvTableStore.IndexEntryWrite> entries,
        bool backfillMode,
        CancellationToken cancellationToken)
    {
        await table.Store.PutIndexEntriesBatch(tx, target.IndexId, entries, target.Unique, backfillMode, cancellationToken).ConfigureAwait(false);
        entries.Clear();
    }

    /// <summary>
    /// Adds the entries of <paramref name="target"/> for one decoded row to <paramref name="entries"/>: one
    /// key for an ordered index (none on a unique index when a key column is NULL), one entry per distinct
    /// document entry for a GIN index.
    /// </summary>
    private static void AddRowEntries(
        List<KvTableStore.IndexEntryWrite> entries,
        Target target,
        TableIndexSchema keyShape,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> row,
        int mutationLimit,
        CamusDBOptions options)
    {
        // The values of the expression key parts, under their hidden key names. A row decoded here holds
        // every column the index reads, so an absent one is a bug, not a NULL.
        Dictionary<string, ColumnValue> keyRow = IndexKeyRows.For(keyShape, row);

        // NULLs are distinct: a unique index holds no entry for a row with a NULL (or absent) value in any
        // key column, so several such rows can coexist. The insert path applies the same rule, which keeps
        // a backfilled index identical to one built row by row. A non-unique index keeps every row (the
        // row id breaks ties).
        if (target.Unique && HasNullKeyColumn(keyRow, target.KeyColumns))
            return;

        // A GIN index: one non-unique entry per distinct entry of the document, none for NULL. The insert
        // path builds the same entries, so a backfilled index matches one built row by row.
        if (target.Inverted)
        {
            if (JsonbGinIndexKeys.EntriesOf(target.KeyColumns[0], row) is { } documentEntries)
            {
                JsonbGinIndexKeys.CheckEntryWrites(target.IndexName, target.KeyColumns[0], documentEntries.Count, mutationLimit);
                ColumnValue rowIdValue = JsonbGinIndexKeys.RowIdValue(rowId);
                foreach (string entry in documentEntries)
                    entries.Add(new(JsonbGinIndexKeys.Key(entry, rowIdValue), rowId));
            }
            return;
        }

        int k = 0;
        ColumnValue[] columnValues = target.Unique
            ? new ColumnValue[target.KeyColumns.Length]
            : new ColumnValue[target.KeyColumns.Length + 1];

        foreach (string columnName in target.KeyColumns)
        {
            ColumnValue? keyValue = keyRow.GetValueOrDefault(columnName);
            if (keyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"A null value was found for index key field '{columnName}'"
                );
            columnValues[k++] = keyValue;
        }

        if (!target.Unique)
            columnValues[k] = new(ColumnType.Id, rowId.ToString());

        CompositeColumnValue compositeKey = new(columnValues);

        // The stored (INCLUDE) values of a covering index; they may be NULL, as payload, unlike a key
        // column. EncodeTupleChecked enforces the per-entry byte ceiling before the KV write.
        byte[]? includeTuple = target.IncludeColumns is { } includeColumns
            ? IndexIncludeValueCodec.EncodeTupleChecked(includeColumns, row, target.IndexName, options)
            : null;

        entries.Add(new(compositeKey, rowId, includeTuple));
    }

    /// <summary>
    /// True when any key column is absent from the row or holds NULL. Such a row is exempt from a
    /// unique index (NULLs are distinct), on the insert path and here alike.
    /// </summary>
    private static bool HasNullKeyColumn(Dictionary<string, ColumnValue> row, string[] keyColumns)
    {
        foreach (string columnName in keyColumns)
        {
            ColumnValue? value = row.GetValueOrDefault(columnName);
            if (value is null || value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }
}
