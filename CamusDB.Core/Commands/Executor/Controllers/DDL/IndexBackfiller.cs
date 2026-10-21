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
/// time. The standalone flux (<see cref="TableIndexAdder"/>) and the coordinator-driven cluster path
/// (<see cref="SchemaDdlService.BackfillIndexEntriesAsync"/>) both run this, so the two build an index
/// the same way. The index is <c>WriteOnly</c> while this runs: every statement that starts now
/// maintains it, and this fills in the rows written before that.
///
/// <para><b>Each batch locks its rows before it reads them.</b> A batch scans a run of row ids without
/// a lock, takes an exclusive lock on each row
/// (<see cref="KvTableStore.LockAndReadRowsForMutationAsync"/>), reads the rows under the lock,
/// writes one entry per row and commits. The lock refuses every other writer's write of the row
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
/// <para><b>A row's entry may already exist.</b> An UPDATE that committed while the index was
/// <c>WriteOnly</c> wrote the row's entry before this backfill reached the row. The write is therefore
/// the idempotent form of <see cref="KvTableStore.PutIndexEntry"/> (<c>backfillMode</c>): on a unique
/// index, an existing entry that names the same row is accepted, and one that names another row is
/// the duplicate it looks like. A resumed cluster backfill re-runs at most its last batch for the same
/// reason.</para>
///
/// <para>After each full batch commits, the checkpoint callback receives the last row id, which the
/// cluster coordinator persists so a leader-change resume skips the rows already indexed. The trailing
/// short batch does not checkpoint: the element reaching <c>Public</c> signals completion.</para>
/// </summary>
internal static class IndexBackfiller
{
    /// <summary>
    /// Rows scanned per backfill batch. It bounds the row locks one batch holds and the entries one
    /// transaction writes. On the cluster path it is also the resume-checkpoint granularity, so raising
    /// it makes a leader change replay more work. It is a ceiling, not a fixed size: a batch that loses
    /// its locks is halved by <see cref="BackfillBatchRunner"/>.
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
    /// carries none.
    /// </summary>
    internal readonly record struct Target(
        string IndexId,
        string IndexName,
        bool Unique,
        string[] KeyColumns,
        string[]? IncludeColumns
    );

    /// <summary>
    /// What a backfill did: the rows it visited under their locks, and the id of the last row the
    /// scan reached (null for an empty table).
    /// </summary>
    internal readonly record struct Result(int Rows, string? LastRowId);

    /// <summary>
    /// Builds <paramref name="target"/> over every row of <paramref name="table"/> after
    /// <paramref name="afterRowId"/>, as the class summary describes. <paramref name="priority"/> is
    /// the admission priority of each batch transaction, or null for the engine default.
    /// </summary>
    internal static async Task<Result> BackfillAsync(
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
        HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase);
        foreach (string columnName in target.KeyColumns)
            requiredColumns.Add(columnName);
        if (target.IncludeColumns is { } includeColumns)
        {
            foreach (string columnName in includeColumns)
                requiredColumns.Add(columnName);
        }

        LargeValueFetch? largeValues = LargeValueFetch.Columns(table.Schema, requiredColumns);
        string element = $"index '{target.IndexName}' of table '{table.Name}'";

        int totalRows = 0;
        string? lastRowId = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<ObjectIdValue> rowIds = await BackfillBatchRunner.ScanRowIdsAsync(database, table, afterRowId, BatchRows, logger, element, cancellationToken).ConfigureAwait(false);
            if (rowIds.Count == 0)
                break;

            totalRows += await BackfillBatchRunner.RunAsync(
                rowIds,
                (ids, ct) => WriteEntriesAsync(database, table, target, requiredColumns, largeValues, ids, priority, options, ct),
                logger,
                element,
                cancellationToken).ConfigureAwait(false);

            lastRowId = rowIds[^1].ToString();

            // A short batch means the scan reached the end of the table.
            if (rowIds.Count < BatchRows)
                break;

            // More rows may remain: advance the cursor, then checkpoint it. Every row up to the cursor
            // is committed, so a resume from the checkpoint re-runs nothing it must not.
            afterRowId = rowIds[^1];
            if (onCheckpoint is not null)
                await onCheckpoint(lastRowId).ConfigureAwait(false);
        }

        return new Result(totalRows, lastRowId);
    }

    /// <summary>
    /// One attempt over one batch: locks the rows, reads them under the lock, writes one entry per
    /// row that still exists and commits. Returns the rows that still existed. A retryable refusal
    /// (a lock a writer holds, a commit the coordinator refused) propagates to the runner, which
    /// retries the batch; everything else ends the build.
    /// </summary>
    private static async Task<int> WriteEntriesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Target target,
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

                // NULLs are distinct: a unique index holds no entry for a row with a NULL (or absent)
                // value in any key column, so several such rows can coexist. The insert path applies the
                // same rule, which keeps a backfilled index identical to one built row by row. A
                // non-unique index keeps every row (the row id breaks ties).
                if (target.Unique && HasNullKeyColumn(row, target.KeyColumns))
                    continue;

                int k = 0;
                ColumnValue[] columnValues = target.Unique
                    ? new ColumnValue[target.KeyColumns.Length]
                    : new ColumnValue[target.KeyColumns.Length + 1];

                foreach (string columnName in target.KeyColumns)
                {
                    ColumnValue? keyValue = row.GetValueOrDefault(columnName);
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

                // The stored (INCLUDE) values of a covering index; they may be NULL, as payload, unlike
                // a key column. EncodeTupleChecked enforces the per-entry byte ceiling before the KV write.
                byte[]? includeTuple = target.IncludeColumns is { } includeColumns
                    ? IndexIncludeValueCodec.EncodeTupleChecked(includeColumns, row, target.IndexName, options)
                    : null;

                await table.Store.PutIndexEntry(
                    tx, target.IndexId, compositeKey, rowId, target.Unique,
                    backfillMode: true, includeTuple: includeTuple, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

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
