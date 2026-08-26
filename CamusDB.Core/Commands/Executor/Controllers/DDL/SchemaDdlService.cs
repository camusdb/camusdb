
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes schema DDL against one database: create/drop/rename/relink table, add and drop columns
/// and indexes, constraints, and comments. Every statement here either runs locally or is handed to
/// <see cref="DdlForwardingCoordinator"/> first, so a follower never mutates schema itself.
///
/// <para><b>There are three execution shapes, and picking the wrong one is the classic bug.</b>
/// <see cref="ExecuteDdlInTransaction"/> is the single-transaction shape for metadata-only work.
/// <see cref="ExecuteClusteredIndexDdlAsync"/> is the two-phase shape used whenever index <em>data</em>
/// is written: phase 1 commits the backfill so the entries are durable and visible, and only then does
/// phase 2 replicate the schema delta that makes the index public. The staged-coordinator shape
/// (<see cref="ExecuteClusterAddColumnAsync"/>, <see cref="ExecuteClusterAddIndexAsync"/>) drives an
/// element through <c>Absent → DeleteOnly → WriteOnly → Public</c> so the cluster is never in a state
/// where one node writes an element another node cannot read.</para>
///
/// <para><b>Every path holds <c>SchemaDdlSemaphore</c> and every path releases it in a
/// <c>finally</c> that also fires the deferred step-down.</b> The semaphore is what makes
/// "resolve the target, then mutate it" atomic against a concurrent drop/recreate of the same name —
/// which is why it is taken in standalone mode too, not only in cluster mode. The step-down must fire
/// after the KV transaction has settled, never before, or leadership can move while a write is still
/// in flight.</para>
///
/// <para>Partial failures compensate rather than unwind: a backfill that spans batches has already
/// committed the earlier ones, so an aborted ADD INDEX purges the orphaned entries
/// (<see cref="CompensateAbortedAddIndexAsync"/>) and a partially staged cluster add drops the
/// element and deletes its coordinator job (<see cref="CompensateClusterAddIndexAsync"/>).</para>
/// </summary>
internal sealed class SchemaDdlService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    private readonly CatalogsManager catalogs;

    private readonly DdlForwardingCoordinator ddlForwarding;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableConstraintAlterer tableConstraintAlterer;

    private readonly CommentSetter commentSetter;

    private readonly TableDropper tableDropper;

    private readonly RowDeleter rowDeleter;

    private readonly QueryExecutor queryExecutor;

    private readonly SqlParserCache sqlParserCache;

    // DDL transaction commits must be bounded — LocateAndCommitTransaction with
    // CancellationToken.None can hang indefinitely if the schema partition Raft actor
    // is stalled. 10 s covers leader-election time in a healthy cluster while still
    // converting permanent stalls into a recoverable CamusDBException.
    private static readonly TimeSpan DdlCommitTimeout = TimeSpan.FromSeconds(10);

    // Number of rows indexed per Kahuna transaction during backfill.  Committing in bounded
    // batches keeps transaction size manageable and allows a leader-change resume to skip
    // already-indexed rows via the persisted StartOffset checkpoint. Shared with the standalone
    // flux backfill so both paths batch identically.
    private const int BackfillBatchSize = TableIndexAdder.IndexBackfillBatchSize;

    /// <summary>
    /// Test-only hook: invoked after each intermediate batch checkpoint is persisted
    /// (i.e., after batch N commits and before batch N+1 starts). Allows tests to inject
    /// a pause or forced leader change between batches without relying on timing.
    /// Set to null in production; cleared in test TearDown.
    /// </summary>
    internal Func<Task>? TestInterceptAfterBackfillCheckpoint;

    internal SchemaDdlService(
        ExecutorContext context,
        CamusDBOptions options,
        CatalogsManager catalogs,
        DdlForwardingCoordinator ddlForwarding,
        TableCreator tableCreator,
        TableColumnAlterer tableColumnAlterer,
        TableIndexAlterer tableIndexAlterer,
        TableConstraintAlterer tableConstraintAlterer,
        CommentSetter commentSetter,
        TableDropper tableDropper,
        RowDeleter rowDeleter,
        QueryExecutor queryExecutor,
        SqlParserCache sqlParserCache
    )
    {
        this.context = context;
        this.options = options;
        this.catalogs = catalogs;
        this.ddlForwarding = ddlForwarding;
        this.tableCreator = tableCreator;
        this.tableColumnAlterer = tableColumnAlterer;
        this.tableIndexAlterer = tableIndexAlterer;
        this.tableConstraintAlterer = tableConstraintAlterer;
        this.commentSetter = commentSetter;
        this.tableDropper = tableDropper;
        this.rowDeleter = rowDeleter;
        this.queryExecutor = queryExecutor;
        this.sqlParserCache = sqlParserCache;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; each statement pins the field once, so an in-flight statement
    /// keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Runs a DDL action inside a <see cref="KvTransaction"/> and commits it. Calls
    /// <paramref name="postCommitInvalidate"/> after a successful commit so the query-result cache
    /// evicts any entries whose schema deps reference the affected table. Schema meta keys use the
    /// <c>{db}/meta/…</c> keyspace and do not match the row/index bucket patterns tracked by
    /// <see cref="KvTransactionsManager"/>'s automatic key-based invalidation, so DDL paths must
    /// supply an explicit invalidation action here rather than relying on the DML commit hook.
    /// </summary>
    /// <remarks>
    /// <b>Intentional asymmetry vs DML invalidation:</b> DML commits drive
    /// <c>CachePublishGate.MarkWriteInFlight / CommitWrite</c> so the invalidation runs inside a
    /// critical section that also bumps the generation counter, atomically fencing concurrent
    /// TryPublishUnderGeneration calls. DDL invalidation (<paramref name="postCommitInvalidate"/>)
    /// runs after <c>CommitAsync</c> returns, outside that critical section — there is a narrow
    /// window in which a concurrent SELECT could publish a stale-schema entry.
    ///
    /// This asymmetry is acceptable: schema meta keys cannot participate in the key-based gate
    /// (they do not map to a table bucket). However, the window is closed at the read side:
    /// <see cref="QueryExecutor"/> performs an in-memory schema dep re-check on every hit
    /// (non-strict and strict alike). An entry that manages to publish in this narrow window
    /// will be evicted on the very first subsequent probe when its schema version is found stale,
    /// so it can never be served.
    /// </remarks>
    internal async Task<T> ExecuteDdlInTransaction<T>(
        DatabaseDescriptor database,
        Func<KvTransaction, Task<T>> action,
        Func<Task>? onAbort = null,
        Action? postCommitInvalidate = null
    )
    {
        // Acquired in BOTH modes. It used to be cluster-only, on the reasoning that a standalone node
        // has no replicated schema log to order proposals against — but the gate is also what makes
        // "resolve the target, then mutate it" atomic against a concurrent drop/recreate of the same
        // object. Skipping it standalone meant metadata-only DDL that *does* take the gate (COMMENT ON,
        // ALTER TABLE SET) serialized against nothing there, and could persist a blob for a table that
        // had just been dropped. One discipline in both modes; DDL is rare enough that the added
        // serialization costs nothing that matters.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);
        try
        {
            T result = await action(tx).ConfigureAwait(false);
            using CancellationTokenSource cts = new(DdlCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);
            postCommitInvalidate?.Invoke();
            return result;
        }
        catch (Exception ex)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);

            if (onAbort is not null)
            {
                try
                {
                    await onAbort().ConfigureAwait(false);
                }
                catch (Exception cleanupEx)
                {
                    context.Logger.LogWarning(
                        cleanupEx,
                        "Failed to run DDL abort compensation for database {DatabaseName} after {ErrorType}",
                        database.Name,
                        ex.GetType().Name
                    );
                }
            }

            throw;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            // Fire after CommitAsync (or RollbackIfNotCompletedAsync on error) so the
            // KV transaction is settled before schema-partition leadership changes.
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    public async Task<CreateTableResult> CreateTable(CreateTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardCreateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new CreateTableResult(database, forwarded.Value);

        // Allocate the table id before the DDL transaction — only the proposer/leader allocates;
        // the id is carried in the replicated payload so every follower applies the same id.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string tableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database, async tx =>
        {
            bool result = await tableCreator.Create(queryExecutor, context.TableOpener, tableIndexAlterer, database, ticket, tx, tableId).ConfigureAwait(false);
            return new CreateTableResult(database, result);
        }).ConfigureAwait(false);
    }

    public async Task<bool> AlterTable(AlterTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Checked before the statement can be forwarded, so the caller is refused here rather than
        // learning about it from another node. The node that ultimately applies the change runs this
        // same path and re-checks against its own schema.
        RequireNoViewDependsOnAlteredColumn(database, ticket);

        bool? forwarded = await ddlForwarding.TryForwardAlterTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        if (context.IsClusterMode && ticket.Operation == AlterTableOperation.AddColumn)
            return await ExecuteClusterAddColumnAsync(database, table, ticket).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database,
            tx => tableColumnAlterer.Alter(queryExecutor, database, table, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses a <c>DROP COLUMN</c> or <c>RENAME COLUMN</c> that a view reads. Adding a column
    /// breaks nothing, so it is not checked.
    /// </summary>
    /// <remarks>
    /// Resolves the column to its immutable id first: views record what they read by id, and a name
    /// is exactly the thing these two statements are about to change.
    /// </remarks>
    internal static void RequireNoViewDependsOnAlteredColumn(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        if (ticket.Operation is not (AlterTableOperation.DropColumn or AlterTableOperation.RenameColumn))
            return;

        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return;

        string columnName = ticket.Column.Name;

        TableColumnSchema? column = tableSchema.Columns?
            .FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

        if (column?.Id is not { Length: > 0 } columnId)
            return;

        ViewDependencyMaintainer.RequireNoDependentViewsOnColumn(
            database.Schema, ticket.TableName, columnName, columnId,
            renaming: ticket.Operation == AlterTableOperation.RenameColumn);
    }

    /// <summary>
    /// Cluster path for ADD COLUMN: drives the column through the staged
    /// Absent → DeleteOnly → WriteOnly → Public sequence via <see cref="SchemaChangeCoordinator"/>,
    /// backfilling row defaults once the column reaches <c>WriteOnly</c> (the first state at
    /// which <see cref="RowEncoder.Encode"/> includes the column in encoded bytes).
    ///
    /// The <c>SchemaDdlSemaphore</c> is held across the entire coordinator sequence so
    /// concurrent DDL on this node cannot observe an intermediate schema version.
    /// </summary>
    internal async Task<bool> ExecuteClusterAddColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterTableTicket ticket
    )
    {
        ColumnInfo columnInfo = ticket.Column;

        // Eagerly reject duplicates before acquiring the semaphore — the coordinator
        // would silently no-op (already-at-Public = empty path) instead of throwing.
        if (table.Schema.Columns?.Any(c => string.Equals(c.Name, columnInfo.Name, StringComparison.OrdinalIgnoreCase)) == true)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate column '{columnInfo.Name}'");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, context.Logger);
            coordinator.BackfillAsync = (db, tableName, column) => BackfillColumnDefaultsAsync(db, tableName, column);

            await coordinator.RunJobAsync(
                database,
                new SchemaChangeJob(database.Name, ticket.TableName, table.Id, columnInfo.Name, SchemaElementState.Public),
                columnDefinition: columnInfo
            ).ConfigureAwait(false);

            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            return true;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Re-encodes every existing row in <paramref name="tableName"/> so that the newly added
    /// <paramref name="column"/> (in <c>WriteOnly</c> state) is stored with its default value.
    /// Used by both the command-path coordinator and the leader-change resume coordinator so
    /// backfill is always part of the resumable sequence.
    /// </summary>
    internal async Task BackfillColumnDefaultsAsync(DatabaseDescriptor database, string tableName, ColumnInfo column)
    {
        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);

        AlterColumnTicket alterTicket = new(
            databaseName: database.Name,
            tableName: tableName,
            column: column,
            operation: AlterTableOperation.AddColumn
        );

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);
        try
        {
            await tableColumnAlterer.BackfillColumnDefaultsAsync(queryExecutor, database, table, alterTicket, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Scans every existing row in <paramref name="tableName"/> and writes an index entry
    /// for each, using backfill mode (idempotent — <c>Set</c> rather than <c>SetIfNotExists</c>
    /// for unique indexes). Processes rows in <see cref="BackfillBatchSize"/>-row transactions;
    /// after each committed batch invokes <paramref name="onCheckpoint"/> with the last rowId so
    /// the coordinator can persist a resume offset. Called by <see cref="SchemaChangeCoordinator"/>
    /// just before the index transitions from <c>WriteOnly</c> to <c>Public</c>.
    /// </summary>
    internal async Task BackfillIndexEntriesAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexInfo,
        string? startOffset,
        Func<string, Task>? onCheckpoint = null
    )
    {
        CamusDBOptions currentOptions = options;

        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
        bool unique = indexInfo.IndexType == IndexType.Unique;

        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(startOffset)
            ? null
            : ObjectId.ToValue(startOffset!);

        // The backfill only reads the index's key and INCLUDE columns, so narrow the per-row decode
        // to them — values for those columns are identical to a full decode, and the scanned rows are
        // never re-encoded, only turned into index entries.
        HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase);
        foreach (string columnName in indexInfo.ColumnNames)
            requiredColumns.Add(columnName);
        if (indexInfo.IncludeColumnNames is { Length: > 0 } includeColumnNames)
        {
            foreach (string columnName in includeColumnNames)
                requiredColumns.Add(columnName);
        }

        int totalRows = 0;

        while (true)
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            int batchRows = 0;
            ObjectIdValue lastRowId = default;

            // Per-batch decode-plan cache; scoped to the batch transaction because the visibility
            // version is re-read from the live schema each batch.
            RowEncoder.DictionaryDecodeState decodeState = new();

            try
            {
                // Each scanned row produces an index entry in this same transaction, so the rows read
                // are a genuine commit dependency and must stay in the read set.
                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    tx, afterRowId: afterRowId).ConfigureAwait(false))
                {
                    Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                        table.Schema, tx.TransactionId, rowId, data,
                        requiredColumns: requiredColumns,
                        visibilitySchemaVersion: table.Schema.Version,
                        decodeState: decodeState).ConfigureAwait(false);

                    // NULLs are distinct: a unique index omits entries for rows with a NULL (or absent)
                    // value in any indexed column, so multiple such rows can coexist. This must match
                    // the incremental insert path so a backfilled index equals one built row-by-row.
                    if (!unique || !HasNullIndexColumn(row, indexInfo.ColumnNames))
                    {
                        int i = 0;
                        ColumnValue[] columnValues = unique
                            ? new ColumnValue[indexInfo.ColumnNames.Length]
                            : new ColumnValue[indexInfo.ColumnNames.Length + 1];

                        foreach (string columnName in indexInfo.ColumnNames)
                        {
                            ColumnValue? keyValue = row.GetValueOrDefault(columnName);
                            if (keyValue is null)
                                throw new CamusDBException(
                                    CamusDBErrorCodes.InvalidInternalOperation,
                                    $"A null value was found for index key field '{columnName}'"
                                );
                            columnValues[i++] = keyValue;
                        }

                        if (!unique)
                            columnValues[i] = new(ColumnType.Id, rowId.ToString());

                        CompositeColumnValue compositeKey = new(columnValues);

                        // Materialize stored/payload (INCLUDE) values (NULL-tolerant) for a covering index.
                        // EncodeTupleChecked enforces the per-entry byte ceiling before the KV write.
                        byte[]? includeTuple = indexInfo.IncludeColumnNames is { Length: > 0 } includeNames
                            ? IndexIncludeValueCodec.EncodeTupleChecked(includeNames, row, indexInfo.IndexName, currentOptions)
                            : null;

                        await table.Store.PutIndexEntry(tx, indexInfo.IndexId, compositeKey, rowId, unique, backfillMode: true, includeTuple: includeTuple).ConfigureAwait(false);
                    }

                    lastRowId = rowId;
                    batchRows++;

                    if (batchRows >= BackfillBatchSize)
                        break;
                }

                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            totalRows += batchRows;

            if (batchRows < BackfillBatchSize)
                break;

            // More rows may remain — checkpoint and advance the cursor.
            afterRowId = lastRowId;
            if (onCheckpoint is not null)
                await onCheckpoint(lastRowId.ToString()).ConfigureAwait(false);

            if (TestInterceptAfterBackfillCheckpoint is not null)
                await TestInterceptAfterBackfillCheckpoint().ConfigureAwait(false);
        }

        Log.LogIndexBackfillComplete(context.Logger, totalRows, indexInfo.IndexName);
    }

    /// <summary>
    /// Returns true when any of the index's columns is absent from the row or holds a NULL value.
    /// Such a row is exempt from a unique index (NULLs are distinct) and is skipped during backfill.
    /// </summary>
    private static bool HasNullIndexColumn(Dictionary<string, ColumnValue> row, string[] columnNames)
    {
        foreach (string columnName in columnNames)
        {
            ColumnValue? value = row.GetValueOrDefault(columnName);
            if (value is null || value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Drives the add-index sequence through the coordinator-owned staged path
    /// (<c>Absent → DeleteOnly → WriteOnly → [backfill] → Public</c>).
    /// Only called in cluster mode.
    /// </summary>
    internal async Task<bool> ExecuteClusterAddIndexAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        CamusDBOptions currentOptions = options;

        if (table.Indexes.ContainsKey(ticket.IndexName))
        {
            if (ticket.IfNotExists)
                return false;
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{ticket.IndexName}' already exists on table '{table.Name}'");
        }

        IndexType indexType = ticket.Operation is AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey
            ? IndexType.Unique
            : IndexType.Multi;

        IndexColumnOrder.RejectDescendingOnUnsupportedType(
            ticket.Columns,
            ticket.IndexName,
            name => table.Schema.Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))?.Type);

        TableIndexAdder.ValidateIncludeColumns(table, ticket, currentOptions);

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = GetColumnIdsForIndex(table, ticket.Columns);
        string[] columnNames = ticket.Columns.Select(c => c.Name).ToArray();
        string[]? includeColumnIds = ticket.IncludeColumns.Length > 0
            ? GetColumnIdsForIndex(table, ticket.IncludeColumns.Select(n => new ColumnIndexInfo(n, OrderType.Ascending)).ToArray())
            : null;
        string[]? includeColumnNames = ticket.IncludeColumns.Length > 0 ? ticket.IncludeColumns : null;

        IndexBuildInfo indexInfo = new(indexId, ticket.IndexName, columnIds, columnNames, indexType, IndexColumnOrder.Extract(ticket.Columns), IncludeColumnIds: includeColumnIds, IncludeColumnNames: includeColumnNames);

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, context.Logger);
            coordinator.IndexBackfillAsync = (db, tbl, info, start, checkpoint) => BackfillIndexEntriesAsync(db, tbl, info, start, checkpoint);

            try
            {
                await coordinator.RunJobAsync(
                    database,
                    new SchemaChangeJob(database.Name, ticket.TableName, table.Id, ticket.IndexName, SchemaElementState.Public, SchemaElementKind.Index),
                    indexBuildInfo: indexInfo
                ).ConfigureAwait(false);

                database.Cache?.InvalidateByTableId(database.Id, table.Id);

                return true;
            }
            catch
            {
                // Compensate: if the index was partially committed to the schema (in DeleteOnly
                // or WriteOnly state) but did not reach Public, emit DropIndex on all nodes and
                // delete the persisted coordinator job, leaving the cluster in a clean state.
                // Note: if this node is now degraded, compensation may be skipped by the
                // degraded gate in ReplicateDropIndexAsync; a healthy peer's ResumeJobsAsync
                // will reconcile the state after the step-down below.
                await CompensateClusterAddIndexAsync(database, table.Id, ticket.TableName, ticket.IndexName).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    // Shared helper — delegates to DatabaseDescriptor.FireDeferredSchemaStepDownAsync,
    // adding the caller's logger for the step-down failure case. Called from the finally blocks
    // of ExecuteDdlInTransaction, ExecuteClusterAddColumnAsync, ExecuteClusterAddIndexAsync, and
    // ExecuteClusteredIndexDdlAsync so all DDL paths release leadership on degradation.
    private async Task FireDeferredStepDownIfRequestedAsync(DatabaseDescriptor database)
    {
        try
        {
            await database.FireDeferredSchemaStepDownAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogError(
                ex,
                "Schema partition step-down after persist exhaustion failed for database {DatabaseName}",
                database.Name
            );
        }
    }

    private async Task CompensateClusterAddIndexAsync(DatabaseDescriptor database, string tableId, string tableName, string indexName)
    {
        try
        {
            if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema) &&
                tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase)) == true)
            {
                await catalogs.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Failed to compensate partial index add for {IndexName} on {TableName}", indexName, tableName);
        }

        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, tableId, indexName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Failed to delete coordinator job for index {IndexName} on {TableName}", indexName, tableName);
        }
    }

    private static string[] GetColumnIdsForIndex(TableDescriptor table, ReadOnlySpan<ColumnIndexInfo> columns)
    {
        string[] columnIds = new string[columns.Length];
        int i = 0;

        foreach (ColumnIndexInfo columnIndex in columns)
        {
            bool found = false;
            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name != columnIndex.Name)
                    continue;

                if (!SchemaElementStateRules.IsReadable(column) || !SchemaElementStateRules.IsWritable(column))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnIndex.Name}' is not public on table '{table.Name}'"
                    );

                columnIds[i++] = column.Id;
                found = true;
                break;
            }

            if (!found)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{columnIndex.Name}' does not exist on table '{table.Name}'"
                );
        }

        return columnIds;
    }

    public async Task<bool> AlterIndex(AlterIndexTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardAlterIndexAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        // RenameIndex is metadata-only (no KV data changes); use single-phase DDL on all paths.
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
            return await ExecuteDdlInTransaction(database,
                tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
            ).ConfigureAwait(false);

        bool addIndexOperation = ticket.Operation is
            AlterIndexOperation.AddIndex or
            AlterIndexOperation.AddUniqueIndex or
            AlterIndexOperation.AddPrimaryKey;

        if (context.IsClusterMode && addIndexOperation)
            return await ExecuteClusterAddIndexAsync(database, table, ticket).ConfigureAwait(false);

        bool indexExistedBefore = table.Indexes.ContainsKey(ticket.IndexName);
        bool compensateOnAbort = addIndexOperation && !indexExistedBefore;

        // Both cluster (non-add) and standalone paths use two-phase DDL: local work + schema
        // replication. The old standalone-only ExecuteDdlInTransaction path omitted Phase 2
        // (ReplicateIndexChangeAsync), leaving the schema unpersisted across close/reopen.
        return await ExecuteClusteredIndexDdlAsync(
            database, table, ticket, compensateOnAbort,
            tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Undoes a partially applied ADD INDEX after its DDL transaction was rolled back: purges any
    /// index entries that reached KV, then removes the index from the in-memory schema.
    ///
    /// <para>
    /// The purge matters because a backfill over a table larger than one batch commits its later
    /// batches in their own transactions (see <c>TableIndexAdder.BackfillIndex</c>), so a rollback of
    /// the DDL transaction does not take those entries with it. They are unreachable — their index id
    /// never becomes public — but they would occupy the keyspace forever. Runs in its own transaction,
    /// which the caller must start only after the DDL transaction has settled: the two contend for the
    /// same entry keys.
    /// </para>
    ///
    /// <para>
    /// Best-effort: a failed purge is logged and swallowed so the schema cleanup below still happens.
    /// Leaving orphaned entries behind is strictly better than leaving a phantom index in the schema.
    /// </para>
    /// </summary>
    private async Task CompensateAbortedAddIndexAsync(DatabaseDescriptor database, TableDescriptor table, string indexName)
    {
        string? indexKvId = table.Schema.Indexes?
            .FirstOrDefault(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase))?.Id;

        if (indexKvId is not null)
        {
            KvTransaction cleanupTx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            try
            {
                await table.Store.DropIndexEntries(cleanupTx, indexKvId).ConfigureAwait(false);
                await database.Transactions.CommitAsync(cleanupTx).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await database.Transactions.RollbackIfNotCompletedAsync(cleanupTx).ConfigureAwait(false);

                context.Logger.LogWarning(
                    ex,
                    "Failed to purge index entries of aborted index {IndexName} on table {TableName}; they are unreachable but remain in storage",
                    indexName,
                    table.Name
                );
            }
        }

        table.MutateIndexes(indexes => indexes.Remove(indexName));

        await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            table.Schema.Indexes?.RemoveAll(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }
    }

    /// <summary>
    /// Two-phase execution for cluster index DDL. Phase 1 commits the backfill data
    /// (tx1) so the index KV entries are visible before Phase 2 replicates the schema
    /// delta. Both phases run under <c>SchemaDdlSemaphore</c> so <c>SchemaVersion</c>
    /// stays stable across the pair. Only called in cluster mode.
    /// </summary>
    internal async Task<bool> ExecuteClusteredIndexDdlAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket,
        bool compensateOnAbort,
        Func<KvTransaction, Task<bool>> localWork
    )
    {
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Phase 1: run local DDL (including backfill) and commit so the index KV
            // entries are durable and visible before the schema delta is published.
            KvTransaction tx1 = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            bool result;
            try
            {
                result = await localWork(tx1).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx1).ConfigureAwait(false);
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx1).ConfigureAwait(false);
                if (compensateOnAbort)
                    await CompensateAbortedAddIndexAsync(database, table, ticket.IndexName).ConfigureAwait(false);
                throw;
            }

            if (!result) return result;

            // Phase 2: index data is committed — replicate the schema change so every
            // node updates its TableSchema.Indexes and evicts its TableDescriptor cache.
            // A fresh transaction supplies the HLC timestamp for the schema-log entry;
            // no KV writes happen under it (ReplicateIndexChangeAsync creates its own
            // internal checkpoint transaction via PersistSchemaCheckpointAsync).
            KvTransaction tx2 = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0
            ).ConfigureAwait(false);
            try
            {
                await catalogs.ReplicateIndexChangeAsync(database, ticket, table, tx2).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx2).ConfigureAwait(false);
            }

            // Schema has been replicated; evict stale cache entries for this table. Phase 1's
            // row/index KV writes are already handled by the CommitAsync invalidation hook, but
            // schema-dep entries (keyed by tableId, not by KV key) need an explicit call here.
            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            // Re-populate the descriptor cache: ReplicateIndexChangeAsync fires
            // InvalidateAppliedTableDescriptor which evicts the table. Re-opening here
            // ensures callers that rely on TableDescriptors find it immediately after DDL.
            await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            return result;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    public async Task<bool> DropTable(DropTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardDropTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        if (ticket.IfExists && !catalogs.TableExists(database, ticket.TableName))
            return false;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database,
            tx => tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, ticket, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, table.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Empties a base table by replacing the physical key-space its rows and index entries live in.
    /// </summary>
    /// <remarks>
    /// <para><b>The cost does not grow with the row count</b>, because no row and no index entry is
    /// read or deleted. What the statement waits for instead is the exclusive fence over the old row
    /// key-space, the replicated schema entry, and the checkpoint that makes the retired generation
    /// recoverable — so it is constant in cardinality, not constant in wall-clock time.</para>
    ///
    /// <para><b>Order is the whole design.</b> The fence is taken <em>before</em> the cut timestamp is
    /// minted and before anything is proposed. Kahuna's durable range-lock write fence aborts any
    /// transaction — optimistic or pessimistic — that staged a write into the old key-space before the
    /// fence was acquired, and makes later writers wait. That is what stops a writer from having a row
    /// acknowledged into storage nothing reads afterwards, and it is what makes the cut a real
    /// linearization point rather than a hopeful one.</para>
    ///
    /// <para><b>Once the schema entry commits, the truncate has happened.</b> It is cluster-wide and
    /// irrevocable at that point, so a later failure to finalize the fence-owning transaction is
    /// logged and the committed outcome is reported. Reporting a rollback there would tell the caller
    /// the table still holds rows it no longer holds.</para>
    /// </remarks>
    public async Task<bool> TruncateTable(TruncateTableTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardTruncateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        RequireTruncatableRelation(table);
        RequireTruncatePrivileges(database, table);

        // Allocated before the DDL transaction, like a table id on CREATE: only the leader allocates,
        // and the value rides the replicated payload so every node adopts the same key-space.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string newStorageId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        KvTransaction fenceTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            mutationLimitOverride: 0
        ).ConfigureAwait(false);

        bool deltaCommitted = false;
        try
        {
            // Both index lists are read under the DDL gate, not before it. An ADD INDEX that landed
            // between the read and the proposal would leave its entries out of the retired
            // generation's frozen catalog — unnamed, and so never reclaimed — and out of the new
            // generation's seeded one. The gate is what excludes it: index DDL takes the same
            // semaphore, and cluster-wide it serializes through this node as the schema leader.
            //
            // The retired list comes from the persisted catalog rather than the live index list,
            // because the retired generation owns entries for every index it ever had, and one
            // dropped before the truncate is named nowhere else. Reading it before the fence
            // transaction stages anything keeps the read off its own write intents.
            string[] retiredIndexIds = await catalogs.ReadStorageIndexCatalogAsync(database, table.Schema).ConfigureAwait(false);

            string[] newIndexIds = table.Schema.Indexes is null
                ? []
                : [.. table.Schema.Indexes.Where(index => !string.IsNullOrEmpty(index.Id)).Select(index => index.Id!)];

            await table.Store.AcquireExclusiveRowSpaceFenceAsync(fenceTx).ConfigureAwait(false);

            // Re-read under the fence. Between resolving the descriptor and holding the fence another
            // contents change may have landed; proposing the values read earlier would ask apply to
            // swap a generation this statement never saw, and apply would refuse it anyway.
            string expectedStorageId = table.Schema.EffectiveStorageId;
            long expectedGeneration = table.Schema.ContentsGeneration;

            HLCTimestamp cut = context.SharedNode is null
                ? fenceTx.TransactionId
                : context.SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());

            await catalogs.TruncateTableContentsAsync(
                database,
                table.Id,
                table.Name,
                expectedStorageId,
                expectedGeneration,
                newStorageId,
                cut,
                retiredIndexIds,
                newIndexIds
            ).ConfigureAwait(false);

            deltaCommitted = true;

            using CancellationTokenSource cts = new(DdlCommitTimeout);
            await database.Transactions.CommitAsync(fenceTx, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(fenceTx).ConfigureAwait(false);

            if (!deltaCommitted)
                throw;

            context.Logger.LogWarning(
                ex,
                "TRUNCATE of table {Table} in database {Database} committed its schema entry but failed to " +
                "finalize the fence-owning transaction; the truncate stands and the fence expires on its own",
                table.Name, database.Name);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }

        database.Cache?.InvalidateByTableId(database.Id, table.Id);
        return true;
    }

    /// <summary>
    /// Refuses a <c>TRUNCATE</c> whose target is not an ordinary base table.
    /// </summary>
    /// <remarks>
    /// A materialized view is stored as a relation and already uses the same storage-swap machinery
    /// for its own refresh, so a truncate would contend with the refresh path for one field and would
    /// retire the view's contents outside the refresh's lease and fence. A plain view never reaches
    /// here: resolving one for a write already reports that it is a view.
    /// </remarks>
    private static void RequireTruncatableRelation(TableDescriptor table)
    {
        if (!table.Schema.IsMaterializedView)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.ViewNotUpdatable,
            $"'{table.Name}' is a materialized view and cannot be truncated; its contents come from its " +
            $"defining query. Use REFRESH MATERIALIZED VIEW {table.Name} WITH NO DATA to empty it.");
    }

    /// <summary>
    /// Requires DELETE <b>and</b> DROP on the target.
    /// </summary>
    /// <remarks>
    /// <para>The two are checked one at a time on purpose. A privilege test asks whether a single
    /// grant record carries the whole requested mask, so <c>Delete | Drop</c> as one mask refuses a
    /// principal who was granted DELETE in one statement and DROP in another — a perfectly ordinary
    /// way to hold both.</para>
    ///
    /// <para>DELETE is also the privilege the per-table chokepoint checks while resolving the table,
    /// so on a request that carries a caller it is verified twice. That is cheap, and it keeps the two
    /// checks reading the same ambient scope.</para>
    ///
    /// <para><b>An absent scope is not an unauthenticated caller.</b> It means no request-level entry
    /// point published one — an internal caller, or the schema leader running a statement a follower
    /// already authorized before forwarding it. The per-table chokepoint treats that the same way, and
    /// diverging here would fail every forwarded truncate on an authenticated cluster.</para>
    /// </remarks>
    private static void RequireTruncatePrivileges(DatabaseDescriptor database, TableDescriptor table)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;
        if (scope.Principal is not { } principal || scope.RequiredPrivilege is null)
            return;

        foreach (Privilege required in TruncateRequiredPrivileges)
        {
            if (!principal.HasPrivilege(required, database.Id, table.Id))
                throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    $"Missing {required} privilege on table '{database.Name}.{table.Name}'");
        }
    }

    /// <summary>Both privileges <c>TRUNCATE</c> requires, checked independently. Never OR them.</summary>
    private static readonly Privilege[] TruncateRequiredPrivileges = [Privilege.Delete, Privilege.Drop];

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) table by reattaching it to the schema under a new name,
    /// reusing its preserved id and retained row/index data. Like other table DDL it forwards to the
    /// schema leader in cluster mode. Fails with <see cref="CamusDBErrorCodes.OrphanNotFound"/> if no
    /// orphan record exists for the id, or <see cref="CamusDBErrorCodes.TableAlreadyExists"/> if the new
    /// name is taken.
    /// </summary>
    public async Task<bool> RelinkTable(RelinkTableTicket ticket)
    {
        CamusDBOptions currentOptions = options;

        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardRelinkTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        // Fence this table id against a concurrent GC reclamation (and a second relink). The reclaimer
        // takes the same per-table drop-intent key before purging, so the two never interleave. All the
        // state decisions below happen under this fence.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string fenceId = DatabaseRegistry.TableFenceId(database.Id, ticket.OrphanTableId);
        bool fenced = await registry.AcquireDropIntentAsync(fenceId).ConfigureAwait(false);
        if (!fenced)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A concurrent operation on orphan table id '{ticket.OrphanTableId}' is in progress; retry later");

        try
        {
            // Load the record first: retired contents and a dropped relation are recovered differently,
            // and the liveness tests below are only meaningful for a dropped relation. For retired
            // contents the source relation is normally still live at this very id.
            OrphanTableRecord? record = await catalogs.TryGetTableOrphanAsync(database, ticket.OrphanTableId).ConfigureAwait(false);

            if (record is { Kind: OrphanKind.RetiredContents })
                return await RelinkRetiredContentsAsync(database, record, ticket, currentOptions).ConfigureAwait(false);

            // Idempotency by id: if a live table already has this id — a relink that committed its apply
            // but crashed before deleting the orphan record — don't reattach a second alias.
            TableSchema? liveWithId = null;
            foreach (TableSchema t in database.Schema.Tables.Values)
                if (string.Equals(t.Id, ticket.OrphanTableId, StringComparison.Ordinal)) { liveWithId = t; break; }

            if (liveWithId is not null)
            {
                if (!string.Equals(liveWithId.Name, ticket.NewTableName, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TableAlreadyExists,
                        $"Table id '{ticket.OrphanTableId}' is already live under name '{liveWithId.Name}'");

                // Already relinked to this exact name — finish idempotently by cleaning any stale record.
                await DeleteTableOrphanRecordAsync(database, ticket.OrphanTableId).ConfigureAwait(false);
                return true;
            }

            if (catalogs.TableExists(database, ticket.NewTableName))
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"Table '{ticket.NewTableName}' already exists");

            OrphanTableRecord? orphan = record;
            if (orphan is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.OrphanNotFound,
                    $"No orphaned table with id '{ticket.OrphanTableId}' is available to relink in database '{ticket.DatabaseName}'");

            // Relink adds a live table, so it counts against the per-database table limit like CREATE.
            int maxTables = currentOptions.MaxTablesPerDatabase;
            if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
                throw new CamusDBException(
                    CamusDBErrorCodes.SchemaLimitExceeded,
                    $"Database '{ticket.DatabaseName}' would exceed the maximum of {maxTables} tables per database");

            await ExecuteDdlInTransaction(database,
                tx => catalogs.RelinkTable(database, orphan, ticket.NewTableName, tx),
                postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, orphan.TableId)
            ).ConfigureAwait(false);
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Recovers one retired contents generation as a new relation, under the caller's fence.
    /// </summary>
    /// <remarks>
    /// <para>Resumable at every crash point. The target id is written into the record before the
    /// relation is published, so a retry finishes what a crash interrupted instead of allocating a
    /// second target; and a target that is already live with the retired storage means the recovery
    /// completed and only the record was left behind.</para>
    /// </remarks>
    private async Task<bool> RelinkRetiredContentsAsync(
        DatabaseDescriptor database, OrphanTableRecord record, RelinkTableTicket ticket, CamusDBOptions currentOptions)
    {
        TableSchema? existingTarget = null;
        if (record.RelinkTargetId is { Length: > 0 } recordedTarget)
        {
            foreach (TableSchema live in database.Schema.Tables.Values)
            {
                if (string.Equals(live.Id, recordedTarget, StringComparison.Ordinal))
                {
                    existingTarget = live;
                    break;
                }
            }
        }

        if (existingTarget is not null)
        {
            // The relation was already published for this record. Finish idempotently rather than
            // publishing a second one; the record is the only thing still outstanding.
            if (!string.Equals(existingTarget.Name, ticket.NewTableName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"The retired contents of table id '{ticket.OrphanTableId}' were already recovered as '{existingTarget.Name}'");

            await DeleteTableOrphanRecordAsync(database, record.RetiredStorageId).ConfigureAwait(false);
            return true;
        }

        if (catalogs.TableExists(database, ticket.NewTableName))
            throw new CamusDBException(
                CamusDBErrorCodes.TableAlreadyExists,
                $"Table '{ticket.NewTableName}' already exists");

        int maxTables = currentOptions.MaxTablesPerDatabase;
        if (maxTables > 0 && database.Schema.Tables.Count >= maxTables)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Database '{ticket.DatabaseName}' would exceed the maximum of {maxTables} tables per database");

        // A recorded target that is not live means a previous attempt crashed before publishing.
        // Reuse its id so the retry converges instead of leaving a second allocated id behind.
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string targetId = record.RelinkTargetId is { Length: > 0 } resumed
            ? resumed
            : await registry.AllocateTableIdAsync().ConfigureAwait(false);

        // Durable before the relation exists: this is the only signal that separates a recovery in
        // progress from one that never started, and the collector must not purge the storage in between.
        await catalogs.RecordRelinkTargetAsync(database, record, targetId).ConfigureAwait(false);

        await ExecuteDdlInTransaction(database,
            tx => catalogs.RelinkRetiredContents(database, record, targetId, ticket.NewTableName, tx),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, targetId)
        ).ConfigureAwait(false);

        return true;
    }

    /// <summary>Deletes a stale table orphan record in its own committed transaction (idempotent cleanup).</summary>
    private async Task DeleteTableOrphanRecordAsync(DatabaseDescriptor database, string tableId)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await catalogs.DeleteTableOrphanAsync(database, tableId, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <param name="dependentViews">
    /// Rewritten bodies for views that read this relation, applied in the rename's own delta. When
    /// null they are computed here; a caller that already resolved them (the materialized-view rename
    /// path) passes them so the work is not repeated.
    /// </param>
    public async Task<bool> RenameTable(
        RenameTableTicket ticket,
        Dictionary<string, ViewDefinition>? dependentViews = null)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardRenameTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor renameTable = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        dependentViews ??= ViewDependencyMaintainer.BuildRenameConversions(
            database.Schema, renameTable.Id, sql => SQLParserProcessor.Parse(sql, sqlParserCache));

        return await ExecuteDdlInTransaction(database,
            tx => catalogs.RenameTable(database, ticket, tx, dependentViews),
            postCommitInvalidate: () => database.Cache?.InvalidateByTableId(database.Id, renameTable.Id)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table by <b>database name</b>, resolving the database through the registry first.
    ///
    /// <para>For request-scoped callers that only have a name. Code that already holds a
    /// <see cref="DatabaseDescriptor"/> must call <see cref="OpenTableWithDescriptor"/> instead —
    /// passing <c>descriptor.Name</c> back into this method re-resolves a cached display name, which
    /// after a RENAME DATABASE was the pre-rename name and made every INSERT fail with
    /// "Database '&lt;old&gt;' does not exist". It is also two redundant lookups on a hot path.</para>
    /// </summary>
    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = descriptor.Use();
        return await context.TableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a table against an already-resolved database. The preferred entry point whenever the
    /// caller holds a descriptor: it skips the registry round-trip and cannot be affected by a rename
    /// that happened after the descriptor was cached. <c>ticket.DatabaseName</c> is carried for
    /// diagnostics only — the descriptor decides which database is used.
    /// </summary>
    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await context.TableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds or drops a CHECK (or named NOT NULL) constraint on an existing table.
    /// For ADD CHECK, scans all existing rows and rejects if any row violates the expression.
    /// Replicates in cluster mode; applies directly in standalone mode.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> AlterConstraint(AlterConstraintTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        bool? forwarded = await ddlForwarding.TryForwardAlterConstraintAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        bool ok = await tableConstraintAlterer.Alter(
            catalogs, database, table, ticket, context.IsClusterMode
        ).ConfigureAwait(false);
        database.Cache?.InvalidateByTableId(database.Id, table.Id);
        return new ExecuteDDLSQLResult(database, ok);
    }

    /// <summary>
    /// Attaches or removes a comment on a table, column, index, or database. Public so a ticket caller
    /// can invoke it without going through SQL. The database target is routed to the registry; the
    /// other three open the table and go through <see cref="CommentSetter"/>.
    /// </summary>
    public async Task<ExecuteDDLSQLResult> Comment(CommentTicket ticket)
    {
        context.Validator.Validate(ticket);

        if (ticket.Target == CommentTarget.Database)
        {
            await CommentDatabase(ticket).ConfigureAwait(false);

            // No descriptor to hand back — the registry write needs no open database — but the
            // operation did succeed, so report that rather than a defaulted (false) result.
            return new ExecuteDDLSQLResult(null!, true);
        }

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await Comment(database, ticket).ConfigureAwait(false);
    }

    /// <summary>
    /// Applies a table/column/index comment against an already-opened database, forwarding to the
    /// schema leader first when this node is a follower. Serialized against other DDL on this
    /// database by <c>SchemaDdlSemaphore</c>, exactly like <c>ALTER TABLE … SET</c>.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> Comment(DatabaseDescriptor database, CommentTicket ticket)
    {
        bool? forwarded = await ddlForwarding.TryForwardCommentAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new ExecuteDDLSQLResult(database, forwarded.Value);

        // The table is opened INSIDE the gate, not before it. Resolving first and validating later
        // is a check-then-act: a drop-and-recreate of the same name in between leaves the validation
        // looking at the old object while the delta — which names its target by table name, not id —
        // lands on the replacement. The apply deliberately no-ops on a missing column/index (for
        // replay safety), so that mismatch would report success while doing nothing at all.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName!).ConfigureAwait(false);

            bool ok = await commentSetter.Set(catalogs, database, table, ticket, context.IsClusterMode).ConfigureAwait(false);
            return new ExecuteDDLSQLResult(database, ok);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    /// <summary>
    /// Sets a database's comment on the cross-database registry. No schema-leader forwarding: the
    /// registry write is a plain replicated KV write, exactly like RENAME DATABASE.
    /// </summary>
    internal async Task CommentDatabase(CommentTicket ticket)
    {
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        await registry.SetCommentAsync(ticket.DatabaseName, ticket.Comment).ConfigureAwait(false);
    }
}
