
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core;
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
using Kahuna.Shared.KeyValue;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexAdder
{
    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Rows processed per index-backfill transaction, on both the standalone flux path here and the
    /// coordinator-driven cluster path. It bounds how many locks and modified keys a single backfill
    /// transaction accumulates; on the cluster path it is also the resume-checkpoint granularity, so
    /// raising it makes a leader change replay more work.
    /// </summary>
    internal const int IndexBackfillBatchSize = 500;

    /// <summary>
    /// Test-only seam fired once while a newly added index is still in
    /// <see cref="SchemaElementState.WriteOnly"/> — after backfill has scanned the existing rows
    /// and immediately before the index is published. Tests use it to perform a concurrent DML
    /// write whose index maintenance must be routed into the index's immutable-id keyspace (the
    /// same keyspace backfill and post-publish readers use), not the mutable-name keyspace. It is
    /// always <c>null</c> in production; the field is cleared by the handler before invoking it so
    /// it can never fire twice or re-enter.
    /// </summary>
    internal static Func<Task>? TestHookBeforePublish;

    public TableIndexAdder(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, AlterIndexTicket ticket, CamusDBOptions options)
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

        // Index count cap — only for user-visible secondary indexes (not PK or ~-prefixed internals).
        if (ticket.Operation == AlterIndexOperation.AddIndex || ticket.Operation == AlterIndexOperation.AddUniqueIndex)
        {
            int maxIdx = options.MaxIndexesPerTable;
            if (maxIdx > 0)
            {
                int userIndexCount = 0;
                foreach (string key in table.Indexes.Keys)
                {
                    if (!key.StartsWith('~'))
                        userIndexCount++;
                }
                if (userIndexCount + 1 > maxIdx)
                    throw new CamusDBException(
                        CamusDBErrorCodes.SchemaLimitExceeded,
                        $"Table '{table.Name}' would exceed the maximum of {maxIdx} indexes per table");
            }
        }

        IndexColumnOrder.RejectDescendingOnUnsupportedType(
            ticket.Columns,
            ticket.IndexName,
            name => table.Schema.Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))?.Type);

        foreach (ColumnIndexInfo indexColumn in ticket.Columns)
        {
            TableColumnSchema? tableColumn = null;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (string.Equals(column.Name, indexColumn.Name, StringComparison.OrdinalIgnoreCase))
                {
                    tableColumn = column;
                    break;
                }
            }

            if (tableColumn is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' does not exist on table '{table.Name}'"
                );

            if (!SchemaElementStateRules.IsReadable(tableColumn) || !SchemaElementStateRules.IsWritable(tableColumn))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' is not public on table '{table.Name}'"
                );
        }

        ValidateIncludeColumns(table, ticket, options);
    }

    /// <summary>
    /// Validates the optional INCLUDE (stored/payload) column list: the combined key+include column
    /// count must be within <see cref="CamusDBOptions.MaxIndexColumns"/>, and each included column must
    /// exist and be public, must not duplicate another include column, and must not also be a key
    /// column of the same index (it would be stored twice with an ambiguous covered slot). Key-column
    /// existence/public checks already ran in <see cref="Validate"/>. Included columns are unordered
    /// payload, so no direction/type-ordering restriction applies here. Runs on both the standalone and
    /// the cluster add paths.
    /// </summary>
    /// <summary>
    /// Enforces the combined key+include column ceiling (<see cref="CamusDBOptions.MaxIndexColumns"/>)
    /// for a single index. A covering index duplicates each included value into every entry, so an
    /// unbounded column count is a storage/replication amplification hazard. Disabled when the config
    /// value is <c>&lt;= 0</c>. Shared by the standalone/cluster add paths and the inline
    /// <c>CREATE TABLE</c> constraint builder.
    /// </summary>
    internal static void ValidateIndexColumnCount(string indexName, int keyColumnCount, int includeColumnCount, CamusDBOptions options)
    {
        int max = options.MaxIndexColumns;
        if (max <= 0)
            return;

        int total = keyColumnCount + includeColumnCount;
        if (total > max)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Index '{indexName}' spans {total} columns ({keyColumnCount} key + {includeColumnCount} INCLUDE), exceeding the maximum of {max}");
    }

    internal static void ValidateIncludeColumns(TableDescriptor table, AlterIndexTicket ticket, CamusDBOptions options)
    {
        ValidateIndexColumnCount(ticket.IndexName, ticket.Columns.Length, ticket.IncludeColumns.Length, options);

        if (ticket.IncludeColumns.Length == 0)
            return;

        HashSet<string> keyColumns = new(StringComparer.Ordinal);
        foreach (ColumnIndexInfo keyColumn in ticket.Columns)
            keyColumns.Add(keyColumn.Name);

        HashSet<string> seenIncludes = new(StringComparer.Ordinal);

        foreach (string includeName in ticket.IncludeColumns)
        {
            if (!seenIncludes.Add(includeName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate INCLUDE column '{includeName}' on index '{ticket.IndexName}'");

            if (keyColumns.Contains(includeName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{includeName}' is already indexed as a key column of index '{ticket.IndexName}'");

            TableColumnSchema? tableColumn = table.Schema.Columns!.Find(c => string.Equals(c.Name, includeName, StringComparison.OrdinalIgnoreCase));

            if (tableColumn is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"INCLUDE column '{includeName}' does not exist on table '{table.Name}'");

            if (!SchemaElementStateRules.IsReadable(tableColumn) || !SchemaElementStateRules.IsWritable(tableColumn))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"INCLUDE column '{includeName}' is not public on table '{table.Name}'");
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
        Validate(table, ticket, database.Options);

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

    private async Task<FluxAction> AddWriteOnlySystemObject(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;
        IndexType indexType = ticket.Operation is AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey
            ? IndexType.Unique
            : IndexType.Multi;

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = GetColumnIds(table, ticket.Columns);
        OrderType[]? columnDirections = IndexColumnOrder.Extract(ticket.Columns);
        string[]? includeColumnIds = ticket.IncludeColumns.Length > 0
            ? GetColumnIdsByName(table, ticket.IncludeColumns)
            : null;

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            table.Schema.Indexes ??= [];
            table.Schema.Indexes.Add(new TableIndexSchema(
                indexId,
                ticket.IndexName,
                columnIds,
                indexType,
                SchemaElementState.WriteOnly,
                startOffset: null,
                columnDirections: columnDirections,
                includeColumnIds: includeColumnIds
            ));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.MutateIndexes(indexes => indexes.Add(
            ticket.IndexName,
            new TableIndexSchema(ticket.IndexName, ticket.Columns.Select(x => x.Name).ToArray(), indexType, SchemaElementState.WriteOnly, id: indexId, columnDirections: columnDirections, includeColumns: ticket.IncludeColumns.Length > 0 ? ticket.IncludeColumns : null)
        ));
        table.Store.RegisterIndexName(indexId, ticket.IndexName);
        table.Store.RegisterIndexDirections(indexId, columnDirections);

        state.IndexId = indexId;

        return FluxAction.Continue;
    }

    /// <summary>
    /// Writes one index entry for every existing row, in batches of
    /// <see cref="IndexBackfillBatchSize"/> rows, each in its own transaction committed as soon as the
    /// batch completes. A single transaction over the whole table would accumulate every acquired lock
    /// and modified key for as long as the build runs — a memory hazard, and a long-held-lock hazard
    /// for concurrent DML.
    ///
    /// <para>
    /// The batches deliberately do <em>not</em> reuse the caller's DDL transaction, not even for the
    /// first one: a later batch writing a unique key that an earlier batch already wrote would then
    /// collide with that transaction's own uncommitted write intent and fail as a transaction conflict
    /// instead of the duplicate-key error the statement owes the user. Committing each batch means the
    /// next one validates against committed state, so <c>SetIfNotExists</c> reports a genuine duplicate.
    /// </para>
    ///
    /// <para>
    /// Committing early is safe because the index is <see cref="SchemaElementState.WriteOnly"/> for
    /// the whole backfill: concurrent DML already maintains it, and no reader or planner can use it
    /// until <see cref="PublishIndex"/> runs. If the DDL later aborts, the committed entries live under
    /// an index id that is never published — unreachable, but not free, so the abort compensation
    /// purges them (see <c>CommandExecutor.CompensateAbortedAddIndexAsync</c>).
    /// </para>
    /// </summary>
    private async Task<FluxAction> BackfillIndex(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;

        bool unique = ticket.Operation is AlterIndexOperation.AddPrimaryKey or AlterIndexOperation.AddUniqueIndex;
        string indexId = state.IndexId
            ?? table.Schema.Indexes?.FirstOrDefault(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.OrdinalIgnoreCase))?.Id
            ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Index '{ticket.IndexName}' was not found in schema");

        TableIndexSchema? schemaIndex = table.Schema.Indexes?.FirstOrDefault(ix => ix.Id == indexId);
        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(schemaIndex?.StartOffset)
            ? null
            : ObjectId.ToValue(schemaIndex.StartOffset!);

        // The backfill only reads the index's key and INCLUDE columns, so narrow the per-row decode
        // to them — values for those columns are identical to a full decode, and the scanned rows are
        // never re-encoded, only turned into index entries.
        HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnIndexInfo columnIndex in ticket.Columns)
            requiredColumns.Add(columnIndex.Name);
        foreach (string includeColumn in ticket.IncludeColumns)
            requiredColumns.Add(includeColumn);

        int totalRows = 0;

        while (true)
        {
            // Background priority: the backfill is bulk, deferrable work that already runs in
            // resumable batches, so on a saturated node it should yield the door to user traffic.
            // This is the one engine path where admission priority genuinely applies — the backfill
            // opens a coordinator session per batch, and each batch re-enters the gate.
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                mutationLimitOverride: 0,
                priority: TransactionPriority.Background
            ).ConfigureAwait(false);

            int batchRows = 0;
            ObjectIdValue lastRowId = default;

            // Per-batch decode-plan cache; scoped to the batch transaction because the visibility
            // version is re-read from the live schema each batch.
            RowEncoder.DictionaryDecodeState decodeState = new();

            try
            {
                // The backfill writes one index entry per row it reads, so the rows it observed are a
                // real commit dependency: a concurrent modification to one of them must invalidate
                // this batch.
                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    tx,
                    afterRowId: afterRowId).ConfigureAwait(false))
                {
                    Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                        table.Schema,
                        tx.TransactionId,
                        rowId,
                        data,
                        requiredColumns: requiredColumns,
                        visibilitySchemaVersion: table.Schema.Version,
                        decodeState: decodeState).ConfigureAwait(false);

                    int i = 0;
                    ColumnValue[] columnValues = unique
                        ? new ColumnValue[ticket.Columns.Length]
                        : new ColumnValue[ticket.Columns.Length + 1];

                    foreach (ColumnIndexInfo columnIndex in ticket.Columns)
                    {
                        ColumnValue? keyValue = GetColumnValue(row, columnIndex.Name);

                        if (keyValue is null)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInternalOperation,
                                $"A null value was found for key field '{columnIndex.Name}'"
                            );

                        columnValues[i++] = keyValue;
                    }

                    if (!unique)
                        columnValues[i] = new(ColumnType.Id, rowId.ToString());

                    CompositeColumnValue compositeKey = new(columnValues);

                    // Materialize the stored/payload (INCLUDE) values for a covering index. Unlike key
                    // columns, included columns may be NULL (they are payload, not part of the key).
                    // EncodeTupleChecked enforces the per-entry byte ceiling before the KV write.
                    byte[]? includeTuple = ticket.IncludeColumns.Length > 0
                        ? IndexIncludeValueCodec.EncodeTupleChecked(ticket.IncludeColumns, row, ticket.IndexName, state.Database.Options)
                        : null;

                    await table.Store.PutIndexEntry(tx, indexId, compositeKey, rowId, unique, includeTuple: includeTuple).ConfigureAwait(false);
                    state.LastBackfilledRowId = rowId.ToString();

                    lastRowId = rowId;
                    batchRows++;

                    if (batchRows >= IndexBackfillBatchSize)
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

            // A short batch means the scan reached the end of the table.
            if (batchRows < IndexBackfillBatchSize)
                break;

            afterRowId = lastRowId;
        }

        state.ModifiedRows = totalRows;

        Log.LogIndexRowsAdded(logger, totalRows, ticket.IndexName);

        return FluxAction.Continue;
    }

    private async Task<FluxAction> PublishIndex(AddIndexFluxState state)
    {
        // Test-only: drive a concurrent write while the index is still WriteOnly (pre-publish).
        // Cleared before invoking so it fires at most once and cannot re-enter.
        if (TestHookBeforePublish is { } hook)
        {
            TestHookBeforePublish = null;
            await hook().ConfigureAwait(false);
        }

        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;
        string indexId = state.IndexId
            ?? table.Schema.Indexes?.FirstOrDefault(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.OrdinalIgnoreCase))?.Id
            ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Index '{ticket.IndexName}' was not found in schema");
        string finalOffset = state.LastBackfilledRowId ?? "";

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            if (table.Schema.Indexes is not null)
            {
                for (int i = 0; i < table.Schema.Indexes.Count; i++)
                {
                    if (table.Schema.Indexes[i].Id != indexId)
                        continue;

                    TableIndexSchema old = table.Schema.Indexes[i];
                    table.Schema.Indexes[i] = new TableIndexSchema(
                        old.Id!,
                        old.Name,
                        old.ColumnIds,
                        old.Type,
                        SchemaElementState.Public,
                        finalOffset,
                        columnDirections: old.ColumnDirections,
                        includeColumnIds: old.IncludeColumnIds,
                        comment: old.Comment
                    );
                    break;
                }
            }

        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.MutateIndexes(indexes =>
        {
            TableIndexSchema current = indexes[ticket.IndexName];
            indexes[ticket.IndexName] = new TableIndexSchema(current.Name, current.Columns ?? [], current.Type, SchemaElementState.Public, id: current.Id, columnDirections: current.ColumnDirections, includeColumns: current.IncludeColumns.Length > 0 ? current.IncludeColumns : null, comment: current.Comment);
        });

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
                if (string.Equals(column.Name, columnIndex.Name, StringComparison.OrdinalIgnoreCase))
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

    /// <summary>
    /// Resolves a bare column-name list (used for INCLUDE columns) to immutable column ids, in the
    /// same order. Throws if a name is unknown — callers must have validated existence first.
    /// </summary>
    private static string[] GetColumnIdsByName(TableDescriptor table, string[] columnNames)
    {
        string[] columnIds = new string[columnNames.Length];

        for (int i = 0; i < columnNames.Length; i++)
        {
            TableColumnSchema? column = table.Schema.Columns!.Find(c => string.Equals(c.Name, columnNames[i], StringComparison.OrdinalIgnoreCase));

            if (column is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Couldn't get column id for column '{columnNames[i]}'");

            columnIds[i] = column.Id;
        }

        return columnIds;
    }

    private async Task<int> AlterIndexInternal(FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine, AddIndexFluxState state)
    {
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(AddIndexFluxSteps.AddWriteOnlySystemObject, AddWriteOnlySystemObject);
        machine.When(AddIndexFluxSteps.BackfillIndex, BackfillIndex);
        machine.When(AddIndexFluxSteps.PublishIndex, PublishIndex);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogIndexAdded(logger, ticket.IndexName, table.Name, timeTaken);

        return state.ModifiedRows;
    }
}
