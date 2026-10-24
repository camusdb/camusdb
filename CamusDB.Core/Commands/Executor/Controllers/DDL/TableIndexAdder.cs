
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

        TableIndexSchema schemaEntry = new(
            indexId,
            ticket.IndexName,
            columnIds,
            indexType,
            SchemaElementState.WriteOnly,
            startOffset: null,
            columnDirections: columnDirections,
            includeColumnIds: includeColumnIds
        );

        // Kahuna must see a key space registered before the first write to it. Once the WriteOnly index
        // is published below, every statement writes entries into its key space, so the space is
        // registered here, while it is still empty. Left to the next table open, the registration would
        // change the routing of a space whose entries and write intents were placed by hash routing,
        // under writers that still hold those intents. The rule is the one TableOpener applies.
        if (database.Options.KeyRangeShardingEnabled && table.Schema.Columns is { Count: > 0 } columns)
        {
            Dictionary<string, ColumnType> columnTypeById = columns.ToDictionary(c => c.Id, c => c.Type);
            if (TableOpener.IsIndexRangeable(schemaEntry, columnTypeById))
                await database.Kahuna.Kahuna.RegisterKeyRangeAsync(table.Store.IndexKeySpace(indexId)).ConfigureAwait(false);
        }

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            table.Schema.Indexes ??= [];
            table.Schema.Indexes.Add(schemaEntry);
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
    /// Writes one index entry for every existing row through <see cref="IndexBackfiller"/>: batches of
    /// <see cref="IndexBackfiller.BatchRows"/> rows, each locked, read and committed in its own
    /// transaction, and each retried until it wins its locks. A single transaction over the whole table
    /// would accumulate every acquired lock and modified key for as long as the build runs — a memory
    /// hazard, and a long-held-lock hazard for concurrent DML.
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

        // The index is WriteOnly in the schema and in the descriptor, so every statement that starts
        // now maintains it. A transaction that wrote the table before that did not, and the backfill
        // reads committed rows only. The fence refuses the later commit of such a transaction, and
        // waits for one whose commit is already in flight, so that its rows are either visible to the
        // backfill or never exist. Without it such a row gets no entry and the index stays incomplete.
        await database.FenceWritersAndWaitAsync(table.Schema, database.Kahuna.SchemaAckWaitTimeout).ConfigureAwait(false);

        TableIndexSchema? schemaIndex = table.Schema.Indexes?.FirstOrDefault(ix => ix.Id == indexId);
        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(schemaIndex?.StartOffset)
            ? null
            : ObjectId.ToValue(schemaIndex.StartOffset!);

        string[] keyColumns = new string[ticket.Columns.Length];
        for (int i = 0; i < ticket.Columns.Length; i++)
            keyColumns[i] = ticket.Columns[i].Name;

        IndexBackfiller.Target target = new(
            indexId,
            ticket.IndexName,
            unique,
            keyColumns,
            ticket.IncludeColumns.Length > 0 ? ticket.IncludeColumns : null);

        // Background priority: the backfill is bulk, deferrable work that already runs in resumable
        // batches, so on a saturated node it yields the door to user traffic. The backfill opens a
        // coordinator session per batch, and each batch re-enters the admission gate.
        IndexBackfiller.Result result = await IndexBackfiller.BackfillAsync(
            database, table, target, afterRowId,
            TransactionPriority.Background,
            onCheckpoint: null,
            logger,
            database.Options,
            CancellationToken.None).ConfigureAwait(false);

        state.LastBackfilledRowId = result.LastRowId;
        state.ModifiedRows = result.Rows;

        Log.LogIndexRowsAdded(logger, result.Rows, ticket.IndexName);

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
                        comment: old.Comment,
                        ownerConstraintId: old.OwnerConstraintId
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
            indexes[ticket.IndexName] = new TableIndexSchema(current.Name, current.Columns ?? [], current.Type, SchemaElementState.Public, id: current.Id, columnDirections: current.ColumnDirections, includeColumns: current.IncludeColumns.Length > 0 ? current.IncludeColumns : null, comment: current.Comment, ownerConstraintId: current.OwnerConstraintId);
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
