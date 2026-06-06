
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Facade for executing commands on the database and tables
/// </summary>
public sealed class CommandExecutor : IAsyncDisposable
{
    private readonly ILogger<ICamusDB> logger;

    private readonly CatalogsManager catalogs;

    private readonly DatabaseOpener databaseOpener;

    private readonly DatabaseCreator databaseCreator;

    private readonly DatabaseCloser databaseCloser;

    private readonly DatabaseDropper databaseDroper;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly TableOpener tableOpener;

    private readonly TableCreator tableCreator;

    private readonly TableColumnAlterer tableColumnAlterer;

    private readonly TableIndexAlterer tableIndexAlterer;

    private readonly TableDropper tableDropper;

    private readonly RowInserter rowInserter;

    private readonly RowUpdater rowUpdater;

    private readonly RowDeleter rowDeleter;

    private readonly StatisticsManager statisticsManager;

    public StatisticsManager Statistics => statisticsManager;

    private readonly QueryExecutor queryExecutor;

    private readonly SqlExecutor sqlExecutor;

    private readonly SchemaQuerier schemaQuerier;

    private readonly QueryBinder queryBinder;

    private readonly SubqueryRewriter subqueryRewriter;

    private readonly ExistsSubqueryPreparer existsSubqueryPreparer;

    private readonly ExplainExecutor explainExecutor;

    private readonly SelectQueryCreator selectQueryCreator = new();

    private readonly CommandValidator validator;

    private readonly ISchemaDdlForwarder? schemaDdlForwarder;

    /// <summary>
    /// Initializes the commands executor
    /// </summary>
    /// <param name="validator"></param>
    /// <param name="catalogs"></param>
    /// <param name="logger"></param>
    /// <param name="loggerFactory">Optional factory forwarded to the embedded Kahuna node so its internal logs are visible.</param>
    /// <param name="clusterNode">Process-level Kahuna node shared across all databases in cluster mode; null in standalone mode.</param>
    public CommandExecutor(
        CommandValidator validator,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        ILoggerFactory? loggerFactory = null,
        EmbeddedKahuna? clusterNode = null,
        ISchemaDdlForwarder? schemaDdlForwarder = null)
    {
        this.validator = validator;
        this.catalogs = catalogs;
        this.logger = logger;
        this.schemaDdlForwarder = schemaDdlForwarder;

        databaseDescriptors = new();
        databaseOpener = new(this, databaseDescriptors, catalogs, logger, clusterNode, loggerFactory);
        databaseCloser = new(databaseDescriptors, logger);
        databaseDroper = new(databaseDescriptors, logger);
        databaseCreator = new(logger);
        tableOpener = new(catalogs, logger);
        tableCreator = new(catalogs, logger);
        tableColumnAlterer = new(catalogs, logger);
        tableIndexAlterer = new(catalogs, logger);
        tableDropper = new(catalogs, logger);
        rowInserter = new(logger);
        rowUpdater = new(logger);
        rowDeleter = new(logger);
        statisticsManager = new(logger);
        queryExecutor = new(logger, statisticsManager);
        sqlExecutor = new(logger);
        schemaQuerier = new(catalogs, logger);
        queryBinder = new QueryBinder(tableOpener);
        SubqueryQueryExecutor subqueryQueryExecutor = new(queryBinder, queryExecutor);
        ExistsSubqueryExecutor existsSubqueryExecutor = new(subqueryQueryExecutor);
        subqueryRewriter = new SubqueryRewriter(
            new ScalarSubqueryExecutor(subqueryQueryExecutor),
            new InSubqueryExecutor(subqueryQueryExecutor));
        existsSubqueryPreparer = new ExistsSubqueryPreparer(existsSubqueryExecutor, queryBinder);
        explainExecutor = new ExplainExecutor(subqueryRewriter, queryBinder, existsSubqueryPreparer, queryExecutor, statisticsManager);
    }

    #region database

    public async Task<DatabaseDescriptor> CreateDatabase(CreateDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        databaseCreator.Create(ticket);

        return await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
    {
        return await databaseOpener.Open(database, recoveryMode).ConfigureAwait(false);
    }

    public async Task CloseDatabase(CloseDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        // Flush tail stats before the descriptor is torn down so debounced deltas survive shutdown.
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        await statisticsManager.FlushAllAsync(database).ConfigureAwait(false);

        await databaseCloser.Close(ticket.DatabaseName).ConfigureAwait(false);
    }

    public async Task DropDatabase(DropDatabaseTicket ticket)
    {
        validator.Validate(ticket);

        await databaseDroper.Drop(ticket.DatabaseName).ConfigureAwait(false);
    }

    #endregion

    #region DDL

    /// <summary>
    /// Executes a DDL action in a self-managed Kahuna transaction.
    /// Begins a transaction, runs <paramref name="action"/>, commits on success,
    /// or rolls back (and re-throws) on any exception.
    /// </summary>
    private async Task<T> ExecuteDdlInTransaction<T>(
        DatabaseDescriptor database,
        Func<KvTransaction, Task<T>> action,
        Func<Task>? onAbort = null
    )
    {
        if (!database.OwnsKahuna)
            await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);
        try
        {
            T result = await action(tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
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
                    logger.LogWarning(
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
            if (!database.OwnsKahuna)
                database.SchemaDdlSemaphore.Release();
            // F1a: fire after CommitAsync (or RollbackIfNotCompletedAsync on error) so the
            // KV transaction is settled before schema-partition leadership changes.
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    public async Task<CreateTableResult> CreateTable(CreateTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        bool? forwarded = await TryForwardCreateTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return new CreateTableResult(database, forwarded.Value);

        return await ExecuteDdlInTransaction(database, async tx =>
        {
            bool result = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, ticket, tx).ConfigureAwait(false);
            return new CreateTableResult(database, result);
        }).ConfigureAwait(false);
    }

    public async Task<bool> AlterTable(AlterTableTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        bool? forwarded = await TryForwardAlterTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        if (!database.OwnsKahuna && ticket.Operation == AlterTableOperation.AddColumn)
            return await ExecuteClusterAddColumnAsync(database, table, ticket).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database, tx =>
            tableColumnAlterer.Alter(queryExecutor, database, table, ticket, tx)
        ).ConfigureAwait(false);
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
    private async Task<bool> ExecuteClusterAddColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterTableTicket ticket
    )
    {
        ColumnInfo columnInfo = ticket.Column;

        // Eagerly reject duplicates before acquiring the semaphore — the coordinator
        // would silently no-op (already-at-Public = empty path) instead of throwing.
        if (table.Schema.Columns?.Any(c => c.Name == columnInfo.Name) == true)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate column '{columnInfo.Name}'");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, logger);
            coordinator.BackfillAsync = (db, tableName, column) => BackfillColumnDefaultsAsync(db, tableName, column);

            await coordinator.RunJobAsync(
                database,
                new SchemaChangeJob(database.Name, ticket.TableName, columnInfo.Name, SchemaElementState.Public),
                columnDefinition: columnInfo
            ).ConfigureAwait(false);

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
    /// Used by both the command-path coordinator and the D2 leader-change resume coordinator so
    /// backfill is always part of the resumable sequence.
    /// </summary>
    internal async Task BackfillColumnDefaultsAsync(DatabaseDescriptor database, string tableName, ColumnInfo column)
    {
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);

        AlterColumnTicket alterTicket = new(
            databaseName: database.Name,
            tableName: tableName,
            column: column,
            operation: AlterTableOperation.AddColumn
        );

        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);
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

    // Number of rows indexed per Kahuna transaction during backfill.  Committing in bounded
    // batches keeps transaction size manageable and allows a leader-change resume to skip
    // already-indexed rows via the persisted StartOffset checkpoint.
    private const int BackfillBatchSize = 500;

    /// <summary>
    /// Test-only hook: invoked after each intermediate batch checkpoint is persisted
    /// (i.e., after batch N commits and before batch N+1 starts). Allows tests to inject
    /// a pause or forced leader change between batches without relying on timing.
    /// Set to null in production; cleared in test TearDown.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal Func<Task>? TestInterceptAfterBackfillCheckpoint;

    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal CatalogsManager Catalogs => catalogs;

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
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);
        bool unique = indexInfo.IndexType == IndexType.Unique;

        ObjectIdValue? afterRowId = string.IsNullOrWhiteSpace(startOffset)
            ? null
            : ObjectId.ToValue(startOffset!);

        int totalRows = 0;

        while (true)
        {
            KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);
            int batchRows = 0;
            ObjectIdValue lastRowId = default;

            try
            {
                await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(
                    tx.TransactionId, afterRowId: afterRowId).ConfigureAwait(false))
                {
                    Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                        table.Schema, tx.TransactionId, rowId, data,
                        visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);

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
                    await table.Store.PutIndexEntry(tx, indexInfo.IndexName, compositeKey, rowId, unique, backfillMode: true).ConfigureAwait(false);

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

        logger.LogInformation("Backfilled {Rows} rows into index {IndexName}", totalRows, indexInfo.IndexName);
    }

    /// <summary>
    /// Drives the add-index sequence through the coordinator-owned staged path
    /// (<c>Absent → DeleteOnly → WriteOnly → [backfill] → Public</c>).
    /// Only called when <c>!database.OwnsKahuna</c>.
    /// </summary>
    private async Task<bool> ExecuteClusterAddIndexAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        if (table.Indexes.ContainsKey(ticket.IndexName))
        {
            if (ticket.IfNotExists)
                return false;
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{ticket.IndexName}' already exists on table '{table.Name}'");
        }

        IndexType indexType = ticket.Operation is AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey
            ? IndexType.Unique
            : IndexType.Multi;

        string indexId = ObjectIdGenerator.Generate().ToString();
        string[] columnIds = GetColumnIdsForIndex(table, ticket.Columns);
        string[] columnNames = ticket.Columns.Select(c => c.Name).ToArray();

        IndexBuildInfo indexInfo = new(indexId, ticket.IndexName, columnIds, columnNames, indexType);

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            SchemaChangeCoordinator coordinator = new(catalogs, logger);
            coordinator.IndexBackfillAsync = (db, tbl, info, start, checkpoint) => BackfillIndexEntriesAsync(db, tbl, info, start, checkpoint);

            try
            {
                await coordinator.RunJobAsync(
                    database,
                    new SchemaChangeJob(database.Name, ticket.TableName, ticket.IndexName, SchemaElementState.Public, SchemaElementKind.Index),
                    indexBuildInfo: indexInfo
                ).ConfigureAwait(false);

                return true;
            }
            catch
            {
                // Compensate: if the index was partially committed to the schema (in DeleteOnly
                // or WriteOnly state) but did not reach Public, emit DropIndex on all nodes and
                // delete the persisted coordinator job, leaving the cluster in a clean state.
                // Note: if this node is now degraded (F1a), compensation may be skipped by the
                // degraded gate in ReplicateDropIndexAsync; a healthy peer's ResumeJobsAsync
                // will reconcile the state after the step-down below.
                await CompensateClusterAddIndexAsync(database, ticket.TableName, ticket.IndexName).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    // F1a: shared helper — delegates to DatabaseDescriptor.FireDeferredSchemaStepDownAsync,
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
            logger.LogError(
                ex,
                "Schema partition step-down after persist exhaustion failed for database {DatabaseName}",
                database.Name
            );
        }
    }

    private async Task CompensateClusterAddIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        try
        {
            if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema) &&
                tableSchema.Indexes?.Any(ix => ix.Name == indexName) == true)
            {
                await catalogs.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to compensate partial index add for {IndexName} on {TableName}", indexName, tableName);
        }

        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, tableName, indexName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to delete coordinator job for index {IndexName} on {TableName}", indexName, tableName);
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
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        bool? forwarded = await TryForwardAlterIndexAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        bool addIndexOperation = ticket.Operation is
            AlterIndexOperation.AddIndex or
            AlterIndexOperation.AddUniqueIndex or
            AlterIndexOperation.AddPrimaryKey;

        if (!database.OwnsKahuna && addIndexOperation)
            return await ExecuteClusterAddIndexAsync(database, table, ticket).ConfigureAwait(false);

        bool indexExistedBefore = table.Indexes.ContainsKey(ticket.IndexName);
        bool compensateOnAbort = addIndexOperation && !indexExistedBefore;

        if (!database.OwnsKahuna)
            return await ExecuteClusteredIndexDdlAsync(
                database, table, ticket, compensateOnAbort,
                tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx)
            ).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(
            database,
            tx => tableIndexAlterer.Alter(queryExecutor, database, table, ticket, tx),
            onAbort: compensateOnAbort
                ? () => CompensateAbortedAddIndexAsync(database, table, ticket.IndexName)
                : null
        ).ConfigureAwait(false);
    }

    private static async Task CompensateAbortedAddIndexAsync(DatabaseDescriptor database, TableDescriptor table, string indexName)
    {
        table.Indexes.Remove(indexName);

        await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            table.Schema.Indexes?.RemoveAll(ix => ix.Name == indexName);
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
    /// stays stable across the pair. Must only be called when <c>!database.OwnsKahuna</c>.
    /// </summary>
    private async Task<bool> ExecuteClusteredIndexDdlAsync(
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
            KvTransaction tx1 = await database.Transactions.BeginAsync().ConfigureAwait(false);
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
            KvTransaction tx2 = await database.Transactions.BeginAsync().ConfigureAwait(false);
            try
            {
                await catalogs.ReplicateIndexChangeAsync(database, ticket, table, tx2).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx2).ConfigureAwait(false);
            }

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
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        bool? forwarded = await TryForwardDropTableAsync(database, ticket).ConfigureAwait(false);
        if (forwarded is not null)
            return forwarded.Value;

        if (ticket.IfExists && !catalogs.TableExists(database, ticket.TableName))
            return false;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        return await ExecuteDdlInTransaction(database, tx =>
            tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, ticket, tx)
        ).ConfigureAwait(false);
    }

    public async Task<TableDescriptor> OpenTable(OpenTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        return await tableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    public async Task<TableDescriptor> OpenTableWithDescriptor(DatabaseDescriptor descriptor, OpenTableTicket ticket)
    {
        return await tableOpener.Open(descriptor, ticket.TableName).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardCreateTableAsync(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardCreateTableAsync(leader, ticket, opId, ct),
            () => ForwardedCreateTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardAlterTableAsync(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterTableAsync(leader, ticket, opId, ct),
            () => ForwardedAlterTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardAlterIndexAsync(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterIndexAsync(leader, ticket, opId, ct),
            () => ForwardedAlterIndexApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardDropTableAsync(DatabaseDescriptor database, DropTableTicket ticket)
    {
        return await TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardDropTableAsync(leader, ticket, opId, ct),
            () => ForwardedDropTableApplied(database, ticket)
        ).ConfigureAwait(false);
    }

    private async Task<bool?> TryForwardDdlAsync(
        DatabaseDescriptor database,
        Func<string, string, CancellationToken, Task<bool?>> forward,
        Func<bool> wasApplied
    )
    {
        if (database.OwnsKahuna)
            return null;

        // F1a: degraded nodes must not propose or forward DDL — reject immediately so the
        // caller gets a typed "degraded" error rather than a generic "not leader" error.
        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

        if (await database.Kahuna.AmISchemaLeaderAsync(database.Name, CancellationToken.None).ConfigureAwait(false))
            return null;

        if (schemaDdlForwarder is null)
        {
            string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Name, CancellationToken.None).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"DDL must be executed by schema leader '{leader}' for database '{database.Name}'"
            );
        }

        // One stable id for all retry attempts so a C3 dedup receiver can
        // recognise retransmissions of the same logical operation.
        string operationId = Guid.NewGuid().ToString("N");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            long fromVersion = database.Schema.SchemaVersion;

            for (int attempt = 0; attempt < 3; attempt++)
            {
                string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Name, CancellationToken.None).ConfigureAwait(false);
                bool? result = await forward(leader, operationId, CancellationToken.None).ConfigureAwait(false);
                if (result is not null)
                {
                    if (result.Value)
                        await WaitForForwardedSchemaApplyAsync(database, fromVersion, wasApplied).ConfigureAwait(false);

                    return result;
                }
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Failed to forward DDL to schema leader for database '{database.Name}'"
        );
    }

    private static async Task WaitForForwardedSchemaApplyAsync(DatabaseDescriptor database, long fromVersion, Func<bool> wasApplied)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            if (database.Schema.SchemaVersion > fromVersion && wasApplied())
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for forwarded schema apply for database '{database.Name}' after version {fromVersion}"
        );
    }

    private static bool ForwardedCreateTableApplied(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        return database.Schema.Tables.ContainsKey(ticket.TableName);
    }

    private static bool ForwardedAlterTableApplied(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        return ticket.Operation switch
        {
            // A forwarded AddColumn is complete only when the column is Public — intermediate
            // staged states (DeleteOnly, WriteOnly) are not yet visible to queries.
            AlterTableOperation.AddColumn =>
                tableSchema.Columns?.Any(c => c.Name == ticket.Column.Name && c.State == SchemaElementState.Public) == true,
            AlterTableOperation.DropColumn =>
                tableSchema.Columns?.Any(c => c.Name == ticket.Column.Name) != true,
            _ => false
        };
    }

    private static bool ForwardedAlterIndexApplied(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        // Check TableSchema.Indexes (the B1/B2 source of truth). Fall back to SystemSchema
        // for nodes that haven't yet applied the B1 migration (legacy path).
        bool existsInSchema = database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema) &&
                              tableSchema.Indexes is not null &&
                              tableSchema.Indexes.Any(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.Ordinal));

        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey => existsInSchema,
            AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey => !existsInSchema,
            _ => false
        };
    }

    private static bool ForwardedDropTableApplied(DatabaseDescriptor database, DropTableTicket ticket)
    {
        return !database.Schema.Tables.ContainsKey(ticket.TableName)
            && !database.TableDescriptors.ContainsKey(ticket.TableName);
    }

    public async Task<ExecuteDDLSQLResult> ExecuteDDLSQL(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        switch (ast.nodeType)
        {
            case NodeType.CreateTable:
            case NodeType.CreateTableIfNotExists:
                {
                    CreateTableTicket createTableTicket = sqlExecutor.CreateCreateTableTicket(ticket, ast);
                    validator.Validate(createTableTicket);

                    bool? forwarded = await TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableCreator.Create(queryExecutor, tableOpener, tableIndexAlterer, database, createTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddColumn:
            case NodeType.AlterTableDropColumn:
                {
                    AlterTableTicket alterTableTicket = sqlExecutor.CreateAlterTableTicket(ticket, ast);
                    validator.Validate(alterTableTicket);

                    bool? forwarded = await TryForwardAlterTableAsync(database, alterTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await tableOpener.Open(database, alterTableTicket.TableName).ConfigureAwait(false);

                    if (!database.OwnsKahuna && alterTableTicket.Operation == AlterTableOperation.AddColumn)
                    {
                        bool ok = await ExecuteClusterAddColumnAsync(database, table, alterTableTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableColumnAlterer.Alter(queryExecutor, database, table, alterTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            case NodeType.AlterTableAddIndex:
            case NodeType.AlterTableAddIndexIfNotExists:
            case NodeType.AlterTableAddUniqueIndex:
            case NodeType.AlterTableAddUniqueIndexIfNotExists:
            case NodeType.AlterTableDropIndex:
            case NodeType.AlterTableAddPrimaryKey:
            case NodeType.AlterTableDropPrimaryKey:
                {
                    AlterIndexTicket alterIndexTicket = sqlExecutor.CreateAlterIndexTicket(ticket, ast);
                    validator.Validate(alterIndexTicket);

                    bool? forwarded = await TryForwardAlterIndexAsync(database, alterIndexTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    TableDescriptor table = await tableOpener.Open(database, alterIndexTicket.TableName).ConfigureAwait(false);

                    bool sqlAddIndex = alterIndexTicket.Operation is
                        AlterIndexOperation.AddIndex or
                        AlterIndexOperation.AddUniqueIndex or
                        AlterIndexOperation.AddPrimaryKey;

                    if (!database.OwnsKahuna && sqlAddIndex)
                    {
                        bool ok = await ExecuteClusterAddIndexAsync(database, table, alterIndexTicket).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    if (!database.OwnsKahuna)
                    {
                        bool ok = await ExecuteClusteredIndexDdlAsync(
                            database, table, alterIndexTicket, compensateOnAbort: false,
                            tx => tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx)
                        ).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            case NodeType.DropTable:
            case NodeType.DropTableIfExists:
                {
                    DropTableTicket dropTableTicket = sqlExecutor.CreateDropTableTicket(ticket, ast);
                    validator.Validate(dropTableTicket);

                    bool? forwarded = await TryForwardDropTableAsync(database, dropTableTicket).ConfigureAwait(false);
                    if (forwarded is not null)
                        return new ExecuteDDLSQLResult(database, forwarded.Value);

                    if (dropTableTicket.IfExists && !catalogs.TableExists(database, dropTableTicket.TableName))
                        return new(database, false);

                    TableDescriptor table = await tableOpener.Open(database, dropTableTicket.TableName).ConfigureAwait(false);

                    return await ExecuteDdlInTransaction(database, async tx =>
                    {
                        bool ok = await tableDropper.Drop(queryExecutor, tableIndexAlterer, rowDeleter, database, table, dropTableTicket, tx).ConfigureAwait(false);
                        return new ExecuteDDLSQLResult(database, ok);
                    }).ConfigureAwait(false);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown DDL AST stmt: " + ast.nodeType);
        }
    }

    #endregion

    #region DML

    public async Task<InsertResult> Insert(InsertTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int inserted = await rowInserter.Insert(database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackInsert(database, table, inserted);
        return new(database, table, inserted);
    }

    /// <summary>
    /// Updates rows specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<UpdateResult> Update(UpdateTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int updated = await rowUpdater.Update(queryExecutor, database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackUpdate(database, table, updated);
        return new(database, table, updated);
    }

    /// <summary>
    /// Deletes rows specifying a filter criteria
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<DeleteResult> Delete(DeleteTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        int deleted = await rowDeleter.Delete(queryExecutor, database, table, ticket).ConfigureAwait(false);
        statisticsManager.TrackDelete(database, table, deleted);
        return new(database, table, deleted);
    }

    /// <summary>
    /// Queries table data specifying filters and sorts
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)> Query(QueryTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        return (database, queryExecutor.Query(database, table, ticket));
    }

    /// <summary>
    /// Queries a table by the row's id
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> QueryById(QueryByIdTicket ticket)
    {
        validator.Validate(ticket);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        PinSchemaVersion(database, table, ticket.TxnState);

        return queryExecutor.QueryById(database, table, ticket);
    }

    /// <summary>
    /// Execute a SQL statement that doesn't return rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of inserted/modified/deleted rows</returns>
    public async Task<ExecuteNonSQLResult> ExecuteNonSQLQuery(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);

        switch (ast.nodeType)
        {
            case NodeType.Insert:
                {
                    InsertTicket insertTicket = await sqlExecutor.CreateInsertTicket(this, database, ticket, ast).ConfigureAwait(false);

                    TableDescriptor table = await tableOpener.Open(database, insertTicket.TableName).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return new(database, table, await rowInserter.Insert(database, table, insertTicket).ConfigureAwait(false));
                }

            case NodeType.Update:
                {
                    UpdateTicket updateTicket = sqlExecutor.CreateUpdateTicket(ticket, ast);

                    TableDescriptor table = await tableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return new(database, table, await rowUpdater.Update(queryExecutor, database, table, updateTicket));
                }

            case NodeType.Delete:
                {
                    DeleteTicket deleteTicket = sqlExecutor.CreateDeleteTicket(ticket, ast);

                    TableDescriptor table = await tableOpener.Open(database, deleteTicket.TableName).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return new(database, table, await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false));
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Execute a SQL statement that returns rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<(DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)> ExecuteSQLQuery(ExecuteSQLTicket ticket)
    {
        validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);

        switch (ast.nodeType)
        {
            case NodeType.Select:
                {
                    SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(ast);
                    selectQuery = await subqueryRewriter
                        .RewriteSelectQueryAsync(database, selectQuery, ticket)
                        .ConfigureAwait(false);
                    BoundSelectQuery boundQuery = await queryBinder.BindAsync(database, selectQuery).ConfigureAwait(false);
                    (selectQuery, ExistsSubqueryRegistry? existsRegistry) = await existsSubqueryPreparer
                        .PrepareAsync(
                            database,
                            selectQuery,
                            boundQuery.Sources,
                            boundQuery.DerivedSources,
                            ticket)
                        .ConfigureAwait(false);
                    boundQuery = new BoundSelectQuery(
                        selectQuery,
                        boundQuery.Sources,
                        boundQuery.RowNames,
                        boundQuery.DerivedSources);
                    QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(boundQuery, ticket, existsRegistry);
                    PinSchemaVersions(database, boundQuery.Sources, ticket.TxnState);

                    if (boundQuery.IsMultiSource)
                        return (database, queryExecutor.ExecuteJoinQuery(database, boundQuery, queryTicket));

                    TableDescriptor table = boundQuery.PrimaryTable;

                    return (database, queryExecutor.Query(database, table, queryTicket));
                }

            case NodeType.ShowTables:
                {
                    return (database, schemaQuerier.ShowTables(database));
                }

            case NodeType.ShowColumns:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowColumns(table));
                }

            case NodeType.ShowIndexes:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowIndexes(table));
                }

            case NodeType.ShowCreateTable:
                {
                    TableDescriptor table = await tableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    PinSchemaVersion(database, table, ticket.TxnState);

                    return (database, schemaQuerier.ShowCreateTable(table));
                }

            case NodeType.ShowDatabase:
                {
                    return (database, schemaQuerier.ShowDatabase(database));
                }

            case NodeType.Explain:
            case NodeType.ExplainPhysical:
                {
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "physical"));
                }

            case NodeType.ExplainLogical:
                {
                    return (database, explainExecutor.ExplainQuery(database, ast.leftAst!, ticket, "logical"));
                }

            case NodeType.ExplainAnalyze:
                {
                    return (database, explainExecutor.ExplainAnalyzeQuery(database, ast.leftAst!, ticket));
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    #endregion   

    private static void PinSchemaVersions(
        DatabaseDescriptor database,
        IEnumerable<BoundTableSource> sources,
        KvTransaction tx
    )
    {
        foreach (BoundTableSource source in sources)
            PinSchemaVersion(database, source.Table, tx);
    }

    private static void PinSchemaVersion(DatabaseDescriptor database, TableDescriptor table, KvTransaction tx)
    {
        string resource = $"{database.Name}/{table.Id}";
        tx.PinSchemaVersion(
            resource,
            table.Schema.Version,
            () => table.Schema.Version,
            () => database.Schema.Tables.TryGetValue(table.Name, out TableSchema? current)
                  && current.Id == table.Id
        );
    }

    public async ValueTask DisposeAsync()
    {
        await databaseCloser.DisposeAsync();
    }
}
