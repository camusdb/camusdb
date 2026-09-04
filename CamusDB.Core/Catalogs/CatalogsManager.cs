
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Serializer;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// The entry point to a database's catalog: the tables, views, indexes, constraints and comments
/// that describe what the database contains. Callers across the engine hold one of these, so it
/// stays the stable surface while the work behind it is divided by responsibility.
///
/// <para><b>This class delegates and does nothing else.</b> Every member is a one-line forward to
/// one of the collaborators below. Adding logic here is the mistake to avoid: it is the one type
/// that everything reaches, so anything placed here is reachable from everywhere.</para>
///
/// <para><b>Who owns what</b></para>
/// <list type="table">
/// <item><term><see cref="RelationCatalog"/></term><description>create, alter, drop, rename, relink
/// and truncate a relation</description></item>
/// <item><term><see cref="ViewCatalog"/></term><description>views and materialized-view
/// state</description></item>
/// <item><term><see cref="TableCommentWriter"/></term><description>COMMENT ON, single-node
/// path</description></item>
/// <item><term><c>Replication.SchemaElementReplicator</c></term><description>column, index,
/// constraint, settings and comment deltas</description></item>
/// <item><term><c>Replication.SchemaChangeEntryFactory</c></term><description>builds every
/// <see cref="Catalogs.Models.SchemaChangeLogEntry"/>; nothing else constructs one</description></item>
/// <item><term><c>Replication.SchemaChangePublisher</c></term><description>the Raft round-trip and
/// the ack gates</description></item>
/// <item><term><c>Apply.*</c></term><description>applies a committed delta to in-memory schema; by
/// construction it cannot reach KV</description></item>
/// <item><term><c>Meta.*</c></term><description>key construction, KV reads and writes, the load
/// path, the checkpoint, and the per-object record stores</description></item>
/// </list>
///
/// <para><b>The rule that shapes the whole package:</b> applying a delta and persisting it are
/// separate, and must stay separate. Apply runs inside the schema partition's commit pipeline on
/// every node; a KV write from there re-enters the same partition and deadlocks it. The proposer
/// persists afterwards. No type under <c>Catalogs.Apply</c> takes an <c>IKahuna</c> or a
/// <c>KvTransaction</c>, so the compiler holds that line rather than a comment.</para>
///
/// <para>One instance serves every database in the engine. The collaborators hold no per-database
/// state; each takes the <see cref="CommandsExecutor.Models.DatabaseDescriptor"/> it acts on.</para>
/// </summary>
public sealed class CatalogsManager
{
    private readonly ILogger<ICamusDB> logger;

    private readonly SchemaCheckpointWriter checkpoints;

    private readonly SchemaChangePublisher publisher;

    private readonly SchemaElementReplicator elements;

    private readonly RelationCatalog relations;

    private readonly ViewCatalog views;

    /// <summary>
    /// Test hook, written through to <see cref="SchemaCheckpointWriter"/>: when non-null, every
    /// checkpoint persist throws it instead of writing, so a test can drive the exhausted-retry path
    /// without a real KV fault. Kept on the facade because that is where the tests reach it.
    /// </summary>
    internal Exception? TestPersistCheckpointException {
        get => checkpoints.TestPersistCheckpointException;
        set => checkpoints.TestPersistCheckpointException = value;
    }

    public CatalogsManager(ILogger<ICamusDB> logger)
    {
        this.logger = logger;

        checkpoints = new(logger);
        publisher = new(checkpoints, logger);
        elements = new(publisher);
        relations = new(publisher);
        views = new(publisher);
    }

    // -----------------------------------------------------------------------
    // Schema persistence
    // -----------------------------------------------------------------------

    // Persisting one catalog object is delegated to SchemaMetaStore, which owns the pairing of
    // every object write with the schema-version bump and the "never under the schema lock"
    // assertion. These entry points exist because callers across the engine hold a CatalogsManager,
    // not the store.

    public async Task PersistMetaAsync(DatabaseDescriptor database, KvTransaction tx)
        => await SchemaMetaStore.PersistMetaAsync(database, tx).ConfigureAwait(false);

    public async Task PersistSystemMetaAsync(DatabaseDescriptor database, KvTransaction tx)
        => await SchemaMetaStore.PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

    public async Task PersistSchemaViewAsync(DatabaseDescriptor database, ViewSchema viewSchema, KvTransaction tx)
        => await SchemaMetaStore.PersistSchemaViewAsync(database, viewSchema, tx).ConfigureAwait(false);

    public async Task DeleteSchemaViewAsync(DatabaseDescriptor database, string viewId, KvTransaction tx)
        => await SchemaMetaStore.DeleteSchemaViewAsync(database, viewId, tx).ConfigureAwait(false);

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, KvTransaction tx)
        => await SchemaMetaStore.PersistSchemaTableAsync(database, tableSchema, tx).ConfigureAwait(false);

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, long schemaVersion, KvTransaction tx)
        => await SchemaMetaStore.PersistSchemaTableAsync(database, tableSchema, schemaVersion, tx).ConfigureAwait(false);

    public async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
        => await SchemaMetaStore.PersistDroppedTableAsync(database, tableId, tx).ConfigureAwait(false);

    public Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx)
        => SchemaMetaStore.PersistDroppedTableAsync(database, tableId, schemaVersion, tx);

    public async Task PersistDroppedTableAsync(
        DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx,
        bool deferred, string formerName, HLCTimestamp droppedAt)
        => await SchemaMetaStore.PersistDroppedTableAsync(
            database, tableId, schemaVersion, tx, deferred, formerName, droppedAt).ConfigureAwait(false);

    /// <summary>
    /// Loads <c>Schema.Tables</c>, <c>Schema.Views</c> and <c>SystemSchema</c> from KV into the
    /// in-memory descriptor. See <see cref="SchemaLoader"/> for the ordering rules this obeys.
    /// </summary>
    public async Task LoadMetaAsync(DatabaseDescriptor database)
        => await SchemaLoader.LoadMetaAsync(database, logger).ConfigureAwait(false);

    internal static SchemaCheckpoint LoadSchemaCheckpoint(ReadOnlySpan<byte> buffer)
        => SchemaLoader.LoadSchemaCheckpoint(buffer);

    /// <summary>
    /// Reloads the in-memory schema from the durable KV checkpoint when the checkpoint version is
    /// ahead of memory — the repair for committed schema deltas that were never delivered to this
    /// node. Returns true only when a newer snapshot was installed. See
    /// <see cref="SchemaFreshnessReconciler"/> for the delivery gap this closes.
    /// </summary>
    public async Task<bool> ReconcileSchemaFreshnessAsync(DatabaseDescriptor database, long cooldownMs = 0)
        => await SchemaFreshnessReconciler.TryReconcileAsync(database, cooldownMs, logger).ConfigureAwait(false);

    /// <summary>
    /// Copies the source database's metadata namespace into the branch's namespace, as-of
    /// <paramref name="forkT"/>. See <see cref="BranchMetaCopier"/> for the snapshot and locking
    /// contract the caller must satisfy.
    /// </summary>
    public async Task CopyMetaForBranchAsync(DatabaseDescriptor source, string branchDbId, HLCTimestamp forkT)
        => await BranchMetaCopier.CopyMetaForBranchAsync(source, branchDbId, forkT).ConfigureAwait(false);


    // -----------------------------------------------------------------------
    // Relations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Adds a new table to the database schema together with its inline indexes and constraints.
    /// <paramref name="tableId"/> is pre-allocated by the proposer and carried verbatim, so every
    /// node applies the same id; a follower must never generate one.
    /// </summary>
    public Task<TableSchema> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx, string tableId)
        => relations.CreateTable(database, ticket, tx, tableId);

    /// <summary>Adds or removes a column on an existing table.</summary>
    public Task<TableSchema> AlterTable(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
        => relations.AlterTable(database, ticket, tx);

    /// <summary>
    /// Removes a table from the schema. A deferred drop detaches the relation but retains its
    /// key-space, writing an orphan record in the same transaction so the data stays reclaimable.
    /// </summary>
    public Task<TableSchema?> DropTableSchema(DatabaseDescriptor database, string tableName, string tableId, KvTransaction tx, bool deferred = false)
        => relations.DropTableSchema(database, tableName, tableId, tx, deferred);

    public TableSchema GetTableSchema(DatabaseDescriptor database, string tableName)
        => relations.GetTableSchema(database, tableName);

    public bool TableExists(DatabaseDescriptor database, string tableName)
        => relations.TableExists(database, tableName);

    public async Task<TableSchema> RelinkTable(DatabaseDescriptor database, OrphanTableRecord orphan, string newName, KvTransaction tx)
        => await relations.RelinkTable(database, orphan, newName, tx).ConfigureAwait(false);

    public async Task<TableSchema> RelinkRetiredContents(
        DatabaseDescriptor database, OrphanTableRecord orphan, string newTableId, string newName, KvTransaction tx)
        => await relations.RelinkRetiredContents(database, orphan, newTableId, newName, tx).ConfigureAwait(false);

    public async Task RecordRelinkTargetAsync(DatabaseDescriptor database, OrphanTableRecord orphan, string targetId)
        => await relations.RecordRelinkTargetAsync(database, orphan, targetId).ConfigureAwait(false);

    public void RegisterTableSystemObject(DatabaseDescriptor database, TableSchema tableSchema)
        => relations.RegisterTableSystemObject(database, tableSchema);

    public Task<bool> RenameTable(
        DatabaseDescriptor database, RenameTableTicket ticket, KvTransaction tx,
        Dictionary<string, ViewDefinition>? dependentViews = null)
        => relations.RenameTable(database, ticket, tx, dependentViews);

    public async Task<bool> RenameIndexInTableAsync(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        KvTransaction tx
    )
        => await relations.RenameIndexInTableAsync(database, ticket, tx).ConfigureAwait(false);

    public async Task TruncateTableContentsAsync(
        DatabaseDescriptor database,
        string tableId,
        string tableName,
        string expectedStorageId,
        long expectedContentsGeneration,
        string newStorageId,
        HLCTimestamp contentsValidFrom,
        string[] retiredIndexIds,
        string[] newIndexIds)
        => await relations.TruncateTableContentsAsync(
            database, tableId, tableName, expectedStorageId, expectedContentsGeneration,
            newStorageId, contentsValidFrom, retiredIndexIds, newIndexIds).ConfigureAwait(false);

    // -----------------------------------------------------------------------
    // Views
    // -----------------------------------------------------------------------

    public static bool ViewExists(DatabaseDescriptor database, string viewName)
        => ViewCatalog.ViewExists(database, viewName);

    public async Task CreateViewAsync(
        DatabaseDescriptor database,
        string viewId,
        string viewName,
        ViewDefinition definition,
        string? comment,
        bool replace)
        => await views.CreateViewAsync(database, viewId, viewName, definition, comment, replace).ConfigureAwait(false);

    public async Task DropViewAsync(DatabaseDescriptor database, string viewName)
        => await views.DropViewAsync(database, viewName).ConfigureAwait(false);

    public async Task RenameViewAsync(
        DatabaseDescriptor database, string viewName, string newName,
        Dictionary<string, ViewDefinition>? dependentViews = null)
        => await views.RenameViewAsync(database, viewName, newName, dependentViews).ConfigureAwait(false);

    public async Task SetViewDefinitionAsync(DatabaseDescriptor database, string viewName, ViewDefinition definition)
        => await views.SetViewDefinitionAsync(database, viewName, definition).ConfigureAwait(false);

    public async Task SetMaterializedViewStateAsync(
        DatabaseDescriptor database,
        string tableId,
        bool isPopulated,
        HLCTimestamp? refreshedAt,
        string? swapToTableId = null,
        HLCTimestamp publishHlc = default,
        long? expectedMetadataGeneration = null)
        => await views.SetMaterializedViewStateAsync(
            database, tableId, isPopulated, refreshedAt, swapToTableId, publishHlc,
            expectedMetadataGeneration).ConfigureAwait(false);

    /// <summary>
    /// Applies a COMMENT ON and persists it on the single-node path. See
    /// <see cref="TableCommentWriter"/> for the compensating rollback this relies on.
    /// </summary>
    public async Task SetTableCommentAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        CommentTarget target,
        string? elementName,
        string? comment)
        => await TableCommentWriter.SetTableCommentAsync(database, table, target, elementName, comment).ConfigureAwait(false);

    // -----------------------------------------------------------------------
    // Element-level replicated DDL
    // -----------------------------------------------------------------------
    //
    // SchemaElementReplicator owns the take-lock / build / validate / release / replicate sequence
    // these all share. These entry points exist because callers across the engine hold a
    // CatalogsManager rather than the replicator.

    public async Task ReplicateIndexChangeAsync(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        TableDescriptor table,
        KvTransaction tx
    )
        => await elements.ReplicateIndexChangeAsync(database, ticket, table, tx).ConfigureAwait(false);

    public async Task ReplicateAddColumnInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        ColumnInfo column,
        SchemaElementState initialState
    )
        => await elements.ReplicateAddColumnInStateAsync(database, tableName, column, initialState).ConfigureAwait(false);

    public async Task ReplicateElementStateAsync(
        DatabaseDescriptor database,
        string tableName,
        string elementName,
        SchemaElementState targetState,
        SchemaElementKind elementKind = SchemaElementKind.Column
    )
        => await elements.ReplicateElementStateAsync(database, tableName, elementName, targetState, elementKind).ConfigureAwait(false);

    public async Task ReplicateAddIndexInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexBuildInfo,
        SchemaElementState initialState
    )
        => await elements.ReplicateAddIndexInStateAsync(database, tableName, indexBuildInfo, initialState).ConfigureAwait(false);

    public async Task ReplicateDropIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
        => await elements.ReplicateDropIndexAsync(database, tableName, indexName).ConfigureAwait(false);

    public async Task ReplicateSetTableSettingsAsync(
        DatabaseDescriptor database,
        string tableName,
        IReadOnlyDictionary<string, string> settings,
        IReadOnlyCollection<string>? removedKeys = null)
        => await elements.ReplicateSetTableSettingsAsync(database, tableName, settings, removedKeys).ConfigureAwait(false);

    public async Task ReplicateSetCommentAsync(
        DatabaseDescriptor database,
        string tableName,
        CommentTarget target,
        string? elementName,
        string? comment)
        => await elements.ReplicateSetCommentAsync(database, tableName, target, elementName, comment).ConfigureAwait(false);

    public async Task ReplicateAddCheckConstraintAsync(
        DatabaseDescriptor database,
        string tableName,
        string constraintName,
        string expression,
        string[] referencedColumns)
        => await elements.ReplicateAddCheckConstraintAsync(database, tableName, constraintName, expression, referencedColumns).ConfigureAwait(false);

    public async Task ReplicateDropCheckConstraintAsync(
        DatabaseDescriptor database,
        string tableName,
        string constraintName)
        => await elements.ReplicateDropCheckConstraintAsync(database, tableName, constraintName).ConfigureAwait(false);

    public async Task ReplicateSetColumnNotNullAsync(
        DatabaseDescriptor database,
        string tableName,
        string columnName,
        bool notNull,
        string? constraintName)
        => await elements.ReplicateSetColumnNotNullAsync(database, tableName, columnName, notNull, constraintName).ConfigureAwait(false);

    /// <summary>
    /// Re-persists the complete in-memory schema in a single transaction. Called after log replay
    /// completes, to bring the on-disk checkpoint up to the committed head.
    /// </summary>
    public async Task PersistFullSchemaCheckpointAsync(DatabaseDescriptor database)
        => await checkpoints.PersistFullSchemaCheckpointAsync(database).ConfigureAwait(false);

    // -----------------------------------------------------------------------
    // Delta application
    // -----------------------------------------------------------------------
    //
    // The deltas themselves live under Catalogs/Apply/, which by construction cannot reach KV:
    // apply runs inside the schema partition's commit pipeline, and a write from there re-enters
    // the same partition and deadlocks it. These forwarders exist for the callers that hold a
    // CatalogsManager rather than the appliers.

    public static TableSchema? ApplySchemaDelta(Schema schema, SchemaChangeLogEntry entry)
        => SchemaDeltaApplier.ApplySchemaDelta(schema, entry);

    public static TableSchema? ApplySchemaDelta(Schema schema, DatabaseDescriptor database, SchemaChangeLogEntry entry)
        => SchemaDeltaApplier.ApplySchemaDelta(schema, database, entry);

    /// <summary>
    /// Rebuilds the parsed condition tree of every CHECK constraint whose tree is not built yet.
    /// Only the expression text is persisted, so this must run after a load and after an apply.
    /// </summary>
    internal static void ParseCheckConstraintAsts(TableSchema tableSchema)
        => ConstraintDeltaApplier.ParseCheckConstraintAsts(tableSchema);

    /// <summary>
    /// Records, in memory only, that a committed delta replaced a relation's contents and left a
    /// key-space nothing else names. Called from the apply pipeline, so it must never write KV.
    /// </summary>
    internal static void CaptureContentsRetirementIntent(DatabaseDescriptor database, SchemaChangeLogEntry entry)
        => ContentsRetirementStore.CaptureContentsRetirementIntent(database, entry);

    /// <summary>
    /// Reads every index key-space id a relation's storage generation ever owned. Called by the
    /// proposer, never from apply — the result is frozen into the schema-log payload.
    /// </summary>
    internal async Task<string[]> ReadStorageIndexCatalogAsync(DatabaseDescriptor database, TableSchema tableSchema)
        => await ContentsRetirementStore.ReadStorageIndexCatalogAsync(database, tableSchema).ConfigureAwait(false);

    // -----------------------------------------------------------------------
    // Per-object record stores
    // -----------------------------------------------------------------------
    //
    // Three small key families, each owned by its own store: the orphan record of a deferred-dropped
    // relation, the resumable schema-change coordinator job, and the in-flight materialized-view
    // refresh job. These entry points exist because callers across the engine hold a CatalogsManager
    // rather than the stores.

    public async Task DeleteTableOrphanAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
        => await OrphanTableStore.DeleteTableOrphanAsync(database, tableId, tx).ConfigureAwait(false);

    public async Task<OrphanTableRecord?> TryGetTableOrphanAsync(DatabaseDescriptor database, string tableId)
        => await OrphanTableStore.TryGetTableOrphanAsync(database, tableId).ConfigureAwait(false);

    public async Task<List<OrphanTableRecord>> LoadTableOrphansAsync(DatabaseDescriptor database)
        => await OrphanTableStore.LoadTableOrphansAsync(database).ConfigureAwait(false);

    public async Task PersistCoordinatorJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob job)
        => await CoordinatorJobStore.PersistCoordinatorJobAsync(database, job).ConfigureAwait(false);

    public async Task DeleteCoordinatorJobAsync(DatabaseDescriptor database, string tableId, string elementName)
        => await CoordinatorJobStore.DeleteCoordinatorJobAsync(database, tableId, elementName).ConfigureAwait(false);

    public async Task DeleteCoordinatorJobsForTableAsync(
        DatabaseDescriptor database,
        string tableId,
        KvTransaction tx)
        => await CoordinatorJobStore.DeleteCoordinatorJobsForTableAsync(database, tableId, tx).ConfigureAwait(false);

    public async Task<List<PersistedCoordinatorJob>> LoadCoordinatorJobsAsync(DatabaseDescriptor database)
        => await CoordinatorJobStore.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);

    public async Task PersistRefreshJobAsync(DatabaseDescriptor database, MaterializedViewRefreshJob job)
        => await MaterializedViewRefreshJobStore.PersistRefreshJobAsync(database, job).ConfigureAwait(false);

    public async Task DeleteRefreshJobAsync(DatabaseDescriptor database, string viewTableId)
        => await MaterializedViewRefreshJobStore.DeleteRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

    public async Task<MaterializedViewRefreshJob?> TryGetRefreshJobAsync(DatabaseDescriptor database, string viewTableId)
        => await MaterializedViewRefreshJobStore.TryGetRefreshJobAsync(database, viewTableId).ConfigureAwait(false);

    public Task<List<MaterializedViewRefreshJob>> ListRefreshJobsAsync(DatabaseDescriptor database)
        => MaterializedViewRefreshJobStore.ListRefreshJobsAsync(database);

    /// <summary>
    /// The listing by database id, for callers that do not have the database open — the background
    /// sweep asks "is there anything to reclaim here" without opening every database on every tick.
    /// </summary>
    public static async Task<List<MaterializedViewRefreshJob>> ListRefreshJobsAsync(IKahuna kahuna, string dbId)
        => await MaterializedViewRefreshJobStore.ListRefreshJobsAsync(kahuna, dbId).ConfigureAwait(false);

}
