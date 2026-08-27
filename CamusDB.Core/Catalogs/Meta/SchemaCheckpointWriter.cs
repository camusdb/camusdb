
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Writes the durable KV checkpoint of the schema after a delta has been committed and applied.
///
/// <para><b>The checkpoint is an optimization, not the source of truth.</b> The committed schema log
/// is authoritative; the checkpoint exists so a node can open a database without replaying the whole
/// log. That is why a failed persist is retried and then surfaced as a typed error rather than
/// rolling anything back — the delta is already committed and cannot be un-committed.</para>
///
/// <para><b>Nothing here may run from the apply callback.</b> These writes re-enter the schema
/// partition, and apply runs inside that partition's commit pipeline. The proposer calls this after
/// the replication round-trip returns, never before. Every entry point asserts
/// <c>Schema.LockDepth == 0</c> for the same reason.</para>
///
/// <para><b>A rename must persist the dependent views in the same transaction as the relation.</b>
/// Renaming a table rewrites the stored bodies of the views that read it. A checkpoint that saved
/// the table but not those bodies would leave views resolving a name that no longer exists.</para>
///
/// <para>This class is per-engine, not per-database, and holds no database state: every method takes
/// the <see cref="DatabaseDescriptor"/> it acts on.</para>
/// </summary>
internal sealed class SchemaCheckpointWriter
{
    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Test hook. When non-null, every checkpoint persist throws it instead of writing, so a test
    /// can drive the exhausted-retry path and the recovery that follows it without needing a real
    /// KV fault. Held per engine rather than statically: a single process runs several independent
    /// engines, and a process-wide hook would fire in all of them at once.
    /// </summary>
    internal Exception? TestPersistCheckpointException;

    public SchemaCheckpointWriter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Bound on the checkpoint commit. The delta is already committed, so a checkpoint that cannot
    /// land promptly must fail and be retried rather than hold the proposer indefinitely.
    /// </summary>
    private static readonly TimeSpan CheckpointCommitTimeout = TimeSpan.FromSeconds(30);

    internal async Task PersistSchemaCheckpointWithRetryAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId,
        string? droppedViewId
    )
    {
        const int maxAttempts = 3;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await PersistSchemaCheckpointAsync(database, entry, droppedTableId, droppedViewId).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogWarning(
                    ex,
                    "Schema checkpoint persist attempt {Attempt} failed for database {DbName} version {Version}; retrying",
                    attempt,
                    database.Name,
                    entry.ToVersion
                );

                await Task.Delay(50 * attempt).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Persist exhausted — the Raft commit already succeeded and the change is
                // live cluster-wide, so do NOT surface this to the client. Mark this node's
                // schema subsystem degraded and request a deferred schema-partition step-down.
                // The step-down is deferred (fired after the in-flight DDL CommitAsync) because
                // in single-partition clusters the schema and KV partitions are the same: stepping
                // down before CommitAsync would invalidate the in-flight KV transaction.
                // Restart replay will recover the checkpoint on the next open.
                Log.LogSchemaCheckpointExhausted(logger, ex, maxAttempts, database.Name, entry.ToVersion);

                database.MarkSchemaSubsystemDegraded();
                database.RequestDeferredSchemaStepDown();

                return; // swallow: committed log is the source of truth; degraded flag gates future DDL
            }
        }
    }

    internal async Task PersistSchemaCheckpointAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId,
        string? droppedViewId
    )
    {
        if (TestPersistCheckpointException is { } fault)
            throw fault;

        // Before the transaction opens, never inside it — see the method's remarks.
        await SchemaHistoryStore.PreloadContentsRetirementHistoriesAsync(database).ConfigureAwait(false);

        // High priority: this persists the KV checkpoint for a schema change that Raft has already
        // committed. Delaying it behind a queue of ordinary traffic stalls DDL for the whole cluster,
        // and the commit here is already bounded by CheckpointCommitTimeout — so waiting at the
        // admission gate would eat that budget and turn a busy node into a persist failure.
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            priority: TransactionPriority.High
        ).ConfigureAwait(false);

        try
        {
            // View ops write the view meta key family, not a table blob. They are checked first
            // because GetEntryTableName below has no answer for them — a view is not a table, and
            // asking it for one would throw on every view DDL.
            if (entry.Op is SchemaOp.CreateView or SchemaOp.ReplaceView or SchemaOp.RenameView or SchemaOp.SetViewDefinition)
            {
                string viewName = GetEntryViewName(entry);
                if (database.Schema.Views.TryGetValue(viewName, out ViewSchema? viewSchema))
                    await SchemaMetaStore.PersistSchemaViewAsync(database, viewSchema, tx).ConfigureAwait(false);

                await PersistRenameDependentViewsAsync(database, entry, tx).ConfigureAwait(false);
            }
            else if (entry.Op == SchemaOp.DropView)
            {
                // Null means the view was already gone when this entry was applied — an idempotent
                // re-delivery. There is no key left to delete, so only the version advances.
                if (droppedViewId is not null)
                    await SchemaMetaStore.DeleteSchemaViewAsync(database, droppedViewId, tx).ConfigureAwait(false);
            }
            else if (entry.Op == SchemaOp.SetMaterializedViewState)
            {
                // Keyed by table id, so resolve the relation the same way apply did rather than by
                // name — a concurrent rename must not send this checkpoint to the wrong relation.
                SchemaSetMatViewStatePayload matViewPayload = SchemaDeltaApplier.DecodePayload<SchemaSetMatViewStatePayload>(entry);

                // Keyed by the materialized view's own id in both cases: a swap keeps that id and
                // changes only which key-space it reads, so the checkpoint always writes the same key.
                TableSchema? persisted = SchemaDeltaApplier.FindRelationById(database.Schema, matViewPayload.TableId);

                if (persisted is not null && !string.IsNullOrEmpty(matViewPayload.SwapToTableId))
                    await ContentsRetirementStore.RetireReplacedMaterializedViewStorageAsync(
                        database, persisted, matViewPayload.SwapToTableId, entry, tx).ConfigureAwait(false);

                if (persisted is not null)
                    await SchemaMetaStore.PersistSchemaTableAsync(database, persisted, entry.ToVersion, tx).ConfigureAwait(false);
            }
            else if (entry.Op == SchemaOp.TruncateTable)
            {
                // Keyed by id, like the materialized-view arm: a truncate keeps the relation's name,
                // but a concurrent rename must not be able to send this checkpoint elsewhere.
                SchemaTruncateTablePayload truncatePayload = SchemaDeltaApplier.DecodePayload<SchemaTruncateTablePayload>(entry);
                TableSchema? truncated = SchemaDeltaApplier.FindRelationById(database.Schema, truncatePayload.TableId);

                if (truncated is not null)
                    await SchemaMetaStore.PersistSchemaTableAsync(database, truncated, entry.ToVersion, tx).ConfigureAwait(false);
            }
            else if (entry.Op == SchemaOp.DropTable)
            {
                if (droppedTableId is not null)
                {
                    SchemaDropTablePayload dropPayload = SchemaDeltaApplier.DecodePayload<SchemaDropTablePayload>(entry);
                    // A deferred drop co-commits the orphan record with the meta-key delete in THIS
                    // checkpoint transaction, so the detach can never be durable without the recovery
                    // record. FormerName + DroppedAt come from the replicated entry so they are identical
                    // regardless of which node persists.
                    await SchemaMetaStore.PersistDroppedTableAsync(
                        database, droppedTableId, entry.ToVersion, tx,
                        deferred: dropPayload.Deferred,
                        formerName: dropPayload.TableName,
                        droppedAt: entry.Ts).ConfigureAwait(false);
                }
            }
            else
            {
                string tableName = GetEntryTableName(entry);
                if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
                    await SchemaMetaStore.PersistSchemaTableAsync(database, tableSchema, entry.ToVersion, tx).ConfigureAwait(false);

                // A rename carries its dependents' rewritten bodies, so they are checkpointed in the
                // very transaction that persists the rename. Splitting them would put back on disk the
                // same half-applied state the single delta removes from memory.
                await PersistRenameDependentViewsAsync(database, entry, tx).ConfigureAwait(false);
            }

            // In the same transaction as the live table meta above, so the relation can never be
            // durable on its new key-space without the old one being recoverable.
            IReadOnlyList<ContentsRetirementIntent> retirements =
                await ContentsRetirementStore.PersistContentsRetirementsAsync(database, tx).ConfigureAwait(false);

            using CancellationTokenSource cts = new(CheckpointCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);

            database.CompleteContentsRetirements(retirements);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Persists the view bodies a rename rewrote, in the caller's checkpoint transaction. A no-op for
    /// any other operation, and for a rename nothing depended on.
    /// </summary>
    internal async Task PersistRenameDependentViewsAsync(
        DatabaseDescriptor database, SchemaChangeLogEntry entry, KvTransaction tx)
    {
        if (entry.Op is not (SchemaOp.RenameTable or SchemaOp.RenameView))
            return;

        SchemaRenamePayload payload = SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry);

        if (payload.DependentViewDefinitions is not { Count: > 0 } rewrites)
            return;

        foreach (string viewName in rewrites.Keys)
        {
            if (database.Schema.Views.TryGetValue(viewName, out ViewSchema? dependent))
                await SchemaMetaStore.PersistSchemaViewAsync(database, dependent, tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The name the view lives under <b>after</b> the entry has been applied — which for a rename is
    /// the new name, since the checkpoint has to find the view in the map to persist it.
    /// </summary>
    private static string GetEntryViewName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.CreateView or SchemaOp.ReplaceView => SchemaDeltaApplier.DecodePayload<SchemaViewPayload>(entry).ViewName,
        SchemaOp.RenameView => SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry).NewName,
        SchemaOp.SetViewDefinition => SchemaDeltaApplier.DecodePayload<SchemaSetViewDefinitionPayload>(entry).ViewName,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Cannot resolve view name for schema operation '{entry.Op}'"
        )
    };

    private static string GetEntryTableName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.CreateTable => SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry).TableName,
        SchemaOp.RelinkTable => SchemaDeltaApplier.DecodePayload<SchemaRelinkTablePayload>(entry).TableName,
        SchemaOp.AddColumn or SchemaOp.DropColumn => SchemaDeltaApplier.DecodePayload<SchemaAlterColumnPayload>(entry).TableName,
        SchemaOp.SetElementState => SchemaDeltaApplier.DecodePayload<SchemaElementStatePayload>(entry).TableName,
        SchemaOp.DropTable => SchemaDeltaApplier.DecodePayload<SchemaDropTablePayload>(entry).TableName,
        SchemaOp.AddIndex or SchemaOp.DropIndex => SchemaDeltaApplier.DecodePayload<SchemaIndexPayload>(entry).TableName,
        // For RenameColumn/RenameIndex the table name is unchanged; for RenameTable the table
        // lives under the new name after apply, so use NewName so the persist path finds it.
        SchemaOp.RenameTable => SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry).NewName,
        SchemaOp.RenameColumn or SchemaOp.RenameIndex => SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry).TableName,
        SchemaOp.AddCheckConstraint or SchemaOp.DropCheckConstraint => SchemaDeltaApplier.DecodePayload<SchemaCheckConstraintPayload>(entry).TableName,
        SchemaOp.SetTableSettings => SchemaDeltaApplier.DecodePayload<SchemaSetTableSettingsPayload>(entry).TableName,
        SchemaOp.SetComment => SchemaDeltaApplier.DecodePayload<SchemaSetCommentPayload>(entry).TableName,
        SchemaOp.SetColumnNotNull => SchemaDeltaApplier.DecodePayload<SchemaSetColumnNotNullPayload>(entry).TableName,
        SchemaOp.TruncateTable => SchemaDeltaApplier.DecodePayload<SchemaTruncateTablePayload>(entry).TableName,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Cannot resolve table name for schema operation '{entry.Op}'"
        )
    };

    /// <summary>
    /// Re-persists the complete in-memory schema (all live tables + current version) in a
    /// single KV transaction. Called after log replay completes (<c>OnRestoreFinished</c>)
    /// to bring the on-disk checkpoint up to the committed head. Respects
    /// <see cref="TestPersistCheckpointException"/> so checkpoint fault-injection tests are not
    /// accidentally fired here.
    /// </summary>
    internal async Task PersistFullSchemaCheckpointAsync(DatabaseDescriptor database)
    {
        if (TestPersistCheckpointException is { } fault)
            throw fault;

        // Must not be called while Schema lock is held (deadlock risk — see class doc).
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistFullSchemaCheckpointAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        IKahuna kahuna = database.Kahuna.Kahuna;
        long schemaVersion = database.Schema.SchemaVersion;

        // Before the transaction opens, never inside it — see the method's remarks.
        await SchemaHistoryStore.PreloadContentsRetirementHistoriesAsync(database).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);
            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(database.Id), versionBytes).ConfigureAwait(false);

            // Snapshot the table set: callers may invoke this without holding Schema.Semaphore
            // (e.g. OnSchemaRestoreFinishedAsync, which must not hold the apply lock across these KV
            // writes — see the deadlock note there), so a concurrent live apply could otherwise
            // mutate Tables mid-iteration. A best-effort checkpoint over a point-in-time snapshot is
            // fine; the committed schema log remains the source of truth.
            foreach (TableSchema table in database.Schema.Tables.Values.ToArray())
            {
                if (string.IsNullOrWhiteSpace(table.Id))
                    continue;

                byte[] tableBytes = MetaJsonSerializer.Serialize(SchemaMetaStore.WithoutHistory(table), MetaJsonContext.Default.TableSchema);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.TableKey(database.Id, table.Id), tableBytes).ConfigureAwait(false);

                if (table.SchemaHistory is not null)
                {
                    TableSchemaHistory? current = table.SchemaHistory.FirstOrDefault(x => x.Version == table.Version);
                    if (current is not null)
                    {
                        byte[] historyBytes = MetaJsonSerializer.Serialize(current, MetaJsonContext.Default.TableSchemaHistory);
                        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.HistoryKey(database.Id, table.Id, current.Version), historyBytes).ConfigureAwait(false);
                    }
                }
            }

            // Reconciled here, in the transaction that also rewrites the table blobs, and before any
            // of those blobs can overwrite a pre-swap table meta. A replay that recreated the live
            // generation without recreating the retired one's record would leave that key-space
            // unreachable: nothing in the schema names it any more.
            IReadOnlyList<ContentsRetirementIntent> retirements =
                await ContentsRetirementStore.PersistContentsRetirementsAsync(database, tx).ConfigureAwait(false);

            using CancellationTokenSource cts = new(CheckpointCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);

            database.CompleteContentsRetirements(retirements);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }
}
