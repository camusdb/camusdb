
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Grpc.Core;
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
/// <c>!Schema.IsHeldByCurrentFlow</c> for the same reason.</para>
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

    /// <summary>Attempts of a checkpoint whatever its failure is.</summary>
    private const int MinPersistAttempts = 3;

    /// <summary>
    /// How long a checkpoint starts further attempts after a transient failure. A checkpoint writes through
    /// the partition leaders, and one of them can be a node that died a moment ago: the write fails at once
    /// until the others elect a leader. Attempts that end before the election would mark a healthy node
    /// degraded, and it refuses schema changes until it restarts.
    /// </summary>
    private const int TransientPersistWindowMs = 10_000;

    private const int MaxPersistRetryDelayMs = 500;

    /// <summary>
    /// Writes the KV checkpoint of a committed entry, again after a failure. Never throws: a checkpoint that
    /// cannot be written marks this node degraded and asks it to give up the schema leadership.
    /// </summary>
    internal async Task PersistSchemaCheckpointWithRetryAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId,
        string? droppedViewId,
        string? droppedSequenceId,
        IReadOnlyList<ChangeSetCheckpointDelta>? changeSetDropped = null
    )
    {
        long started = Environment.TickCount64;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await PersistSchemaCheckpointAsync(database, entry, droppedTableId, droppedViewId, droppedSequenceId, changeSetDropped).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < MinPersistAttempts
                || IsTransientPersistFailure(ex) && Environment.TickCount64 - started < TransientPersistWindowMs)
            {
                logger.LogWarning(
                    ex,
                    "Schema checkpoint persist attempt {Attempt} failed for database {DbName} version {Version}; retrying",
                    attempt,
                    database.Name,
                    entry.ToVersion
                );

                await Task.Delay(Math.Min(50 * attempt, MaxPersistRetryDelayMs)).ConfigureAwait(false);
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
                Log.LogSchemaCheckpointExhausted(logger, ex, attempt, database.Name, entry.ToVersion);

                database.MarkSchemaSubsystemDegraded();
                database.RequestDeferredSchemaStepDown();

                return; // swallow: committed log is the source of truth; degraded flag gates future DDL
            }
        }
    }

    /// <summary>
    /// True for a failure that a leader election or a short wait ends: a partition leader that cannot be
    /// reached, or a commit that was refused and may be tried again.
    /// </summary>
    private static bool IsTransientPersistFailure(Exception ex) => ex switch
    {
        CamusDBException refused => refused.Code is CamusDBErrorCodes.TransactionMustRetry or CamusDBErrorCodes.TransactionConflict,
        KahunaServerException failure => failure.ResponseType != KeyValueResponseType.StorageCorrupt,
        RpcException unreachable => KahunaRetryPolicy.IsTransientTransportFailure(unreachable, CancellationToken.None),
        _ => false,
    };

    internal async Task PersistSchemaCheckpointAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId,
        string? droppedViewId,
        string? droppedSequenceId,
        IReadOnlyList<ChangeSetCheckpointDelta>? changeSetDropped = null
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
            if (Apply.ChangeSetDeltaApplier.IsChangeSetOp(entry.Op))
                await StageChangeSetCheckpointAsync(database, entry, changeSetDropped, tx).ConfigureAwait(false);
            else
                await StageEntryCheckpointAsync(database, entry, droppedTableId, droppedViewId, droppedSequenceId, tx).ConfigureAwait(false);

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
    /// Writes into <paramref name="tx"/> what one applied delta changed: the meta key of the object it
    /// created, altered or renamed, or the deletion of the object it dropped, plus the version key. Reads
    /// the object from the schema after the apply, by the name it has then.
    /// </summary>
    private async Task StageEntryCheckpointAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId,
        string? droppedViewId,
        string? droppedSequenceId,
        KvTransaction tx,
        TableSchema? droppedTable = null,
        HashSet<string>? persistedTables = null)
    {
        {
            // View ops write the view meta key family, not a table blob. They are checked first
            // because GetEntryTableName below has no answer for them — a view is not a table, and
            // asking it for one would throw on every view DDL.
            // Sequence ops write the sequence meta key family, not a table blob, and are checked
            // first for the same reason the view arm is: GetEntryTableName has no answer for them.
            // Extension ops write the extension meta key family, not a table blob, and are checked
            // first for the same reason the sequence arm is.
            if (entry.Op is SchemaOp.CreateExtension or SchemaOp.UpdateExtension or SchemaOp.DropExtension)
            {
                await SchemaMetaStore.PersistSchemaExtensionAsync(database, ExtensionDeltaApplier.GetExtensionName(entry), tx).ConfigureAwait(false);
            }
            // Function ops write the function meta key family, and are checked first for the same reason.
            else if (InlineFunctionDeltaApplier.IsFunctionOp(entry.Op))
            {
                await SchemaMetaStore.PersistSchemaFunctionAsync(database, InlineFunctionDeltaApplier.GetFunctionName(entry), tx).ConfigureAwait(false);
            }
            else if (entry.Op is SchemaOp.CreateSequence or SchemaOp.RenameSequence or SchemaOp.AlterSequence)
            {
                SequenceSchema? sequenceSchema = ResolveEntrySequence(database, entry);

                // Null means the sequence was already gone when this entry applied — an idempotent
                // re-delivery, or an alter that lost to a drop. Only the version advances.
                if (sequenceSchema is not null)
                    await SchemaMetaStore.PersistSchemaSequenceAsync(database, sequenceSchema, tx).ConfigureAwait(false);
            }
            else if (entry.Op == SchemaOp.DropSequence)
            {
                // Null means the sequence was already gone when this entry was applied. There is no
                // key left to delete, so only the version advances.
                if (droppedSequenceId is not null)
                    await SchemaMetaStore.DeleteSchemaSequenceAsync(database, droppedSequenceId, tx).ConfigureAwait(false);
            }
            else if (entry.Op is SchemaOp.CreateView or SchemaOp.ReplaceView or SchemaOp.RenameView or SchemaOp.SetViewDefinition)
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
                        droppedAt: entry.Ts,
                        capturedSchema: droppedTable).ConfigureAwait(false);
                }
            }
            else
            {
                string tableName = GetEntryTableName(entry);

                // The publish of a change set stages every delta, and several deltas can name one table;
                // its final state is written once.
                if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema)
                    && (persistedTables is null || tableSchema.Id is null || persistedTables.Add(tableSchema.Id)))
                    await SchemaMetaStore.PersistSchemaTableAsync(database, tableSchema, entry.ToVersion, tx).ConfigureAwait(false);

                // A materialized view created WITH DATA adopted its staging relation in this entry. The
                // staging relation is gone from the schema; its meta key goes in the same transaction, or a
                // reopen would load a relation nothing can reach that owns the view's key-space.
                if (entry.Op is SchemaOp.CreateTable or SchemaOp.CreatePopulatedMaterializedView
                    && SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry).AdoptStagingTableId is { Length: > 0 } adopted)
                    await MetaKeyWriter.DeleteMetaKey(database.Kahuna.Kahuna, tx, MetaKeys.TableKey(database.Id, adopted)).ConfigureAwait(false);

                // A rename carries its dependents' rewritten bodies, so they are checkpointed in the
                // very transaction that persists the rename. Splitting them would put back on disk the
                // same half-applied state the single delta removes from memory.
                await PersistRenameDependentViewsAsync(database, entry, tx).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Writes into <paramref name="tx"/> what a change-set entry changed.
    /// <list type="bullet">
    /// <item>A pending entry writes its pending record, so a reopen restores the change set even when the
    /// schema log no longer holds the entry, and the version key.</item>
    /// <item>A publish stages every delta of its change set as <see cref="StageEntryCheckpointAsync"/> stages
    /// a single entry, with the dropped ids the publisher captured before the apply, then deletes the
    /// pending record and the decision key. All of it commits in this one transaction.</item>
    /// <item>An abort deletes the pending record, and the decision key when a resolver wrote one. The
    /// decision key of a transaction that aborts itself is absent, and its lock can still be held: the
    /// rollback of the transaction may not have reached it, and only the lease frees it then. A delete would
    /// wait for that lock, use up the attempts of the checkpoint, and mark this node degraded.</item>
    /// </list>
    /// A decision for a change set that was not pending stages only the version key: the publish or the
    /// abort that ended it already wrote its checkpoint.
    /// </summary>
    private async Task StageChangeSetCheckpointAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        IReadOnlyList<ChangeSetCheckpointDelta>? changeSetDropped,
        KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        if (entry.Op == SchemaOp.PendingChangeSet)
        {
            SchemaChangeSetPayload pending = SchemaDeltaApplier.DecodePayload<SchemaChangeSetPayload>(entry);

            // Only when the entry recorded the change set: a refused pending entry left nothing pending.
            if (database.Schema.PendingChangeSet is { } recorded
                && string.Equals(recorded.ChangeSetId, pending.ChangeSetId, StringComparison.Ordinal))
                await MetaKeyWriter.WriteMetaKey(
                    kahuna, tx, MetaKeys.ChangeSetKey(database.Id, pending.ChangeSetId),
                    Replication.SchemaChangeLogEntryCodec.Encode(recorded.PendingEntry)).ConfigureAwait(false);

            await WriteVersionKeyAsync(kahuna, tx, database.Id, entry.ToVersion).ConfigureAwait(false);
            return;
        }

        SchemaChangeSetDecisionPayload decision = SchemaDeltaApplier.DecodePayload<SchemaChangeSetDecisionPayload>(entry);

        if (entry.Op == SchemaOp.PublishChangeSet && changeSetDropped is not null)
        {
            HashSet<string> persistedTables = new(StringComparer.Ordinal);

            for (int i = 0; i < changeSetDropped.Count; i++)
            {
                ChangeSetCheckpointDelta dropped = changeSetDropped[i];
                await StageEntryCheckpointAsync(
                    database, dropped.Delta, dropped.TableId, dropped.ViewId, dropped.SequenceId, tx,
                    dropped.DroppedTable, persistedTables).ConfigureAwait(false);
            }
        }

        await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.ChangeSetKey(database.Id, decision.ChangeSetId)).ConfigureAwait(false);

        string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, decision.ChangeSetId);

        // Read outside the transaction: the latest committed value, which no lock holds back.
        if (entry.Op != SchemaOp.AbortChangeSet
            || await Transactional.ChangeSetCommitter.ReadDecisionAsync(kahuna, HLCTimestamp.Zero, decisionKey, CancellationToken.None).ConfigureAwait(false) is not null)
            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, decisionKey).ConfigureAwait(false);

        await WriteVersionKeyAsync(kahuna, tx, database.Id, entry.ToVersion).ConfigureAwait(false);
    }

    private static async Task WriteVersionKeyAsync(IKahuna kahuna, KvTransaction tx, string databaseId, long version)
    {
        byte[] versionBytes = MetaJsonSerializer.Serialize(version, MetaJsonContext.Default.Int64);
        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(databaseId), versionBytes).ConfigureAwait(false);
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
    /// The sequence record this entry left in the schema, resolved the same way apply resolved it:
    /// by name for a create, by the post-rename name for a rename, and by immutable id for an
    /// alter — which is keyed by id precisely so a concurrent rename cannot redirect it.
    /// </summary>
    private static SequenceSchema? ResolveEntrySequence(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        switch (entry.Op)
        {
            case SchemaOp.CreateSequence:
                database.Schema.Sequences.TryGetValue(
                    SchemaDeltaApplier.DecodePayload<SchemaSequencePayload>(entry).SequenceName, out SequenceSchema? created);
                return created;

            case SchemaOp.RenameSequence:
                database.Schema.Sequences.TryGetValue(
                    SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry).NewName, out SequenceSchema? renamed);
                return renamed;

            case SchemaOp.AlterSequence:
                return database.Schema.FindSequenceById(
                    SchemaDeltaApplier.DecodePayload<SchemaAlterSequencePayload>(entry).SequenceId);

            default:
                return null;
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
        SchemaOp.CreateTable or SchemaOp.CreatePopulatedMaterializedView
            => SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry).TableName,
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
        SchemaOp.SetColumnStorage => SchemaDeltaApplier.DecodePayload<SchemaSetColumnStoragePayload>(entry).TableName,
        SchemaOp.TruncateTable => SchemaDeltaApplier.DecodePayload<SchemaTruncateTablePayload>(entry).TableName,
        SchemaOp.AddForeignKey => SchemaDeltaApplier.DecodePayload<SchemaAddForeignKeyPayload>(entry).TableName,
        SchemaOp.CreateTrigger => SchemaDeltaApplier.DecodePayload<SchemaCreateTriggerPayload>(entry).TableName,
        SchemaOp.DropTrigger => SchemaDeltaApplier.DecodePayload<SchemaDropTriggerPayload>(entry).TableName,
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
            !database.Schema.IsHeldByCurrentFlow,
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

            // Sequence records are rewritten alongside the tables. A full checkpoint replaces what a
            // reopen reads, so a family left out here is silently dropped from disk by the very
            // write that was meant to bring disk up to the committed head.
            foreach (SequenceSchema sequence in database.Schema.Sequences.Values.ToArray())
            {
                if (string.IsNullOrWhiteSpace(sequence.Id))
                    continue;

                byte[] sequenceBytes = MetaJsonSerializer.Serialize(sequence, MetaJsonContext.Default.SequenceSchema);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.SequenceKey(database.Id, sequence.Id), sequenceBytes).ConfigureAwait(false);
            }

            // Installed extensions too, for the same reason as sequences: a full checkpoint is what a
            // reopen reads. A record on disk that memory no longer holds is deleted: a replayed drop
            // whose proposer never checkpointed it would otherwise come back as installed on reopen,
            // and the log, which resumes after this checkpoint's version, would never remove it again.
            // Inline functions follow the same rule, for the same reason, in the same scan.
            IReadOnlyDictionary<string, ExtensionDefinition> installed = database.Schema.Extensions;
            IReadOnlyDictionary<string, InlineFunctionDefinition> functions = database.Schema.Functions;
            List<string> staleExtensionKeys = [];
            string extensionKeyPrefix = MetaKeys.ExtensionKeyPrefix(database.Id);
            string functionKeyPrefix = MetaKeys.FunctionKeyPrefix(database.Id);

            // The pending change set of a committing transaction is replicated state too: a reopen that
            // loads this checkpoint must keep refusing other changes until it is published or aborted.
            // A record of a change set that memory no longer holds pending is deleted, for the same reason
            // a stale extension record is.
            PendingSchemaChangeSet? pendingChangeSet = database.Schema.PendingChangeSet;
            string changeSetKeyPrefix = MetaKeys.ChangeSetKeyPrefix(database.Id);

            // A plain read of the latest committed keys, outside the write transaction (no transaction
            // id), so the scan adds nothing to what that transaction reads or locks.
            await foreach ((string key, ReadOnlyKeyValueEntry stored) in kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, MetaKeys.MetaBucketPrefix(database.Id), null, true, null, true, 512,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (stored.Value is not null && key.StartsWith(extensionKeyPrefix, StringComparison.Ordinal) &&
                    !installed.ContainsKey(key[extensionKeyPrefix.Length..]))
                    staleExtensionKeys.Add(key);

                if (stored.Value is not null && key.StartsWith(functionKeyPrefix, StringComparison.Ordinal) &&
                    !functions.ContainsKey(key[functionKeyPrefix.Length..]))
                    staleExtensionKeys.Add(key);

                if (stored.Value is not null && key.StartsWith(changeSetKeyPrefix, StringComparison.Ordinal) &&
                    !string.Equals(pendingChangeSet?.ChangeSetId, key[changeSetKeyPrefix.Length..], StringComparison.Ordinal))
                    staleExtensionKeys.Add(key);
            }

            if (pendingChangeSet is not null)
                await MetaKeyWriter.WriteMetaKey(
                    kahuna, tx, MetaKeys.ChangeSetKey(database.Id, pendingChangeSet.ChangeSetId),
                    Replication.SchemaChangeLogEntryCodec.Encode(pendingChangeSet.PendingEntry)).ConfigureAwait(false);

            foreach (string staleKey in staleExtensionKeys)
                await MetaKeyWriter.DeleteMetaKey(kahuna, tx, staleKey).ConfigureAwait(false);

            foreach (ExtensionDefinition extension in installed.Values)
            {
                byte[] extensionBytes = MetaJsonSerializer.Serialize(extension, MetaJsonContext.Default.ExtensionDefinition);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.ExtensionKey(database.Id, extension.Name), extensionBytes).ConfigureAwait(false);
            }

            foreach (InlineFunctionDefinition function in functions.Values)
            {
                byte[] functionBytes = MetaJsonSerializer.Serialize(function, MetaJsonContext.Default.InlineFunctionDefinition);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.FunctionKey(database.Id, function.Name), functionBytes).ConfigureAwait(false);
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

/// <summary>
/// One delta of a published change set, with the versions of the publish entry, and the ids of the
/// relation, view or sequence it drops. The publisher reads them from a copy of the schema advanced delta
/// by delta, before the publish applies, because the apply removes the objects and the checkpoint still
/// has to delete their meta keys by id. <see cref="DroppedTable"/> is the dropped relation as it was right
/// before its drop: the orphan record of a deferred drop is built from it, because the persisted table key
/// is absent for a relation the same change set created, and holds an older layout for one it altered.
/// </summary>
internal readonly record struct ChangeSetCheckpointDelta(
    SchemaChangeLogEntry Delta,
    string? TableId,
    string? ViewId,
    string? SequenceId,
    TableSchema? DroppedTable = null);
