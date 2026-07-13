/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Bridges Kahuna/Kommander's replication callbacks to the catalog. <see cref="Register"/>
/// subscribes a database to its schema-log partition; from then on every committed
/// <see cref="SchemaChangeLogEntry"/> arrives at <see cref="ApplyAsync"/> (live replication)
/// or <see cref="RestoreAsync"/> (log recovery on open).
///
/// <see cref="ApplyAsync"/> enforces ordering/idempotency (skip already-applied versions,
/// throw on gaps) and mutates the in-memory schema on every node. It does <b>not</b> persist
/// the durable KV checkpoint: doing so from inside this commit-pipeline callback re-enters the
/// same Raft partition and deadlocks. The proposer persists the checkpoint after the
/// replication round-trip returns (<c>CatalogsManager.ReplicateAndWaitLocalApplyAsync</c>);
/// the committed schema log is the source of truth and the checkpoint is a load-time
/// optimization. Acks (per node, per database) are recorded only after a delta is actually
/// applied — they drive the two-version invariant gate in <see cref="SchemaAckTracker"/>.
///
/// Restart-replay durability: <see cref="OnSchemaRestoreFinishedAsync"/> re-persists the
/// KV checkpoint after WAL replay so a node whose checkpoint was behind (e.g. after
/// checkpoint-persist exhaustion during a prior run) recovers without manual intervention. The callback reaches <see cref="OnSchemaRestoreFinishedAsync"/>
/// via <see cref="EmbeddedKahuna.RegisterSchemaApply"/>'s late-subscriber buffer: Raft WAL restore
/// fires <c>OnLogRestored</c>/<c>OnRestoreFinished</c> during <c>StartAsync</c> before any
/// <c>OpenDatabase</c> subscriber exists; <see cref="EmbeddedKahuna"/> buffers those entries and
/// replays them to the first <c>RegisterSchemaApply</c> caller for that partition, so
/// <see cref="OnSchemaRestoreFinishedAsync"/> always fires after the schema is at the WAL head.
/// See the architecture documentation
/// </summary>
public sealed class SchemaReplicator
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public SchemaReplicator(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
    }

    public void Register(DatabaseDescriptor database, SchemaChangeCoordinator? coordinator = null)
    {
        ArgumentNullException.ThrowIfNull(database);

        database.Kahuna.RecordAndPublishSchemaApplied(database.Id, database.Schema.SchemaVersion);

        IDisposable applySubscription = database.Kahuna.RegisterSchemaApply(
            (partitionId, bytes) => ApplyAsync(database, partitionId, bytes),
            (_, bytes) => RestoreAsync(database, bytes),
            db: database.Id,
            onRestoreFinished: () => OnSchemaRestoreFinishedAsync(database)
        );

        IDisposable? leaderSubscription = coordinator is not null
            ? database.Kahuna.RegisterSchemaLeaderCallback(
                database.Id,
                async () =>
                {
                    try
                    {
                        await coordinator.ResumeJobsAsync(database).ConfigureAwait(false);
                    }
                    finally
                    {
                        // If persist exhaustion occurred during resume, fire the deferred
                        // step-down so a healthy peer can take over via the next leader election.
                        try
                        {
                            await database.FireDeferredSchemaStepDownAsync().ConfigureAwait(false);
                        }
                        catch (Exception stepDownEx)
                        {
                            logger.LogError(
                                stepDownEx,
                                "Schema partition step-down after persist exhaustion during resume failed for database {DbName}",
                                database.Name
                            );
                        }
                    }
                })
            : null;

        IDisposable composite = leaderSubscription is not null
            ? new CompositeDisposable(applySubscription, leaderSubscription)
            : applySubscription;

        database.SetSchemaReplicationSubscription(composite);
    }

    public async Task<bool> ApplyAsync(DatabaseDescriptor database, int partitionId, byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(bytes);

        SchemaChangeLogEntry entry = DecodeEntry(bytes);

        if (!string.Equals(entry.Database, database.Id, StringComparison.Ordinal))
            return true;

        Diagnostics.SchemaDiag.Log(
            $"APPLY node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} part={partitionId} " +
            $"entry={entry.FromVersion}->{entry.ToVersion} op={entry.Op} localVer={database.Schema.SchemaVersion}");

        // Fence: record the committed head before acquiring the lock so the fence gap
        // (HeadSchemaVersion − SchemaVersion) is visible to concurrent DML during apply.
        database.ObserveSchemaEntryHead(entry.ToVersion);

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (entry.FromVersion != database.Schema.SchemaVersion)
            {
                if (entry.ToVersion <= database.Schema.SchemaVersion && WasSchemaDeltaApplied(database.Schema, entry))
                {
                    Diagnostics.SchemaDiag.Log(
                        $"APPLY-DUP node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} " +
                        $"entry={entry.FromVersion}->{entry.ToVersion} localVer={database.Schema.SchemaVersion} (already applied; re-ack)");
                    database.Kahuna.RecordAndPublishSchemaApplied(database.Id, entry.ToVersion);
                    return true;
                }

                Diagnostics.SchemaDiag.Log(
                    $"APPLY-OOO node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} " +
                    $"entry={entry.FromVersion}->{entry.ToVersion} localVer={database.Schema.SchemaVersion} (OUT OF ORDER; throwing)");
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Schema change for database '{database.Name}' is out of order: expected from-version {database.Schema.SchemaVersion}, got {entry.FromVersion}"
                );
            }

            if (entry.ToVersion <= database.Schema.SchemaVersion)
            {
                database.Kahuna.RecordAndPublishSchemaApplied(database.Id, entry.ToVersion);
                return true;
            }

            // Apply the committed delta to the in-memory schema on every node (leader and
            // follower alike). Durable checkpoint persistence is intentionally NOT performed
            // here: this callback runs inside the schema partition's commit pipeline, and
            // issuing the checkpoint's KV writes (which themselves replicate on the same
            // partition) from this context deadlocks the partition (ProposalTimeout). The
            // proposer persists the checkpoint after the replication round-trip returns —
            // see CatalogsManager.ReplicateAndWaitLocalApplyAsync. The committed schema log
            // remains the source of truth; the KV checkpoint is a load-time optimization.
            TableSchema? appliedTableSchema = CatalogsManager.ApplySchemaDelta(database.Schema, entry);
            InvalidateAppliedTableDescriptor(database, entry, appliedTableSchema);

            Log.LogSchemaChangeApplied(logger, entry.Op, database.Name, entry.FromVersion, entry.ToVersion);

            Diagnostics.SchemaDiag.Log(
                $"APPLIED node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} " +
                $"entry={entry.FromVersion}->{entry.ToVersion} newLocalVer={database.Schema.SchemaVersion}");

            database.Kahuna.RecordAndPublishSchemaApplied(database.Id, entry.ToVersion);

            return true;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    private static void InvalidateAppliedTableDescriptor(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        TableSchema? tableSchema
    )
    {
        if (entry.Op == SchemaOp.DropTable && tableSchema?.Name is not null)
        {
            database.TableDescriptors.TryRemove(tableSchema.Name, out _);
            return;
        }

        // For index changes the table stays alive but its cached descriptor no longer
        // reflects the updated index list — evict it so the next open rebuilds from
        // the updated table.Schema.Indexes.
        if ((entry.Op == SchemaOp.AddIndex || entry.Op == SchemaOp.DropIndex) && tableSchema?.Name is not null)
            database.TableDescriptors.TryRemove(tableSchema.Name, out _);

        // For SetElementState targeting an index, evict the descriptor so that
        // TableDescriptor.Indexes is rebuilt with the updated index state on next access.
        // Column SetElementState does not need this because DML reads column state directly
        // from table.Schema.Columns (updated in place by ApplyElementState).
        if (entry.Op == SchemaOp.SetElementState)
        {
            SchemaElementStatePayload payload = DecodePayload<SchemaElementStatePayload>(entry);
            if (payload.ElementKind == SchemaElementKind.Index)
                database.TableDescriptors.TryRemove(payload.TableName, out _);
        }

        // Renames: evict so the next open rebuilds the descriptor with the new name(s).
        // For table rename, evict both old and new in case a stale entry exists under either.
        // For column/index rename the table name is unchanged; evict by table name.
        if (entry.Op == SchemaOp.RenameTable || entry.Op == SchemaOp.RenameColumn || entry.Op == SchemaOp.RenameIndex)
        {
            SchemaRenamePayload payload = DecodePayload<SchemaRenamePayload>(entry);
            database.TableDescriptors.TryRemove(payload.TableName, out _);
            if (entry.Op == SchemaOp.RenameTable)
                database.TableDescriptors.TryRemove(payload.NewName, out _);
        }
    }

    /// <summary>
    /// Fired after WAL restore entries have been replayed into this subscriber's schema
    /// (delivered either in real-time during startup if the subscription was registered before
    /// <c>OnRestoreFinished</c> fired, or via the late-subscriber buffer in
    /// <see cref="EmbeddedKahuna.RegisterSchemaApply"/> otherwise). Re-persists the full
    /// in-memory schema to bring the KV checkpoint up to the committed WAL head, then clears
    /// <see cref="DatabaseDescriptor.SchemaSubsystemDegraded"/> so nodes that degraded due to
    /// checkpoint-persist exhaustion recover on the next open without manual intervention.
    /// </summary>
    private async Task OnSchemaRestoreFinishedAsync(DatabaseDescriptor database)
    {
        long restoredVersion = database.Schema.SchemaVersion;

        // Nothing was replayed → nothing to checkpoint. This re-persist exists to bring the
        // durable KV checkpoint up to the head a WAL *replay* reached. On a fresh node / empty WAL,
        // OnRestoreFinished still fires but the schema is untouched at version 0 (replaying schema
        // deltas only ever advances the version from 0, so version 0 ⟺ no replay). Running the
        // checkpoint's 2PC in that case is pure cost AND races the node's very first live DDL on the
        // schema partition — the source of the CreateTable/INSERT 2PC conflicts and the deadlock
        // below. A node that genuinely created tables persists its checkpoint via the proposer path
        // (CatalogsManager.ReplicateAndWaitLocalApplyAsync), never here. So skip when there is no
        // replayed schema; still clear the degraded flag (a no-op when it was never set).
        if (restoredVersion == 0)
        {
            database.ClearSchemaSubsystemDegraded();
            Diagnostics.SchemaDiag.Log($"RESTORE-FIN-SKIP node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} (nothing replayed; schema at version 0)");
            return;
        }

        // CRITICAL: do NOT hold Schema.Semaphore across PersistFullSchemaCheckpointAsync.
        //
        // That call opens a full 2PC KV transaction writing the {db}/meta/* checkpoint keys, which
        // route to the schema partition. The schema partition applies its committed log entries
        // SERIALLY and inline (see EmbeddedKahuna.RegisterSchemaApply), and a live schema delta's
        // ApplyAsync yields on this very semaphore — so holding it here lets the checkpoint 2PC's
        // entries queue behind a delta's ApplyAsync that is itself blocked on the semaphore we hold:
        // the partition's apply pipeline deadlocks, the 2PC never commits, the semaphore is never
        // released, and the leader's schema-ack gate times out (confirmed via SCHEMA-DIAG: a node
        // logs RESTORE-FIN-LOCKED then never PERSISTED/DONE while its APPLY for the next version
        // never reaches APPLIED). ReplicateAndWaitLocalApplyAsync documents the same hazard and
        // deliberately persists the checkpoint OUTSIDE the schema commit pipeline.
        //
        // The checkpoint is a best-effort durability optimization (the committed schema log is the
        // source of truth and is replayed on restart), so persisting a slightly racy snapshot
        // without the apply lock is safe; PersistFullSchemaCheckpointAsync snapshots the table set
        // so a concurrent apply cannot corrupt the iteration.
        Diagnostics.SchemaDiag.Log($"RESTORE-FIN-ENTER node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} localVer={restoredVersion}");
        try
        {
            await catalogs.PersistFullSchemaCheckpointAsync(database).ConfigureAwait(false);
            Diagnostics.SchemaDiag.Log($"RESTORE-FIN-PERSISTED node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name}");

            database.ClearSchemaSubsystemDegraded();

            Log.LogSchemaCheckpointRepersisted(logger, database.Schema.SchemaVersion, database.Name);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Failed to re-persist schema checkpoint after restore for database {DbName}; node will retry on next restart",
                database.Name
            );
        }
        finally
        {
            Diagnostics.SchemaDiag.Log($"RESTORE-FIN-DONE node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name}");
        }
    }

    public async Task<bool> RestoreAsync(DatabaseDescriptor database, byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(bytes);

        SchemaChangeLogEntry entry = DecodeEntry(bytes);

        if (!string.Equals(entry.Database, database.Id, StringComparison.Ordinal))
            return true;

        Diagnostics.SchemaDiag.Log($"RESTORE node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} entry={entry.FromVersion}->{entry.ToVersion} localVer={database.Schema.SchemaVersion}");
        database.ObserveSchemaEntryHead(entry.ToVersion);
        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (entry.ToVersion <= database.Schema.SchemaVersion)
            {
                database.Kahuna.RecordAndPublishSchemaApplied(database.Id, entry.ToVersion);
                return true;
            }

            if (entry.FromVersion != database.Schema.SchemaVersion)
            {
                // OnLogRestored delivers the full committed tail deterministically and in order.
                // A FromVersion mismatch here is a genuine gap — data corruption or a bug — not
                // a normal condition. Throw so the caller (Kommander) fires a replication-error
                // event and the node is left in a visibly inconsistent state rather than silently
                // behind. This path does NOT silently skip: the committed log is the source of truth
                // and a missing delta must surface loudly.
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Schema restore for database '{database.Name}' has a gap: expected from-version {database.Schema.SchemaVersion}, got {entry.FromVersion} (target {entry.ToVersion}). OnLogRestored must deliver entries in order; this is a bug."
                );
            }

            CatalogsManager.ApplySchemaDelta(database.Schema, entry);

            Log.LogSchemaChangeRestored(logger, entry.Op, database.Name, entry.FromVersion, entry.ToVersion);

            database.Kahuna.RecordAndPublishSchemaApplied(database.Id, entry.ToVersion);

            return true;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    private static SchemaChangeLogEntry DecodeEntry(byte[] bytes)
    {
        SchemaChangeLogEntry? entry = Serializator.Unserialize<SchemaChangeLogEntry>(bytes);
        if (entry is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid schema replication entry");

        return entry;
    }

    private static bool WasSchemaDeltaApplied(Schema schema, SchemaChangeLogEntry entry)
    {
        return entry.Op switch
        {
            SchemaOp.CreateTable => schema.Tables.ContainsKey(DecodePayload<SchemaCreateTablePayload>(entry).TableName),
            SchemaOp.DropTable => !schema.Tables.ContainsKey(DecodePayload<SchemaDropTablePayload>(entry).TableName),
            SchemaOp.AddColumn => HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            SchemaOp.DropColumn => !HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            SchemaOp.SetElementState => HasElementState(schema, DecodePayload<SchemaElementStatePayload>(entry)),
            SchemaOp.AddIndex => HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.DropIndex => !HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.RenameTable or SchemaOp.RenameColumn or SchemaOp.RenameIndex => WasRenamed(schema, DecodePayload<SchemaRenamePayload>(entry)),
            _ => schema.SchemaVersion >= entry.ToVersion
        };
    }

    private static bool WasRenamed(Schema schema, SchemaRenamePayload payload)
    {
        return payload.Kind switch
        {
            SchemaRenameKind.Table =>
                schema.Tables.ContainsKey(payload.NewName) && !schema.Tables.ContainsKey(payload.TableName),
            SchemaRenameKind.Column =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? ct) &&
                ct.Columns is not null &&
                ct.Columns.Any(c => c.Name == payload.NewName),
            SchemaRenameKind.Index =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? it) &&
                it.Indexes is not null &&
                it.Indexes.Any(ix => ix.Name == payload.NewName),
            _ => false
        };
    }

    private static bool HasIndex(Schema schema, SchemaIndexPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Indexes is not null &&
               table.Indexes.Any(ix => ix.Name == payload.IndexName);
    }

    private static bool HasColumn(Schema schema, SchemaAlterColumnPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Columns is not null &&
               table.Columns.Any(column => column.Name == payload.Column.Name);
    }

    private static bool HasElementState(Schema schema, SchemaElementStatePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
            return payload.State == SchemaElementState.Absent;

        if (payload.ElementKind == SchemaElementKind.Index)
        {
            TableIndexSchema? index = table.Indexes?.FirstOrDefault(ix => ix.Name == payload.ElementName);
            return payload.State == SchemaElementState.Absent
                ? index is null
                : index?.State == payload.State;
        }

        if (table.Columns is null)
            return payload.State == SchemaElementState.Absent;

        TableColumnSchema? column = table.Columns.FirstOrDefault(column => column.Name == payload.ElementName);
        return payload.State == SchemaElementState.Absent
            ? column is null
            : column?.State == payload.State;
    }

    private static T DecodePayload<T>(SchemaChangeLogEntry entry) where T : new()
    {
        T payload = Serializator.Unserialize<T>(entry.Payload);

        if (payload is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid payload for schema operation '{entry.Op}'");

        return payload;
    }

    internal static Schema CloneSchema(Schema schema)
    {
        Schema clone = new()
        {
            SchemaVersion = schema.SchemaVersion,
            Tables = new Dictionary<string, TableSchema>(schema.Tables.Count, schema.Tables.Comparer)
        };

        foreach ((string tableName, TableSchema table) in schema.Tables)
            clone.Tables[tableName] = CloneTable(table);

        return clone;
    }

    private static TableSchema CloneTable(TableSchema table)
    {
        return new()
        {
            Id = table.Id,
            Version = table.Version,
            Name = table.Name,
            Columns = table.Columns is null ? null : [.. table.Columns],
            // TableIndexSchema is immutable so a shallow list copy is sufficient.
            Indexes = table.Indexes is null ? null : [.. table.Indexes],
            // Shallow list copy: the CheckConstraintSchema instances (and their cached parsed AST)
            // are shared, matching the Indexes treatment. Omitting this dropped all checks from the
            // clone, silently losing them for any consumer that promotes the clone to a live schema.
            CheckConstraints = table.CheckConstraints is null ? null : [.. table.CheckConstraints],
            SchemaHistory = table.SchemaHistory is null
                ? null
                : table.SchemaHistory.Select(CloneHistory).ToList(),
            SchemaHistoryLoader = table.SchemaHistoryLoader
        };
    }

    private static TableSchemaHistory CloneHistory(TableSchemaHistory history)
    {
        return new()
        {
            Version = history.Version,
            Columns = history.Columns is null ? null : [.. history.Columns]
        };
    }
}

/// <summary>
/// Disposes two <see cref="IDisposable"/> instances together. Used by
/// <see cref="SchemaReplicator.Register"/> to bundle the schema-apply subscription
/// with the optional leader-callback subscription into a single token.
/// </summary>
file sealed class CompositeDisposable : IDisposable
{
    private readonly IDisposable first;
    private readonly IDisposable second;

    public CompositeDisposable(IDisposable first, IDisposable second)
    {
        this.first = first;
        this.second = second;
    }

    public void Dispose()
    {
        first.Dispose();
        second.Dispose();
    }
}
