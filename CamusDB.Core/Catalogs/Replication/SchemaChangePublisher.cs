
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Replication;

/// <summary>
/// Proposes one committed schema delta through Raft and does not return until every node that must
/// see it has applied it. This is the whole distributed-DDL round-trip, and its five steps run in
/// this order for reasons that are not interchangeable:
///
/// <list type="number">
/// <item>Wait for the previous version's acks, so two versions are never in flight at once.</item>
/// <item>Replicate the entry and require a Committed outcome.</item>
/// <item>Wait for the local apply, checked structurally rather than by version alone.</item>
/// <item>Persist the KV checkpoint, from here and never from the apply callback.</item>
/// <item>Wait for the post-commit ack gate across live nodes.</item>
/// </list>
///
/// <para><b>The caller must not hold the schema lock.</b> Replication re-enters the schema
/// partition's serial apply pipeline, which yields on that lock; calling with it held deadlocks the
/// pipeline. Every proposer builds and validates its delta under the lock, releases, and only then
/// calls in. The <c>Debug.Assert</c> at the top of
/// <see cref="ReplicateAndWaitLocalApplyAsync"/> is that contract, and a non-zero depth is a bug in
/// the caller.</para>
///
/// <para><b>The dropped table and view ids are captured before the round-trip, not after.</b> Apply
/// removes them from the in-memory schema, and the checkpoint still has to delete their meta keys by
/// id once they are gone. Reading them later finds nothing and silently leaks the keys.</para>
///
/// <para><b>A degraded schema subsystem rejects proposals before any work is done</b>, and a
/// QuorumBackstop outcome is logged with the laggard nodes by name — it is the only signal that a
/// live follower is behind and will be fenced until it catches up.</para>
/// </summary>
internal sealed class SchemaChangePublisher
{
    private readonly SchemaCheckpointWriter checkpoints;

    private readonly ILogger<ICamusDB> logger;

    public SchemaChangePublisher(SchemaCheckpointWriter checkpoints, ILogger<ICamusDB> logger)
    {
        // Captured, not read per call: a wrong construction order must fail here at startup rather
        // than as a null reference in the middle of a committed DDL.
        ArgumentNullException.ThrowIfNull(checkpoints);

        this.checkpoints = checkpoints;
        this.logger = logger;
    }

    /// <summary>
    /// The error code a schema-log proposal that did not commit is reported with. A
    /// <see cref="SchemaReplicationOutcome.NotLeader"/> outcome is a routing transient: the schema
    /// leader moved while the proposal was in flight, every forward was answered "not the leader", and
    /// nothing was committed. It is reported as <see cref="CamusDBErrorCodes.TransactionMustRetry"/>, the
    /// code of a pre-write transient, so a client that follows the retry contract runs the statement
    /// again. A timeout or a failure may have committed the entry after all, so a replay could apply the
    /// change twice; those keep the generic internal code and are not retried.
    /// </summary>
    internal static string RefusalCodeFor(SchemaReplicationOutcome outcome) =>
        outcome == SchemaReplicationOutcome.NotLeader
            ? CamusDBErrorCodes.TransactionMustRetry
            : CamusDBErrorCodes.InvalidInternalOperation;

    /// <summary>
    /// Sends an entry to the database's schema leader when this node does not lead its schema log, and
    /// waits until this node has applied it. Answers null when this node should propose the entry
    /// itself: standalone, or this node leads. Set once at startup in cluster mode; null otherwise.
    /// </summary>
    /// <remarks>
    /// Statements with a statement-level forward (table, index, constraint, rename, truncate and
    /// comment DDL) are sent before they build an entry and never get here on a follower. What gets
    /// here is a change built by code with no forward of its own: view DDL, a materialized view's
    /// relations, a refresh's publish and table settings. Proposed from a follower, those used to fail
    /// with NotLeader, reported as "the leader moved, retry", which never helped.
    /// </remarks>
    internal Func<DatabaseDescriptor, SchemaChangeLogEntry, Task<bool?>>? ForwardWhenNotLeader { get; set; }

    internal async Task ReplicateAndWaitLocalApplyAsync(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // A DDL statement of an explicit transaction with transactional DDL built this delta against the
        // transaction's private schema. It applies there and joins the transaction's change set; the
        // schema log sees it only when the transaction commits.
        if (Transactional.TransactionSchemaScope.OverlayFor(database) is { } overlay)
        {
            await overlay.ApplyStatementDeltaAsync(entry).ConfigureAwait(false);
            return;
        }

        // Replicating the schema-log delta (and the checkpoint persist below) re-enters the
        // schema partition's serial, inline apply pipeline — which yields on the schema lock. Doing
        // it while the lock is held deadlocks that pipeline (the root cause). DDL proposers must
        // build/validate + apply the delta under the lock, then RELEASE before calling this. A
        // hold by this flow here is a bug. (Another DDL holding it meanwhile is not: see
        // Schema.IsHeldByCurrentFlow.)
        System.Diagnostics.Debug.Assert(
            !database.Schema.IsHeldByCurrentFlow,
            $"ReplicateAndWaitLocalApplyAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

        // On a follower the leader proposes, persists the checkpoint and waits for the acks; this node
        // only waits to apply the result.
        if (ForwardWhenNotLeader is { } forward && await forward(database, entry).ConfigureAwait(false) is not null)
            return;

        await WaitForPreviousVersionAcksAsync(database, entry).ConfigureAwait(false);

        // For DropTable the table is removed from the in-memory schema during apply, so capture
        // its immutable id now (the checkpoint delete needs it once the table is gone).
        string? droppedTableId = entry.Op == SchemaOp.DropTable
            ? ResolveTableId(database, SchemaDeltaApplier.DecodePayload<SchemaDropTablePayload>(entry).TableName)
            : null;

        // Same reason as droppedTableId: apply removes the view from the in-memory map, and the
        // checkpoint still has to delete its meta key by id afterwards.
        string? droppedViewId = entry.Op == SchemaOp.DropView
            && database.Schema.Views.TryGetValue(SchemaDeltaApplier.DecodePayload<SchemaDropViewPayload>(entry).ViewName, out ViewSchema? viewBeingDropped)
                ? viewBeingDropped.Id
                : null;

        // Same reason as droppedTableId: apply removes the sequence from the in-memory map, and the
        // checkpoint still has to delete its meta key by id afterwards.
        string? droppedSequenceId = entry.Op == SchemaOp.DropSequence
            && database.Schema.Sequences.TryGetValue(SchemaDeltaApplier.DecodePayload<SchemaDropSequencePayload>(entry).SequenceName, out SequenceSchema? sequenceBeingDropped)
                ? sequenceBeingDropped.Id
                : null;

        // Same reason again: the publish of a change set removes the objects its deltas drop, and its
        // checkpoint deletes their meta keys by id.
        IReadOnlyList<ChangeSetCheckpointDelta>? changeSetDeltas = entry.Op == SchemaOp.PublishChangeSet
            ? await CaptureChangeSetDeltasAsync(database, entry).ConfigureAwait(false)
            : null;

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(entry);
        SchemaReplicationResult result = await database.Kahuna.ReplicateSchemaChangeAsync(database.Id, bytes, CancellationToken.None).ConfigureAwait(false);

        if (result.Outcome != SchemaReplicationOutcome.Committed)
            throw new CamusDBException(
                RefusalCodeFor(result.Outcome),
                $"Schema change '{entry.Op}' for database '{database.Name}' was not committed: {result.Outcome} {result.Status}" +
                (result.Outcome == SchemaReplicationOutcome.NotLeader
                    ? "; the schema leader moved while the change was in flight and nothing was committed, retry the statement"
                    : "")
            );

        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (database.Schema.SchemaVersion >= entry.ToVersion && SchemaDeltaApplier.WasSchemaDeltaApplied(database.Schema, entry))
                break;

            await Task.Delay(25).ConfigureAwait(false);
        }

        if (database.Schema.SchemaVersion < entry.ToVersion || !SchemaDeltaApplier.WasSchemaDeltaApplied(database.Schema, entry))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Timed out waiting for local schema apply for database '{database.Name}' version {entry.ToVersion}"
            );

        // Committed, but applied as a refusal: a delta committed ahead of this one moved the schema
        // this one was validated against. Every node refused it, so nothing changed anywhere and there
        // is no checkpoint to persist; the caller is told why. See SchemaDeltaRefusedException.
        if (database.Schema.FindRefusal(entry) is { } refusal)
            throw new CamusDBException(refusal.Code, refusal.Message);

        // Persist the durable KV checkpoint from this proposer context — NOT from the schema
        // apply callback, which runs inside the schema partition's commit pipeline and would
        // deadlock when its KV writes re-enter the same partition. The committed schema log is
        // already the source of truth; the checkpoint is a load-time optimization, so on a
        // persist failure we retry and then surface a typed error.
        await checkpoints.PersistSchemaCheckpointWithRetryAsync(
            database, entry, droppedTableId, droppedViewId, droppedSequenceId, changeSetDeltas).ConfigureAwait(false);

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            entry.ToVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            enforceFullConvergence: RequiresEveryLiveNode(entry.Op),
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (!acked)
        {
            string timedOutLaggards = FormatLaggards(database.Kahuna.LastGateLaggards);

            // A pending change set that not every node fences refuses the commit of its transaction, which
            // has committed nothing yet, so the client can run the transaction again.
            throw new CamusDBException(
                entry.Op == SchemaOp.PendingChangeSet ? CamusDBErrorCodes.TransactionMustRetry : CamusDBErrorCodes.InvalidInternalOperation,
                $"Timed out waiting for live schema apply acknowledgements for database '{database.Name}' " +
                $"version {entry.ToVersion}; nodes that never acked: {timedOutLaggards}"
            );
        }

        if (database.Kahuna.LastGateOutcome == SchemaAckOutcome.QuorumBackstop)
            logger.LogWarning(
                "Schema ack post-commit gate for database '{Database}' version {Version} " +
                "completed via QuorumBackstop — these live nodes did not ack within the " +
                "backstop window ({BackstopMs}ms) and are lagging (will be fenced until they apply " +
                "the committed schema entry): {Laggards}",
                database.Name, entry.ToVersion,
                (long)database.Kahuna.SchemaAckQuorumBackstopDelay.TotalMilliseconds,
                FormatLaggards(database.Kahuna.LastGateLaggards)
            );
    }

    /// <summary>
    /// True for an entry whose post-commit gate must hear from every live node, where a quorum is not
    /// enough:
    /// <list type="bullet">
    /// <item><see cref="SchemaOp.PendingChangeSet"/>: the KV commit of the transaction follows the gate. A
    /// node that did not apply the entry does not fence the tables of the change set, and would read the
    /// committed rows with the schema from before it, or use an index the change set drops.</item>
    /// <item><see cref="SchemaOp.PublishChangeSet"/>: the success of <c>COMMIT</c> says that the schema
    /// changes are visible on every live node.</item>
    /// </list>
    /// Every other entry keeps the quorum backstop: the next proposal waits for every live node anyway.
    /// </summary>
    private static bool RequiresEveryLiveNode(SchemaOp op) =>
        op is SchemaOp.PendingChangeSet or SchemaOp.PublishChangeSet;

    private static async Task WaitForPreviousVersionAcksAsync(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // Safety: this is the PRE-PROPOSAL gate that enforces the two-version invariant.
        // The quorum backstop MUST NOT fire here — allowing quorum-only convergence on this gate
        // would let the proposer advance N→N+1 while a minority sits at N−1, breaking the
        // invariant and exposing those nodes to mis-decode. enforceFullConvergence=true disables
        // the backstop for this call while keeping it active for the post-commit gate below.
        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            entry.FromVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            enforceFullConvergence: true,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (acked)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for live schema apply acknowledgements before proposing schema change '{entry.Op}' for database '{database.Name}' from version {entry.FromVersion}"
        );
    }

    /// <summary>
    /// The deltas of the pending change set that <paramref name="entry"/> publishes, with the versions of
    /// the publish entry and the ids of what each one drops. The ids are read from a copy of the schema,
    /// advanced delta by delta, so a delta that drops a table an earlier delta created or renamed still
    /// finds its id. Null when the change set is no longer pending, in which case the publish changes
    /// nothing and its checkpoint writes only the version.
    /// </summary>
    private static async Task<IReadOnlyList<ChangeSetCheckpointDelta>?> CaptureChangeSetDeltasAsync(
        DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        Schema copy;
        List<SchemaChangeLogEntry> deltas;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (ChangeSetDeltaApplier.FindDecided(database.Schema, entry) is not { } pending)
                return null;

            deltas = ChangeSetDeltaApplier.DecodeDeltas(pending.Payload, entry);
            copy = deltas.Exists(static delta => delta.Op is SchemaOp.DropTable or SchemaOp.DropView or SchemaOp.DropSequence)
                ? SchemaReplicator.CloneSchema(database.Schema, (_, table) => table.Id is { } id && pending.AffectedTableIds.Contains(id))
                : database.Schema;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        // Only a drop needs the ids read before the apply; a change set without one skips the walk.
        if (!deltas.Exists(static delta => delta.Op is SchemaOp.DropTable or SchemaOp.DropView or SchemaOp.DropSequence))
            return deltas.ConvertAll(static delta => new ChangeSetCheckpointDelta(delta, null, null, null));

        copy.PendingChangeSet = null;

        List<ChangeSetCheckpointDelta> captured = new(deltas.Count);

        foreach (SchemaChangeLogEntry delta in deltas)
        {
            TableSchema? droppedTable = delta.Op == SchemaOp.DropTable
                && copy.Tables.TryGetValue(SchemaDeltaApplier.DecodePayload<SchemaDropTablePayload>(delta).TableName, out TableSchema? table)
                    ? SchemaReplicator.CloneTable(table)
                    : null;

            string? tableId = droppedTable?.Id;

            string? viewId = delta.Op == SchemaOp.DropView
                && copy.Views.TryGetValue(SchemaDeltaApplier.DecodePayload<SchemaDropViewPayload>(delta).ViewName, out ViewSchema? view)
                    ? view.Id
                    : null;

            string? sequenceId = delta.Op == SchemaOp.DropSequence
                && copy.Sequences.TryGetValue(SchemaDeltaApplier.DecodePayload<SchemaDropSequencePayload>(delta).SequenceName, out SequenceSchema? sequence)
                    ? sequence.Id
                    : null;

            captured.Add(new ChangeSetCheckpointDelta(delta, tableId, viewId, sequenceId, droppedTable));

            SchemaDeltaApplier.ApplyOrRefuse(copy, database, delta);
        }

        return captured;
    }

    private static string FormatLaggards(IReadOnlyList<string> laggards)
        => laggards.Count == 0 ? "(none)" : string.Join(", ", laggards);

    private static string? ResolveTableId(DatabaseDescriptor database, string tableName)
        => database.Schema.Tables.TryGetValue(tableName, out TableSchema? table) ? table.Id : null;
}
