
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

    internal async Task ReplicateAndWaitLocalApplyAsync(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // Replicating the schema-log delta (and the checkpoint persist below) re-enters the
        // schema partition's serial, inline apply pipeline — which yields on the schema lock. Doing
        // it while the lock is held deadlocks that pipeline (the root cause). DDL proposers must
        // build/validate + apply the delta under the lock, then RELEASE before calling this. A
        // non-zero depth here is a bug.
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"ReplicateAndWaitLocalApplyAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

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

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(entry);
        SchemaReplicationResult result = await database.Kahuna.ReplicateSchemaChangeAsync(database.Id, bytes, CancellationToken.None).ConfigureAwait(false);

        if (result.Outcome != SchemaReplicationOutcome.Committed)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema change '{entry.Op}' for database '{database.Name}' was not committed: {result.Outcome} {result.Status}"
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

        // Persist the durable KV checkpoint from this proposer context — NOT from the schema
        // apply callback, which runs inside the schema partition's commit pipeline and would
        // deadlock when its KV writes re-enter the same partition. The committed schema log is
        // already the source of truth; the checkpoint is a load-time optimization, so on a
        // persist failure we retry and then surface a typed error.
        await checkpoints.PersistSchemaCheckpointWithRetryAsync(database, entry, droppedTableId, droppedViewId).ConfigureAwait(false);

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            entry.ToVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (!acked)
        {
            string timedOutLaggards = FormatLaggards(database.Kahuna.LastGateLaggards);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
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

    private static string FormatLaggards(IReadOnlyList<string> laggards)
        => laggards.Count == 0 ? "(none)" : string.Join(", ", laggards);

    private static string? ResolveTableId(DatabaseDescriptor database, string tableName)
        => database.Schema.Tables.TryGetValue(tableName, out TableSchema? table) ? table.Id : null;
}
