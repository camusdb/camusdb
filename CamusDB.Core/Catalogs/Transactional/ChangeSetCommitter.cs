
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using CamusDB.Core.Storage.Kv;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// Commits the change set of an explicit transaction together with the transaction, in both single-node
/// and cluster mode (both keep the schema in a schema log). The steps, in order:
///
/// <list type="number">
/// <item><b>Prepare</b>, before the KV commit request leaves the node. Write the decision key
/// <c>{dbId}/meta/ddlcommit:{id}</c> = committed into the transaction, after an exclusive lock and a read
/// that finds it absent; write the history key of every new table layout into the transaction, each under a
/// lock with a lease; then propose a <see cref="SchemaOp.PendingChangeSet"/> entry. Its apply refuses the change set when the
/// schema moved since the transaction's first schema change, and otherwise reserves the schema for it and
/// fences its tables. The step ends when every live node applied the entry; a quorum is not enough,
/// because the rows commit next.</item>
/// <item>The KV commit of the transaction: rows, decision key and layouts land together or not at
/// all.</item>
/// <item><b>Decide</b>. After a commit, propose <see cref="SchemaOp.PublishChangeSet"/>; after a definite
/// abort, <see cref="SchemaOp.AbortChangeSet"/>. After an unknown outcome, do nothing: a resumed commit or
/// <see cref="PendingChangeSetResolver"/> decides from the decision key.</item>
/// </list>
///
/// <para><b>Why the decision key settles every crash.</b> The transaction holds the key's exclusive lock
/// from the prepare until its KV commit ends. A resolver that finds the change set pending takes the same
/// lock in a transaction of its own and reads the key: present means the transaction committed, so it
/// publishes; absent means it did not, so the resolver writes <c>aborted</c> and commits that before it
/// proposes the abort. The transaction's own read under the lock then finds <c>aborted</c> and refuses
/// its commit. One of the two writes the key, never both.</para>
///
/// <para><b>The locks of the prepare have a lease</b> (<see cref="StagedKeyLeaseMs"/>). The node that
/// runs the transaction can die between the prepare and the KV commit. A lock tied to its session would then
/// live as long as a session can, and the change set would stay pending for all of it. With a lease, the resolver gets the
/// decision key when the lease ran out, and a commit that comes after that is refused for certain
/// (<see cref="CatalogKeyLock"/>). The other keys of the prepare have the same lease, so that the statements
/// and the removals that write them next do not wait for a transaction that is gone either.</para>
///
/// <para><b>Elements staged on shared tables.</b> The prepare also deletes, in the transaction, the
/// coordinator job record of every element the transaction staged on a shared table
/// (<see cref="TransactionSchemaOverlay.OwnedJobs"/>), after it read the record under its lock. A record that
/// is gone means that the coordinator's resume removed the element, because the transaction lost the DDL
/// lock, and the commit is refused. The KV commit makes the delete permanent together
/// with the rows, so a record that names the change set as its owner always means that the transaction
/// did not commit, and the coordinator's resume removes the element. A transaction whose only schema
/// changes are staged CHECK or NOT NULL constraints has no delta: those constraints are in the shared
/// schema already. Its prepare writes the decision key and the record deletes and proposes no pending
/// entry, and the decide step has nothing to publish.</para>
///
/// <para><b>Lock order.</b> The DDL lock (held since the transaction's first DDL statement), then the
/// node's DDL gate semaphore, then the schema lock. See <see cref="TransactionalDdlLock"/>.</para>
///
/// <para>Every method runs with <see cref="TransactionSchemaScope"/> cleared: it changes the shared
/// schema and must never see the transaction's private one.</para>
/// </summary>
internal sealed class ChangeSetCommitter
{
    private const int DecisionAttempts = 5;

    /// <summary>
    /// How long a decision is retried. An attempt that waits out a schema-acknowledgement gate takes the
    /// whole gate timeout, and a second one would not help: the resolver ends the change set later.
    /// </summary>
    private const int DecisionRetryBudgetMs = 10_000;

    /// <summary>The value the committing transaction writes to the decision key.</summary>
    internal static readonly byte[] CommittedDecision = "committed"u8.ToArray();

    /// <summary>The value a resolver writes to the decision key of a transaction that did not commit.</summary>
    internal static readonly byte[] AbortedDecision = "aborted"u8.ToArray();

    private const int DecisionKeyLockAttempts = 32;

    /// <summary>What the lease of a staged key has left when the pending entry used its whole acknowledgement wait.</summary>
    private const int StagedKeyLeaseMarginMs = 15_000;

    /// <summary>
    /// The lease of the locks that a commit's prepare takes. Long enough for the wait for every live node to
    /// acknowledge the pending entry, which comes between the prepare and the KV commit.
    /// </summary>
    internal static int StagedKeyLeaseMs(DatabaseDescriptor database) =>
        (int)Math.Min(int.MaxValue, database.Kahuna.SchemaAckWaitTimeout.TotalMilliseconds + StagedKeyLeaseMarginMs);

    private readonly CatalogsManager catalogs;

    private readonly ILogger logger;

    /// <summary>
    /// Test-only: awaited at each <see cref="ChangeSetStep"/> of a commit or a rollback. A fault test stops
    /// the driver here with a task that never ends, fails it with a task that throws, or moves a leader
    /// while it waits. Per committer, never static. Null in production; a test clears it after use.
    /// </summary>
    internal Func<ChangeSetStep, Task>? TestAtStep;

    internal ChangeSetCommitter(CatalogsManager catalogs, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(catalogs);

        this.catalogs = catalogs;
        this.logger = logger;
    }

    /// <summary>
    /// The prepare step; see the class summary. A throw refuses the commit, and the transactions manager
    /// then rolls the transaction back and calls <see cref="DiscardAsync"/>.
    /// </summary>
    internal async Task PrepareAsync(TransactionSchemaOverlay overlay, CancellationToken cancellationToken)
    {
        TransactionSchemaScope.Suppress();

        DatabaseDescriptor database = overlay.Database;
        KvTransaction tx = overlay.Owner;

        if (!overlay.HasChanges)
        {
            // Statements discard an overlay that stays empty, so this is a statement that failed before
            // its first delta, which leaves the transaction rollback-only, or an overlay that holds only
            // staged constraints. Checked anyway: an overlay with no delta announces nothing, so this is
            // the only check that its private copy is still the live schema.
            if (database.Schema.SchemaVersion != overlay.BaseVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"Transaction {tx.UniqueId} planned against schema version {overlay.BaseVersion} of database " +
                    $"'{database.Name}', which is at version {database.Schema.SchemaVersion}; it was rolled back, retry it from BEGIN");

            if (!overlay.HoldsOwnedJobs)
                return;
        }

        overlay.MarkPreparing();

        IKahuna kahuna = database.Kahuna.Kahuna;
        int leaseMs = StagedKeyLeaseMs(database);

        overlay.AllowsCatalogWrites = true;
        try
        {
            string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, overlay.ChangeSetId);

            await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, decisionKey, DecisionKeyLockAttempts, "decision", leaseMs).ConfigureAwait(false);

            if (await ReadDecisionAsync(kahuna, tx.TransactionId, decisionKey, cancellationToken).ConfigureAwait(false) is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"The schema changes of transaction {tx.UniqueId} were aborted while it committed; nothing was " +
                    "committed, retry the transaction from BEGIN");

            await MetaKeyWriter.WriteMetaKey(kahuna, tx, decisionKey, CommittedDecision, leaseMs).ConfigureAwait(false);

            foreach ((string tableId, string elementName) in overlay.OwnedJobs)
                await DeleteOwnedJobRecordAsync(database, overlay, tx, tableId, elementName, leaseMs).ConfigureAwait(false);

            await WriteNewLayoutsAsync(database, overlay, tx, leaseMs).ConfigureAwait(false);

            await WriteDeferredPurgesAsync(database, overlay, tx, leaseMs).ConfigureAwait(false);

            // Starts the lease again, so the decision key is the last of the staged keys that others get.
            await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, decisionKey, DecisionKeyLockAttempts, "decision", leaseMs).ConfigureAwait(false);
        }
        finally
        {
            overlay.AllowsCatalogWrites = false;
        }

        await AtStepAsync(ChangeSetStep.Staged).ConfigureAwait(false);

        // Only staged constraints: nothing to announce, and nothing to publish after the KV commit.
        if (!overlay.HasChanges)
            return;

        await database.SchemaDdlSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SchemaChangeLogEntry pending;

            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                if (database.Schema.SchemaVersion != overlay.BaseVersion)
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionConflict,
                        $"Transaction {tx.UniqueId} changed the schema of database '{database.Name}' from version " +
                        $"{overlay.BaseVersion}, but the schema is at version {database.Schema.SchemaVersion}: another " +
                        "schema change landed while the transaction was open; it was rolled back, retry it from BEGIN");

                SchemaChangeSetPayload payload = new()
                {
                    ChangeSetId = overlay.ChangeSetId,
                    BaseVersion = overlay.BaseVersion,
                    Deltas = new List<byte[]>(overlay.Deltas.Count),
                };

                foreach (SchemaChangeLogEntry delta in overlay.Deltas)
                    payload.Deltas.Add(SchemaChangeLogEntryCodec.Encode(delta));

                pending = new SchemaChangeLogEntry
                {
                    Ts = tx.TransactionId,
                    Database = database.Id,
                    FromVersion = database.Schema.SchemaVersion,
                    ToVersion = database.Schema.SchemaVersion + 1,
                    Op = SchemaOp.PendingChangeSet,
                    Payload = SchemaChangeLogEntryCodec.EncodePayload(payload),
                };

                SchemaDeltaApplier.ValidateSchemaDelta(database, pending);
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            try
            {
                await catalogs.ProposeChangeSetEntryAsync(database, pending).ConfigureAwait(false);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.InvalidInternalOperation && !database.SchemaSubsystemDegraded)
            {
                // No leader of the schema log answered, or the entry was not confirmed in time. The KV
                // commit never starts after this, so the client can run the transaction again.
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"The schema changes of transaction {tx.UniqueId} could not be announced: {ex.Message}; nothing was " +
                    "committed, retry the transaction from BEGIN",
                    ex);
            }

            overlay.MarkPending();
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        await AtStepAsync(ChangeSetStep.Announced).ConfigureAwait(false);
    }

    /// <summary>
    /// The decide step after the KV commit has an outcome; see the class summary. Always ends the overlay
    /// and frees the DDL lock first, so no later work of this commit holds other DDL back.
    /// <list type="bullet">
    /// <item><b>Committed</b>: publish, and confirm that every live node applied the publish. Then run the
    /// work after the publish. When the publish cannot be confirmed, throw the retryable
    /// <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/> and leave the transaction resumable
    /// (<see cref="ResumePublicationAsync"/>): the rows are committed, and a client that read a plain
    /// success would find a table that does not exist yet.</item>
    /// <item><b>Aborted</b>: abort the change set and run the rollback work. Never throws; a change set
    /// left pending is ended by the resolver, which reads the same decision key.</item>
    /// <item><b>Unknown</b>: the change set stays pending. A resumed commit of the transaction calls this
    /// again with the final outcome, and otherwise the resolver decides it. The lock is not kept for that
    /// wait, because the transactions reaper never rolls back a transaction that is finalizing.</item>
    /// </list>
    /// </summary>
    internal async Task CompleteAsync(TransactionSchemaOverlay overlay, SchemaChangesCommitOutcome outcome)
    {
        TransactionSchemaScope.Suppress();

        if (outcome == SchemaChangesCommitOutcome.Unknown)
        {
            overlay.OutcomeUnknown = true;
            await overlay.CloseAsync().ConfigureAwait(false);
            return;
        }

        DatabaseDescriptor database = overlay.Database;

        if (outcome == SchemaChangesCommitOutcome.Aborted)
        {
            try
            {
                await AtStepAsync(ChangeSetStep.Aborting).ConfigureAwait(false);

                if (overlay.HasChanges && IsPendingInLog(database, overlay.ChangeSetId))
                    await DecideAsync(database, overlay.ChangeSetId, publish: false).ConfigureAwait(false);

                await AtStepAsync(ChangeSetStep.Aborted).ConfigureAwait(false);

                // Before the DDL lock is freed: the rollback work removes the elements the transaction
                // staged on shared tables, and the coordinator's resume removes them too once the lock is
                // free. With the lock held, the two never run at once.
                await overlay.RunOnRollbackAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex, "Could not abort change set {ChangeSetId} of database {Database}; the resolver ends it",
                    overlay.ChangeSetId, database.Name);
            }
            finally
            {
                await overlay.CloseAsync().ConfigureAwait(false);
            }

            return;
        }

        if (!overlay.HasChanges)
        {
            await overlay.CloseAsync().ConfigureAwait(false);

            if (overlay.HoldsOwnedJobs)
                await AfterOwnedOnlyCommitAsync(database, overlay).ConfigureAwait(false);

            return;
        }

        overlay.PublicationOutstanding = true;
        await CompletePublicationAsync(overlay).ConfigureAwait(false);
    }

    /// <summary>
    /// Completes the publish of a committed change set for a retried COMMIT; see
    /// <see cref="CompleteAsync"/>.
    /// </summary>
    internal Task ResumePublicationAsync(TransactionSchemaOverlay overlay)
    {
        if (!overlay.PublicationOutstanding)
            return Task.CompletedTask;

        return CompletePublicationAsync(overlay);
    }

    /// <summary>
    /// Publishes a committed change set (or finds it published by the resolver), confirms that every live
    /// node applied it, frees the DDL lock, and then runs the work after the publish. Throws the retryable
    /// <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/> while the publish is not confirmed.
    /// </summary>
    private async Task CompletePublicationAsync(TransactionSchemaOverlay overlay)
    {
        TransactionSchemaScope.Suppress();

        DatabaseDescriptor database = overlay.Database;
        Exception? failure = null;
        bool confirmed = false;

        try
        {
            await AtStepAsync(ChangeSetStep.RowsCommitted).ConfigureAwait(false);

            confirmed = await PublishAndConfirmAsync(database, overlay.ChangeSetId).ConfigureAwait(false);

            if (confirmed)
                await AtStepAsync(ChangeSetStep.Published).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            // The lock serializes DDL transactions until the change set is decided; the decision key
            // decides it now, and the resolver finishes a publish this node cannot.
            await overlay.CloseAsync().ConfigureAwait(false);
        }

        if (!confirmed)
        {
            logger.LogWarning(
                failure,
                "The publish of change set {ChangeSetId} of database {Database} is not confirmed; the transaction committed, " +
                "and a retried COMMIT or the resolver completes the publish",
                overlay.ChangeSetId, database.Name);

            throw new CamusDBException(
                CamusDBErrorCodes.TransactionFinalizeUnresolved,
                $"Transaction {overlay.Owner.UniqueId} committed, but its schema changes are not yet visible on every node " +
                $"({failure?.Message ?? "not every live node acknowledged them"}); retry COMMIT on the same transaction to " +
                "wait for them (do not re-run the transaction)");
        }

        overlay.PublicationOutstanding = false;

        await AfterPublishAsync(database, overlay).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes the publish of a committed change set and returns true once every live node acknowledged
    /// it. When the change set is no longer pending here, another party published it (the decision key
    /// says committed, so nothing can abort it); the acknowledgement of the current schema version, which
    /// covers the publish, is then awaited.
    /// </summary>
    private async Task<bool> PublishAndConfirmAsync(DatabaseDescriptor database, string changeSetId)
    {
        if (IsPendingInLog(database, changeSetId)
            && await DecideAsync(database, changeSetId, publish: true).ConfigureAwait(false))
            return true;

        // Every live node, as for the publish itself: a quorum would report a commit whose schema changes
        // a live node cannot see yet.
        return await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            database.Schema.SchemaVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            enforceFullConvergence: true,
            cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Ends the overlay of a transaction that rolls back, or whose commit was refused before the KV commit
    /// request. A change set that reached the schema log as pending is aborted here, unless an earlier
    /// commit attempt of the transaction has an unknown outcome: then the decision key decides, through
    /// the resolver. Never throws.
    /// </summary>
    internal async Task DiscardAsync(TransactionSchemaOverlay overlay)
    {
        TransactionSchemaScope.Suppress();

        DatabaseDescriptor database = overlay.Database;

        try
        {
            if (!overlay.OutcomeUnknown)
            {
                await AtStepAsync(ChangeSetStep.Aborting).ConfigureAwait(false);

                if (overlay.HasChanges && IsPendingInLog(database, overlay.ChangeSetId))
                    await DecideAsync(database, overlay.ChangeSetId, publish: false).ConfigureAwait(false);

                await AtStepAsync(ChangeSetStep.Aborted).ConfigureAwait(false);

                await overlay.RunOnRollbackAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex, "Could not abort change set {ChangeSetId} of database {Database}; the resolver ends it",
                overlay.ChangeSetId, database.Name);
        }
        finally
        {
            await overlay.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Proposes the publish or the abort of the pending change set <paramref name="changeSetId"/>. Returns
    /// false when it is no longer pending: the other party (the committing node or a resolver) already
    /// decided it. Retries a transient refusal a few times, within <see cref="DecisionRetryBudgetMs"/>, then
    /// throws.
    /// </summary>
    internal async Task<bool> DecideAsync(DatabaseDescriptor database, string changeSetId, bool publish)
    {
        TransactionSchemaScope.Suppress();

        long retryUntil = Environment.TickCount64 + DecisionRetryBudgetMs;

        for (int attempt = 1; ; attempt++)
        {
            Exception? transient = null;

            await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                SchemaChangeLogEntry decision;

                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try
                {
                    if (database.Schema.PendingChangeSet is not { } pending
                        || !string.Equals(pending.ChangeSetId, changeSetId, StringComparison.Ordinal))
                        return false;

                    decision = new SchemaChangeLogEntry
                    {
                        Ts = pending.PendingEntry.Ts,
                        Database = database.Id,
                        FromVersion = database.Schema.SchemaVersion,
                        ToVersion = database.Schema.SchemaVersion + 1,
                        Op = publish ? SchemaOp.PublishChangeSet : SchemaOp.AbortChangeSet,
                        Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaChangeSetDecisionPayload { ChangeSetId = changeSetId }),
                    };
                }
                finally
                {
                    database.Schema.ReleaseLock();
                }

                await catalogs.ProposeChangeSetEntryAsync(database, decision).ConfigureAwait(false);
                return true;
            }
            catch (CamusDBException ex) when (attempt < DecisionAttempts
                                               && Environment.TickCount64 < retryUntil
                                               && ex.Code is CamusDBErrorCodes.TransactionMustRetry or CamusDBErrorCodes.InvalidInternalOperation)
            {
                transient = ex;
            }
            finally
            {
                database.SchemaDdlSemaphore.Release();
            }

            logger.LogWarning(
                transient, "Attempt {Attempt} to {Decision} change set {ChangeSetId} of database {Database} failed; retrying",
                attempt, publish ? "publish" : "abort", changeSetId, database.Name);

            await Task.Delay(100 * attempt).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads the decision key of a change set: <see cref="CommittedDecision"/>, <see cref="AbortedDecision"/>,
    /// or null when absent. The caller holds the key's exclusive lock in <paramref name="transactionId"/>.
    /// </summary>
    internal static async Task<byte[]?> ReadDecisionAsync(
        IKahuna kahuna, HLCTimestamp transactionId, string decisionKey, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await KahunaRetryPolicy.RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(
                transactionId, decisionKey, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, cancellationToken).AsTask(),
            cancellationToken).ConfigureAwait(false);

        if (type == KeyValueResponseType.Get)
            return entry?.Value;

        if (type == KeyValueResponseType.DoesNotExist)
            return null;

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionMustRetry,
            $"The decision key '{decisionKey}' could not be read ({type}); retry");
    }

    /// <summary>
    /// Deletes in the transaction the job record of an element it staged, and refuses the commit when the
    /// record is not the transaction's any more. The resume deletes the record only after it removed the
    /// element, and deletes the decision key after the last record, so a missing record is the proof that
    /// stays. Its lock keeps the record until the commit has an outcome.
    /// </summary>
    private static async Task DeleteOwnedJobRecordAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, KvTransaction tx, string tableId, string elementName, int leaseMs)
    {
        PersistedCoordinatorJob? job = await CoordinatorJobStore.ReadLockedCoordinatorJobAsync(
            database, tx, tableId, elementName, leaseMs).ConfigureAwait(false);

        if (job is null || !string.Equals(job.OwnerChangeSetId, overlay.ChangeSetId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionConflict,
                $"'{elementName}', which transaction {tx.UniqueId} added to a table of database '{database.Name}', was removed " +
                "while the transaction was open; nothing was committed, retry the transaction from BEGIN");

        await MetaKeyWriter.DeleteMetaKey(
            database.Kahuna.Kahuna, tx, MetaKeys.CoordinatorKey(database.Id, tableId, elementName), leaseMs).ConfigureAwait(false);
    }

    private Task AtStepAsync(ChangeSetStep step) => TestAtStep is { } hook ? hook(step) : Task.CompletedTask;

    private static bool IsPendingInLog(DatabaseDescriptor database, string changeSetId) =>
        database.Schema.PendingChangeSet is { } pending
        && string.Equals(pending.ChangeSetId, changeSetId, StringComparison.Ordinal);

    /// <summary>
    /// Writes into the transaction the history key of every table layout the change set created: for a
    /// table the change set created, every version from 0; for a table it changed, every version after
    /// the shared one. A node that meets a row of the transaction before it applies the publish loads
    /// the row's layout through this key, under the same commit as the row.
    /// </summary>
    private static async Task WriteNewLayoutsAsync(DatabaseDescriptor database, TransactionSchemaOverlay overlay, KvTransaction tx, int leaseMs)
    {
        Dictionary<string, int> sharedVersions = new(database.Schema.Tables.Count, StringComparer.Ordinal);

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            foreach (TableSchema shared in database.Schema.Tables.Values)
            {
                if (shared.Id is { Length: > 0 } id)
                    sharedVersions[id] = shared.Version;
            }
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        IKahuna kahuna = database.Kahuna.Kahuna;

        foreach (TableSchema table in overlay.Schema.Tables.Values.Concat(overlay.DroppedTables))
        {
            if (table.Id is not { Length: > 0 } tableId)
                continue;

            int firstNew = sharedVersions.TryGetValue(tableId, out int sharedVersion) ? sharedVersion + 1 : 0;

            for (int version = firstNew; version <= table.Version; version++)
            {
                TableSchemaHistory? layout = version == table.Version
                    ? new TableSchemaHistory { Version = version, Columns = table.Columns }
                    : table.SchemaHistory?.Find(history => history.Version == version);

                if (layout is null)
                    continue;

                byte[] bytes = MetaJsonSerializer.Serialize(layout, MetaJsonContext.Default.TableSchemaHistory);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.HistoryKey(database.Id, tableId, version), bytes, leaseMs).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Writes into the transaction the record of every piece of data the change set makes unreachable, so
    /// the records commit exactly when the transaction does. A run removes the data after the publish; see
    /// <see cref="DeferredPurgeRecord"/>.
    /// </summary>
    private static async Task WriteDeferredPurgesAsync(DatabaseDescriptor database, TransactionSchemaOverlay overlay, KvTransaction tx, int leaseMs)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        foreach (DeferredPurgeRecord record in overlay.DeferredPurges)
        {
            // The base the pending entry is proposed from. A staged statement on a shared table can move
            // the base after the record was added, and the due rule counts from the pending entry.
            record.BaseVersion = overlay.BaseVersion;

            byte[] bytes = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.DeferredPurgeRecord);
            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.PurgeKey(database.Id, record.PurgeId), bytes, leaseMs).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The work after the commit of a transaction whose only schema changes are staged constraints. The
    /// constraints are in the shared schema already, so only the cached results of their tables are
    /// dropped, and the decision key is deleted: no publish checkpoint deletes it. Best effort: a decision
    /// key left behind is never read again, because the transaction's job records are gone.
    /// </summary>
    private async Task AfterOwnedOnlyCommitAsync(DatabaseDescriptor database, TransactionSchemaOverlay overlay)
    {
        foreach ((string tableId, _) in overlay.OwnedJobs)
            database.Cache?.InvalidateByTableId(database.Id, tableId);

        try
        {
            await PendingChangeSetResolver.DeleteDecisionAsync(database, overlay.ChangeSetId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not delete the decision key of change set {ChangeSetId} of database {Database}", overlay.ChangeSetId, database.Name);
        }

        await overlay.RunAfterPublishAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// The work after a publish on the committing node: evict the cached results of the tables the change
    /// set touched, carry the private copy of the legacy system schema over, and run the deferred work the
    /// statements recorded.
    /// </summary>
    private static async Task AfterPublishAsync(DatabaseDescriptor database, TransactionSchemaOverlay overlay)
    {
        foreach (string tableId in overlay.TouchedTableIds)
            database.Cache?.InvalidateByTableId(database.Id, tableId);

        await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            database.SystemSchema.Tables = new(overlay.SystemSchema.Tables);
            database.SystemSchema.Indexes = new(overlay.SystemSchema.Indexes);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        await overlay.RunAfterPublishAsync().ConfigureAwait(false);
    }
}
