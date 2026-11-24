
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// Ends a pending change set whose committing transaction is gone: its node crashed, the client vanished
/// during <c>COMMIT</c>, or the commit outcome stayed unknown. A pending change set refuses every other
/// schema change of its database, so it must never stay pending.
///
/// <para><b>The decision key decides.</b> The resolver takes the key's exclusive lock in a transaction
/// of its own and reads it. <c>committed</c>: the transaction's KV commit landed, so the resolver
/// publishes. <c>aborted</c>: a resolver already decided, so it aborts. Absent: the transaction did not
/// commit and cannot any more once the resolver writes <c>aborted</c> and commits that write, so the
/// resolver aborts. The transaction writes the key under the same lock after reading it absent, so the
/// two can never both write it. When the transaction still holds the lock, the resolver's lock attempt
/// fails and the next run tries again. The lock of a transaction whose node died runs out with its lease
/// (<see cref="ChangeSetCommitter.StagedKeyLeaseMs"/>).</para>
///
/// <para><b>When it runs.</b> On the schema leader of the database, for a change set that has been pending
/// on this node longer than <see cref="CamusDBOptions.TransactionalDdlResolveAfterMs"/>. Two sources drive
/// it: the watcher that <see cref="SchemaReplicator"/> starts on every node while a change set is pending
/// (when the pending entry applies, when it is replayed, and when a database opens with one), and the
/// coordinator-job sweep. The watcher checks leadership at each run, so a leader change needs no event of
/// its own. The delay keeps the resolver out of the way of a live commit, which normally publishes within
/// one KV commit and one schema-log round trip; a restart starts the delay again. A run that races a live
/// commit is still correct, by the decision-key argument above.</para>
/// </summary>
internal sealed class PendingChangeSetResolver
{
    /// <summary>
    /// How often the watcher of a database looks at its pending change set; at most a quarter of the
    /// configured delay, so a short delay is honored, and at least 50 ms.
    /// </summary>
    internal static TimeSpan PollInterval(DatabaseDescriptor database) =>
        TimeSpan.FromMilliseconds(Math.Clamp(database.Options.TransactionalDdlResolveAfterMs / 4, 50, 5_000));

    private const int DecisionKeyLockAttempts = 8;

    private readonly ChangeSetCommitter committer;

    private readonly ILogger logger;

    internal PendingChangeSetResolver(ChangeSetCommitter committer, ILogger logger)
    {
        this.committer = committer;
        this.logger = logger;
    }

    /// <summary>
    /// Ends the pending change set of <paramref name="database"/> when it has waited longer than
    /// <see cref="ResolveAfter"/>, or at once when <paramref name="force"/> is set (tests). Returns the
    /// decision taken, or null when nothing was pending, the change set had not waited long enough, or
    /// another party decided it first. The caller runs it on the schema leader only.
    /// </summary>
    internal async Task<bool?> ResolveAsync(DatabaseDescriptor database, bool force = false)
    {
        TransactionSchemaScope.Suppress();

        if (database.Schema.PendingChangeSet is not { } pending)
            return null;

        if (!force && Environment.TickCount64 - pending.RecordedAtTicks < database.Options.TransactionalDdlResolveAfterMs)
            return null;

        bool committed = await ReadOrAbortDecisionAsync(database, pending.ChangeSetId).ConfigureAwait(false);

        logger.LogWarning(
            "Resolving change set {ChangeSetId} of database {Database} left pending by its transaction: {Decision}",
            pending.ChangeSetId, database.Name, committed ? "publish" : "abort");

        bool decided = await committer.DecideAsync(database, pending.ChangeSetId, publish: committed).ConfigureAwait(false);
        return decided ? committed : null;
    }

    /// <summary>
    /// The current record of <paramref name="job"/>, a job that a transaction owns, when the transaction
    /// ended without a commit; null when there is nothing to remove. The coordinator's resume calls it with
    /// the DDL lock held, for a job it read earlier.
    ///
    /// <para><b>Why the record is read again, under the decision key's lock.</b> The owner's KV commit
    /// deletes its job records and writes the decision key together, and the work after the commit then
    /// deletes the decision key. An absent decision key therefore means that the owner never committed only
    /// while its record still exists. The owner holds the decision key's exclusive lock from its prepare to
    /// the end of its KV commit, so under that lock the commit has ended, and the record read here shows it,
    /// or it cannot start any more: <c>aborted</c> is committed before this method returns a record, and the
    /// owner reads the key under the same lock.</para>
    ///
    /// <para>A record that names another owner is the job of a later transaction on an element of the same
    /// name; it is not this job's business.</para>
    /// </summary>
    internal static async Task<PersistedCoordinatorJob?> AbortOwnerAndReadJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob job)
    {
        string owner = job.OwnerChangeSetId!;
        IKahuna kahuna = database.Kahuna.Kahuna;
        string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, owner);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, decisionKey, DecisionKeyLockAttempts, "decision").ConfigureAwait(false);

            byte[]? decision = await ChangeSetCommitter.ReadDecisionAsync(
                kahuna, tx.TransactionId, decisionKey, CancellationToken.None).ConfigureAwait(false);

            if (decision is not null && decision.AsSpan().SequenceEqual(ChangeSetCommitter.CommittedDecision))
                return null;

            if (await CoordinatorJobStore.ReadLockedCoordinatorJobAsync(database, tx, job.TableId, job.ElementName).ConfigureAwait(false) is not { } stored
                || !CoordinatorJobStore.IsOwnedBy(stored, job.ElementKind, owner))
                return null;

            if (decision is null)
            {
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, decisionKey, ChangeSetCommitter.AbortedDecision).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }

            return stored;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// True when the decision key of <paramref name="changeSetId"/> says committed. When the key is absent,
    /// writes <c>aborted</c> and commits it before it returns false, so the transaction can no longer
    /// commit. Only for a pending change set: its record keeps the decision key in place until the publish
    /// or the abort. A staged job whose owner is gone goes through <see cref="AbortOwnerAndReadJobAsync"/>.
    /// </summary>
    internal static async Task<bool> ReadOrAbortDecisionAsync(DatabaseDescriptor database, string changeSetId)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, changeSetId);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, decisionKey, DecisionKeyLockAttempts, "decision").ConfigureAwait(false);

            byte[]? decision = await ChangeSetCommitter.ReadDecisionAsync(
                kahuna, tx.TransactionId, decisionKey, CancellationToken.None).ConfigureAwait(false);

            if (decision is not null)
                return decision.AsSpan().SequenceEqual(ChangeSetCommitter.CommittedDecision);

            await MetaKeyWriter.WriteMetaKey(kahuna, tx, decisionKey, ChangeSetCommitter.AbortedDecision).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

            return false;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deletes the decision key that <see cref="AbortOwnerAndReadJobAsync"/> wrote for
    /// <paramref name="changeSetId"/>, after the last element that the transaction staged is removed. Only for
    /// a change set that is not pending: the checkpoint of an abort deletes the key of a pending one. The
    /// transaction cannot commit without the key either, because its prepare finds no job record.
    /// </summary>
    internal static async Task DeleteAbortedDecisionAsync(DatabaseDescriptor database, string changeSetId)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, changeSetId);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, decisionKey, DecisionKeyLockAttempts, "decision").ConfigureAwait(false);

            byte[]? decision = await ChangeSetCommitter.ReadDecisionAsync(
                kahuna, tx.TransactionId, decisionKey, CancellationToken.None).ConfigureAwait(false);

            if (decision is null || !decision.AsSpan().SequenceEqual(ChangeSetCommitter.AbortedDecision))
                return;

            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, decisionKey).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deletes the decision key of <paramref name="changeSetId"/> in a transaction of its own. Only for a
    /// change set that no publish or abort checkpoint ends, because it had no delta to announce: the
    /// committer calls it after such a transaction committed.
    /// </summary>
    internal static async Task DeleteDecisionAsync(DatabaseDescriptor database, string changeSetId)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string decisionKey = MetaKeys.ChangeSetDecisionKey(database.Id, changeSetId);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, decisionKey).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }
}
