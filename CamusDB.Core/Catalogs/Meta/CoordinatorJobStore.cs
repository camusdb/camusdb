
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Reads and writes persisted schema-change coordinator jobs, which let a staged element change
/// (<c>Absent -> DeleteOnly -> WriteOnly -> Public</c>) be resumed by whichever node holds
/// leadership after a failover, rather than being abandoned half-applied.
///
/// <para><b><see cref="DeleteCoordinatorJobsForTableAsync"/> reuses the caller's transaction, and
/// must.</b> DROP TABLE already holds metadata write intents in the same bucket. A nested
/// transaction's snapshot scan would wait on those intents while the outer transaction waits on
/// the scan — a self-deadlock. Running discovery and deletion in the caller's transaction also
/// makes the cleanup atomic with the drop.</para>
///
/// <para>The other three methods open their own transaction, because a coordinator job outlives
/// any single DDL statement by design.</para>
/// </summary>
internal static class CoordinatorJobStore
{
    /// <summary>
    /// Writes <paramref name="job"/> under its <c>{tableId}~{elementName}</c> key, replacing any
    /// earlier record for the same element.
    ///
    /// <para><b>A job must name its table by id.</b> Resume matches a record to a live table through
    /// <see cref="PersistedCoordinatorJob.TableId"/> only, never through the table name, so a record
    /// without an id matches nothing: the next leader deletes it as stale and the element stays in its
    /// intermediate state with no job left to finish it. That loss is silent and shows up only much
    /// later, so a record without an id or an element name is refused here, where the caller that
    /// built it can be seen.</para>
    ///
    /// <para><b>Retried on a definite non-commit.</b> The write is one blind put, so replaying it is
    /// safe. Without the retry, a commit aborted by a conflict or a routing change — likely right
    /// after a leadership change, which is exactly when resume runs — would fail the resume pass of
    /// that job, and nothing drives the job again until the next leader change.</para>
    /// </summary>
    internal static async Task PersistCoordinatorJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob job)
    {
        if (string.IsNullOrEmpty(job.TableId))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Coordinator job for '{job.TableName}.{job.ElementName}' has no table id; a resume could never match it to a live table");

        if (string.IsNullOrEmpty(job.ElementName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Coordinator job for table '{job.TableName}' has no element name");

        IKahuna kahuna = database.Kahuna.Kahuna;
        byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.PersistedCoordinatorJob);
        string key = MetaKeys.CoordinatorKey(database.Id, job.TableId, job.ElementName);

        await SerializableRetryHelper.ExecuteAutocommitAsync(async _ =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, key, bytes).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes the record of one job. Deleting a key that is already gone is a no-op, so the delete is
    /// retried on a definite non-commit for the same reason as
    /// <see cref="PersistCoordinatorJobAsync"/>: a record left behind after its job finished costs a
    /// resume attempt at every later leader change until it is cleaned up.
    /// </summary>
    internal static async Task DeleteCoordinatorJobAsync(DatabaseDescriptor database, string tableId, string elementName)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string key = MetaKeys.CoordinatorKey(database.Id, tableId, elementName);

        await SerializableRetryHelper.ExecuteAutocommitAsync(async _ =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                await MetaKeyWriter.DeleteMetaKey(kahuna, tx, key).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes all persisted coordinator jobs belonging to <paramref name="tableId"/> inside the
    /// caller's DDL transaction. Reusing <paramref name="tx"/> is required because DROP TABLE already
    /// owns metadata write intents in the same bucket; a nested transaction's snapshot scan would wait
    /// for those intents while the outer transaction waits for the scan, creating a self-deadlock.
    /// Keeping discovery and deletion in one transaction also makes the cleanup atomic with the drop.
    /// </summary>
    internal static async Task DeleteCoordinatorJobsForTableAsync(
        DatabaseDescriptor database,
        string tableId,
        KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string tableJobPrefix = $"{MetaKeys.CoordinatorKeyPrefix(database.Id)}{tableId}~";
        string? tableJobPrefixEnd = MetaKeys.PrefixUpperBound(tableJobPrefix);

        List<string> keysToDelete = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            MetaKeys.MetaBucketPrefix(database.Id),
            tableJobPrefix, true,
            tableJobPrefixEnd, false,
            128,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (key.StartsWith(tableJobPrefix, StringComparison.Ordinal) && entry.Value is not null)
                keysToDelete.Add(key);
        }

        foreach (string key in keysToDelete)
            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, key).ConfigureAwait(false);
    }

    /// <summary>
    /// Lists the persisted coordinator jobs of <paramref name="database"/> (leader resume after a
    /// leadership change, and the branch-creation fence that refuses to fork while a job is in
    /// flight). Reads with the zero-identity transaction — latest committed value per key — and no
    /// server session. A session would add nothing (nothing is written, no own uncommitted state is
    /// read) and it made this load fail intermittently: a transactional scan snapshots each key it
    /// visits, and page 0 of the meta bucket can stop at a key that holds a live write intent from
    /// a concurrent schema change, answer <c>WaitingForReplication</c>, and be retried; by then the
    /// intent has committed, the key's revision has moved past the snapshot recorded on the first
    /// attempt, and Kahuna answers the retried page with <c>Aborted</c>, which fails the whole
    /// scan. Without a session there is no snapshot to be stale against.
    /// </summary>
    internal static async Task<List<PersistedCoordinatorJob>> LoadCoordinatorJobsAsync(DatabaseDescriptor database)
    {
        List<PersistedCoordinatorJob> jobs = [];
        IKahuna kahuna = database.Kahuna.Kahuna;
        string keyPrefix = MetaKeys.CoordinatorKeyPrefix(database.Id);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            MetaKeys.MetaBucketPrefix(database.Id),
            null, true,
            null, true,
            128,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(keyPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            PersistedCoordinatorJob job = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.PersistedCoordinatorJob);
            jobs.Add(job);
        }

        return jobs;
    }
}
