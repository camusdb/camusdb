
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
    internal static async Task PersistCoordinatorJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob job)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.PersistedCoordinatorJob);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.CoordinatorKey(database.Id, job.TableId, job.ElementName), bytes).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    internal static async Task DeleteCoordinatorJobAsync(DatabaseDescriptor database, string tableId, string elementName)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.CoordinatorKey(database.Id, tableId, elementName)).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
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

    internal static async Task<List<PersistedCoordinatorJob>> LoadCoordinatorJobsAsync(DatabaseDescriptor database)
    {
        List<PersistedCoordinatorJob> jobs = [];
        IKahuna kahuna = database.Kahuna.Kahuna;
        string keyPrefix = MetaKeys.CoordinatorKeyPrefix(database.Id);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
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
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }
}
