
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
/// Reads and writes the durable record of an in-flight materialized-view refresh, one per view.
///
/// <para><b>The record is written before the staging relation it describes, never after.</b> A
/// record with no relation is inert — the reclaimer finds nothing to drop and removes it. A
/// relation with no record is an untracked leak: nothing names it, so nothing will ever reclaim
/// its key-space. The ordering is the whole point of the record.</para>
///
/// <para>Each write opens and commits its own transaction, because a refresh job is not part of
/// the caller's DDL transaction: the record must survive whether or not the refresh that wrote it
/// goes on to succeed.</para>
/// </summary>
internal static class MaterializedViewRefreshJobStore
{
    /// <summary>
    /// Records that a refresh is building into a staging relation, so that relation has a durable owner
    /// even if this process never runs again. Written before the staging relation is created: a record
    /// with no relation is inert, whereas a relation with no record is an untracked leak.
    /// </summary>
    internal static async Task PersistRefreshJobAsync(DatabaseDescriptor database, MaterializedViewRefreshJob job)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
        try
        {
            byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.MaterializedViewRefreshJob);
            await MetaKeyWriter.WriteMetaKey(database.Kahuna.Kahuna, tx, MetaKeys.RefreshJobKey(database.Id, job.ViewTableId), bytes).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>Removes a refresh job record: the attempt finished, or its staging storage was reclaimed.</summary>
    internal static async Task DeleteRefreshJobAsync(DatabaseDescriptor database, string viewTableId)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
        try
        {
            await MetaKeyWriter.DeleteMetaKey(database.Kahuna.Kahuna, tx, MetaKeys.RefreshJobKey(database.Id, viewTableId)).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>Reads the refresh job recorded for one materialized view, or null when none is in flight.</summary>
    internal static async Task<MaterializedViewRefreshJob?> TryGetRefreshJobAsync(DatabaseDescriptor database, string viewTableId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, MetaKeys.RefreshJobKey(database.Id, viewTableId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is not { Length: > 0 })
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.MaterializedViewRefreshJob);
    }

    /// <summary>
    /// Every refresh job recorded in this database. Used by the background reclaimer, which is what
    /// covers the cases a later refresh cannot: a materialized view that was dropped, or that is simply
    /// never refreshed again.
    /// </summary>
    internal static Task<List<MaterializedViewRefreshJob>> ListRefreshJobsAsync(DatabaseDescriptor database)
        => ListRefreshJobsAsync(database.Kahuna.Kahuna, database.Id);

    /// <summary>
    /// The same listing, by database id, for callers that do not have the database open.
    /// </summary>
    /// <remarks>
    /// Exists so the background sweep can ask "is there anything to reclaim here" without opening every
    /// database on every tick — which is real work, and on a busy node is also avoidable contention.
    /// </remarks>
    internal static async Task<List<MaterializedViewRefreshJob>> ListRefreshJobsAsync(IKahuna kahuna, string dbId)
    {
        string metaBucket = MetaKeys.MetaBucketPrefix(dbId);
        string prefix = MetaKeys.RefreshJobKeyPrefix(dbId);
        List<MaterializedViewRefreshJob> jobs = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            jobs.Add(MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.MaterializedViewRefreshJob));
        }

        return jobs;
    }
}
