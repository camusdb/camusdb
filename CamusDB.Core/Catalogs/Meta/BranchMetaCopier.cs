
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
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Copies a source database's metadata namespace into a new branch's namespace, so the branch opens
/// with the schema the source had at the fork point.
///
/// <para><b>The read is taken as-of the fork timestamp, not as-of now.</b> A branch is a
/// copy-on-write fork of a point in time; metadata committed to the source after that point must
/// not appear in the branch, or the branch would open with a schema describing rows its key-space
/// does not contain.</para>
///
/// <para><b>The caller holds the source's <c>SchemaDdlSemaphore</c> across this call.</b> Without
/// it a concurrent DDL on the source could commit between the individual key reads, and the branch
/// would be assembled from two different schema versions.</para>
///
/// <para>The caller is also responsible for the ordering around this method: a durable pending
/// marker is written <em>before</em> it runs, so a crash part-way through leaves a namespace that
/// startup recovery can find and purge. A half-copied namespace with no marker is unreclaimable.
/// </para>
/// </summary>
internal static class BranchMetaCopier
{
    /// <summary>
    /// Copies the <b>published</b> schema metadata of <paramref name="source"/> into the namespace
    /// identified by <paramref name="branchDbId"/>, rewriting only the database-id segment of each key.
    /// This produces an independent, consistent schema starting point for a branch database.
    ///
    /// <para><b>Not every key in the source's meta bucket is part of its published schema.</b> Some
    /// keys record work the source is doing <em>right now</em>: a schema-change coordinator job, the
    /// keyspace catalog (routing bookkeeping), and a materialized-view refresh job together with the
    /// staging relation it is building into. Those describe an operation owned by the source, fenced
    /// by keys in the source's own namespace; copied into a branch they describe nothing that is
    /// happening there, and a copied refresh job is worse than inert — the branch's abandoned-refresh
    /// sweep finds a record whose fence (keyed by the branch's id) nobody holds, concludes the run is
    /// dead, and <em>rebuilds the branch's materialized view</em> from whatever the branch's base
    /// tables hold at that moment. The user never asked for it, and it replaces the fork-time contents
    /// with later branch writes. See <see cref="IsSourceWorkInProgress"/> for the exact exclusion.</para>
    ///
    /// <para>Leaving the refresh work out is consistent in every interleaving with the source's
    /// refresh, because the record is written before the staging relation exists and deleted after
    /// the swap has removed that relation from the schema: at any <paramref name="forkT"/> the view's
    /// own record either still names its pre-refresh storage (the branch reads the old contents
    /// through ancestry) or already names the rebuilt storage (whose rows all predate the swap and so
    /// are visible at <paramref name="forkT"/>). Neither case needs the job or the staging relation.
    /// Dropping only the job while keeping the staging relation would strand a relation no statement
    /// can name and nothing owns, which is exactly the leak the record exists to prevent.</para>
    ///
    /// <para>The caller is responsible for holding the source's <c>SchemaDdlSemaphore</c> across this
    /// call to prevent a concurrent DDL on the same node from mutating the source schema between
    /// the stability check and the copy. In cluster mode a remote DDL can still commit between
    /// <paramref name="forkT"/> and the scan; reading at <paramref name="forkT"/> ensures the copy
    /// is consistent with the row/index snapshot the branch will read at that timestamp.</para>
    ///
    /// <para>The scan uses no live transaction (<c>HLCTimestamp.Zero</c> as the transaction id)
    /// and <paramref name="forkT"/> as the MVCC read timestamp. This matches the snapshot-read
    /// pattern used for ancestry reads in <see cref="KvTableStore"/> and guarantees the branch
    /// schema is consistent with the ancestor rows it inherits.</para>
    /// </summary>
    internal static async Task CopyMetaForBranchAsync(DatabaseDescriptor source, string branchDbId, HLCTimestamp forkT)
    {
        IKahuna kahuna = source.Kahuna.Kahuna;
        string sourceBucket = MetaKeys.MetaBucketPrefix(source.Id);
        string sourcePrefix = source.Id + "/meta/";
        string branchPrefix = branchDbId + "/meta/";
        string sourceCoordinatorPrefix = MetaKeys.CoordinatorKeyPrefix(source.Id);
        string sourceKeyspacePrefix = MetaKeys.KeyspaceCatalogKeyPrefix(source.Id);
        string sourceRefreshJobPrefix = MetaKeys.RefreshJobKeyPrefix(source.Id);
        string sourceTablePrefix = MetaKeys.TableKeyPrefix(source.Id);

        List<(string destKey, byte[] value)> toCopy = [];

        // Storage ids of relations that exist only because a refresh of the source is mid-rebuild.
        // Their table records are dropped as they are met; their history keys are dropped afterwards,
        // because history keys sort before table keys and so are met first.
        HashSet<string>? stagingTableIds = null;

        // Scan source metadata as-of forkT: any schema change committed after forkT is invisible,
        // so the branch gets exactly the schema the row/index snapshot at forkT reflects.
        // Uses HLCTimestamp.Zero as the transaction id (no live tx) and forkT as the read timestamp,
        // matching the ancestor-read pattern in KvBranchReader.ScanRowsRawAsync.
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            sourceBucket,
            null, true,
            null, true,
            512,
            forkT,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            if (key.StartsWith(sourceCoordinatorPrefix, StringComparison.Ordinal) ||
                key.StartsWith(sourceKeyspacePrefix, StringComparison.Ordinal))
                continue;

            if (!key.StartsWith(sourcePrefix, StringComparison.Ordinal))
                continue;

            if (IsSourceWorkInProgress(key, entry.Value, sourceRefreshJobPrefix, sourceTablePrefix, ref stagingTableIds))
                continue;

            string destKey = branchPrefix + key[sourcePrefix.Length..];
            toCopy.Add((destKey, entry.Value));
        }

        if (stagingTableIds is not null)
            RemoveStagingHistory(toCopy, branchDbId, stagingTableIds);

        if (toCopy.Count == 0)
            return;

        KvTransaction writeTx = await source.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            foreach ((string destKey, byte[] value) in toCopy)
                await MetaKeyWriter.WriteMetaKey(kahuna, writeTx, destKey, value).ConfigureAwait(false);

            await source.Transactions.CommitAsync(writeTx).ConfigureAwait(false);
        }
        finally
        {
            await source.Transactions.RollbackIfNotCompletedAsync(writeTx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// True for a key that records a materialized-view refresh the source has in flight: the job
    /// record itself, or the table record of the staging relation the rebuild writes into.
    /// </summary>
    /// <remarks>
    /// A staging relation is recognized two independent ways, so that neither the naming convention
    /// nor the ownership record has to be the only thing standing between a fork and a copied
    /// rebuild: by its name (every staging relation carries the prefix
    /// <see cref="MaterializedViewNaming.StagingPrefix"/>, which no user statement can produce), and
    /// by the storage id the job record names. A record whose staging relation does not exist yet, or
    /// a relation whose record is already gone, is still excluded by the other test.
    ///
    /// <para>The table record is decoded to read its name. That is one JSON parse per relation, once
    /// per fork — the copy is already O(schema size) and this does not change that.</para>
    /// </remarks>
    private static bool IsSourceWorkInProgress(
        string key,
        byte[] value,
        string refreshJobPrefix,
        string tablePrefix,
        ref HashSet<string>? stagingTableIds)
    {
        if (key.StartsWith(refreshJobPrefix, StringComparison.Ordinal))
        {
            MaterializedViewRefreshJob job = MetaJsonSerializer.Deserialize(value, MetaJsonContext.Default.MaterializedViewRefreshJob);
            if (!string.IsNullOrEmpty(job.StagingTableId))
                (stagingTableIds ??= new HashSet<string>(StringComparer.Ordinal)).Add(job.StagingTableId);
            return true;
        }

        if (!key.StartsWith(tablePrefix, StringComparison.Ordinal))
            return false;

        string tableId = key[tablePrefix.Length..];
        if (stagingTableIds is not null && stagingTableIds.Contains(tableId))
            return true;

        TableSchema table = MetaJsonSerializer.Deserialize(value, MetaJsonContext.Default.TableSchema);
        if (table.Name is null || !MaterializedViewNaming.IsStagingRelation(table.Name))
            return false;

        (stagingTableIds ??= new HashSet<string>(StringComparer.Ordinal)).Add(tableId);
        return true;
    }

    /// <summary>
    /// Drops the column-history keys of every excluded staging relation from the copy list. They are
    /// removed after the scan because history keys sort before the table record that identifies the
    /// relation as staging, so at the moment they are met nothing yet says they belong to one.
    /// </summary>
    private static void RemoveStagingHistory(
        List<(string destKey, byte[] value)> toCopy, string branchDbId, HashSet<string> stagingTableIds)
    {
        List<string> prefixes = new(stagingTableIds.Count);
        foreach (string stagingTableId in stagingTableIds)
            prefixes.Add(MetaKeys.HistoryKeyPrefix(branchDbId, stagingTableId));

        toCopy.RemoveAll(item =>
        {
            foreach (string prefix in prefixes)
            {
                if (item.destKey.StartsWith(prefix, StringComparison.Ordinal))
                    return true;
            }

            return false;
        });
    }
}
