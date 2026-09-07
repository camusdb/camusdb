
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
    /// The form of a table's schema that is written to its meta key: everything except the retained
    /// column history, which lives in its own per-version keys.
    /// </summary>
    /// <remarks>
    /// This projection is the durable record of the relation, so every field that must survive a
    /// reopen has to be listed here — a field omitted is silently lost on the next restart while
    /// looking perfectly correct in memory for the whole life of the process. That is why the
    /// materialized-view fields are here: without them a materialized view would come back from disk
    /// as an ordinary table, readable and writable and with no way left to refresh it.
    /// </remarks>
    /// <summary>
    /// Copies all schema metadata keys from <paramref name="source"/> into the namespace identified
    /// by <paramref name="branchDbId"/>, rewriting only the database-id segment of each key.
    /// This produces an independent, consistent schema starting point for a branch database.
    ///
    /// Keys under the coordinator prefix (in-flight DDL state specific to the source) and the
    /// keyspace prefix (Kommander routing metadata) are skipped; all other keys — version, system,
    /// table, and history — are copied verbatim so the branch opens with the same schema as the
    /// source had at the fork point.
    ///
    /// The caller is responsible for holding the source's <c>SchemaDdlSemaphore</c> across this
    /// call to prevent a concurrent DDL on the same node from mutating the source schema between
    /// the stability check and the copy. In cluster mode a remote DDL can still commit between
    /// <paramref name="forkT"/> and the scan; reading at <paramref name="forkT"/> ensures the copy
    /// is consistent with the row/index snapshot the branch will read at that timestamp.
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

        List<(string destKey, byte[] value)> toCopy = [];

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

            string destKey = branchPrefix + key[sourcePrefix.Length..];
            toCopy.Add((destKey, entry.Value));
        }

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
}
