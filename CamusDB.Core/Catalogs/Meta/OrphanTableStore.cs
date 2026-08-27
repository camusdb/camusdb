
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
/// Reads and writes the orphan record of a deferred-dropped relation. The record is what keeps a
/// detached table's data reclaimable by the garbage collector and re-attachable by a relink: once
/// the table is gone from the schema, this record is the only thing that still names its key-space.
///
/// <para>Writing an orphan record is not done here — it happens inside
/// <see cref="SchemaMetaStore.PersistDroppedTableAsync"/>, in the same transaction that deletes the
/// table's meta key, so a crash can never detach a table without leaving its recovery record.
/// This class covers the read and the delete halves.</para>
///
/// <para>The reads are lock-free point reads at a zero snapshot. They see the last committed value,
/// which is what a reclaim or relink decision needs, and they never block a concurrent DDL.</para>
/// </summary>
internal static class OrphanTableStore
{
    /// <summary>
    /// Deletes a table orphan record within <paramref name="tx"/>. Called on relink (the table is being
    /// reattached) and by the GC after the table's data has been purged.
    /// </summary>
    internal static async Task DeleteTableOrphanAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.OrphanKey(database.Id, tableId)).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the orphan record for <paramref name="tableId"/>, or <c>null</c> if none exists. Lock-free
    /// point read (zero-identity snapshot); used by relink and the GC to confirm the record before acting.
    /// </summary>
    internal static async Task<OrphanTableRecord?> TryGetTableOrphanAsync(DatabaseDescriptor database, string tableId)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, MetaKeys.OrphanKey(database.Id, tableId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.OrphanTableRecord);
    }

    /// <summary>
    /// Scans the database meta bucket and returns every orphaned-table record. Backing store for
    /// <c>SHOW ORPHAN TABLES</c> and the GC reclamation sweep.
    /// </summary>
    internal static async Task<List<OrphanTableRecord>> LoadTableOrphansAsync(DatabaseDescriptor database)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string metaBucket = MetaKeys.MetaBucketPrefix(database.Id);
        string orphanPrefix = MetaKeys.OrphanKeyPrefix(database.Id);
        List<OrphanTableRecord> orphans = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(orphanPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            orphans.Add(MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.OrphanTableRecord));
        }

        return orphans;
    }
}
