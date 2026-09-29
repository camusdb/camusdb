/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Orphan database records (deferred drop / relink / GC reclamation).
///
/// <para>A non-FORCE DROP DATABASE of a root database does not purge its keyspace; it unregisters the
/// name and writes an orphan record so the id + data remain recoverable via
/// <c>CREATE DATABASE ... RELINK TO {id}</c> until the garbage collector reclaims them after the
/// retention window. Records are KV-only (no in-memory cache): SHOW ORPHAN DATABASES and the GC
/// scan them on demand, which keeps orphan state out of the schema-replication path.</para>
///
/// <para>Keys: <c>_system/dbregistry/orphan:{dbId}</c> (value is a serialized
/// <see cref="OrphanDatabaseRecord"/>)</para>
/// </summary>
public sealed class RegistryOrphanRecords
{
    private readonly RegistryKeyspace keyspace;

    internal RegistryOrphanRecords(RegistryKeyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    private string OrphanKeyPrefix => keyspace.Key("orphan:");

    private string OrphanKey(string dbId) => $"{OrphanKeyPrefix}{dbId}";

    /// <summary>
    /// Persists an orphan record for a deferred-dropped database. Written <em>before</em>
    /// <see cref="DatabaseRegistry.UnregisterAsync"/> so a crash between the two leaves the database
    /// still live (stale record, harmless) rather than data stranded with no recovery path.
    /// Idempotent: a repeated drop simply refreshes the record (and its
    /// <see cref="OrphanDatabaseRecord.DroppedAt"/>).
    /// </summary>
    public Task WriteDatabaseOrphanAsync(OrphanDatabaseRecord record) =>
        keyspace.SetAsync(
            OrphanKey(record.Id),
            MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.OrphanDatabaseRecord),
            $"Failed to write orphan record for database id '{record.Id}'");

    /// <summary>
    /// Reads the orphan record for <paramref name="dbId"/>, or <c>null</c> if none exists (never
    /// dropped as an orphan, or already reclaimed). Used by relink and by the GC to re-check the
    /// record still exists before acting.
    /// </summary>
    public async Task<OrphanDatabaseRecord?> TryGetDatabaseOrphanAsync(string dbId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await keyspace.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, OrphanKey(dbId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.OrphanDatabaseRecord);
    }

    /// <summary>
    /// Removes the orphan record for <paramref name="dbId"/>. Called on relink (recovery) and after a
    /// GC purge completes. Best-effort: a failure is swallowed; a stranded record is re-evaluated on the
    /// next GC pass and blocks nothing (the id is already unregistered).
    /// </summary>
    public Task DeleteDatabaseOrphanAsync(string dbId) => keyspace.DeleteBestEffortAsync(OrphanKey(dbId));

    /// <summary>
    /// Scans the registry bucket and returns every orphaned-database record. Backing store for
    /// <c>SHOW ORPHAN DATABASES</c> and the GC reclamation sweep. Reads through the synthetic
    /// read-only identity (see <see cref="DatabaseRegistry.ScanAllEntriesAsync"/>), never a read-write
    /// transaction: a read-write scan binds an OCC snapshot to every registry key it touches, and a
    /// concurrent registry writer — relink versus GC contention is the ordinary case — that commits
    /// past a bound snapshot aborts the whole scan. The reclaimer re-confirms each orphan under its
    /// per-id fence before acting, so per-key read-committed reads are sufficient here. Transient
    /// scan failures are absorbed by <see cref="RegistryKeyspace.RetryTransientScanAsync"/>.
    /// </summary>
    public Task<List<OrphanDatabaseRecord>> LoadDatabaseOrphansAsync() => RegistryKeyspace.RetryTransientScanAsync(async () =>
    {
        string orphanPrefix = OrphanKeyPrefix;
        List<OrphanDatabaseRecord> orphans = [];

        KvTransaction tx = keyspace.Transactions.CreateReadOnlyTransaction();
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in keyspace.Kahuna.LocateAndScanRange(
                tx.TransactionId,
                keyspace.Bucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(orphanPrefix, StringComparison.Ordinal) || kve.Value is null)
                    continue;

                orphans.Add(MetaJsonSerializer.Deserialize(kve.Value, MetaJsonContext.Default.OrphanDatabaseRecord));
            }
        }
        finally
        {
            await keyspace.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return orphans;
    });
}
