
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Repairs a live descriptor whose in-memory schema fell behind the durable KV checkpoint.
///
/// <para><b>Why this exists.</b> Kommander delivers a committed schema-log entry exactly once, to
/// whatever subscriber exists at that instant, and never redelivers it. A database subscribes only
/// while it is open, and the subscription is registered after the checkpoint is read
/// (<c>DatabaseOpener.LoadDatabase</c>). An entry committed while the database is unopened, or
/// inside that load-to-register gap, therefore never reaches this node's catalog. The fence cannot
/// see the loss — <see cref="DatabaseDescriptor.HeadSchemaVersion"/> only advances on delivery — so
/// without this reconciler the node serves a stale schema forever and answers every request against
/// a missed table with <see cref="CamusDBErrorCodes.TableDoesntExist"/>.</para>
///
/// <para><b>How it repairs.</b> The proposer persists the <c>{db}/meta/*</c> checkpoint after every
/// committed delta, so the checkpoint version is a durable, cluster-visible floor on the committed
/// head. The reconciler probes that one key; when it is ahead of memory it loads a full snapshot
/// (no lock held across the KV reads), then installs it under <see cref="Schema.Semaphore"/> only
/// if the snapshot is still ahead. The version-check-under-lock makes the install monotonic: a
/// concurrent live apply that raced past the snapshot wins and the snapshot is discarded.</para>
///
/// <para><b>What it cannot repair.</b> A checkpoint that is itself behind the committed log
/// (persist exhaustion on the proposer) is repaired by WAL replay on restart, not here — this
/// reconciler never reads the log. It also must not run inside the schema apply pipeline: it
/// takes the schema lock, and the apply callback yields on that same lock.</para>
/// </summary>
internal static class SchemaFreshnessReconciler
{
    /// <summary>
    /// Cooldown between miss-triggered probes per database. A query against a genuinely
    /// nonexistent table must not turn into a KV read storm; one probe per second per database
    /// bounds the cost while a real staleness episode still heals within a second.
    /// </summary>
    internal const long MissTriggeredCooldownMs = 1_000;

    /// <summary>
    /// Compares the in-memory schema version with the persisted checkpoint version and installs a
    /// fresh checkpoint snapshot when memory is behind. Returns true only when a newer snapshot was
    /// installed. Safe to call concurrently: a per-descriptor gate makes the check single-flight,
    /// and <paramref name="cooldownMs"/> additionally rate-limits repeated probes (0 disables the
    /// cooldown but keeps the single-flight gate).
    /// </summary>
    internal static async Task<bool> TryReconcileAsync(
        DatabaseDescriptor database, long cooldownMs, ILogger<ICamusDB> logger)
    {
        if (!database.TryEnterSchemaFreshnessCheck(cooldownMs))
            return false;

        try
        {
            long? persisted = await SchemaLoader.TryReadPersistedVersionAsync(database).ConfigureAwait(false);
            if (persisted is null || persisted.Value <= database.Schema.SchemaVersion)
                return false;

            Diagnostics.SchemaDiag.Log(
                $"FRESHNESS-STALE node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} " +
                $"localVer={database.Schema.SchemaVersion} persistedVer={persisted.Value}");

            // The snapshot is read WITHOUT the schema lock: the apply pipeline yields on that lock,
            // and holding it across KV reads stalls the schema partition. The price is that a live
            // apply can advance memory while the snapshot loads; the re-check under the lock below
            // resolves that race monotonically.
            SchemaSnapshot snapshot = await SchemaLoader.LoadSnapshotAsync(database).ConfigureAwait(false);

            List<string>? invalidatedTableIds = null;
            long staleVersion;

            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                if (!snapshot.HasPersistedSchema || snapshot.SchemaVersion <= database.Schema.SchemaVersion)
                    return false;

                staleVersion = database.Schema.SchemaVersion;

                // Everything the swap retires or introduces must drop out of the per-node caches;
                // collect the union of both table sets before the maps are replaced.
                invalidatedTableIds = CollectTableIds(database.Schema.Tables, snapshot.Tables);

                database.Schema.SchemaVersion = snapshot.SchemaVersion;
                database.Schema.Tables = snapshot.Tables;
                database.Schema.Views = snapshot.Views;
                if (snapshot.System is not null)
                    database.SystemSchema = snapshot.System;

                database.Schema.RebuildRelationNameIndex();
                SchemaLoader.MigrateIndexesFromSystemSchema(database);

                // The swap replaced every TableSchema instance; any open descriptor captured the
                // old references and must be rebuilt on next access.
                database.TableDescriptors.Clear();
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            // The missed deltas' targeted invalidations never ran on this node, so invalidate
            // broadly: cached results and statistics for every relation the swap touched.
            foreach (string tableId in invalidatedTableIds)
            {
                database.Cache?.InvalidateByTableId(database.Id, tableId);
                database.EvictTableStatistics?.Invoke(tableId);
            }

            // Keep the fence invariant HeadSchemaVersion ≥ SchemaVersion, and tell the schema
            // leader's ack gate this node reached the checkpoint version — the deltas that would
            // have acked were never delivered here.
            database.ObserveSchemaEntryHead(snapshot.SchemaVersion);
            database.Kahuna.RecordAndPublishSchemaApplied(database.Id, snapshot.SchemaVersion);

            logger.LogWarning(
                "Schema for database '{Db}' was stale on this node (memory at {OldVersion}, checkpoint at {NewVersion}); " +
                "reloaded from the durable checkpoint. This indicates committed schema changes were never delivered to this node",
                database.Name, staleVersion, snapshot.SchemaVersion);

            Diagnostics.SchemaDiag.Log(
                $"FRESHNESS-RECONCILED node={database.Kahuna.Raft.GetLocalEndpoint()} db={database.Name} " +
                $"newVer={snapshot.SchemaVersion}");

            return true;
        }
        finally
        {
            database.ExitSchemaFreshnessCheck();
        }
    }

    private static List<string> CollectTableIds(
        Dictionary<string, TableSchema> oldTables, Dictionary<string, TableSchema> newTables)
    {
        HashSet<string> ids = new(oldTables.Count + newTables.Count, StringComparer.Ordinal);

        foreach (TableSchema table in oldTables.Values)
        {
            if (table.Id is { Length: > 0 } id)
                ids.Add(id);
        }

        foreach (TableSchema table in newTables.Values)
        {
            if (table.Id is { Length: > 0 } id)
                ids.Add(id);
        }

        return [.. ids];
    }
}
