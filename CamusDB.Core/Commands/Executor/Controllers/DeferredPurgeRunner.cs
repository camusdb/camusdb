
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Storage.Kv;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Removes the data that published change sets made unreachable, from their
/// <see cref="DeferredPurgeRecord"/>s. Two paths call it:
/// <list type="bullet">
/// <item>The committing node, right after its publish, for the records of its own change set
/// (<see cref="RunAsync"/>). This is the normal path.</item>
/// <item>The coordinator-job sweep on the schema leader (<see cref="RunDueAsync"/>), for records that a
/// crash, a lost connection or a failed run left behind. It survives a restart and a leader change,
/// because the records are in KV and any node can finish them.</item>
/// </list>
///
/// <para>Both paths can run the same record at once. That is safe: the removal is a verified,
/// idempotent purge of one key space, and the record is deleted only after the key space is confirmed
/// empty, so an interrupted run leaves the record for the next one.</para>
/// </summary>
internal sealed class DeferredPurgeRunner
{
    private readonly DatabaseDropper dropper;

    private readonly ILogger<ICamusDB> logger;

    internal DeferredPurgeRunner(DatabaseDropper dropper, ILogger<ICamusDB> logger)
    {
        this.dropper = dropper;
        this.logger = logger;
    }

    /// <summary>
    /// Test-only. When set, <see cref="RunAsync"/> does nothing, as if the committing node stopped right
    /// after its publish, so a test can check that <see cref="RunDueAsync"/> finishes the records.
    /// </summary>
    internal bool TestSkipRunsAfterPublish { get; set; }

    /// <summary>
    /// Runs the records of one change set on the node that published it. A record that is not done is left
    /// for the sweep, and the database is marked so the next sweep looks for it.
    /// </summary>
    internal async Task RunAsync(DatabaseDescriptor database, IReadOnlyList<DeferredPurgeRecord> records, CancellationToken ct = default)
    {
        if (TestSkipRunsAfterPublish || records.Count == 0)
            return;

        foreach (DeferredPurgeRecord record in records)
        {
            if (!await TryRunAsync(database, record, ct).ConfigureAwait(false))
                database.MarkDeferredPurgesDue();
        }
    }

    /// <summary>
    /// Runs every record of <paramref name="database"/> that is due, and returns how many it finished.
    /// Scans the meta bucket only when the database was marked (<see cref="DatabaseDescriptor.MarkDeferredPurgesDue"/>);
    /// a record that is not due yet, or not done, marks it again for the next run.
    /// </summary>
    internal async Task<int> RunDueAsync(DatabaseDescriptor database, CancellationToken ct = default)
    {
        if (!database.TakeDeferredPurgesDue())
            return 0;

        List<DeferredPurgeRecord> records;
        try
        {
            records = await ScanAsync(database, ct).ConfigureAwait(false);
        }
        catch
        {
            database.MarkDeferredPurgesDue();
            throw;
        }

        int finished = 0;
        bool left = false;

        foreach (DeferredPurgeRecord record in records)
        {
            if (await TryRunAsync(database, record, ct).ConfigureAwait(false))
                finished++;
            else
                left = true;
        }

        if (left)
            database.MarkDeferredPurgesDue();

        return finished;
    }

    /// <summary>
    /// True once the change set of <paramref name="record"/> ended on this node; see
    /// <see cref="DeferredPurgeRecord.BaseVersion"/>. A committed change set always ends in a publish, so
    /// an ended one is a published one.
    /// </summary>
    private static bool IsDue(DatabaseDescriptor database, DeferredPurgeRecord record)
    {
        if (database.Schema.PendingChangeSet is { } pending
            && string.Equals(pending.ChangeSetId, record.ChangeSetId, StringComparison.Ordinal))
            return false;

        return database.Schema.SchemaVersion >= record.BaseVersion + 2;
    }

    /// <summary>Removes the data of one due record, then the record. False when it is not due or not done.</summary>
    private async Task<bool> TryRunAsync(DatabaseDescriptor database, DeferredPurgeRecord record, CancellationToken ct)
    {
        if (!IsDue(database, record))
            return false;

        try
        {
            IKahuna kahuna = database.Kahuna.Kahuna;

            bool removed = record.Kind switch
            {
                DeferredPurgeKind.IndexEntries => await PurgeIndexesAsync(kahuna, database, record, ct).ConfigureAwait(false),

                // The table key, its orphan record and its owned sequences are already gone with the
                // publish, so the purge finds them absent; it removes the data, the statistics and the
                // key-space records. The record names every index, because a branch database has no
                // key-space catalog of its own for a table it inherited.
                DeferredPurgeKind.TableData => await dropper.PurgeTableKeyspaceAsync(
                    kahuna, database.Id, record.TableId, record.StorageId, ct, record.IndexIds).ConfigureAwait(false),

                _ => false,
            };

            if (!removed)
            {
                logger.LogWarning(
                    "Deferred purge {PurgeId} ({Kind}) of table {TableId} in database {Database} is not complete; a later run retries it",
                    record.PurgeId, record.Kind, record.TableId, database.Name);
                return false;
            }

            return await dropper.DeleteKeyVerifiedAsync(kahuna, MetaKeys.PurgeKey(database.Id, record.PurgeId), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Deferred purge {PurgeId} ({Kind}) of table {TableId} in database {Database} failed; a later run retries it",
                record.PurgeId, record.Kind, record.TableId, database.Name);
            return false;
        }
    }

    /// <summary>
    /// Purges the entries of one index, by storage id and stable index id, and returns true when the key
    /// space is confirmed empty. For an index that leaves the schema without a purge record: one that a
    /// transaction staged on a shared table and never published.
    /// </summary>
    internal Task<bool> PurgeIndexKeySpaceAsync(DatabaseDescriptor database, string storageId, string indexId, CancellationToken ct = default) =>
        dropper.PurgeKeySpaceAsync(database.Kahuna.Kahuna, database.Id, KvKeyBuilder.IndexSpaceOf(database.Id, storageId, indexId), ct);

    /// <summary>Purges the key space of every index the record names; true only when all are confirmed empty.</summary>
    private async Task<bool> PurgeIndexesAsync(IKahuna kahuna, DatabaseDescriptor database, DeferredPurgeRecord record, CancellationToken ct)
    {
        bool complete = true;

        foreach (string indexId in record.IndexIds)
            complete &= await dropper.PurgeKeySpaceAsync(
                kahuna, database.Id, KvKeyBuilder.IndexSpaceOf(database.Id, record.StorageId, indexId), ct).ConfigureAwait(false);

        return complete;
    }

    /// <summary>Reads every purge record of <paramref name="database"/>, with a plain read of the latest committed keys.</summary>
    private static async Task<List<DeferredPurgeRecord>> ScanAsync(DatabaseDescriptor database, CancellationToken ct)
    {
        string prefix = MetaKeys.PurgeKeyPrefix(database.Id);
        List<DeferredPurgeRecord> records = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            MetaKeys.MetaBucketPrefix(database.Id),
            prefix, true,
            MetaKeys.PrefixUpperBound(prefix), false,
            128,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            ct).ConfigureAwait(false))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal) && entry.Value is { Length: > 0 } bytes)
                records.Add(MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.DeferredPurgeRecord));
        }

        return records;
    }
}
