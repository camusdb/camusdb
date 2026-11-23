
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
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
/// Does the work that published change sets left in their <see cref="DeferredPurgeRecord"/>s: the removal of
/// data they made unreachable, and the change of a sequence's counter. Two paths call it:
/// <list type="bullet">
/// <item>The committing node, right after its publish, for the records of its own change set. This is the
/// normal path. A removal runs behind the commit (<see cref="Schedule"/>): it takes time in proportion to
/// the data, the record is durable already, and nobody can reach the data any more, so the client does not
/// wait for it. A change of a counter runs before <c>COMMIT</c> returns (<see cref="RunNowAsync"/>), so the
/// client's next draw follows the new definition.</item>
/// <item>The coordinator-job sweep on the schema leader (<see cref="RunDueAsync"/>), for records that a
/// crash, a lost connection or a failed run left behind. It survives a restart and a leader change,
/// because the records are in KV and any node can finish them.</item>
/// </list>
///
/// <para>Two nodes can run the same record at once; one node runs it once at a time. That is safe: the
/// removal is a verified, idempotent purge of one key space, and the record is deleted only after the key
/// space is confirmed empty, so an interrupted run leaves the record for the next one.</para>
///
/// <para>A scheduled run stops when its database closes or this runner is disposed, and marks the database
/// so that a sweep finishes the record.</para>
/// </summary>
internal sealed class DeferredPurgeRunner : IAsyncDisposable
{
    /// <summary>How many scheduled records this node removes at once, so a batch of drops does not flood the store.</summary>
    private const int MaxScheduledRuns = 2;

    private readonly DatabaseDropper dropper;

    private readonly SequenceAllocator sequences;

    private readonly ILogger<ICamusDB> logger;

    private readonly SemaphoreSlim scheduledRuns = new(MaxScheduledRuns, MaxScheduledRuns);

    /// <summary>The scheduled runs that have not ended, so the disposal can wait for them.</summary>
    private readonly ConcurrentDictionary<Task, byte> scheduled = new();

    /// <summary>The records a run of this node is removing, by purge id.</summary>
    private readonly ConcurrentDictionary<string, byte> inProgress = new(StringComparer.Ordinal);

    private readonly CancellationTokenSource stopping = new();

    internal DeferredPurgeRunner(DatabaseDropper dropper, SequenceAllocator sequences, ILogger<ICamusDB> logger)
    {
        this.dropper = dropper;
        this.sequences = sequences;
        this.logger = logger;
    }

    /// <summary>
    /// Test-only. When set, <see cref="Schedule"/> and <see cref="RunNowAsync"/> do nothing, as if the
    /// committing node stopped right after its publish, so a test can check that <see cref="RunDueAsync"/>
    /// finishes the records.
    /// </summary>
    internal bool TestSkipRunsAfterPublish { get; set; }

    /// <summary>
    /// Test-only. Awaited before each purge starts, so a test can hold the removal of the data and look at
    /// the state that a client sees meanwhile. Null in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforePurge { get; set; }

    /// <summary>Test-only. Waits for the scheduled runs, so a test can look at what they leave.</summary>
    internal Task WaitForScheduledRunsForTestsAsync() => Task.WhenAll(scheduled.Keys);

    /// <summary>
    /// Starts the removal of one record on the node that published its change set, and returns at once. A
    /// record that is not done is left for the sweep, and the database is marked so the next sweep looks
    /// for it.
    /// </summary>
    internal void Schedule(DatabaseDescriptor database, DeferredPurgeRecord record)
    {
        if (TestSkipRunsAfterPublish)
            return;

        if (stopping.IsCancellationRequested)
        {
            database.MarkDeferredPurgesDue();
            return;
        }

        // The run outlives the statement and the request that committed, so it takes none of their
        // ambient state with it (the private schema of the transaction, the principal, the trace).
        Task run;
        if (ExecutionContext.IsFlowSuppressed())
        {
            run = Task.Run(() => RunScheduledAsync(database, record));
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                run = Task.Run(() => RunScheduledAsync(database, record));
        }

        scheduled.TryAdd(run, 0);
        _ = run.ContinueWith(
            static (ended, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(ended, out _),
            scheduled, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    /// <summary>
    /// Does the work of one record on the node that published its change set, and returns when it is done or
    /// failed. Never throws: the transaction committed, and a record that is not done is left for the sweep.
    /// </summary>
    internal async Task RunNowAsync(DatabaseDescriptor database, DeferredPurgeRecord record)
    {
        if (TestSkipRunsAfterPublish)
            return;

        bool done = false;

        try
        {
            using CancellationTokenSource ends = CancellationTokenSource.CreateLinkedTokenSource(stopping.Token, database.Lifetime);
            using DatabaseUseHandle _ = database.Use();
            done = await TryRunAsync(database, record, ends.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
        {
            // The database closed or the engine stops; the record is durable and a sweep finishes it.
        }
        finally
        {
            if (!done)
                database.MarkDeferredPurgesDue();
        }
    }

    private async Task RunScheduledAsync(DatabaseDescriptor database, DeferredPurgeRecord record)
    {
        bool done = false;

        try
        {
            using CancellationTokenSource ends = CancellationTokenSource.CreateLinkedTokenSource(stopping.Token, database.Lifetime);

            await scheduledRuns.WaitAsync(ends.Token).ConfigureAwait(false);
            try
            {
                using DatabaseUseHandle _ = database.Use();
                done = await TryRunAsync(database, record, ends.Token).ConfigureAwait(false);
            }
            finally
            {
                scheduledRuns.Release();
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
        {
            // The database closed or the engine stops; the record is durable and a sweep finishes it.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "The scheduled purge {PurgeId} in database {Database} failed; a later run retries it", record.PurgeId, database.Name);
        }
        finally
        {
            if (!done)
                database.MarkDeferredPurgesDue();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await stopping.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(scheduled.Keys).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "A scheduled purge failed while the engine stopped");
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

    /// <summary>
    /// Does the work of one due record, then removes the record. False when it is not due or not done, or
    /// when another run of this node is at it.
    /// </summary>
    private async Task<bool> TryRunAsync(DatabaseDescriptor database, DeferredPurgeRecord record, CancellationToken ct)
    {
        if (!IsDue(database, record) || !inProgress.TryAdd(record.PurgeId, 0))
            return false;

        try
        {
            if (TestInterceptBeforePurge is { } intercept)
                await intercept().ConfigureAwait(false);

            IKahuna kahuna = database.Kahuna.Kahuna;

            bool done = record.Kind switch
            {
                DeferredPurgeKind.IndexEntries => await PurgeIndexesAsync(kahuna, database, record, ct).ConfigureAwait(false),

                // The table key, its orphan record and its owned sequences are already gone with the
                // publish, so the purge finds them absent; it removes the data, the statistics and the
                // key-space records. The record names every index, because a branch database has no
                // key-space catalog of its own for a table it inherited.
                DeferredPurgeKind.TableData => await dropper.PurgeTableKeyspaceAsync(
                    kahuna, database.Id, record.TableId, record.StorageId, ct, record.IndexIds).ConfigureAwait(false),

                DeferredPurgeKind.SequenceCounter => await AlignCounterAsync(kahuna, database, record, ct).ConfigureAwait(false),

                _ => false,
            };

            if (!done)
            {
                logger.LogWarning(
                    "Deferred purge {PurgeId} ({Kind}) of {Object} in database {Database} is not complete; a later run retries it",
                    record.PurgeId, record.Kind, ObjectOf(record), database.Name);
                return false;
            }

            return await dropper.DeleteKeyVerifiedAsync(kahuna, MetaKeys.PurgeKey(database.Id, record.PurgeId), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Deferred purge {PurgeId} ({Kind}) of {Object} in database {Database} failed; a later run retries it",
                record.PurgeId, record.Kind, ObjectOf(record), database.Name);
            return false;
        }
        finally
        {
            inProgress.TryRemove(record.PurgeId, out _);
        }
    }

    private static string ObjectOf(DeferredPurgeRecord record) =>
        record.Kind == DeferredPurgeKind.SequenceCounter ? "sequence " + record.SequenceId : "table " + record.TableId;

    /// <summary>
    /// Brings the counter of the record's sequence to the definition the shared schema holds now, or removes
    /// it when the sequence is gone; see <see cref="DeferredPurgeKind.SequenceCounter"/>.
    /// </summary>
    private async Task<bool> AlignCounterAsync(IKahuna kahuna, DatabaseDescriptor database, DeferredPurgeRecord record, CancellationToken ct)
    {
        if (database.SharedSchema.FindSequenceById(record.SequenceId) is { } sequence)
            await sequences.AlignAsync(kahuna, database.Id, sequence, record.SetsStartValue, ct).ConfigureAwait(false);
        else
            await sequences.DeleteAsync(kahuna, database.Id, record.SequenceId, ct).ConfigureAwait(false);

        return true;
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
