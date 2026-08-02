
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using Nito.AsyncEx;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A DROP whose descriptor is missing from the local cache or whose load lazy has faulted must NOT be
/// reported as a verified purge. It must fall back to a headless purge-by-id so the keyspace is actually
/// reclaimed, and keep the recovery marker (return false) when it cannot purge.
/// </summary>
public sealed class TestDropHeadlessPurge : BaseTest
{
    private static async Task<int> CountKeysUnderAsync(DatabaseDescriptor readVia, string bucket, string keyPrefix)
    {
        IKahuna kahuna = readVia.Kahuna.Kahuna;
        KvTransaction tx = await readVia.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
        try
        {
            int count = 0;
            await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                tx.TransactionId, bucket, null, true, null, true, 512,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                    count++;
            }
            return count;
        }
        finally
        {
            await readVia.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    private async Task<(string dbId, string tableId, DatabaseDescriptor db)> CreateDbWithRows()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < 3; i++)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                tx, dbName, $"INSERT INTO t (id, name) VALUES (gen_id(), \"row{i}\")", null));
            await db.Transactions.CommitAsync(tx);
        }

        string tableId = db.Schema.Tables["t"].Id!;
        return (db.Id, tableId, db);
    }

    /// <summary>An absent descriptor (empty cache) must purge the keyspace headlessly, not report success.</summary>
    [Test]
    [NonParallelizable]
    public async Task Drop_AbsentDescriptor_PurgesHeadlessly()
    {
        (string dbId, string tableId, DatabaseDescriptor db) = await CreateDbWithRows();

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(3), "sanity: rows were written");

        // A dropper whose descriptor cache does not contain the id → the absent path.
        DatabaseDropper dropper = new(new DatabaseDescriptors(), logger, Options);
        bool purged = await dropper.Drop(dbId, purge: true, headlessKahuna: TestNode!.Kahuna);

        Assert.That(purged, Is.True, "the headless purge must verifiably complete");
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(0),
            "an absent descriptor must still reclaim the keyspace, not leak it");
    }

    /// <summary>A faulted load lazy must also purge headlessly rather than declaring the drop complete.</summary>
    [Test]
    [NonParallelizable]
    public async Task Drop_FaultedDescriptorLazy_PurgesHeadlessly()
    {
        (string dbId, string tableId, DatabaseDescriptor db) = await CreateDbWithRows();

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(3), "sanity: rows were written");

        // Seed the dropper's cache with a lazy that faults on await (simulates a failed LoadDatabase).
        DatabaseDescriptors descriptors = new();
        descriptors.Descriptors[dbId] = new AsyncLazy<DatabaseDescriptor>(() =>
            throw new InvalidOperationException("injected load failure"));

        DatabaseDropper dropper = new(descriptors, logger, Options);
        bool purged = await dropper.Drop(dbId, purge: true, headlessKahuna: TestNode!.Kahuna);

        Assert.That(purged, Is.True, "the headless purge must verifiably complete for a faulted descriptor");
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(0),
            "a faulted descriptor must still reclaim the keyspace, not leak it while clearing the marker");
    }

    /// <summary>
    /// A writer that holds a use-reference past the drain timeout must not have the keyspace purged out
    /// from under it: the drop reports incomplete (so the caller keeps the recovery marker) and no key is
    /// deleted while the writer is still live. Once the writer drains, a resume can finish the purge.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Drop_DrainTimeout_WithLiveWriter_DefersPurge()
    {
        (string dbId, string tableId, DatabaseDescriptor db) = await CreateDbWithRows();

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(3), "sanity: rows were written");

        // Simulate an in-flight operation that outlives the drain window by holding a use-reference.
        DatabaseUseHandle liveWriter = db.Use();
        try
        {
            // Route the drop through a dropper that holds this live descriptor so it takes the drain path.
            DatabaseDescriptors descriptors = new();
            descriptors.Descriptors[dbId] = new AsyncLazy<DatabaseDescriptor>(() => Task.FromResult(db));
            DatabaseDropper dropper = new(descriptors, logger, Options);

            bool purged = await dropper.Drop(
                dbId, purge: true, headlessKahuna: TestNode!.Kahuna, drainTimeout: TimeSpan.FromMilliseconds(300));

            Assert.That(purged, Is.False,
                "a drop whose writers never drain must report incomplete, not a completed purge");
            Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(3),
                "no key may be purged while a writer is still live");
        }
        finally
        {
            liveWriter.Dispose();
        }

        // With the writer drained, a resume (fresh dropper, headless) can now finish the purge.
        DatabaseDropper resumeDropper = new(new DatabaseDescriptors(), logger, Options);
        bool resumed = await resumeDropper.Drop(dbId, purge: true, headlessKahuna: TestNode!.Kahuna);
        Assert.That(resumed, Is.True, "once the writer drains, the purge can complete");
        Assert.That(await CountKeysUnderAsync(db, rowBucket, rowPrefix), Is.EqualTo(0), "resume must reclaim the keyspace");
    }

    /// <summary>With no shared node to purge through, the drop must report incomplete (keep the marker).</summary>
    [Test]
    [NonParallelizable]
    public async Task Drop_AbsentDescriptor_NoSharedNode_ReportsIncomplete()
    {
        (string dbId, _, _) = await CreateDbWithRows();

        DatabaseDropper dropper = new(new DatabaseDescriptors(), logger, Options);
        bool purged = await dropper.Drop(dbId, purge: true, headlessKahuna: null);

        Assert.That(purged, Is.False,
            "without a node to purge through, the drop is unverified and must keep the recovery marker");
    }
}
