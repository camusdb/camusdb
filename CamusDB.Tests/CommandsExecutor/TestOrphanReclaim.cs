
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Reclamation: the <c>OrphanReclaimer</c> garbage collector physically reclaims
/// deferred-dropped databases/tables once past <see cref="CamusDBOptions.OrphanRetentionMs"/>, leaves
/// unexpired ones alone, respects the disable switch, and (crash-window guard) never purges an object
/// that has since been relinked.
/// </summary>
[NonParallelizable]
internal sealed class TestOrphanReclaim : SharedNodeBaseTest
{



    private async Task<int> CountKeysAsync(string bucket, string keyPrefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor, string dbId, string tableId)> SetupDbWithRows(int rows, CamusDBOptions? options = null)
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        TrackDatabase(dbName, executor);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < rows; i++)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName,
                $"INSERT INTO robots (id, name) VALUES (gen_id(), \"r{i}\")", null));
            await db.Transactions.CommitAsync(tx);
        }

        return (dbName, db, executor, db.Id, db.Schema.Tables["robots"].Id!);
    }

    // -----------------------------------------------------------------------
    // Database orphans
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Gc_ReclaimsExpiredDatabaseOrphan()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };

        (string dbName, _, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(5, options);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        Assert.IsNotNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId), "sanity: orphan record present after deferred drop");

        await Task.Delay(50);

        int reclaimed = await executor.RunOrphanReclaimForTestsAsync();

        Assert.GreaterOrEqual(reclaimed, 1, "GC must reclaim the expired database orphan");
        Assert.IsNull(await sharedRegistry.TryGetDatabaseOrphanAsync(dbId), "orphan record must be gone after reclaim");
        Assert.AreEqual(0, await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/"), "row data must be physically purged");
        Assert.AreEqual(0, await CountKeysAsync($"{dbId}/meta", $"{dbId}/"), "meta namespace must be physically purged");
    }

    [Test]
    [NonParallelizable]
    public async Task Gc_LeavesUnexpiredDatabaseOrphan()
    {
        (string dbName, _, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(3);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        // Default 7-day retention → not expired.
        int reclaimed = await executor.RunOrphanReclaimForTestsAsync();

        Assert.AreEqual(0, reclaimed, "unexpired orphan must not be reclaimed");
        Assert.IsNotNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId));
        Assert.AreEqual(3, await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/"), "row data must remain");
    }

    [Test]
    [NonParallelizable]
    public async Task Gc_DisabledWhenRetentionNonPositive()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 0 }; // reclamation disabled
        (string dbName, _, CommandExecutor executor, string dbId, _) = await SetupDbWithRows(2, options);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        await Task.Delay(50);

        int reclaimed = await executor.RunOrphanReclaimForTestsAsync();

        Assert.AreEqual(0, reclaimed, "retention <= 0 must disable reclamation");
        Assert.IsNotNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId), "orphan must be kept when reclamation is disabled");
    }

    [Test]
    [NonParallelizable]
    public async Task Gc_SkipsRelinkedDatabase_AndCleansStaleOrphanRecord()
    {
        // A live (registered) database with a stale orphan record for its id — simulates a relink that
        // crashed after re-registering but before deleting the orphan record. The GC must clean the
        // stale record and NOT purge the live database's data.
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };
        (string dbName, _, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(4, options);

        await sharedRegistry!.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord
        {
            Id = dbId,
            FormerName = dbName,
            DroppedAt = HLCTimestamp.Zero, // always past any retention window
        });

        await Task.Delay(50);

        await executor.RunOrphanReclaimForTestsAsync();

        Assert.IsNull(await sharedRegistry.TryGetDatabaseOrphanAsync(dbId), "stale orphan record for a live database must be cleaned");
        Assert.AreEqual(4, await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/"), "live database data must NOT be purged");
    }

    // -----------------------------------------------------------------------
    // Table orphans
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Gc_ReclaimsExpiredTableOrphan()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };

        (string dbName, _, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(6, options);
        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false));

        await Task.Delay(50);

        int reclaimed = await executor.RunOrphanReclaimForTestsAsync();

        Assert.GreaterOrEqual(reclaimed, 1, "GC must reclaim the expired table orphan");
        Assert.AreEqual(0, await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/"), "table row data must be physically purged");
    }

    [Test]
    [NonParallelizable]
    public async Task Gc_SkipsRelinkedTable_AndCleansStaleOrphanRecord()
    {
        // A live table with a stale orphan record for its id (relink crash window). The GC must clean
        // the record and keep the table's rows.
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(4, options);

        // Write a stale orphan record for the still-live "robots" table (its meta/table key exists).
        byte[] recordBytes = MetaJsonSerializer.Serialize(new OrphanTableRecord
        {
            TableId = tableId,
            FormerName = "robots",
            DroppedAt = HLCTimestamp.Zero,
            Schema = db.Schema.Tables["robots"],
        }, MetaJsonContext.Default.OrphanTableRecord);

        await SharedKahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{dbId}/meta/orphan:{tableId}", recordBytes, null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);

        await Task.Delay(50);

        await executor.RunOrphanReclaimForTestsAsync();

        (KeyValueResponseType type, _) = await SharedKahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{dbId}/meta/orphan:{tableId}", -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreNotEqual(KeyValueResponseType.Get, type, "stale orphan record for a live table must be cleaned");
        Assert.AreEqual(4, await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/"), "live table data must NOT be purged");
    }

    // -----------------------------------------------------------------------
    // Contention: relink vs GC on the same id
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task RelinkVsGc_Contention_ExactlyOneWins_NeverHalfState()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 }; // GC is eligible
        (string dbName, _, CommandExecutor executor, string dbId, string tableId) = await SetupDbWithRows(6, options);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        await Task.Delay(50);

        string recovered = "db_" + Guid.NewGuid().ToString("n");

        // Fire a relink and a GC sweep of the SAME id concurrently. They take the same per-id fence, so
        // exactly one wins; the loser observes the orphan gone (OrphanNotFound) or the fence held.
        Task<DatabaseDescriptor?> relinkTask = Task.Run(async () =>
        {
            try { return await executor.RelinkDatabase(new RelinkDatabaseTicket(recovered, dbId)); }
            catch (CamusDBException e) when (e.Code is CamusDBErrorCodes.OrphanNotFound or CamusDBErrorCodes.InvalidInput)
            {
                return null; // relink lost the race
            }
        });
        Task<int> gcTask = executor.RunOrphanReclaimForTestsAsync();
        await Task.WhenAll(relinkTask, gcTask);

        int rowCount = await CountKeysAsync($"{dbId}:{tableId}:r", $"{dbId}:{tableId}:r/");
        Assert.IsNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId), "the orphan record is resolved either way");

        if (relinkTask.Result is not null)
        {
            // Relink won → the database is fully revived, data intact.
            TrackDatabase(recovered, executor);
            Assert.AreEqual(6, rowCount, "relink won → data must be FULLY intact, never half-purged");
            Assert.AreEqual(dbId, relinkTask.Result!.Id);
        }
        else
        {
            // GC won → the keyspace is fully gone.
            Assert.AreEqual(0, rowCount, "GC won → data must be FULLY purged, never half-relinked");
        }
    }
}
