/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// What a crash between the schema-log commit and the metadata checkpoint costs a truncate.
///
/// <para>The committed log entry is the source of truth, so the swap itself always survives. What
/// must also survive is the <em>retirement</em>: without its record and its frozen index catalog, the
/// key-space the relation stopped reading is named by nothing and can never be recovered or
/// reclaimed. These tests drive the checkpoint into failure and then run the reconciliation a
/// recovered node runs, proving it reconstructs every missing record — including the intermediate
/// generation of two truncates that committed between two checkpoints, which the live schema no
/// longer mentions at all.</para>
///
/// <para><b>Scope.</b> The reconciliation entry point here is
/// <c>PersistFullSchemaCheckpointAsync</c>, which is what a node calls once WAL restore has replayed
/// the committed tail. The WAL replay itself needs a real process restart and is not reproducible in
/// this fixture — reopening a database in the same process reads the KV checkpoint rather than
/// replaying. What these tests do cover is the half that decides whether a retired key-space is
/// reachable at all.</para>
/// </summary>
[NonParallelizable]
internal sealed class TestTruncateCheckpointReplay : BaseTest
{
    private async Task<int> CountKeysAsync(DatabaseDescriptor db, string storageId)
    {
        int count = 0;
        string bucket = $"{db.Id}:{storageId}:r";
        string prefix = $"{db.Id}:{storageId}:r/";

        await foreach ((string key, ReadOnlyKeyValueEntry _) in db.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private static async Task RunNonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId)>
        SetupRobots(int rows)
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < rows; i++)
            await RunNonQuery(dbName, db, executor,
                $"INSERT INTO robots (id, name) VALUES (gen_id(), \"r{i}\")");

        return (dbName, db, executor, db.Schema.Tables["robots"].Id!);
    }

    [Test]
    public async Task CheckpointFailure_IsRepairedByTheReconciliationAndSurvivesAReopen()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRobots(4);

        // Fail every checkpoint attempt: the schema entry commits, its metadata does not.
        executor.Catalogs.TestPersistCheckpointException =
            new InvalidOperationException("Injected checkpoint failure (test-only)");

        Assert.True(await executor.TruncateTable(new TruncateTableTicket(dbName, "robots")),
            "the committed schema entry stands even when its checkpoint cannot be written");

        string secondGeneration = db.Schema.Tables["robots"].EffectiveStorageId;
        Assert.AreNotEqual(tableId, secondGeneration);

        // Nothing durable yet: the record the retired key-space depends on was never written.
        CatalogsManager catalogs = new(logger);
        Assert.IsEmpty(await catalogs.LoadTableOrphansAsync(db));

        executor.Catalogs.TestPersistCheckpointException = null;

        // The reconciliation a recovered node runs after replaying its committed tail.
        await executor.Catalogs.PersistFullSchemaCheckpointAsync(db);

        List<OrphanTableRecord> orphans = await catalogs.LoadTableOrphansAsync(db);
        Assert.AreEqual(1, orphans.Count, "reconciliation must recreate the retirement the failed checkpoint lost");
        Assert.AreEqual(OrphanKind.RetiredContents, orphans[0].Kind);
        Assert.AreEqual(tableId, orphans[0].RetiredStorageId);
        Assert.AreEqual("robots", orphans[0].FormerName);

        Assert.AreEqual(4, await CountKeysAsync(db, tableId),
            "the retired rows must still be on disk and reachable through the recreated record");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbName));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbName);

        Assert.AreEqual(secondGeneration, reopened.Schema.Tables["robots"].EffectiveStorageId,
            "the repaired checkpoint must name the live generation the committed log describes");
        Assert.AreEqual(1, reopened.Schema.Tables["robots"].ContentsGeneration);
        Assert.AreEqual(1, (await catalogs.LoadTableOrphansAsync(reopened)).Count);
    }

    [Test]
    public async Task TwoTruncatesAfterOneCheckpoint_RecoverTwoRetiredGenerations()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRobots(3);

        executor.Catalogs.TestPersistCheckpointException =
            new InvalidOperationException("Injected checkpoint failure (test-only)");

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        string secondGeneration = db.Schema.Tables["robots"].EffectiveStorageId;

        // A failed checkpoint degrades this node's schema subsystem; clearing it is what a recovered
        // node does, and it lets the test commit a second swap with the checkpoint still failing.
        db.ClearSchemaSubsystemDegraded();

        await RunNonQuery(dbName, db, executor, "INSERT INTO robots (id, name) VALUES (gen_id(), \"middle\")");

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        string thirdGeneration = db.Schema.Tables["robots"].EffectiveStorageId;

        Assert.AreNotEqual(secondGeneration, thirdGeneration);

        executor.Catalogs.TestPersistCheckpointException = null;

        // One reconciliation must repair both swaps, not only the last one.
        await executor.Catalogs.PersistFullSchemaCheckpointAsync(db);

        List<OrphanTableRecord> orphans = await new CatalogsManager(logger).LoadTableOrphansAsync(db);

        HashSet<string> retired = [.. orphans.Select(o => o.RetiredStorageId)];
        Assert.AreEqual(2, retired.Count,
            "both generations need a record: the intermediate one is named nowhere in the live schema");
        Assert.True(retired.Contains(tableId));
        Assert.True(retired.Contains(secondGeneration));

        Assert.AreEqual(3, await CountKeysAsync(db, tableId));
        Assert.AreEqual(1, await CountKeysAsync(db, secondGeneration),
            "the middle generation's single row must still be reachable");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbName));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbName);

        Assert.AreEqual(thirdGeneration, reopened.Schema.Tables["robots"].EffectiveStorageId);
        Assert.AreEqual(2, reopened.Schema.Tables["robots"].ContentsGeneration);
        Assert.AreEqual(2, (await new CatalogsManager(logger).LoadTableOrphansAsync(reopened)).Count);
    }

    [Test]
    public async Task ReplayOfACheckpointedTruncate_DoesNotResurrectAPurgedRecord()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRobots(2);

        // A normal truncate: the checkpoint succeeds and writes the record.
        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        CatalogsManager catalogs = new(logger);
        Assert.AreEqual(1, (await catalogs.LoadTableOrphansAsync(db)).Count);

        // Retention expires and the collector reclaims the generation, record and all.
        KvTransaction tx = await db.Transactions.BeginAsync();
        await catalogs.DeleteTableOrphanAsync(db, tableId, tx);
        await db.Transactions.CommitAsync(tx);

        Assert.IsEmpty(await catalogs.LoadTableOrphansAsync(db));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbName));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbName);

        Assert.IsEmpty(await catalogs.LoadTableOrphansAsync(reopened),
            "replaying an already-checkpointed entry must not resurrect a record retention already purged");
    }
}
