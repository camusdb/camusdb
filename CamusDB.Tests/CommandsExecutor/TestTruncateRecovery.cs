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
/// What happens to the contents generation a <c>TRUNCATE</c> retires: recovering it under a new
/// relation, reclaiming it once retention expires, and the subsystem guards that keep the live
/// relation and the retired one from interfering with each other.
///
/// <para>The sharpest case in here is the first truncate of a table, where the retired key-space is
/// named by the still-live relation's own id. Every identity test in the reclaim and recovery paths
/// has to keep the two apart, and a test that only ever truncates twice would never see it.</para>
/// </summary>
[NonParallelizable]
internal sealed class TestTruncateRecovery : SharedNodeBaseTest
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

    private static async Task RunNonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId)>
        SetupRobots(int rows, CamusDBOptions? options = null)
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        TrackDatabase(dbName, executor);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING, year INT64)", parameters: null));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE INDEX year_idx ON robots (year)", parameters: null));

        for (int i = 0; i < rows; i++)
            await RunNonQuery(dbName, db, executor,
                $"INSERT INTO robots (id, name, year) VALUES (gen_id(), \"r{i}\", {2000 + i})");

        return (dbName, db, executor, db.Schema.Tables["robots"].Id!);
    }

    // -----------------------------------------------------------------------
    // Reclamation
    // -----------------------------------------------------------------------

    [Test]
    public async Task Reclaim_PurgesRetiredContentsAndLeavesTheLiveRelationAlone()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };

        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) =
            await SetupRobots(6, options);

        string retiredRowBucket = $"{db.Id}:{tableId}:r";
        string retiredRowPrefix = $"{db.Id}:{tableId}:r/";

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        // Sanity: the retired key-space is the relation's own id on a first truncate. This is the
        // shape that a table-id-based reclaim guard gets wrong.
        Assert.AreEqual(tableId, (await new CatalogsManager(logger).LoadTableOrphansAsync(db))[0].RetiredStorageId);

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"survivor\", 2099)");

        await Task.Delay(50);

        int reclaimed = await executor.RunOrphanReclaimForTestsAsync();
        Assert.GreaterOrEqual(reclaimed, 1, "the expired retired generation must be reclaimed");

        Assert.AreEqual(0, await CountKeysAsync(retiredRowBucket, retiredRowPrefix),
            "the retired generation's rows must be physically gone");

        Assert.IsEmpty(await new CatalogsManager(logger).LoadTableOrphansAsync(db),
            "the record is deleted last, and only after the data is verified gone");

        // The live relation is untouched: same name, same id, and the row written after the truncate.
        Assert.True(db.Schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(tableId, db.Schema.Tables["robots"].Id);

        List<QueryResultRow> live = await RunSelect(dbName, executor, "SELECT name FROM robots");
        Assert.AreEqual(1, live.Count);
        Assert.AreEqual("survivor", live[0].Row["name"].StrValue);
    }

    [Test]
    public async Task Reclaim_LeavesAnUnexpiredRetiredGenerationAlone()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 600_000 };

        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) =
            await SetupRobots(4, options);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        await executor.RunOrphanReclaimForTestsAsync();

        Assert.AreEqual(4, await CountKeysAsync($"{db.Id}:{tableId}:r", $"{db.Id}:{tableId}:r/"),
            "retention has not elapsed, so the retired rows must still be recoverable");

        Assert.AreEqual(1, (await new CatalogsManager(logger).LoadTableOrphansAsync(db)).Count);
    }

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    [Test]
    public async Task Relink_RecoversTheRetiredRowsUnderAFreshRelationId()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRobots(5);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: $"CREATE TABLE robots_before RELINK TO '{tableId}'", parameters: null));

        List<QueryResultRow> recovered = await RunSelect(dbName, executor, "SELECT name FROM robots_before");
        Assert.AreEqual(5, recovered.Count, "every retired row must come back");

        List<QueryResultRow> byIndex = await RunSelect(dbName, executor, "SELECT name FROM robots_before WHERE year = 2003");
        Assert.AreEqual(1, byIndex.Count, "the retired index entries must come back with the rows");

        TableSchema recoveredSchema = db.Schema.Tables["robots_before"];
        Assert.AreNotEqual(tableId, recoveredSchema.Id,
            "a recovery must never reuse the retired storage id as a relation id: the source still holds it");
        Assert.AreEqual(tableId, recoveredSchema.EffectiveStorageId,
            "the recovered relation reads the retired key-space");

        // The source relation is untouched and still empty.
        Assert.AreEqual(tableId, db.Schema.Tables["robots"].Id);
        Assert.IsEmpty(await RunSelect(dbName, executor, "SELECT name FROM robots"));

        Assert.IsEmpty(await new CatalogsManager(logger).LoadTableOrphansAsync(db),
            "the record is removed once the recovery is durable");
    }

    [Test]
    public async Task Relink_ASecondTimeDoesNotPublishASecondRelation()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRobots(3);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        await executor.RelinkTable(new RelinkTableTicket(dbName, "robots_before", tableId));

        // The record is gone, and the id the caller named is the source relation's own id — which is
        // still live under its original name. Refusing names that, rather than inventing a second
        // relation on a key-space the recovery already took.
        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkTable(new RelinkTableTicket(dbName, "robots_again", tableId)));

        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, exception!.Code);
        Assert.False(db.Schema.Tables.ContainsKey("robots_again"));
        Assert.AreEqual(3, (await RunSelect(dbName, executor, "SELECT name FROM robots_before")).Count);
    }

    [Test]
    public async Task Reclaim_AfterRecovery_DropsTheStaleRecordWithoutPurgingTheRecoveredRows()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };

        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) =
            await SetupRobots(4, options);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        await executor.RelinkTable(new RelinkTableTicket(dbName, "robots_before", tableId));

        // Put the record back, exactly as a recovery that crashed before deleting it would leave it.
        OrphanTableRecord stale = new()
        {
            Kind = OrphanKind.RetiredContents,
            TableId = tableId,
            SourceTableId = tableId,
            RetiredStorageId = tableId,
            RelinkTargetId = db.Schema.Tables["robots_before"].Id,
            FormerName = "robots",
            DroppedAt = HLCTimestamp.Zero,
            Schema = new TableSchema { Id = tableId, Name = "robots" },
        };

        KvTransaction tx = await db.Transactions.BeginAsync();
        await WriteOrphanRecordAsync(db, stale, tx);
        await db.Transactions.CommitAsync(tx);

        await Task.Delay(50);
        await executor.RunOrphanReclaimForTestsAsync();

        Assert.IsEmpty(await new CatalogsManager(logger).LoadTableOrphansAsync(db),
            "a record whose target is live on that very storage is stale, so it is removed");

        Assert.AreEqual(4, await CountKeysAsync($"{db.Id}:{tableId}:r", $"{db.Id}:{tableId}:r/"),
            "the recovered relation's rows must survive: the record was stale, the data was not");

        Assert.AreEqual(4, (await RunSelect(dbName, executor, "SELECT name FROM robots_before")).Count);
    }

    private static async Task WriteOrphanRecordAsync(DatabaseDescriptor db, OrphanTableRecord record, KvTransaction tx)
    {
        byte[] bytes = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.OrphanTableRecord);

        (KeyValueResponseType lockType, _, _, _) = await db.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId, $"{db.Id}/meta/orphan:{record.TableId}", 0,
            KeyValueDurability.Persistent, CancellationToken.None,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.AreEqual(KeyValueResponseType.Locked, lockType);

        (KeyValueResponseType setType, _, _) = await db.Kahuna.Kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, $"{db.Id}/meta/orphan:{record.TableId}", bytes, null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.AreEqual(KeyValueResponseType.Set, setType);
        tx.TrackModified($"{db.Id}/meta/orphan:{record.TableId}", KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------
    // Time travel
    // -----------------------------------------------------------------------

    [Test]
    public async Task TimeTravel_BeforeTheCut_IsRefusedInEveryQueryShape()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRobots(3);

        await Task.Delay(60);
        long beforeCut = SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;
        await Task.Delay(60);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        string[] shapes =
        [
            $"SELECT name FROM robots AS OF SYSTEM TIME {beforeCut}",
            $"SELECT COUNT(*) AS n FROM robots AS OF SYSTEM TIME {beforeCut}",
            $"SELECT name FROM robots AS OF SYSTEM TIME {beforeCut} WHERE year = 2001",
            $"SELECT name FROM robots AS OF SYSTEM TIME {beforeCut} ORDER BY year LIMIT 1",
        ];

        foreach (string sql in shapes)
        {
            CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(
                async () => await RunSelect(dbName, executor, sql), $"shape must be refused: {sql}");

            Assert.AreEqual(CamusDBErrorCodes.SnapshotPrecedesContentsGeneration, exception!.Code, sql);
        }
    }

    [Test]
    public async Task TimeTravel_AtOrAfterTheCut_ReadsTheNewEmptyGeneration()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRobots(3);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        HLCTimestamp cut = db.Schema.Tables["robots"].ContentsValidFrom!.Value;

        Assert.IsEmpty(await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {cut.L}"),
            "a read exactly at the cut observes the new generation");

        await Task.Delay(60);
        long afterCut = SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

        Assert.IsEmpty(await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {afterCut}"));
    }

    [Test]
    public async Task TimeTravel_OnATableThatWasNeverTruncated_IsUnaffected()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRobots(3);

        await Task.Delay(60);
        long snapshot = SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

        Assert.AreEqual(3, (await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {snapshot}")).Count);
    }

    [Test]
    public async Task TimeTravel_AfterTwoTruncates_RefusesEverythingBeforeTheLatestCut()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRobots(2);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        HLCTimestamp firstCut = db.Schema.Tables["robots"].ContentsValidFrom!.Value;

        await Task.Delay(60);
        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        HLCTimestamp secondCut = db.Schema.Tables["robots"].ContentsValidFrom!.Value;

        Assert.Greater(secondCut.L, firstCut.L);

        // Between the two cuts is still refused: only the latest generation is locatable.
        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {firstCut.L}"));

        Assert.AreEqual(CamusDBErrorCodes.SnapshotPrecedesContentsGeneration, exception!.Code);
    }

    // -----------------------------------------------------------------------
    // Statistics
    // -----------------------------------------------------------------------

    [Test]
    public async Task Statistics_AreNotCarriedAcrossAContentsSwap()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRobots(12);

        await RunSelect(dbName, executor, "ANALYZE TABLE robots");

        List<QueryResultRow> before = await RunSelect(dbName, executor, "SHOW STATISTICS FOR robots");
        Assert.IsNotEmpty(before, "sanity: the table has statistics before the truncate");

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        // Reopen through a second engine so the answer comes from the persisted blob rather than from
        // an in-memory entry this engine happens to have evicted.
        CommandExecutor reader = CreateCommandExecutor(Options);
        TrackDatabase(dbName, reader);

        List<QueryResultRow> after = await RunSelect(dbName, reader, "SHOW STATISTICS FOR robots");

        foreach (QueryResultRow row in after)
        {
            if (row.Row.TryGetValue("row_count", out ColumnValue? rowCount) && rowCount.Type == ColumnType.Integer64)
                Assert.AreNotEqual(12L, rowCount.LongValue,
                    "the pre-truncate row count must not be reported for the new, unmeasured generation");
        }
    }
}
