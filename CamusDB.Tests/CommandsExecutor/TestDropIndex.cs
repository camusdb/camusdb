/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the standalone <c>DROP INDEX</c> statement: <c>DROP INDEX i ON t</c>,
/// <c>DROP INDEX t.i</c> and <c>IF EXISTS</c> on those and on <c>ALTER TABLE t DROP INDEX</c>. Every
/// form builds the ALTER TABLE ticket, so these tests drive the real SQL entry point in both the
/// standalone and the cluster DDL paths.
/// </summary>
internal abstract class DropIndexScenarios : SharedNodeBaseTest
{
    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    private static async Task<ExecuteDDLSQLResult> Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task NonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task SetupRobots(CommandExecutor executor, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE robots (id int64 PRIMARY KEY NOT NULL, name string, year int64)");
        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES (1, 'a', 2000), (2, 'b', 2001), (3, 'c', 2000)");
    }

    private static bool HasIndex(DatabaseDescriptor database, string table, string index) =>
        database.Schema.Tables[table].Indexes!.Any(ix => string.Equals(ix.Name, index, System.StringComparison.OrdinalIgnoreCase));

    [TestCase("DROP INDEX year_idx ON robots")]
    [TestCase("DROP INDEX robots.year_idx")]
    [TestCase("DROP INDEX IF EXISTS year_idx ON robots")]
    [TestCase("DROP INDEX IF EXISTS robots.year_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX IF EXISTS year_idx")]
    [TestCase("DROP INDEX YEAR_IDX ON ROBOTS")]
    public async Task EachForm_DropsTheIndex_AndTheNameCanBeUsedAgain(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, sql);

        Assert.IsTrue(result.Success, "an index that exists is dropped and reported as applied");
        Assert.IsFalse(HasIndex(database, "robots", "year_idx"));

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 ORDER BY id");
        Assert.AreEqual(2, rows.Count, "rows stay readable after the drop");

        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"));

        rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 ORDER BY id");
        Assert.AreEqual(2, rows.Count, "the index built again under the old name is complete");
    }

    [TestCase("DROP INDEX IF EXISTS no_such_idx ON robots")]
    [TestCase("DROP INDEX IF EXISTS robots.no_such_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX IF EXISTS no_such_idx")]
    [TestCase("DROP INDEX IF EXISTS year_idx ON no_such_table")]
    [TestCase("DROP INDEX IF EXISTS no_such_table.year_idx")]
    public async Task IfExists_OnAMissingIndexOrTable_IsANoOp(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        long versionBefore = database.Schema.SchemaVersion;

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, sql);

        Assert.IsFalse(result.Success, "a no-op reports not applied");
        Assert.AreEqual(versionBefore, database.Schema.SchemaVersion, "a no-op replicates no schema change");
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"), "an index with another name is untouched");
    }

    [TestCase("DROP INDEX no_such_idx ON robots")]
    [TestCase("DROP INDEX robots.no_such_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX no_such_idx")]
    public async Task WithoutIfExists_AMissingIndex_StillFails(string sql)
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("does not exist", exception.Message);
    }

    [TestCase("DROP INDEX year_idx ON no_such_table")]
    [TestCase("DROP INDEX no_such_table.year_idx")]
    public async Task WithoutIfExists_AMissingTable_StillFails(string sql)
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception.Code);
    }

    [Test]
    public async Task ABareIndexName_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"));
    }

    /// <summary>
    /// The primary key is not a droppable index: its SQL alias names nothing in the index list, so
    /// <c>DROP INDEX</c> cannot reach it, and <c>IF EXISTS</c> turns that into a no-op, not a drop.
    /// </summary>
    [Test]
    public async Task ThePrimaryKey_IsNotDroppedByDropIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        int indexesBefore = database.Schema.Tables["robots"].Indexes!.Count;

        Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX robots_pkey ON robots"));
        ExecuteDDLSQLResult result = await Ddl(executor, dbname, "DROP INDEX IF EXISTS robots_pkey ON robots");

        Assert.IsFalse(result.Success);
        Assert.AreEqual(indexesBefore, database.Schema.Tables["robots"].Indexes!.Count);

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT name FROM robots WHERE id = 2");
        Assert.AreEqual(1, rows.Count);
    }

    /// <summary>
    /// <c>IF EXISTS</c> hides only a missing index. An index that exists but that a foreign key needs
    /// is still refused, because the guard runs under the DDL gate before the drop.
    /// </summary>
    [TestCase("DROP INDEX cities_name ON cities")]
    [TestCase("DROP INDEX IF EXISTS cities.cities_name")]
    public async Task AnIndexAForeignKeyNeeds_IsStillRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
        Assert.IsTrue(HasIndex(database, "cities", "cities_name"));
    }

    /// <summary>
    /// The IF EXISTS check runs inside the drop, under the DDL gate, not before it. Two concurrent drops
    /// of one index must give exactly one drop and one no-op; a check made outside the gate would let
    /// both pass it and the second fail on an index the first already removed.
    /// </summary>
    [Test]
    public async Task TwoConcurrentDropsIfExists_GiveOneDropAndOneNoOp()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        for (int round = 0; round < 5; round++)
        {
            if (round > 0)
                await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");

            ExecuteDDLSQLResult[] results = await Task.WhenAll(
                Task.Run(() => Ddl(executor, dbname, "DROP INDEX IF EXISTS year_idx ON robots")),
                Task.Run(() => Ddl(executor, dbname, "DROP INDEX IF EXISTS robots.year_idx")));

            Assert.AreEqual(1, results.Count(r => r.Success), $"round {round}: exactly one statement drops the index");
            Assert.IsFalse(HasIndex(database, "robots", "year_idx"));
        }
    }

    /// <summary>
    /// Two concurrent drops of one index without <c>IF EXISTS</c>: one drops it, and the other, which finds
    /// the index gone when it gets the gate, fails with the error of a missing index.
    /// </summary>
    [Test]
    public async Task TwoConcurrentDrops_GiveOneDropAndOneMissingIndexError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        for (int round = 0; round < 3; round++)
        {
            if (round > 0)
                await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");

            async Task<Exception?> DropAsync(string sql)
            {
                try
                {
                    ExecuteDDLSQLResult result = await Ddl(executor, dbname, sql);
                    return result.Success ? null : new InvalidOperationException("a drop without IF EXISTS reported not applied");
                }
                catch (Exception ex)
                {
                    return ex;
                }
            }

            Exception?[] outcomes = await Task.WhenAll(
                Task.Run(() => DropAsync("DROP INDEX year_idx ON robots")),
                Task.Run(() => DropAsync("DROP INDEX robots.year_idx")));

            Assert.AreEqual(1, outcomes.Count(o => o is null), $"round {round}: exactly one statement drops the index");
            CamusDBException? missing = outcomes.Single(o => o is not null) as CamusDBException;
            Assert.IsNotNull(missing, $"round {round}: the other statement fails with a CamusDB error");
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, missing!.Code);
            StringAssert.Contains("does not exist", missing.Message);
            Assert.IsFalse(HasIndex(database, "robots", "year_idx"));
        }
    }

    /// <summary>
    /// A statement of another kind on the same table, sent while the drop is between two steps, waits for the
    /// drop, because the drop holds the table. It then runs against the table without the index. The drop
    /// finds its index by id, so a rename of the table does not change what it drops.
    /// </summary>
    [TestCase("TRUNCATE TABLE robots")]
    [TestCase("ALTER TABLE robots RENAME TO robots_new")]
    [TestCase("ALTER TABLE robots RENAME INDEX year_idx TO year_idx_new")]
    [TestCase("DROP TABLE robots")]
    public async Task SameTableDdl_WaitsForTheDrop_ThenSeesItsOutcome(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        TaskCompletionSource reachedWriteOnly = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseWriteOnly = new(TaskCreationOptions.RunContinuationsAsynchronously);

        executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state != SchemaElementState.WriteOnly)
                return;

            reachedWriteOnly.TrySetResult();
            await releaseWriteOnly.Task;
        };

        Task<ExecuteDDLSQLResult> drop = Task.Run(() => Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        Task<ExecuteDDLSQLResult>? other = null;
        try
        {
            await reachedWriteOnly.Task.WaitAsync(TimeSpan.FromSeconds(30));

            other = Task.Run(() => Ddl(executor, dbname, sql));
            Task first = await Task.WhenAny(other, Task.Delay(500));
            Assert.AreNotSame(other, first, "the statement waits for the drop");
        }
        finally
        {
            releaseWriteOnly.TrySetResult();
        }

        Assert.IsTrue((await drop.WaitAsync(TimeSpan.FromSeconds(30))).Success);
        executor.TestInterceptAfterIndexDropStep = null;

        if (sql.Contains("RENAME INDEX", StringComparison.Ordinal))
        {
            CamusDBException renameError = Assert.ThrowsAsync<CamusDBException>(async () => await other!.WaitAsync(TimeSpan.FromSeconds(30)))!;
            StringAssert.Contains("year_idx", renameError.Message, "the rename finds the index gone");
        }
        else
        {
            await other!.WaitAsync(TimeSpan.FromSeconds(30));
        }

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace), "the purge of the dropped index left no entry");
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));

        if (sql.StartsWith("DROP TABLE", StringComparison.Ordinal))
        {
            Assert.IsFalse(database.Schema.Tables.ContainsKey("robots"));
            return;
        }

        string table = sql.Contains("RENAME TO", StringComparison.Ordinal) ? "robots_new" : "robots";
        Assert.IsNull(LiveState(database, table, "year_idx"), "the index is gone from the table");
        Assert.IsNull(LiveState(database, table, "year_idx_new"));

        int expectedRows = sql.StartsWith("TRUNCATE", StringComparison.Ordinal) ? 0 : 3;
        Assert.AreEqual(expectedRows, (await Query(executor, dbname, $"SELECT id FROM {table}")).Count);
    }

    /// <summary>The configuration of a test that drives the coordinator's resume by hand: no sweep.</summary>
    private CamusDBOptions NoSweep => Options with { CoordinatorJobSweepIntervalMs = 0 };

    /// <summary>The key space of the entries of <paramref name="index"/>, read while the index exists.</summary>
    private static string IndexSpaceOf(DatabaseDescriptor database, string table, string index)
    {
        TableSchema schema = database.Schema.Tables[table];
        TableIndexSchema found = schema.Indexes!.First(ix => string.Equals(ix.Name, index, StringComparison.OrdinalIgnoreCase));
        return KvKeyBuilder.IndexSpaceOf(database.Id, schema.EffectiveStorageId, found.KvId);
    }

    /// <summary>Counts the stored keys under <paramref name="prefix"/> in <paramref name="keySpace"/>, with a plain read of the latest committed keys.</summary>
    private static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal) && entry.Value is not null)
                count++;
        }

        return count;
    }

    private static Task<int> CountEntriesAsync(DatabaseDescriptor database, string indexSpace) =>
        CountKeysAsync(database, indexSpace, indexSpace + "/");

    private static Task<int> CountPurgeRecordsAsync(DatabaseDescriptor database) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.PurgeKeyPrefix(database.Id));

    private static Task<int> CountCoordinatorJobsAsync(DatabaseDescriptor database) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.CoordinatorKeyPrefix(database.Id));

    /// <summary>The state of <paramref name="index"/> in the live schema of this node, or null when it is gone.</summary>
    private static SchemaElementState? LiveState(DatabaseDescriptor database, string table, string index) =>
        database.Schema.Tables[table].Indexes?.Find(ix => string.Equals(ix.Name, index, StringComparison.OrdinalIgnoreCase))?.State;

    /// <summary>The state of <paramref name="index"/> in the persisted checkpoint of its table, or null when it is gone.</summary>
    private static async Task<SchemaElementState?> PersistedState(DatabaseDescriptor database, string table, string index)
    {
        string key = MetaKeys.TableKey(database.Id, database.Schema.Tables[table].Id!);
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);

        Assert.AreEqual(KeyValueResponseType.Get, type, "the table has a checkpoint");
        TableSchema persisted = MetaJsonSerializer.Deserialize(entry!.Value!, MetaJsonContext.Default.TableSchema);
        return persisted.Indexes?.Find(ix => string.Equals(ix.Name, index, StringComparison.OrdinalIgnoreCase))?.State;
    }

    private static async Task<int> CountYear2000Async(CommandExecutor executor, string dbname) =>
        (await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000")).Count;

    /// <summary>
    /// The statement does not wait for the entries: it returns once the index left the schema, with a
    /// durable purge record in place of its job, and the purge runs behind it and then deletes the record.
    /// </summary>
    [Test]
    public async Task TheDrop_ReturnsBeforeThePurge_ThenThePurgeEmptiesTheKeySpace()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");
        Assert.AreEqual(3, await CountEntriesAsync(database, indexSpace));

        TaskCompletionSource releasePurge = new(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.DeferredPurgesForTests.TestInterceptBeforePurge = () => releasePurge.Task;
        try
        {
            ExecuteDDLSQLResult result = await Ddl(executor, dbname, "DROP INDEX year_idx ON robots").WaitAsync(TimeSpan.FromSeconds(30));

            Assert.IsTrue(result.Success);
            Assert.IsNull(LiveState(database, "robots", "year_idx"), "the index is gone when the statement returns");
            Assert.IsNull(await PersistedState(database, "robots", "year_idx"), "and gone from the checkpoint");
            Assert.AreEqual(3, await CountEntriesAsync(database, indexSpace), "the purge has not run yet");
            Assert.AreEqual(1, await CountPurgeRecordsAsync(database), "a durable record stands for the purge");
            Assert.AreEqual(0, await CountCoordinatorJobsAsync(database), "the record replaced the job of the drop");
            Assert.AreEqual(2, await CountYear2000Async(executor, dbname), "reads are correct without the index");
        }
        finally
        {
            executor.DeferredPurgesForTests.TestInterceptBeforePurge = null;
            releasePurge.TrySetResult();
        }

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace), "the purge removed every entry");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database), "and then deleted its record");
    }

    /// <summary>
    /// The purge reads and deletes one batch at a time, so its memory does not grow with the index. Each batch
    /// after the first starts after the previous batch's last key, so the scans from the start of the key
    /// space, which walk over every deleted entry the store still keeps, stay a fixed number: one, and the
    /// confirming scans at the end. A purge that started every batch from the start would make one per batch.
    /// </summary>
    [Test]
    public async Task ThePurge_DeletesInBatches_AndScansFromTheStartAFixedNumberOfTimes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep with { KeyspacePurgeBatchSize = 2 });
        await SetupRobots(executor, dbname);
        string rows = string.Join(", ", Enumerable.Range(4, 37).Select(i => $"({i}, 'r{i}', {1900 + i})"));
        await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES " + rows);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");
        Assert.AreEqual(40, await CountEntriesAsync(database, indexSpace));

        long batchesBefore = Interlocked.Read(ref DatabaseDropper.PurgeBatchesForTesting);
        long scansFromStartBefore = Interlocked.Read(ref DatabaseDropper.PurgeScansFromStartForTesting);

        await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace));
        Assert.GreaterOrEqual(Interlocked.Read(ref DatabaseDropper.PurgeBatchesForTesting) - batchesBefore, 20, "forty entries in batches of two");
        Assert.LessOrEqual(Interlocked.Read(ref DatabaseDropper.PurgeScansFromStartForTesting) - scansFromStartBefore, 5,
            "one scan from the start, then the confirming scans; never one per batch");
    }

    /// <summary>
    /// A purge that stops after some batches, as a crash would stop it, leaves its record. The entries it
    /// did not reach stay, and the next run finishes them.
    /// </summary>
    [Test]
    public async Task APurgeStoppedAfterSomeBatches_IsFinishedByTheNextRun()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep with { KeyspacePurgeBatchSize = 2 });
        await SetupRobots(executor, dbname);
        await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES (4, 'd', 2002), (5, 'e', 2003), (6, 'f', 2004)");
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        DatabaseDropper.TestInterceptAfterPurgeBatch = bucket => bucket == indexSpace
            ? throw new InvalidOperationException("the purge stops after its first batch")
            : Task.CompletedTask;
        try
        {
            await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
            await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();
        }
        finally
        {
            DatabaseDropper.TestInterceptAfterPurgeBatch = null;
        }

        Assert.AreEqual(4, await CountEntriesAsync(database, indexSpace), "one batch of two was deleted before the stop");
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database), "the record stays for the next run");

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(database), "the failed run marked the database for the next run");
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
    }

    /// <summary>A purge that stopped with its node is found by the first purge run after a reopen.</summary>
    [Test]
    public async Task APurgeStoppedByACrash_IsFinishedAfterAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        // The node stops right after the record replaced the job, before the purge.
        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;
        try
        {
            await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
        }
        finally
        {
            executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;
        }

        Assert.AreEqual(3, await CountEntriesAsync(database, indexSpace));
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "the first run after the reopen finds the record");
        Assert.AreEqual(0, await CountEntriesAsync(reopened, indexSpace));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(reopened));
    }

    /// <summary>
    /// A drop that fails after a step leaves this node's schema exactly as it is persisted, never ahead of
    /// it: the index leaves the schema only through the replicated deltas. Writes in the state reached keep
    /// what that state asks of them, reads stay correct, and the resume finishes the drop. Before the staged
    /// drop, a failed commit took the index out of this node's schema only, so its writes skipped the index
    /// and, after a reopen, a read through the index missed rows.
    /// </summary>
    [TestCase(SchemaElementState.WriteOnly)]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.Absent)]
    public async Task AFailedStep_LeavesTheSchemaAsPersisted_AndTheResumeFinishesTheDrop(SchemaElementState failAfter)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == failAfter
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure after " + state)
            : Task.CompletedTask;
        try
        {
            CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"))!;
            StringAssert.Contains("injected failure", failure.Message);
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        SchemaElementState? expected = failAfter == SchemaElementState.Absent ? null : failAfter;
        Assert.AreEqual(expected, LiveState(database, "robots", "year_idx"), "the live schema holds the state that was replicated");
        Assert.AreEqual(expected, await PersistedState(database, "robots", "year_idx"), "and the checkpoint holds the same state");
        Assert.AreEqual(1, await CountCoordinatorJobsAsync(database), "the job of the drop stays for the resume");

        await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES (4, 'd', 2000)");
        Assert.AreEqual(3, await CountYear2000Async(executor, dbname), "reads are correct while the index is not public");

        if (failAfter == SchemaElementState.WriteOnly)
            Assert.AreEqual(4, await CountEntriesAsync(database, indexSpace), "a WriteOnly index still gets the entry of every write");

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database);
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.IsNull(LiveState(database, "robots", "year_idx"), "the resume finished the drop");
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace), "and the purge left no entry");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.IsNull(LiveState(reopened, "robots", "year_idx"));
        Assert.AreEqual(3, await CountYear2000Async(executor, dbname));
    }

    /// <summary>
    /// The node stops after a step of the drop, and the database is opened again with no manual resume. The
    /// open loads the schema as persisted, and the job resume that the open starts finishes the drop and its
    /// purge.
    /// </summary>
    [TestCase(SchemaElementState.WriteOnly)]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.Absent)]
    public async Task AStepThenAStop_IsFinishedByTheResumeAtTheNextOpen(SchemaElementState stopAfter)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == stopAfter
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected stop after " + state)
            : Task.CompletedTask;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        Assert.AreEqual(1, await CountCoordinatorJobsAsync(database), "the job of the drop is left for the next open");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        bool finished = false;
        for (int i = 0; i < 300 && !finished; i++)
        {
            finished = LiveState(reopened, "robots", "year_idx") is null
                && await CountCoordinatorJobsAsync(reopened) == 0
                && await CountPurgeRecordsAsync(reopened) == 0
                && await CountEntriesAsync(reopened, indexSpace) == 0;

            if (!finished)
                await Task.Delay(100);
        }

        Assert.IsTrue(finished, $"the resume at open finished the drop stopped after {stopAfter} (state {LiveState(reopened, "robots", "year_idx")?.ToString() ?? "gone"})");
        Assert.IsNull(await PersistedState(reopened, "robots", "year_idx"));
        Assert.AreEqual(2, await CountYear2000Async(executor, dbname));
    }

    /// <summary>
    /// A drop stopped after its last step leaves its job under the index's name. A <c>CREATE INDEX</c> of the
    /// same name finishes that drop before it records its own job, so the purge of the old entries is not
    /// lost, and the new index is complete.
    /// </summary>
    [Test]
    public async Task CreateIndexOfTheSameName_FinishesADropThatStoppedAfterItsLastStep()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string oldSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == SchemaElementState.Absent
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure")
            : Task.CompletedTask;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        Assert.AreEqual(1, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));

        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        string newSpace = IndexSpaceOf(database, "robots", "year_idx");
        Assert.AreNotEqual(oldSpace, newSpace, "the new index has its own key space");
        Assert.AreEqual(0, await CountEntriesAsync(database, oldSpace), "the earlier drop was finished and purged");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(3, await CountEntriesAsync(database, newSpace), "the new index has an entry for every row");
        Assert.AreEqual(2, (await Query(executor, dbname, "SELECT id FROM robots@{FORCE_INDEX=year_idx} WHERE year = 2000")).Count);
    }

    /// <summary>A <c>CREATE INDEX</c> of the same name does not wait for the purge, and the purge leaves its entries alone.</summary>
    [Test]
    public async Task CreateIndexOfTheSameName_DuringThePurge_IsComplete()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string oldSpace = IndexSpaceOf(database, "robots", "year_idx");

        TaskCompletionSource releasePurge = new(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.DeferredPurgesForTests.TestInterceptBeforePurge = () => releasePurge.Task;
        string newSpace;
        try
        {
            await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
            await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)").WaitAsync(TimeSpan.FromSeconds(30));

            newSpace = IndexSpaceOf(database, "robots", "year_idx");
            Assert.AreEqual(3, await CountEntriesAsync(database, oldSpace), "the purge is still held");
        }
        finally
        {
            executor.DeferredPurgesForTests.TestInterceptBeforePurge = null;
            releasePurge.TrySetResult();
        }

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountEntriesAsync(database, oldSpace));
        Assert.AreEqual(3, await CountEntriesAsync(database, newSpace), "the purge of the old index left the new one complete");
        Assert.AreEqual(2, (await Query(executor, dbname, "SELECT id FROM robots@{FORCE_INDEX=year_idx} WHERE year = 2000")).Count);
    }

    /// <summary>
    /// Between two steps of a drop, DDL on another table runs, and DDL on the same table waits for the drop.
    /// Reads and writes of the table stay correct at every step, and no entry is left after the purge.
    /// </summary>
    [Test]
    public async Task BetweenSteps_OtherTablesTakeDdl_TheTableWaits_AndReadsStayCorrect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        int year2000 = 2;
        int nextId = 10;
        Task? sameTableDdl = null;

        TaskCompletionSource reachedWriteOnly = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseWriteOnly = new(TaskCreationOptions.RunContinuationsAsynchronously);

        executor.TestInterceptAfterIndexDropStep = async state =>
        {
            await NonQuery(executor, dbname, $"INSERT INTO robots (id, name, year) VALUES ({nextId++}, 'x', 2000)");
            year2000++;
            Assert.AreEqual(year2000, await CountYear2000Async(executor, dbname), $"reads are correct at {state}");

            if (state == SchemaElementState.WriteOnly)
            {
                reachedWriteOnly.TrySetResult();
                await releaseWriteOnly.Task;
            }
        };

        Task<ExecuteDDLSQLResult> drop = Task.Run(() => Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        try
        {
            await reachedWriteOnly.Task.WaitAsync(TimeSpan.FromSeconds(30));

            await Ddl(executor, dbname, "CREATE TABLE other (id int64 PRIMARY KEY NOT NULL, v int64)").WaitAsync(TimeSpan.FromSeconds(30));

            sameTableDdl = Task.Run(() => Ddl(executor, dbname, "ALTER TABLE robots ADD COLUMN extra int64"));
            Task first = await Task.WhenAny(sameTableDdl, Task.Delay(500));
            Assert.AreNotSame(sameTableDdl, first, "DDL on the table waits for the drop");
        }
        finally
        {
            releaseWriteOnly.TrySetResult();
        }

        Assert.IsTrue((await drop).Success);
        executor.TestInterceptAfterIndexDropStep = null;
        await sameTableDdl!.WaitAsync(TimeSpan.FromSeconds(30));

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.IsTrue(database.Schema.Tables.ContainsKey("other"));
        Assert.IsTrue(database.Schema.Tables["robots"].Columns!.Exists(c => c.Name == "extra"));
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace), "no write during the drop left an entry");
        Assert.AreEqual(year2000, await CountYear2000Async(executor, dbname));
    }

    /// <summary>
    /// <c>DROP PRIMARY KEY</c> takes the same steps. While the key is not public, an UPSERT, which finds its
    /// row through the key, is refused rather than reading an index that no write may maintain.
    /// </summary>
    [Test]
    public async Task DropPrimaryKey_IsStaged_AndUpsertRefusesAKeyThatIsNotPublic()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string keySpace = IndexSpaceOf(database, "robots", CamusDBConstants.PrimaryKeyInternalName);
        Assert.AreEqual(3, await CountEntriesAsync(database, keySpace));

        CamusDBException? upsertFailure = null;
        CamusDBException? probeFailure = null;
        executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state != SchemaElementState.DeleteOnly)
                return;

            try
            {
                await NonQuery(executor, dbname, "UPSERT INTO robots (id, name, year) VALUES (1, 'z', 1999)");
            }
            catch (CamusDBException ex)
            {
                upsertFailure = ex;
            }

            // A row written now gets no primary-key entry; a probe through the key would call it missing.
            await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES (4, 'd', 2001)");
            try
            {
                await Query(executor, dbname, "SHOW RANGE FROM TABLE robots FOR ROW (4)");
            }
            catch (CamusDBException ex)
            {
                probeFailure = ex;
            }
        };
        try
        {
            Assert.IsTrue((await Ddl(executor, dbname, "ALTER TABLE robots DROP PRIMARY KEY")).Success);
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        Assert.IsNotNull(upsertFailure, "an UPSERT is refused while the key is not public");
        StringAssert.Contains("not public", upsertFailure!.Message);

        Assert.IsNotNull(probeFailure, "SHOW RANGE ... FOR ROW is refused while the key is not public");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, probeFailure!.Code);
        StringAssert.Contains("that a statement can read", probeFailure.Message);

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.IsNull(LiveState(database, "robots", CamusDBConstants.PrimaryKeyInternalName));
        Assert.AreEqual(0, await CountEntriesAsync(database, keySpace));
        Assert.AreEqual(4, (await Query(executor, dbname, "SELECT id FROM robots")).Count);
    }

    /// <summary>The job of the drop of <paramref name="element"/> on <paramref name="table"/>, read from its key, or null.</summary>
    private static async Task<PersistedCoordinatorJob?> ReadJobAsync(DatabaseDescriptor database, string tableId, string element)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, MetaKeys.CoordinatorKey(database.Id, tableId, element), -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, CancellationToken.None);

        return type == KeyValueResponseType.Get && entry?.Value is { Length: > 0 } bytes
            ? MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.PersistedCoordinatorJob)
            : null;
    }

    /// <summary>
    /// The statement resolved its table before it entered the DDL gate. Another statement dropped the table
    /// meanwhile and created a table of the same name with an index of the same name. The drop is refused,
    /// and the new table keeps its index.
    /// </summary>
    [TestCase("DROP INDEX year_idx ON robots")]
    [TestCase("DROP INDEX IF EXISTS year_idx ON robots")]
    public async Task ATableReplacedBeforeTheGate_KeepsTheIndexOfTheNewTable(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string oldTableId = database.Schema.Tables["robots"].Id!;

        executor.TestInterceptBeforeIndexDropGate = async () =>
        {
            executor.TestInterceptBeforeIndexDropGate = null;
            await Ddl(executor, dbname, "DROP TABLE robots FORCE");
            await Ddl(executor, dbname, "CREATE TABLE robots (id int64 PRIMARY KEY NOT NULL, name string, year int64)");
            await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        };
        try
        {
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
            Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused.Code);
        }
        finally
        {
            executor.TestInterceptBeforeIndexDropGate = null;
        }

        Assert.AreNotEqual(oldTableId, database.Schema.Tables["robots"].Id);
        Assert.AreEqual(SchemaElementState.Public, LiveState(database, "robots", "year_idx"), "the index of the new table is untouched");
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
    }

    /// <summary>The table the statement resolved is dropped before the gate: IF EXISTS is a no-op, a plain drop is an error.</summary>
    [TestCase("DROP INDEX IF EXISTS year_idx ON robots", null)]
    [TestCase("DROP INDEX year_idx ON robots", CamusDBErrorCodes.TableDoesntExist)]
    public async Task ATableDroppedBeforeTheGate_FollowsIfExists(string sql, string? expectedError)
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);

        executor.TestInterceptBeforeIndexDropGate = async () =>
        {
            executor.TestInterceptBeforeIndexDropGate = null;
            await Ddl(executor, dbname, "DROP TABLE robots FORCE");
        };
        try
        {
            if (expectedError is null)
            {
                Assert.IsFalse((await Ddl(executor, dbname, sql)).Success, "IF EXISTS on a table that is gone is not applied");
            }
            else
            {
                CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
                Assert.AreEqual(expectedError, failure.Code);
            }
        }
        finally
        {
            executor.TestInterceptBeforeIndexDropGate = null;
        }
    }

    /// <summary>
    /// A drop stopped after its job was written and before its first step leaves the index public. A foreign
    /// key then claims it as its parent key. The resume gives the drop up rather than take away an index the
    /// constraint names: the index and the constraint both stay, and the job is deleted.
    /// </summary>
    [Test]
    public async Task AForeignKeyThatClaimsTheIndexBeforeTheFirstStep_StopsTheDrop()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        await Ddl(executor, dbname, "CREATE UNIQUE INDEX name_uq ON robots (name)");

        executor.TestInterceptBeforeIndexDropFirstStep = () => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure before the first step");
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX name_uq ON robots"));
        }
        finally
        {
            executor.TestInterceptBeforeIndexDropFirstStep = null;
        }

        Assert.AreEqual(SchemaElementState.Public, LiveState(database, "robots", "name_uq"));
        Assert.AreEqual(1, await CountCoordinatorJobsAsync(database), "the job of the drop waits for the resume");

        await Ddl(executor, dbname, "CREATE TABLE kids (id int64 PRIMARY KEY NOT NULL, robot string REFERENCES robots (name))");
        Assert.IsTrue(database.Schema.Tables["kids"].ForeignKeys is { Count: 1 }, "the constraint claimed the public index");

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database);

        Assert.AreEqual(SchemaElementState.Public, LiveState(database, "robots", "name_uq"), "the index the constraint names stays");
        Assert.IsTrue(database.Schema.Tables["kids"].ForeignKeys is { Count: 1 }, "the constraint stays");
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database), "the drop was given up");
        Assert.AreEqual(1, (await Query(executor, dbname, "SELECT id FROM robots@{FORCE_INDEX=name_uq} WHERE name = 'b'")).Count);

        await NonQuery(executor, dbname, "INSERT INTO kids (id, robot) VALUES (1, 'a')");
        Assert.ThrowsAsync<CamusDBException>(async () => await NonQuery(executor, dbname, "INSERT INTO kids (id, robot) VALUES (2, 'zz')"),
            "the constraint is still enforced through its parent key");
    }

    /// <summary>
    /// The write that replaces the job of a drop with its purge record fails. The index is gone, the job stays,
    /// and the resume writes the record and purges the entries.
    /// </summary>
    [Test]
    public async Task AFailedPurgeRecordWrite_LeavesTheJob_AndTheResumeWritesTheRecord()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptBeforeIndexDropRecord = () => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected record failure");
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptBeforeIndexDropRecord = null;
        }

        Assert.IsNull(LiveState(database, "robots", "year_idx"));
        Assert.AreEqual(1, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
        Assert.AreEqual(3, await CountEntriesAsync(database, indexSpace));

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database);
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace));
    }

    /// <summary>
    /// A drop stopped after its last step leaves its job. A <c>CREATE INDEX</c> of the same name tries to
    /// replace that job with its purge record, and the write fails. The create fails without a take-back of
    /// its own index, so the job of the earlier drop stays, and the resume purges the old entries.
    /// </summary>
    [Test]
    public async Task ACreateThatFailsToSettleAnEarlierDrop_LeavesThatDropsJob()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string tableId = database.Schema.Tables["robots"].Id!;
        string oldIndexId = database.Schema.Tables["robots"].Indexes!.First(ix => ix.Name == "year_idx").KvId;
        string oldSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == SchemaElementState.Absent
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure after the last step")
            : Task.CompletedTask;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        executor.TestInterceptBeforeIndexDropRecord = () => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected record failure");
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)"));
        }
        finally
        {
            executor.TestInterceptBeforeIndexDropRecord = null;
        }

        PersistedCoordinatorJob? job = await ReadJobAsync(database, tableId, "year_idx");
        Assert.IsNotNull(job, "the job key still holds a job");
        Assert.IsTrue(job!.IndexDrop, "and it is the earlier drop");
        Assert.AreEqual(oldIndexId, job.IndexId);
        Assert.IsNull(LiveState(database, "robots", "year_idx"), "the failed create added nothing");
        Assert.AreEqual(3, await CountEntriesAsync(database, oldSpace));

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database);
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.AreEqual(0, await CountEntriesAsync(database, oldSpace), "the resume finished the earlier drop");
        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));

        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        Assert.AreEqual(2, (await Query(executor, dbname, "SELECT id FROM robots@{FORCE_INDEX=year_idx} WHERE year = 2000")).Count);
    }

    /// <summary>
    /// A transaction writes a row while the index is public, and commits after the drop took the index to
    /// <c>DeleteOnly</c>. The steps fenced the writers planned before them, so the commit is refused, and no
    /// entry of that write reaches the key space after the purge.
    /// </summary>
    [Test]
    public async Task AWriterPlannedBeforeTheSteps_CannotLeaveAnEntryBehindThePurge()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        KvTransaction writer = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, dbname, "INSERT INTO robots (id, name, year) VALUES (50, 'w', 2000)", null));

        CamusDBException? refusal = null;
        executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state != SchemaElementState.DeleteOnly)
                return;

            try
            {
                await database.Transactions.CommitAsync(writer);
            }
            catch (CamusDBException ex)
            {
                refusal = ex;
            }
        };
        try
        {
            await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
            await database.Transactions.RollbackIfNotCompletedAsync(writer);
        }

        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        Assert.IsNotNull(refusal, "the write planned while the index was public is refused at its commit");
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace), "no entry is left behind the purge");
        Assert.AreEqual(2, await CountYear2000Async(executor, dbname));
    }

    /// <summary>
    /// A drop stopped at <c>WriteOnly</c>, then a retained <c>DROP TABLE</c>. The table drop replaces the job of
    /// the unfinished drop with its purge record, and <c>RELINK</c> brings the table back without the index, so
    /// no index stays between states with nothing to finish it.
    /// </summary>
    [Test]
    public async Task ARelinkedTable_DoesNotBringBackAnIndexThatADropLeftBetweenStates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string tableId = database.Schema.Tables["robots"].Id!;
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == SchemaElementState.WriteOnly
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure after WriteOnly")
            : Task.CompletedTask;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        Assert.AreEqual(SchemaElementState.WriteOnly, LiveState(database, "robots", "year_idx"));

        await Ddl(executor, dbname, "DROP TABLE robots");

        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database), "the table drop deleted the jobs of the table");
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database), "and kept the purge of the unfinished drop");

        await Ddl(executor, dbname, $"CREATE TABLE robots_back RELINK TO \"{tableId}\"");

        Assert.IsNull(LiveState(database, "robots_back", "year_idx"), "an index between states does not come back");
        Assert.AreEqual(3, (await Query(executor, dbname, "SELECT id FROM robots_back")).Count, "the rows come back");

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(database));
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace));
        Assert.AreEqual(2, (await Query(executor, dbname, "SELECT id FROM robots_back WHERE year = 2000")).Count);
    }

    /// <summary>
    /// A drop stopped after its last step: the index is out of the schema, and its job is the only record of
    /// its entries. <c>DROP TABLE ... FORCE</c> purges the indexes the table lists, which no longer include it,
    /// so the table drop replaces the job with its purge record, and the run removes the entries.
    /// </summary>
    [Test]
    public async Task AForceDropOfTheTable_KeepsThePurgeOfADropStoppedAfterItsLastStep()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(NoSweep);
        await SetupRobots(executor, dbname);
        string indexSpace = IndexSpaceOf(database, "robots", "year_idx");

        executor.TestInterceptAfterIndexDropStep = state => state == SchemaElementState.Absent
            ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "injected failure after the last step")
            : Task.CompletedTask;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx ON robots"));
        }
        finally
        {
            executor.TestInterceptAfterIndexDropStep = null;
        }

        await Ddl(executor, dbname, "DROP TABLE robots FORCE");

        Assert.AreEqual(0, await CountCoordinatorJobsAsync(database));
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database), "the job of the unfinished drop became its purge record");

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(database));
        Assert.AreEqual(0, await CountEntriesAsync(database, indexSpace));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
    }

    [Test]
    public async Task TheDrop_SurvivesCloseAndReopen()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);
        await Ddl(executor, dbname, "CREATE INDEX name_idx ON robots (name)");

        await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
        await Ddl(executor, dbname, "DROP INDEX robots.name_idx");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsFalse(HasIndex(reopened, "robots", "year_idx"));
        Assert.IsFalse(HasIndex(reopened, "robots", "name_idx"));

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 AND name = 'c'");
        Assert.AreEqual(1, rows.Count);
    }
}

[TestFixture]
[NonParallelizable]
internal sealed class TestDropIndexStandalone : DropIndexScenarios
{
    protected override bool ClusterMode => false;
}

[TestFixture]
[NonParallelizable]
internal sealed class TestDropIndexCluster : DropIndexScenarios
{
    protected override bool ClusterMode => true;
}
