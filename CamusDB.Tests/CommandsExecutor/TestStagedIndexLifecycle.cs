
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

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// What a staged index build leaves behind: nothing after a failure, a finished index after a stop and a
/// restart, and never an index that a late resume builds again. A standalone node builds an index through
/// the same coordinator as a cluster node, so each scenario runs in both modes.
/// </summary>
[NonParallelizable]
internal abstract class StagedIndexLifecycleScenarios : SharedNodeBaseTest
{
    /// <summary>Three backfill batches of 500.</summary>
    private const int Rows = 1_200;

    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql) =>
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbname, sql, null));

    private static async Task Dml(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> Ids(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<long> ids = [];
            await foreach (QueryResultRow row in cursor)
                ids.Add(row.Row["id"].LongValue);
            return ids;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Table <c>items</c> with <c>val = id</c>, and the last row's <c>val</c> equal to the first's.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable(bool withDuplicate)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, val INT64 NOT NULL)");

        for (int start = 1; start <= Rows; start += 200)
        {
            string values = string.Join(", ", Enumerable.Range(start, 200).Select(i => $"({i}, {(withDuplicate && i == Rows ? 1 : i)})"));
            await Dml(executor, dbname, "INSERT INTO items (id, val) VALUES " + values);
        }

        return (dbname, database, executor);
    }

    /// <summary>Every key in the key space of one index, read below the table store.</summary>
    private static async Task<int> KeysInIndexSpace(DatabaseDescriptor database, string indexId)
    {
        string space = KvKeyBuilder.IndexSpaceOf(database.Id, database.Schema.Tables["items"].EffectiveStorageId, indexId);

        int keys = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, space, null, true, null, true, 512, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(space + "/", StringComparison.Ordinal))
                keys++;
        }

        return keys;
    }

    private static TableIndexSchema? Index(DatabaseDescriptor database, string name) =>
        database.Schema.Tables["items"].Indexes?.Find(ix => ix.Name == name);

    /// <summary>
    /// A unique build whose duplicate is in its last batch fails after two batches committed. It leaves no
    /// index, no job and no entry, and the statement runs again once the duplicate is gone.
    /// </summary>
    [Test]
    public async Task AFailedBuildLeavesNoIndexNoJobAndNoEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: true);

        string? indexId = null;
        int entriesDuringBuild = 0;
        executor.TestInterceptAfterBackfillCheckpoint = async () =>
        {
            indexId = Index(database, "items_val")!.KvId;
            entriesDuringBuild = await KeysInIndexSpace(database, indexId);
        };

        CamusDBException failed;
        try
        {
            failed = Assert.ThrowsAsync<CamusDBException>(() => Ddl(executor, dbname, "CREATE UNIQUE INDEX items_val ON items (val)"))!;
        }
        finally
        {
            executor.TestInterceptAfterBackfillCheckpoint = null;
        }

        Assert.That(failed.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
        Assert.That(entriesDuringBuild, Is.GreaterThan(0), "batches committed before the failure");
        Assert.That(Index(database, "items_val"), Is.Null, "the index is gone from the schema");
        Assert.That((await executor.OpenTable(new OpenTableTicket(dbname, "items"))).AllIndexes.ContainsKey("items_val"), Is.False);
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "no job is left to build it again");
        Assert.That(await KeysInIndexSpace(database, indexId!), Is.Zero, "the entries of the committed batches are purged");
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));

        await Dml(executor, dbname, $"UPDATE items SET val = {Rows} WHERE id = {Rows}");
        await Ddl(executor, dbname, "CREATE UNIQUE INDEX items_val ON items (val)");

        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await KeysInIndexSpace(database, Index(database, "items_val")!.KvId), Is.EqualTo(Rows));
        Assert.That(await Ids(executor, dbname, $"SELECT id FROM items@{{FORCE_INDEX=items_val}} WHERE val = {Rows}"), Is.EqualTo(new[] { (long)Rows }));
    }

    /// <summary>
    /// The node stopped during the backfill: the index is <c>WriteOnly</c> in the durable schema and the job
    /// is recorded. The open of the database by the next process finds the job and finishes the index.
    /// </summary>
    [Test]
    public async Task AnInterruptedBuildIsFinishedByTheResume()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        string indexId = await StopInWriteOnly(executor, database, IndexType.Multi);

        // A write between the stop and the resume maintains the WriteOnly index.
        await Dml(executor, dbname, $"INSERT INTO items (id, val) VALUES ({Rows + 1}, {Rows + 1})");

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.DatabaseOpened);

        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await KeysInIndexSpace(database, indexId), Is.EqualTo(Rows + 1), "one entry per row");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
    }

    /// <summary>
    /// The job of an index build that stopped in <c>WriteOnly</c>, with the index in that state in the
    /// schema: what a crash during the backfill leaves behind.
    /// </summary>
    private static async Task<string> StopInWriteOnly(
        CommandExecutor executor, DatabaseDescriptor database, IndexType type, int attempts = 0, SchemaElementState target = SchemaElementState.Public)
    {
        TableSchema items = database.Schema.Tables["items"];
        TableColumnSchema val = items.Columns!.Single(c => c.Name == "val");
        string indexId = ObjectIdGenerator.Generate().ToString();

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = items.Id!,
            ElementName = "items_val",
            TargetState = target,
            ElementKind = SchemaElementKind.Index,
            IndexId = indexId,
            IndexColumnIds = [val.Id],
            IndexType = type,
            Attempts = attempts,
        });
        await executor.Catalogs.ReplicateAddIndexInStateAsync(
            database, "items", new IndexBuildInfo(indexId, "items_val", [val.Id], ["val"], type), SchemaElementState.DeleteOnly);
        await executor.Catalogs.ReplicateElementStateAsync(database, "items", "items_val", SchemaElementState.WriteOnly, SchemaElementKind.Index);

        return indexId;
    }

    /// <summary>Asserts that nothing of <c>items_val</c> is left and that the statement can run again.</summary>
    private static async Task AssertRemoved(CommandExecutor executor, DatabaseDescriptor database, string dbname, string indexId)
    {
        Assert.That(Index(database, "items_val"), Is.Null, "the index is gone from the schema");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "no job is left");
        Assert.That(await KeysInIndexSpace(database, indexId), Is.Zero, "no entry is left");
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));

        await Ddl(executor, dbname, "CREATE INDEX items_val ON items (val)");
        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await KeysInIndexSpace(database, Index(database, "items_val")!.KvId), Is.EqualTo(Rows));
    }

    /// <summary>
    /// The build fails and the drop of its index fails too, because the schema subsystem refuses proposals.
    /// The job then records the removal, so a resume finishes it. Without that record the index would stay
    /// <c>WriteOnly</c> for good: its name is taken and no query reads it.
    /// </summary>
    [Test]
    public async Task ADropThatFailsIsFinishedByTheResume()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: true);

        string? indexId = null;
        executor.TestInterceptAfterBackfillCheckpoint = () =>
        {
            indexId = Index(database, "items_val")!.KvId;
            database.MarkSchemaSubsystemDegraded();
            return Task.CompletedTask;
        };

        CamusDBException failed;
        try
        {
            failed = Assert.ThrowsAsync<CamusDBException>(() => Ddl(executor, dbname, "CREATE UNIQUE INDEX items_val ON items (val)"))!;
        }
        finally
        {
            executor.TestInterceptAfterBackfillCheckpoint = null;
            database.ClearSchemaSubsystemDegraded();
        }

        Assert.That(failed.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue), "the statement reports the failure of the build");
        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.WriteOnly), "the drop was refused");

        PersistedCoordinatorJob removal = (await executor.Catalogs.LoadCoordinatorJobsAsync(database)).Single();
        Assert.That(removal.TargetState, Is.EqualTo(SchemaElementState.Absent), "the job records the removal");
        Assert.That(removal.FailureCode, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue), "and the reason");

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        await AssertRemoved(executor, database, dbname, indexId!);
    }

    /// <summary>
    /// A unique build that a crash stopped, over two rows with the same key. No later attempt can finish it,
    /// and until it is gone the index refuses writes. The first resume removes it.
    /// </summary>
    [Test]
    public async Task AResumedUniqueBuildOverADuplicateIsTakenBackAtOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: true);
        string indexId = await StopInWriteOnly(executor, database, IndexType.Unique);

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.DatabaseOpened);

        await AssertRemoved(executor, database, dbname, indexId);
    }

    /// <summary>A build that spent its resume attempts is taken back, and its name is free again.</summary>
    [Test]
    public async Task AnAbandonedBuildIsTakenBack()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        string indexId = await StopInWriteOnly(executor, database, IndexType.Multi, attempts: SchemaChangeCoordinator.MaxResumeAttempts);

        // A row written meanwhile has an entry, which the removal purges.
        await Dml(executor, dbname, "UPDATE items SET val = 999999 WHERE id = 1");
        Assert.That(await KeysInIndexSpace(database, indexId), Is.EqualTo(1));

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        await AssertRemoved(executor, database, dbname, indexId);
    }

    /// <summary>
    /// A removal job is matched to its index by id. The index of that name is a later one here, published
    /// by its own statement, so the stale job is deleted and the index stays.
    /// </summary>
    [Test]
    public async Task ARemovalJobNeverRemovesALaterIndexOfTheSameName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        await Ddl(executor, dbname, "CREATE INDEX items_val ON items (val)");
        TableSchema items = database.Schema.Tables["items"];

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = items.Id!,
            ElementName = "items_val",
            TargetState = SchemaElementState.Absent,
            ElementKind = SchemaElementKind.Index,
            IndexId = ObjectIdGenerator.Generate().ToString(),
            IndexColumnIds = [items.Columns!.Single(c => c.Name == "val").Id],
            IndexType = IndexType.Multi,
        });

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await KeysInIndexSpace(database, Index(database, "items_val")!.KvId), Is.EqualTo(Rows), "its entries stay");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "the stale job is deleted");
    }

    /// <summary>
    /// A resume never removes an index that queries read. The record of a finished build can outlive it,
    /// when its delete failed on the last attempt, and a removal record can be older than the publish.
    /// </summary>
    [TestCase(SchemaElementState.Public, SchemaChangeCoordinator.MaxResumeAttempts)]
    [TestCase(SchemaElementState.Absent, 0)]
    public async Task AResumeNeverRemovesAPublishedIndex(SchemaElementState target, int attempts)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        await Ddl(executor, dbname, "CREATE INDEX items_val ON items (val)");
        TableSchema items = database.Schema.Tables["items"];
        string indexId = Index(database, "items_val")!.KvId;

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = items.Id!,
            ElementName = "items_val",
            TargetState = target,
            ElementKind = SchemaElementKind.Index,
            IndexId = indexId,
            IndexColumnIds = [items.Columns!.Single(c => c.Name == "val").Id],
            IndexType = IndexType.Multi,
            Attempts = attempts,
        });

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await KeysInIndexSpace(database, indexId), Is.EqualTo(Rows), "its entries stay");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "the stale job is deleted");
    }

    /// <summary>
    /// The job of a build whose table was dropped meanwhile is stale: the resume deletes it and changes
    /// nothing else.
    /// </summary>
    [Test]
    public async Task ARemovalJobOfADroppedTableIsDeleted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        await StopInWriteOnly(executor, database, IndexType.Multi, target: SchemaElementState.Absent);
        await Ddl(executor, dbname, "DROP TABLE items FORCE");

        await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
    }

    /// <summary>
    /// A resume reads the job list, then waits for the gate that the statement of the job holds. The
    /// statement fails meanwhile, removes its index and deletes its record. The resume must then leave the
    /// job alone: from the record it read, it would add the index that the client was told does not exist.
    /// </summary>
    [Test]
    public async Task AResumeDoesNotDriveAJobThatEndedWhileItWaitedForTheGate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);
        TableSchema items = database.Schema.Tables["items"];
        TableColumnSchema val = items.Columns!.Single(c => c.Name == "val");

        await database.DdlGate.EnterAsync("items");
        Task resume;
        try
        {
            await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
            {
                TableName = "items",
                TableId = items.Id!,
                ElementName = "items_val",
                TargetState = SchemaElementState.Public,
                ElementKind = SchemaElementKind.Index,
                IndexId = ObjectIdGenerator.Generate().ToString(),
                IndexColumnIds = [val.Id],
                IndexType = IndexType.Multi,
            });

            resume = Task.Run(() => executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep));

            // It has read the list and waits for the gate.
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.That(resume.IsCompleted, Is.False, "the resume waits for the gate");

            await executor.Catalogs.DeleteCoordinatorJobAsync(database, items.Id!, "items_val");
        }
        finally
        {
            database.DdlGate.Exit();
        }

        await resume.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.That(Index(database, "items_val"), Is.Null, "the resume added nothing");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "and recorded nothing");
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
    }

    /// <summary>
    /// A resume that waited for the gate behind a statement that then started its backfill finds the table
    /// held, and leaves the job to that statement without spending one of its resume attempts.
    /// </summary>
    [Test]
    public async Task AResumeLeavesAJobToTheStatementThatHoldsItsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);

        TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.TestInterceptAfterBackfillCheckpoint = async () =>
        {
            reached.TrySetResult();
            await released.Task;
        };

        Task createIndex = Task.Run(() => Ddl(executor, dbname, "CREATE INDEX items_val ON items (val)"));
        try
        {
            await reached.Task.WaitAsync(TimeSpan.FromSeconds(20));

            await executor.ResumeCoordinatorForTests.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

            PersistedCoordinatorJob job = (await executor.Catalogs.LoadCoordinatorJobsAsync(database)).Single();
            Assert.That(job.Attempts, Is.Zero, "the resume spent no attempt");
            Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.WriteOnly));
        }
        finally
        {
            executor.TestInterceptAfterBackfillCheckpoint = null;
            released.TrySetResult();
        }

        await createIndex.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.That(Index(database, "items_val")!.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);
    }

    /// <summary>
    /// A database whose staged change runs its long phase is not evicted. The phase holds no semaphore, and
    /// a job that a resume drives holds no reference to the descriptor either: only the table hold tells.
    /// </summary>
    [Test]
    public async Task ADatabaseWithATableHeldByAStagedChangeIsNotEvicted()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(withDuplicate: false);

        await database.DdlGate.EnterAsync("items");
        SchemaDdlGate.StagedTableHold hold = database.DdlGate.HoldTable(database.Schema.Tables["items"].Id!, "items", "index 'items_val'");
        database.DdlGate.Exit();

        try
        {
            for (int attempt = 0; attempt < 20; attempt++)
            {
                Assert.That(executor.TryEvictDatabaseForTests(database.Id, 0), Is.EqualTo(DatabaseEvictionOutcome.InUse));
                await Task.Delay(25);
            }
        }
        finally
        {
            hold.Dispose();
        }
    }
}

[TestFixture]
internal sealed class TestStagedIndexLifecycleStandalone : StagedIndexLifecycleScenarios
{
    protected override bool ClusterMode => false;
}

[TestFixture]
internal sealed class TestStagedIndexLifecycleClusterMode : StagedIndexLifecycleScenarios
{
    protected override bool ClusterMode => true;
}
