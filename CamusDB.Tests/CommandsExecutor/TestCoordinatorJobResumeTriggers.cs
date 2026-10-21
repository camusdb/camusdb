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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A staged schema change whose driver is gone must be finished by whichever trigger reaches it,
/// not only by the leader election. The election callback is registered when a database opens, so a
/// node that wins the schema leadership before it opens the database never hears about the jobs it
/// now owns; a resume that fails on a transient is not asked again either. Both left a
/// <c>CREATE INDEX</c> cut by a kill in <c>WriteOnly</c> for good: "already exists" to <c>CREATE</c>
/// and missing to every query. The open-time resume and the leader's periodic sweep close that.
/// The node of the shared fixture is the schema leader of every database it opens.
/// </summary>
[NonParallelizable]
internal sealed class TestCoordinatorJobResumeTriggers : SharedNodeBaseTest
{
    private const int Rows = 40;

    private const string IndexName = "items_by_name";

    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(30);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
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

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// A database whose table <c>items</c> holds <see cref="Rows"/> rows, with a sweep that runs only
    /// when a test drives it, so each test exercises one trigger.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable(int sweepIntervalMs = 0)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = sweepIntervalMs });

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)");
        for (int i = 1; i <= Rows; i++)
            await ExecuteNonQuery(executor, dbname, $"INSERT INTO items (id, name) VALUES ({i}, 'n{i}')");

        return (dbname, database, executor);
    }

    /// <summary>
    /// Leaves <c>items</c> as a killed <c>CREATE INDEX</c> driver does: the index replicated up to
    /// <c>WriteOnly</c> on every node, no entries built, and the job record that targets
    /// <c>Public</c> persisted with no attempt made.
    /// </summary>
    private static async Task<PersistedCoordinatorJob> StageWriteOnlyIndex(CommandExecutor executor, DatabaseDescriptor database)
    {
        TableSchema table = database.Schema.Tables["items"];
        string nameId = table.Columns!.First(c => c.Name == "name").Id!;
        string indexId = ObjectIdGenerator.Generate().ToString();

        IndexBuildInfo info = new(indexId, IndexName, [nameId], ["name"], IndexType.Multi);
        await executor.Catalogs.ReplicateAddIndexInStateAsync(database, "items", info, SchemaElementState.DeleteOnly);
        await executor.Catalogs.ReplicateElementStateAsync(database, "items", IndexName, SchemaElementState.WriteOnly, SchemaElementKind.Index);

        PersistedCoordinatorJob job = new()
        {
            TableName = "items",
            TableId = table.Id!,
            ElementName = IndexName,
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Index,
            IndexId = indexId,
            IndexColumnIds = [nameId],
            IndexType = IndexType.Multi,
            Attempts = 0,
        };
        await executor.Catalogs.PersistCoordinatorJobAsync(database, job);

        Assert.AreEqual(SchemaElementState.WriteOnly, IndexState(database), "the staged index starts in WriteOnly");
        return job;
    }

    private static SchemaElementState IndexState(DatabaseDescriptor database) =>
        database.Schema.Tables["items"].Indexes?.FirstOrDefault(ix => ix.Name == IndexName)?.State ?? SchemaElementState.Absent;

    private static Task<PersistedCoordinatorJob?> LoadJob(CommandExecutor executor, DatabaseDescriptor database, PersistedCoordinatorJob job) =>
        executor.Catalogs.TryLoadCoordinatorJobAsync(database, job.TableId, job.ElementName);

    /// <summary>Waits until the job is gone and the index is <c>Public</c> on the given descriptor.</summary>
    private static async Task WaitForPublishedAsync(CommandExecutor executor, DatabaseDescriptor database, PersistedCoordinatorJob job, string because)
    {
        DateTime deadline = DateTime.UtcNow.Add(Soon);

        while (true)
        {
            if (IndexState(database) == SchemaElementState.Public && await LoadJob(executor, database, job) is null)
                return;

            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), because);
            await Task.Delay(50);
        }
    }

    /// <summary>A coordinator of this test's own, with the index backfill replaced by <paramref name="backfill"/>.</summary>
    private SchemaChangeCoordinator Coordinator(CommandExecutor executor, Func<CancellationToken, Task> backfill) => new(executor.Catalogs, logger)
    {
        IndexBackfillAsync = (_, _, _, _, _, cancellationToken) => backfill(cancellationToken),
    };

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// The rolling-kill shape: the node leads the schema partition before it opens the database, so the leader
    /// callback the open registers never fires for the job. The open itself must resume it.
    /// </summary>
    [Test]
    public async Task WriteOnlyIndexJob_IsFinishedWhenTheDatabaseOpensOnTheSchemaLeader()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        PersistedCoordinatorJob job = await StageWriteOnlyIndex(executor, database);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        await WaitForPublishedAsync(executor, reopened, job, "the open on the schema leader must finish the job it found");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id FROM items WHERE name = 'n7'");
        Assert.AreEqual(1, rows.Count, "the published index must answer a lookup on its column");
        Assert.AreEqual(7, rows[0]["id"].LongValue);
    }

    /// <summary>
    /// No election and no open: the sweep alone must find a job that no statement drives on this node
    /// and bring the index to Public.
    /// </summary>
    [Test]
    public async Task WriteOnlyIndexJob_IsFinishedByTheSweepWithoutALeaderChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        PersistedCoordinatorJob job = await StageWriteOnlyIndex(executor, database);

        int swept = await executor.SweepCoordinatorJobsForTests();

        Assert.That(swept, Is.GreaterThanOrEqualTo(1), "the sweep must visit the database this node leads");
        await WaitForPublishedAsync(executor, database, job, "the sweep must finish the job it found");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id FROM items WHERE name = 'n12'");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(12, rows[0]["id"].LongValue);
    }

    /// <summary>The configured sweep loop reaches the job on its own, within a few ticks.</summary>
    [Test]
    public async Task WriteOnlyIndexJob_IsFinishedByTheSweepLoop()
    {
        (string _, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(sweepIntervalMs: 200);
        PersistedCoordinatorJob job = await StageWriteOnlyIndex(executor, database);

        await WaitForPublishedAsync(executor, database, job, "the sweep loop must finish the job within its interval");
    }

    /// <summary>
    /// The leader callback, the open and the sweep can ask for a resume at the same time. One resume
    /// runs per database; the job is picked up once, its backfill runs once, and its attempts count one.
    /// </summary>
    [Test]
    public async Task ConcurrentResumes_DriveAJobOnce()
    {
        (string _, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        PersistedCoordinatorJob job = await StageWriteOnlyIndex(executor, database);

        int backfills = 0;
        SchemaChangeCoordinator coordinator = Coordinator(executor, async _ =>
        {
            Interlocked.Increment(ref backfills);
            await Task.Delay(300);
        });

        Task first = coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.LeaderElected);
        Task second = coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);
        Task third = coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.DatabaseOpened);
        await Task.WhenAll(first, second, third);

        await WaitForPublishedAsync(executor, database, job, "the one resume that ran must finish the job");
        Assert.AreEqual(1, Volatile.Read(ref backfills), "the backfill must run once, not once per request");
    }

    /// <summary>
    /// A request that arrives while a resume runs is not lost: the running resume reads the jobs
    /// again before it ends, so a job recorded during the run is driven without a further trigger.
    /// </summary>
    [Test]
    public async Task ResumeRequestedDuringAResume_RunsAgainAndFindsTheNewJob()
    {
        (string _, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        TaskCompletionSource firstRunStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirstRun = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int backfills = 0;

        // The first resume drives a column job whose fill blocks until the test lets it go; the index
        // job is recorded meanwhile.
        SchemaChangeCoordinator coordinator = new(executor.Catalogs, logger)
        {
            BackfillAsync = async (_, _, _, _) =>
            {
                firstRunStarted.TrySetResult();
                await releaseFirstRun.Task;
            },
            IndexBackfillAsync = (_, _, _, _, _, _) =>
            {
                Interlocked.Increment(ref backfills);
                return Task.CompletedTask;
            },
        };

        TableSchema table = database.Schema.Tables["items"];
        ColumnInfo column = new("note", ColumnType.String, notNull: false, defaultValue: null);
        await executor.Catalogs.ReplicateAddColumnInStateAsync(database, "items", column, SchemaElementState.DeleteOnly);
        await executor.Catalogs.ReplicateElementStateAsync(database, "items", "note", SchemaElementState.WriteOnly, SchemaElementKind.Column);
        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = table.Id!,
            ElementName = "note",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.String,
            Attempts = 0,
        });

        Task firstRun = coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.LeaderElected);
        await firstRunStarted.Task.WaitAsync(Soon);

        // Recorded while the first resume is inside its fill: the sweep's request is turned away by
        // the running resume, which must go round again for it.
        PersistedCoordinatorJob indexJob = await StageWriteOnlyIndex(executor, database);
        Task sweepRequest = coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);
        await sweepRequest.WaitAsync(Soon);

        Assert.AreEqual(0, Volatile.Read(ref backfills), "the turned-away request must not drive the job itself");

        releaseFirstRun.SetResult();
        await firstRun.WaitAsync(Soon);

        await WaitForPublishedAsync(executor, database, indexJob, "the running resume must pick up the job recorded during it");
        Assert.AreEqual(1, Volatile.Read(ref backfills));
        Assert.AreEqual(SchemaElementState.Public,
            database.Schema.Tables["items"].Columns!.First(c => c.Name == "note").State, "the column job finishes too");
    }

    /// <summary>
    /// A job whose attempt failed on this node is not retried at once by the next trigger: it waits
    /// its backoff out, so the sweep cannot burn the attempt budget in a few ticks, and is driven
    /// again after it.
    /// </summary>
    [Test]
    public async Task FailedResume_WaitsForItsBackoffBeforeTheNextAttempt()
    {
        (string _, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        PersistedCoordinatorJob job = await StageWriteOnlyIndex(executor, database);

        int calls = 0;
        SchemaChangeCoordinator coordinator = Coordinator(executor, _ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new InvalidOperationException("the first backfill attempt fails");

            return Task.CompletedTask;
        });
        coordinator.ResumeRetryBackoff = TimeSpan.FromMilliseconds(600);
        coordinator.ResumeRetryBackoffMax = TimeSpan.FromMilliseconds(600);

        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.LeaderElected);

        PersistedCoordinatorJob? afterFailure = await LoadJob(executor, database, job);
        Assert.IsNotNull(afterFailure, "a failed attempt leaves the record");
        Assert.AreEqual(1, afterFailure!.Attempts);
        Assert.AreEqual(SchemaElementState.WriteOnly, IndexState(database));

        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        PersistedCoordinatorJob? inBackoff = await LoadJob(executor, database, job);
        Assert.IsNotNull(inBackoff, "a sweep inside the backoff must leave the job alone");
        Assert.AreEqual(1, inBackoff!.Attempts, "a sweep inside the backoff must not spend an attempt");
        Assert.AreEqual(1, Volatile.Read(ref calls));

        await Task.Delay(700);
        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        await WaitForPublishedAsync(executor, database, job, "the sweep after the backoff must drive the job again");
        Assert.AreEqual(2, Volatile.Read(ref calls));
    }

    /// <summary>A resume that finds no job says so for an election or an open, and the database stays as it was.</summary>
    [Test]
    public async Task ResumeWithNoJobs_IsANoOp()
    {
        (string _, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        SchemaChangeCoordinator coordinator = Coordinator(executor, _ => throw new InvalidOperationException("nothing must be driven"));

        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.LeaderElected);
        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.DatabaseOpened);
        await coordinator.ResumeJobsAsync(database, CoordinatorResumeTrigger.Sweep);

        Assert.AreEqual(1, await executor.SweepCoordinatorJobsForTests());
        Assert.AreEqual(SchemaElementState.Absent, IndexState(database));
    }
}
