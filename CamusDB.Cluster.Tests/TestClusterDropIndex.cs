/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Cluster;

using static CamusDB.Tests.Cluster.ClusterStatements;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>DROP INDEX</c> outside a transaction in a three-node cluster. The index goes down
/// <c>Public → WriteOnly → DeleteOnly → Absent</c> on every node before its entries are purged, so no
/// node reads an index that another node stopped maintaining, and no write lands in the key space after
/// the purge. Before the staged drop, the schema leader purged the entries first: a follower that had not
/// applied the drop yet read the index with no entries and missed every row, and its writes left entries
/// in the purged key space that nothing removed.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterDropIndex
{
    private const int RowCount = 20;

    private static async Task<(InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace)> PrepareAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3,
            wireLeaderForwarder: true,
            options: CamusDBOptions.Default with { CoordinatorJobSweepIntervalMs = 0 });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        await AutocommitAsync(leader, db, "CREATE TABLE base (id int64 PRIMARY KEY, name string)", ddl: true);
        await AutocommitAsync(leader, db, "CREATE INDEX name_idx ON base (name)", ddl: true);
        string rows = string.Join(", ", Enumerable.Range(1, RowCount).Select(i => $"({i}, 'n{i % 4}')"));
        await AutocommitAsync(leader, db, "INSERT INTO base (id, name) VALUES " + rows, ddl: false);
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);

        TableSchema table = leader.Database!.Schema.Tables["base"];
        string indexSpace = KvKeyBuilder.IndexSpaceOf(leader.Database.Id, table.EffectiveStorageId, table.Indexes!.First(i => i.Name == "name_idx").KvId);
        Assert.AreEqual(RowCount, await CountKeysAsync(leader.Database, indexSpace, indexSpace + "/"));

        return (cluster, db, leader, follower, indexSpace);
    }

    private static Task<int> CountEntriesAsync(DatabaseDescriptor database, string indexSpace) =>
        CountKeysAsync(database, indexSpace, indexSpace + "/");

    private static Task<int> CountMetaAsync(DatabaseDescriptor database, string prefix) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), prefix);

    private static async Task<int> CountN1Async(InProcessSchemaCluster.Node node, string db) =>
        (await QueryAsync(node, db, "SELECT id FROM base WHERE name = 'n1'")).Count;

    /// <summary>
    /// Holds the schema lock of <paramref name="node"/>, so the node stays live but applies no schema entry,
    /// and returns the release.
    /// </summary>
    private static async Task<Func<Task>> HoldSchemaApplyAsync(InProcessSchemaCluster.Node node)
    {
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task holder = Task.Run(async () =>
        {
            Schema schema = node.Database!.Schema;
            await schema.AcquireLockAsync();
            try
            {
                held.TrySetResult();
                await release.Task;
            }
            finally
            {
                schema.ReleaseLock();
            }
        });

        await held.Task.WaitAsync(TimeSpan.FromSeconds(10));

        return async () =>
        {
            release.TrySetResult();
            await holder.WaitAsync(TimeSpan.FromSeconds(10));
        };
    }

    /// <summary>
    /// At every step of the drop, every node inserts, updates and deletes rows that the index would hold, and
    /// reads the rows the index would answer, through whatever plan its own schema allows. Each read sees
    /// every row, and after the purge the key space of the index is empty.
    /// </summary>
    [Test]
    public async Task WritersAndReadersOnEveryNode_SeeEveryRow_AndNoEntrySurvivesThePurge()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace) = await PrepareAsync();
        await using InProcessSchemaCluster _ = cluster;

        int expected = await CountN1Async(leader, db);
        int nextId = 1000;
        List<string> failures = [];

        leader.Executor.TestInterceptAfterIndexDropStep = async state =>
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                int inserted = nextId++;
                int moved = nextId++;

                // The update moves a row into the value the reads count, and the delete removes the
                // inserted row again: one more matching row per node and step.
                await AutocommitAsync(node, db, $"INSERT INTO base (id, name) VALUES ({inserted}, 'n1'), ({moved}, 'n3')", ddl: false);
                await AutocommitAsync(node, db, $"UPDATE base SET name = 'n1' WHERE id = {moved}", ddl: false);
                await AutocommitAsync(node, db, $"DELETE FROM base WHERE id = {inserted}", ddl: false);
                expected++;
            }

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                int seen = await CountN1Async(node, db);
                if (seen != expected)
                    failures.Add($"node {node.Index} at {state}: {seen} of {expected} rows");
            }
        };

        try
        {
            // Sent through a follower, which forwards the statement to the schema leader.
            await AutocommitAsync(follower, db, "DROP INDEX name_idx ON base", ddl: true);
        }
        finally
        {
            leader.Executor.TestInterceptAfterIndexDropStep = null;
        }

        CollectionAssert.IsEmpty(failures, "every node read every row at every step");

        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);
        await leader.Executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsFalse(node.Database!.Schema.Tables["base"].Indexes!.Exists(i => i.Name == "name_idx"), $"node {node.Index} dropped the index");
            Assert.AreEqual(expected, await CountN1Async(node, db), $"node {node.Index} reads every row after the drop");
        }

        Assert.AreEqual(0, await CountEntriesAsync(leader.Database, indexSpace), "no write during the drop left an entry behind the purge");
        Assert.AreEqual(0, await CountMetaAsync(leader.Database, MetaKeys.PurgeKeyPrefix(leader.Database.Id)));
        Assert.AreEqual(0, await CountMetaAsync(leader.Database, MetaKeys.CoordinatorKeyPrefix(leader.Database.Id)));
    }

    /// <summary>
    /// The schema leadership moves after a step of the drop. The drop is finished, on the new leader or
    /// through it, the statement reports it, and no entry or job is left.
    /// </summary>
    [TestCase(SchemaElementState.WriteOnly)]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.Absent)]
    public async Task ALeaderChangeAfterAStepOfTheDrop_IsFinished(SchemaElementState moveAfter)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace) = await PrepareAsync();
        await using InProcessSchemaCluster _ = cluster;

        leader.Executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state == moveAfter)
                await cluster.TransferSchemaLeadershipAsync(db, follower);
        };

        try
        {
            await AutocommitAsync(leader, db, "DROP INDEX name_idx ON base", ddl: true);
        }
        finally
        {
            leader.Executor.TestInterceptAfterIndexDropStep = null;
        }

        InProcessSchemaCluster.Node newLeader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        await cluster.WaitForSchemaConvergenceAsync(db, newLeader.Database!.Schema.SchemaVersion);

        // Whichever node finished the drop scheduled the purge; a sweep of the new leader finishes a record
        // that was written but not run.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            await node.Executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();
        newLeader.Database!.MarkDeferredPurgesDue();
        await newLeader.Executor.SweepCoordinatorJobsForTests();

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsFalse(node.Database!.Schema.Tables["base"].Indexes!.Exists(i => i.Name == "name_idx"), $"node {node.Index} dropped the index");

        Assert.AreEqual(0, await CountEntriesAsync(newLeader.Database, indexSpace));
        Assert.AreEqual(0, await CountMetaAsync(newLeader.Database, MetaKeys.PurgeKeyPrefix(newLeader.Database.Id)));
        Assert.AreEqual(0, await CountMetaAsync(newLeader.Database, MetaKeys.CoordinatorKeyPrefix(newLeader.Database.Id)));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(RowCount, (await QueryAsync(node, db, "SELECT * FROM base")).Count, $"node {node.Index} keeps every row");
    }

    /// <summary>
    /// A live node applies the last step late. The statement does not return, and the purge record is not
    /// written, until that node applied it: the last step alone is settled by a majority, so the drop waits for
    /// every live node itself.
    /// </summary>
    [Test]
    public async Task TheDrop_ReturnsOnlyAfterEveryLiveNodeAppliedTheRemoval()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace) = await PrepareAsync();
        await using InProcessSchemaCluster _ = cluster;
        InProcessSchemaCluster.Node slow = cluster.Nodes.First(n => n.Index != leader.Index && n.Index != follower.Index);

        TimeSpan backstop = leader.Kahuna.SchemaAckQuorumBackstopDelay;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            node.Kahuna.SchemaAckQuorumBackstopDelay = TimeSpan.FromMilliseconds(200);

        Func<Task>? release = null;
        leader.Executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state != SchemaElementState.DeleteOnly)
                return;

            // Every node applied DeleteOnly; the slow node then stops applying before the last step.
            await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);
            release = await HoldSchemaApplyAsync(slow);
        };

        Task drop = AutocommitAsync(leader, db, "DROP INDEX name_idx ON base", ddl: true);
        try
        {
            for (int i = 0; i < 300 && leader.Database!.Schema.Tables["base"].Indexes!.Exists(ix => ix.Name == "name_idx"); i++)
                await Task.Delay(100);

            Assert.IsFalse(leader.Database!.Schema.Tables["base"].Indexes!.Exists(ix => ix.Name == "name_idx"), "the leader applied the last step");

            await Task.Delay(1_500);

            Assert.IsFalse(drop.IsCompleted, "the statement waits for the live node that has not applied the last step");
            Assert.AreEqual(SchemaElementState.DeleteOnly, slow.Database!.Schema.Tables["base"].Indexes!.Single(ix => ix.Name == "name_idx").State);
            Assert.AreEqual(0, await CountMetaAsync(leader.Database, MetaKeys.PurgeKeyPrefix(leader.Database.Id)), "no purge record yet");
            Assert.AreEqual(RowCount, await CountEntriesAsync(leader.Database, indexSpace), "no entry is purged yet");
        }
        finally
        {
            leader.Executor.TestInterceptAfterIndexDropStep = null;

            if (release is not null)
                await release();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                node.Kahuna.SchemaAckQuorumBackstopDelay = backstop;
        }

        await drop.WaitAsync(TimeSpan.FromSeconds(30));
        await leader.Executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsFalse(node.Database!.Schema.Tables["base"].Indexes!.Exists(ix => ix.Name == "name_idx"), $"node {node.Index} dropped the index");

        Assert.AreEqual(0, await CountEntriesAsync(leader.Database!, indexSpace));
    }

    /// <summary>
    /// The schema leadership moves in the middle of a drop, the new leader finishes it, and a client creates
    /// an index of the same name at once. The statement that started the drop waits for the outcome by index id,
    /// so the new index of the same name does not make it report a failure.
    /// </summary>
    [Test]
    public async Task AHandedOverDrop_ReportsItsOwnOutcome_WhenTheNameIsUsedAgainAtOnce()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace) = await PrepareAsync();
        await using InProcessSchemaCluster _ = cluster;

        TimeSpan poll = SchemaChangeCoordinator.HandoverOutcomePoll;
        SchemaChangeCoordinator.HandoverOutcomePoll = TimeSpan.FromSeconds(3);

        Task? recreate = null;
        leader.Executor.TestInterceptAfterIndexDropStep = async state =>
        {
            if (state != SchemaElementState.WriteOnly)
                return;

            await cluster.TransferSchemaLeadershipAsync(db, follower);

            recreate = Task.Run(async () =>
            {
                for (int i = 0; i < 300; i++)
                {
                    bool gone = !follower.Database!.Schema.Tables["base"].Indexes!.Exists(ix => ix.Name == "name_idx");
                    if (gone && await CountMetaAsync(follower.Database, MetaKeys.CoordinatorKeyPrefix(follower.Database.Id)) == 0)
                        break;

                    await Task.Delay(50);
                }

                await AutocommitAsync(follower, db, "CREATE INDEX name_idx ON base (name)", ddl: true);
            });
        };

        try
        {
            await AutocommitAsync(leader, db, "DROP INDEX name_idx ON base", ddl: true);
        }
        finally
        {
            leader.Executor.TestInterceptAfterIndexDropStep = null;
            SchemaChangeCoordinator.HandoverOutcomePoll = poll;
        }

        Assert.IsNotNull(recreate);
        await recreate!.WaitAsync(TimeSpan.FromSeconds(60));

        await cluster.WaitForSchemaConvergenceAsync(db, follower.Database!.Schema.SchemaVersion);
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            await node.Executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(SchemaElementState.Public, node.Database!.Schema.Tables["base"].Indexes!.Single(ix => ix.Name == "name_idx").State,
                $"node {node.Index} has the new index of the same name");

        Assert.AreEqual(0, await CountEntriesAsync(follower.Database!, indexSpace), "the old index's entries are purged");
        Assert.AreEqual(5, await CountN1Async(follower, db));
    }

    /// <summary>
    /// The schema leader drops the index and stops before the purge runs. The leadership moves, and the
    /// first sweep of the new leader runs the record the old leader wrote.
    /// </summary>
    [Test]
    public async Task APurgeTheOldLeaderDidNotRun_IsRunByTheNewSchemaLeader()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower, string indexSpace) = await PrepareAsync();
        await using InProcessSchemaCluster _ = cluster;

        leader.Executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;
        try
        {
            await AutocommitAsync(leader, db, "DROP INDEX name_idx ON base", ddl: true);
        }
        finally
        {
            leader.Executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;
        }

        Assert.AreEqual(RowCount, await CountEntriesAsync(leader.Database!, indexSpace), "the old leader stopped before the purge");
        Assert.AreEqual(1, await CountMetaAsync(leader.Database!, MetaKeys.PurgeKeyPrefix(leader.Database!.Id)));

        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);
        await cluster.TransferSchemaLeadershipAsync(db, follower);

        Assert.AreEqual(1, await follower.Executor.SweepCoordinatorJobsForTests(), "the new leader sweeps the database");

        Assert.AreEqual(0, await CountEntriesAsync(follower.Database!, indexSpace), "the new leader purged the entries");
        Assert.AreEqual(0, await CountMetaAsync(follower.Database!, MetaKeys.PurgeKeyPrefix(follower.Database!.Id)), "and deleted the record");
    }
}
