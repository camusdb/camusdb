/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Cluster;
using Kahuna;

using static CamusDB.Tests.Cluster.ClusterStatements;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Faults at every step of the commit and the rollback of a transaction that changes the schema, in a
/// three-node cluster: its node dies, the schema leader moves or dies, or its client goes away. The change
/// ends whole on every live node or on none, exactly as the KV commit of the transaction decided, within a
/// bounded time, and it leaves no record, no lock and no fenced table behind.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterTransactionalDdlFaults
{
    /// <summary>The longest time a change set may take to end after a fault.</summary>
    private const int EndsWithinMs = 90_000;

    /// <summary>The longest time a read may meet partitions without a leader.</summary>
    private const int ElectionLimitMs = 30_000;

    /// <summary>The longest time one statement or one COMMIT may take. It turns a hang into a failure.</summary>
    private static readonly TimeSpan StatementLimit = TimeSpan.FromMinutes(3);

    /// <summary>The step before COMMIT or ROLLBACK: the statements of the transaction ran, and no more.</summary>
    private const string Open = "Open";

    private static readonly string[] Seed =
    [
        "CREATE TABLE base (id int64 PRIMARY KEY, name string)",
        "CREATE INDEX base_name ON base (name)",
        "INSERT INTO base (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "CREATE TABLE other (id int64 PRIMARY KEY, v int64)",
        "INSERT INTO other (id, v) VALUES " + string.Join(", ", Enumerable.Range(1, 50).Select(i => $"({i}, {i})")),
        "CREATE TABLE checked (id int64 PRIMARY KEY, v int64)",
        "INSERT INTO checked (id, v) VALUES (1, 1)",
        "CREATE TABLE old_name (id int64 PRIMARY KEY)",
        "CREATE TABLE dropped (id int64 PRIMARY KEY)",
        "INSERT INTO dropped (id) VALUES (1)",
    ];

    /// <summary>
    /// One statement of each kind: two staged on shared tables, a new table with a row, a new column with a
    /// row, and the drops and the rename, which leave work for after the publish.
    /// </summary>
    private static readonly string[] Migration =
    [
        "CREATE INDEX other_v ON other (v)",
        "ALTER TABLE checked ADD CONSTRAINT v_positive CHECK (v > 0)",
        "CREATE TABLE fresh (id int64 PRIMARY KEY, v int64)",
        "INSERT INTO fresh (id, v) VALUES (1, 10)",
        "ALTER TABLE base ADD COLUMN w int64",
        "INSERT INTO base (id, name, w) VALUES (100, 'hundred', 7)",
        "DROP INDEX base_name ON base",
        "ALTER TABLE old_name RENAME TO new_name",
        "DROP TABLE dropped",
    ];

    private static string shapeBefore = "";

    private static string shapeAfter = "";

    /// <summary>
    /// Starts the cluster of every test here. With a <paramref name="root"/>, each node keeps its data and its
    /// Raft log on disk in a directory of its own, which is what a node that starts again needs.
    /// </summary>
    private static Task<InProcessSchemaCluster> StartClusterAsync(string? root = null) =>
        InProcessSchemaCluster.StartAsync(
            nodeCount: 3,
            partitions: 3,
            wireLeaderForwarder: true,
            configureNode: root is null ? null : node => StoreOnDisk(node, root),
            options: CamusDBOptions.Default with
            {
                TransactionalDdlEnabled = true,
                TransactionalDdlResolveAfterMs = 1_000,
                CoordinatorJobSweepIntervalMs = 2_000,
            });

    private static void StoreOnDisk(EmbeddedKahunaOptions node, string root)
    {
        string data = Path.Combine(root, $"node{node.NodeId}");

        node.Storage = "rocksdb";
        node.StoragePath = Path.Combine(data, "kv");
        node.StorageRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision;
        node.WalStorage = "rocksdb";
        node.WalPath = Path.Combine(data, "wal");
        node.WalRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision;

        Directory.CreateDirectory(node.StoragePath);
        Directory.CreateDirectory(node.WalPath);
    }

    private static async Task WithDataDirectoryAsync(Func<string, Task> test)
    {
        string root = Path.Combine(Path.GetTempPath(), "camusdb-ddl-restart-" + Guid.NewGuid().ToString("N"));

        try
        {
            await test(root);
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>Runs the migration with no fault, to learn the two schemas that a faulted one may end with.</summary>
    [OneTimeSetUp]
    public async Task LearnBothShapes()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);

        shapeBefore = SchemaShape.Of(scenario.Leader.Database!);

        KvTransaction tx = await BeginSessionAsync(scenario.Follower);
        await RunMigrationAsync(scenario.Follower, tx, scenario.Db);
        await scenario.Follower.Database!.Transactions.CommitAsync(tx);

        shapeAfter = SchemaShape.Of(scenario.Leader.Database!);
        Assert.AreNotEqual(shapeBefore, shapeAfter);
        await AssertEndsAsync(scenario, applied: true);
    }

    private sealed record Scenario(InProcessSchemaCluster Cluster, string Db, InProcessSchemaCluster.Node Leader, InProcessSchemaCluster.Node Follower)
    {
        public InProcessSchemaCluster.Node Driver(bool onLeader) => onLeader ? Leader : Follower;

        public InProcessSchemaCluster.Node Third => Cluster.Nodes.First(n => n.Index != Leader.Index && n.Index != Follower.Index);
    }

    private static async Task<Scenario> PrepareAsync(InProcessSchemaCluster cluster)
    {
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        foreach (string sql in Seed)
            await AutocommitAsync(leader, db, sql, ddl: !sql.StartsWith("INSERT", StringComparison.Ordinal));

        // The seed can move the leadership; every scenario is stated in terms of the leader at its start.
        leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);

        return new Scenario(cluster, db, leader, cluster.Nodes.First(n => n.Index != leader.Index));
    }

    private static async Task RunMigrationAsync(InProcessSchemaCluster.Node node, KvTransaction tx, string db)
    {
        foreach (string sql in Migration)
        {
            if (sql.StartsWith("INSERT", StringComparison.Ordinal))
                await Write(node, tx, db, sql);
            else
                await Ddl(node, tx, db, sql);
        }
    }

    private static ChangeSetCommitter CommitterOf(KvTransaction tx) => ((TransactionSchemaOverlay)tx.SchemaChanges!).Committer;

    /// <summary>What differs from a change set that ended, or null when nothing does.</summary>
    private static async Task<string?> DescribeUnfinishedAsync(Scenario scenario, bool applied)
    {
        string expected = applied ? shapeAfter : shapeBefore;

        foreach (InProcessSchemaCluster.Node node in scenario.Cluster.LiveNodes)
        {
            if (node.Database!.Schema.PendingChangeSet is { } pending)
                return $"node {node.Index} still has change set {pending.ChangeSetId} pending";

            string shape = SchemaShape.Of(node.Database!);
            if (!string.Equals(expected, shape, StringComparison.Ordinal))
                return $"node {node.Index} has another schema than the {(applied ? "new" : "old")} one:\n{shape}";
        }

        DatabaseDescriptor database = scenario.Cluster.LiveNodes.First().Database!;
        string metaBucket = MetaKeys.MetaBucketPrefix(database.Id);

        foreach (string prefix in new[]
        {
            MetaKeys.CoordinatorKeyPrefix(database.Id),
            MetaKeys.PurgeKeyPrefix(database.Id),
            MetaKeys.ChangeSetKeyPrefix(database.Id),
            MetaKeys.ChangeSetDecisionKeyPrefix(database.Id),
        })
        {
            int count = await CountKeysAsync(database, metaBucket, prefix);
            if (count != 0)
                return $"{count} key(s) are left under '{prefix}'";
        }

        return null;
    }

    /// <summary>
    /// Waits until the change set ended with the expected schema on every live node and left nothing behind,
    /// then checks the rows, the indexes, and that the database accepts schema changes again.
    /// </summary>
    private static async Task AssertEndsAsync(Scenario scenario, bool applied)
    {
        await WaitUntilEndedAsync(scenario, applied);

        foreach (InProcessSchemaCluster.Node node in scenario.Cluster.LiveNodes)
            await AssertRowsAsync(node, scenario.Db, applied);

        await AssertAcceptsSchemaChangesAsync(scenario);
    }

    /// <summary>Waits until the change set ended with the expected schema on every live node and left nothing behind.</summary>
    private static async Task WaitUntilEndedAsync(Scenario scenario, bool applied)
    {
        Stopwatch timer = Stopwatch.StartNew();
        string? unfinished;

        while (true)
        {
            try
            {
                unfinished = await DescribeUnfinishedAsync(scenario, applied);
            }
            catch (Exception ex) when (timer.ElapsedMilliseconds < EndsWithinMs && ex is not AssertionException)
            {
                // A partition without a leader while the survivors elect one.
                unfinished = ex.Message;
            }

            if (unfinished is null || timer.ElapsedMilliseconds >= EndsWithinMs)
                break;

            await Task.Delay(200);
        }

        Assert.IsNull(unfinished, $"after {timer.ElapsedMilliseconds} ms: {unfinished}");
        await TestContext.Progress.WriteLineAsync($"the change set ended {(applied ? "applied" : "absent")} after {timer.ElapsedMilliseconds} ms");
    }

    /// <summary>
    /// Reads without range locks. A driver that died left its row locks behind, as every transaction of a dead
    /// node does, and a read that locks its range would be refused by them. A read that meets a partition
    /// without a leader runs again while the survivors elect one.
    /// </summary>
    private static async Task<List<Dictionary<string, ColumnValue>>> ReadAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        Stopwatch timer = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                return await QueryAsync(node, db, sql, CamusIsolationLevel.ReadCommitted).WaitAsync(StatementLimit);
            }
            catch (Exception ex) when (timer.ElapsedMilliseconds < ElectionLimitMs && IsTransient(ex))
            {
                await Task.Delay(200);
            }
        }
    }

    private static async Task AssertRowsAsync(InProcessSchemaCluster.Node node, string db, bool applied)
    {
        List<Dictionary<string, ColumnValue>> baseRows = await ReadAsync(node, db, "SELECT * FROM base ORDER BY id");
        Assert.AreEqual(applied ? new long[] { 1, 2, 3, 100 } : [1, 2, 3], baseRows.Select(r => r["id"].LongValue).ToArray(), $"node {node.Index}: the rows of base");

        Assert.AreEqual(50, (await ReadAsync(node, db, "SELECT * FROM other")).Count, $"node {node.Index}");

        if (applied)
        {
            Assert.AreEqual(7, baseRows[3]["w"].LongValue, $"node {node.Index} decodes the row of the new layout");
            Assert.AreEqual(1, (await ReadAsync(node, db, "SELECT * FROM fresh")).Count, $"node {node.Index}");
            Assert.AreEqual(50, (await ReadAsync(node, db, "SELECT id FROM other@{FORCE_INDEX=other_v} WHERE v > 0")).Count, $"node {node.Index}: the new index holds every row");
            Assert.AreEqual(0, (await ReadAsync(node, db, "SELECT * FROM new_name")).Count, $"node {node.Index}");
        }
        else
        {
            Assert.AreEqual(1, (await ReadAsync(node, db, "SELECT id FROM base@{FORCE_INDEX=base_name} WHERE name = 'two'")).Count, $"node {node.Index}: the kept index holds its rows");
            Assert.AreEqual(1, (await ReadAsync(node, db, "SELECT * FROM dropped")).Count, $"node {node.Index} keeps the table");
        }
    }

    /// <summary>The DDL lock is free, no table is fenced, and both forms of a schema change run.</summary>
    private static async Task AssertAcceptsSchemaChangesAsync(Scenario scenario)
    {
        InProcessSchemaCluster.Node node = scenario.Cluster.LiveNodes.First();
        string db = scenario.Db;

        await RetryAsync("an autocommit schema change", async () => await AutocommitAsync(node, db, "CREATE TABLE after_fault (id int64 PRIMARY KEY)", ddl: true));

        await RetryAsync("a transaction that changes the schema", async () =>
        {
            KvTransaction tx = await BeginSessionAsync(node);
            try
            {
                await Ddl(node, tx, db, "ALTER TABLE other ADD COLUMN later int64");
                await Write(node, tx, db, "INSERT INTO other (id, v, later) VALUES (1000, 1000, 1)");
                await node.Database!.Transactions.CommitAsync(tx);
            }
            finally
            {
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        });

        foreach (InProcessSchemaCluster.Node live in scenario.Cluster.LiveNodes)
            Assert.AreEqual(1, (await ReadAsync(live, db, "SELECT later FROM other WHERE id = 1000"))[0]["later"].LongValue, $"node {live.Index}");
    }

    private static async Task RetryAsync(string what, Func<Task> action)
    {
        Stopwatch timer = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await action().WaitAsync(StatementLimit);
                return;
            }
            catch (Exception ex) when (timer.ElapsedMilliseconds < EndsWithinMs && IsTransient(ex))
            {
                await Task.Delay(200);
            }
            catch (Exception ex)
            {
                Assert.Fail($"{what} failed after {timer.ElapsedMilliseconds} ms: {ex}");
            }
        }
    }

    /// <summary>The codes that tell a client to run the transaction again: nothing of it was committed.</summary>
    private static bool IsRetryable(CamusDBException ex) =>
        ex.Code is CamusDBErrorCodes.TransactionConflict or CamusDBErrorCodes.TransactionMustRetry;

    /// <summary>
    /// What a statement after the fault may meet until the survivors have a leader for every partition. A
    /// schema statement that finds no schema leader fails with the generic internal code.
    /// </summary>
    private static bool IsTransient(Exception ex) =>
        ex is KahunaServerException || ex is CamusDBException refused && (IsRetryable(refused) || refused.Code == CamusDBErrorCodes.InvalidInternalOperation);

    /// <summary>
    /// Stops the driver of <paramref name="tx"/> for good when its COMMIT or ROLLBACK reaches
    /// <paramref name="at"/>, as a process that dies there does. The returned action ends the wait, for the
    /// end of the test.
    /// </summary>
    private static (Task Reached, Action Release) StopDriverAt(KvTransaction tx, ChangeSetStep at)
    {
        TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

        CommitterOf(tx).TestAtStep = async step =>
        {
            if (step != at)
                return;

            reached.TrySetResult();
            await stopped.Task;
        };

        return (reached.Task, () => stopped.TrySetException(new OperationCanceledException("the driver died")));
    }

    private static bool IsCommitStep(ChangeSetStep step) => step is not (ChangeSetStep.Aborting or ChangeSetStep.Aborted);

    private static bool RowsAreCommittedAt(ChangeSetStep step) => step is ChangeSetStep.RowsCommitted or ChangeSetStep.Published;

    /// <summary>The node that runs the transaction dies, at each step, on the schema leader and on a follower.</summary>
    [Test]
    public async Task DriverDies_TheChangeEndsAsTheKvCommitDecided(
        [Values(Open, nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting), nameof(ChangeSetStep.Aborted))] string at,
        [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool applied = false;
        Action release = () => { };

        if (at != Open)
        {
            ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);
            applied = RowsAreCommittedAt(step);

            (Task reached, release) = StopDriverAt(tx, step);
            KvTransactionsManager transactions = driver.Database!.Transactions;
            Task finalize = Task.Run(() => IsCommitStep(step) ? (Task)transactions.CommitAsync(tx) : transactions.RollbackAsync(tx));
            _ = finalize.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

            await Task.WhenAny(reached, finalize).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.IsTrue(reached.IsCompleted, $"the driver must reach {at}");
        }

        try
        {
            await cluster.KillNodeAsync(driver.Index);
            await AssertEndsAsync(scenario, applied);
        }
        finally
        {
            release();
        }
    }

    /// <summary>
    /// The node that runs the transaction dies at each step and the same node starts again on its data
    /// directory, at once or after the survivors ended the change set. The node that came back agrees with
    /// them, and it runs schema changes again.
    /// </summary>
    [Test]
    public async Task DriverRestarts_TheChangeEndsAsTheKvCommitDecided(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting))] string at,
        [Values] bool onLeader,
        [Values] bool afterTheSurvivorsEnded)
    {
        await WithDataDirectoryAsync(async root =>
        {
            await using InProcessSchemaCluster cluster = await StartClusterAsync(root);
            Scenario scenario = await PrepareAsync(cluster);
            InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);
            ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);
            bool applied = RowsAreCommittedAt(step);

            Action release = await RunToStepAsync(driver, scenario.Db, step);

            try
            {
                await cluster.KillNodeAsync(driver.Index);
            }
            finally
            {
                // The node reaches nothing any more: what its process still runs ends against blocked links.
                release();
            }

            if (afterTheSurvivorsEnded)
                await WaitUntilEndedAsync(scenario, applied);

            InProcessSchemaCluster.Node restarted = await cluster.RestartNodeAsync(driver.Index);
            await cluster.OpenDatabaseOnNodeAsync(restarted.Index, scenario.Db);

            await AssertEndsAsync(scenario, applied);
            await AssertChangesTheSchemaAsync(scenario, restarted);
        });
    }

    /// <summary>
    /// The schema leader dies while the COMMIT of a driver on a follower is at each step, and the same node
    /// starts again before that COMMIT goes on. The COMMIT meets the new leader, or the old one after its
    /// restart.
    /// </summary>
    [Test]
    public async Task SchemaLeaderRestarts_TheChangeEndsAsTheKvCommitDecided(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting))] string at)
    {
        await WithDataDirectoryAsync(async root =>
        {
            await using InProcessSchemaCluster cluster = await StartClusterAsync(root);
            Scenario scenario = await PrepareAsync(cluster);
            InProcessSchemaCluster.Node driver = scenario.Follower;
            ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);

            KvTransaction tx = await BeginSessionAsync(driver);
            await RunMigrationAsync(driver, tx, scenario.Db);

            bool restarted = false;
            CommitterOf(tx).TestAtStep = async reached =>
            {
                if (reached != step || restarted)
                    return;

                restarted = true;
                await cluster.KillNodeAsync(scenario.Leader.Index);
                InProcessSchemaCluster.Node node = await cluster.RestartNodeAsync(scenario.Leader.Index);
                await cluster.OpenDatabaseOnNodeAsync(node.Index, scenario.Db);
            };

            bool applied = await FinalizeAsync(driver, tx, step, cluster);
            Assert.IsTrue(restarted, $"the driver must reach {at}");

            await AssertEndsAsync(scenario, applied);
            await AssertChangesTheSchemaAsync(scenario, cluster.Nodes[scenario.Leader.Index]);
        });
    }

    /// <summary>
    /// Every node stops while the driver is at each step, and every node starts again. Nothing of the change
    /// set is in memory any more, so the stored records alone must end it.
    /// </summary>
    [Test]
    public async Task EveryNodeRestarts_TheChangeEndsAsTheKvCommitDecided(
        [Values(Open, nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting))] string at,
        [Values] bool onLeader)
    {
        await WithDataDirectoryAsync(async root =>
        {
            await using InProcessSchemaCluster cluster = await StartClusterAsync(root);
            Scenario scenario = await PrepareAsync(cluster);
            InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);

            bool applied = false;
            Action release = () => { };

            if (at == Open)
            {
                KvTransaction tx = await BeginSessionAsync(driver);
                await RunMigrationAsync(driver, tx, scenario.Db);
            }
            else
            {
                ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);
                applied = RowsAreCommittedAt(step);
                release = await RunToStepAsync(driver, scenario.Db, step);
            }

            int[] order = [driver.Index, .. cluster.Nodes.Select(n => n.Index).Where(i => i != driver.Index)];

            try
            {
                foreach (int index in order)
                    await cluster.KillNodeAsync(index);
            }
            finally
            {
                release();
            }

            await Task.WhenAll(order.Select(index => cluster.RestartNodeAsync(index)));

            foreach (int index in order)
                await cluster.OpenDatabaseOnNodeAsync(index, scenario.Db);

            await AssertEndsAsync(scenario, applied);
            await AssertChangesTheSchemaAsync(scenario, cluster.Nodes[driver.Index]);
        });
    }

    /// <summary>
    /// Runs the migration on <paramref name="driver"/> and its COMMIT or ROLLBACK up to <paramref name="step"/>,
    /// where the driver stops. The returned action ends its wait.
    /// </summary>
    private static async Task<Action> RunToStepAsync(InProcessSchemaCluster.Node driver, string db, ChangeSetStep step)
    {
        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, db);

        (Task reached, Action release) = StopDriverAt(tx, step);
        KvTransactionsManager transactions = driver.Database!.Transactions;
        Task finalize = Task.Run(() => IsCommitStep(step) ? (Task)transactions.CommitAsync(tx) : transactions.RollbackAsync(tx));
        _ = finalize.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

        await Task.WhenAny(reached, finalize).WaitAsync(TimeSpan.FromSeconds(60));
        Assert.IsTrue(reached.IsCompleted, $"the driver must reach {step}");

        return release;
    }

    /// <summary>Runs one DDL transaction on <paramref name="node"/>, whose row every live node then reads.</summary>
    private static async Task AssertChangesTheSchemaAsync(Scenario scenario, InProcessSchemaCluster.Node node)
    {
        await RetryAsync("a transaction that changes the schema on the restarted node", async () =>
        {
            KvTransaction tx = await BeginSessionAsync(node);
            try
            {
                await Ddl(node, tx, scenario.Db, "CREATE TABLE after_restart (id int64 PRIMARY KEY)");
                await Write(node, tx, scenario.Db, "INSERT INTO after_restart (id) VALUES (1)");
                await node.Database!.Transactions.CommitAsync(tx);
            }
            finally
            {
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        });

        foreach (InProcessSchemaCluster.Node live in scenario.Cluster.LiveNodes)
            Assert.AreEqual(1, (await ReadAsync(live, scenario.Db, "SELECT id FROM after_restart")).Count, $"node {live.Index}");
    }

    /// <summary>
    /// The schema leadership moves while the driver is at each step. COMMIT succeeds, or it fails with a code
    /// the client can act on; either way the schema ends as the rows did.
    /// </summary>
    [Test]
    public async Task SchemaLeaderMoves_TheChangeEndsAsTheKvCommitDecided(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting), nameof(ChangeSetStep.Aborted))] string at,
        [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);
        ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool moved = false;
        CommitterOf(tx).TestAtStep = async reached =>
        {
            if (reached != step || moved)
                return;

            moved = true;
            await cluster.TransferSchemaLeadershipAsync(scenario.Db, onLeader ? scenario.Follower : scenario.Third);
        };

        bool applied = await FinalizeAsync(driver, tx, step);
        Assert.IsTrue(moved, $"the driver must reach {at}");

        await AssertEndsAsync(scenario, applied);
    }

    /// <summary>
    /// A COMMIT on a follower waits at the schema leader, whose DDL gate another statement holds, and then
    /// the leadership moves to the follower. The COMMIT must not wait for the gate of the old leader: the
    /// statement that holds it can now wait for the gate that the COMMIT holds.
    ///
    /// <para>The schema-log partition also holds rows that the migration writes. Its leader change drops the
    /// locks and the write intents that the transaction holds there, so the KV commit is refused with
    /// <c>LostLock</c>, a code the client can retry. The COMMIT ends either way, and the schema ends as the
    /// rows did.</para>
    /// </summary>
    [Test]
    public async Task SchemaLeadershipMovesToTheDriver_WhileItWaitsAtTheOldLeader()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Follower;

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool applied;

        await scenario.Leader.Database!.SchemaDdlSemaphore.WaitAsync();
        try
        {
            Task<bool> commit = FinalizeAsync(driver, tx, ChangeSetStep.Staged);

            await Task.Delay(1_500);
            Assert.IsFalse(commit.IsCompleted, "the COMMIT must wait at the schema leader");

            await cluster.TransferSchemaLeadershipAsync(scenario.Db, driver);

            // The transfer can take the locks that the old leader granted in the same partition. Kahuna then
            // refuses the commit, which the client may retry, and the change set must end absent.
            applied = await commit.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            scenario.Leader.Database!.SchemaDdlSemaphore.Release();
        }

        await AssertEndsAsync(scenario, applied);
    }

    /// <summary>
    /// The schema leader dies while a driver on a follower is at each step. The survivors end the change set
    /// as the rows did, and a node that joins afterwards gets the same schema. The leader can hold the session
    /// of the transaction, so the COMMIT can end with an unknown outcome; the rows tell it then.
    /// </summary>
    [Test]
    public async Task SchemaLeaderDies_TheChangeEndsAsTheKvCommitDecided(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting), nameof(ChangeSetStep.Aborted))] string at)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Follower;
        ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool died = false;
        CommitterOf(tx).TestAtStep = async reached =>
        {
            if (reached != step || died)
                return;

            died = true;
            await cluster.KillNodeAsync(scenario.Leader.Index);
        };

        bool applied = await FinalizeAsync(driver, tx, step, cluster);
        Assert.IsTrue(died, $"the driver must reach {at}");

        await AssertEndsAsync(scenario, applied);

        InProcessSchemaCluster.Node joined = await cluster.JoinNodeAsync();
        await cluster.OpenDatabaseOnNodeAsync(joined.Index, scenario.Db);

        Assert.AreEqual(SchemaShape.Of(driver.Database!), SchemaShape.Of(joined.Database!), "the node that joined has the schema of the survivors");
        Assert.AreEqual(applied ? 4 : 3, (await ReadAsync(joined, scenario.Db, "SELECT * FROM base")).Count);
    }

    /// <summary>
    /// A node dies that neither runs the transaction nor leads the schema log, while the COMMIT is at each
    /// step. It can hold the session of the transaction, the locks of its keys, or only a replica.
    /// </summary>
    [Test]
    public async Task AnotherNodeDies_TheChangeEndsAsTheKvCommitDecided(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted),
            nameof(ChangeSetStep.Published), nameof(ChangeSetStep.Aborting), nameof(ChangeSetStep.Aborted))] string at,
        [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);
        ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool died = false;
        CommitterOf(tx).TestAtStep = async reached =>
        {
            if (reached != step || died)
                return;

            died = true;
            await cluster.KillNodeAsync(scenario.Third.Index);
        };

        bool applied = await FinalizeAsync(driver, tx, step, cluster);
        Assert.IsTrue(died, $"the driver must reach {at}");

        await AssertEndsAsync(scenario, applied);
    }

    /// <summary>
    /// A COMMIT is refused after its announcement, and the rollback does not reach the nodes that hold the
    /// locks of its staged keys. The abort still goes through the schema leader, which must end the change set
    /// while those locks are held, and must go on to accept schema changes.
    /// </summary>
    [Test]
    public async Task RollbackCannotReachItsLocks_TheSchemaLeaderStillEndsTheChangeSet()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Follower;

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        // The hook belongs to the node: later transactions of the test reach it too.
        bool blocked = false;

        CommitterOf(tx).TestAtStep = reached =>
        {
            if (reached != ChangeSetStep.Announced || blocked)
                return Task.CompletedTask;

            blocked = true;
            cluster.SetKahunaLinksBlocked(driver.Index, blocked: true);
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, "the COMMIT is refused after its announcement");
        };

        CamusDBException refused = Assert.CatchAsync<CamusDBException>(
            async () => await driver.Database!.Transactions.CommitAsync(tx).WaitAsync(StatementLimit))!;
        await TestContext.Progress.WriteLineAsync($"COMMIT was refused: {refused.Code} {refused.Message}");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsFalse(node.Database!.SchemaSubsystemDegraded, $"the schema subsystem of node {node.Index} is degraded");

        cluster.SetKahunaLinksBlocked(driver.Index, blocked: false);

        await AssertEndsAsync(scenario, applied: false);
    }

    /// <summary>
    /// The schema leader cannot write the checkpoint of the entry that ends a change set. The change is in the
    /// log, so COMMIT or ROLLBACK succeeds; the leader is degraded, gives the leadership up, and the database
    /// accepts schema changes on the node that takes it.
    /// </summary>
    [Test]
    public async Task CheckpointOfTheDecisionFails_TheLeaderGivesTheLeadershipUp([Values] bool publish, [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        ChangeSetCommitter committer = CommitterOf(tx);
        bool refused = false;

        committer.TestAtStep = reached =>
        {
            if (reached == (publish ? ChangeSetStep.RowsCommitted : ChangeSetStep.Aborting))
                scenario.Leader.Executor.Catalogs.TestPersistCheckpointException = new IOException("the checkpoint cannot be written");

            if (reached == ChangeSetStep.Announced && !publish)
            {
                refused = true;
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, "the COMMIT is refused after its announcement");
            }

            return Task.CompletedTask;
        };

        try
        {
            await driver.Database!.Transactions.CommitAsync(tx).WaitAsync(StatementLimit);
            Assert.IsTrue(publish, "the COMMIT must be refused");
        }
        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionMustRetry && !publish)
        {
            Assert.IsTrue(refused, ex.Message);
        }

        scenario.Leader.Executor.Catalogs.TestPersistCheckpointException = null;
        Assert.IsTrue(scenario.Leader.Database!.SchemaSubsystemDegraded, "the checkpoint must have failed");

        Stopwatch timer = Stopwatch.StartNew();
        while (await scenario.Leader.Kahuna.AmISchemaLeaderAsync(scenario.Leader.Database!.Id, CancellationToken.None))
        {
            Assert.Less(timer.ElapsedMilliseconds, 30_000, "the degraded node still leads the schema log");
            await Task.Delay(100);
        }

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(scenario.Db);
        await RetryAsync("a schema change on the new leader", async () =>
            await AutocommitAsync(leader, scenario.Db, "CREATE TABLE after_step_down (id int64 PRIMARY KEY)", ddl: true));

        foreach (InProcessSchemaCluster.Node node in cluster.LiveNodes.Where(n => n.Index != scenario.Leader.Index))
            await AssertRowsAsync(node, scenario.Db, applied: publish);
    }

    /// <summary>The build of an index on <c>other (v)</c>, as the statement on the schema leader would drive it.</summary>
    private static (SchemaChangeCoordinator Coordinator, SchemaChangeJob Job, IndexBuildInfo Index) IndexBuildOn(
        Scenario scenario, Func<CancellationToken, Task> backfill, string? owner = null)
    {
        TableSchema table = scenario.Leader.Database!.Schema.Tables["other"];
        IndexBuildInfo index = new(
            CamusDB.Core.Util.ObjectIds.ObjectIdGenerator.Generate().ToString(), "beside_v", [table.Columns!.Single(c => c.Name == "v").Id], ["v"], IndexType.Multi);

        SchemaChangeCoordinator coordinator = new(scenario.Leader.Executor.Catalogs)
        {
            IndexBackfillAsync = (_, _, _, _, _, cancellationToken) => backfill(cancellationToken),
        };

        return (coordinator, new SchemaChangeJob(scenario.Db, "other", table.Id!, index.IndexName, SchemaElementState.Public, SchemaElementKind.Index, owner), index);
    }

    private static SchemaElementState? IndexStateOn(InProcessSchemaCluster.Node node) =>
        node.Database!.Schema.Tables["other"].Indexes?.Find(ix => ix.Name == "beside_v")?.State;

    private static async Task AssertIndexEndsAsync(Scenario scenario, SchemaElementState? expected)
    {
        DatabaseDescriptor database = scenario.Leader.Database!;

        await EventuallyAsync(
            async () => scenario.Cluster.LiveNodes.All(n => IndexStateOn(n) == expected)
                && await CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.CoordinatorKeyPrefix(database.Id)) == 0,
            () => $"the index must end as '{expected}' with no job record; it is " +
                string.Join(", ", scenario.Cluster.LiveNodes.Select(n => $"'{IndexStateOn(n)}' on node {n.Index}")));
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, Func<string> describe)
    {
        Stopwatch timer = Stopwatch.StartNew();

        while (!await condition())
        {
            Assert.Less(timer.ElapsedMilliseconds, EndsWithinMs, describe());
            await Task.Delay(100);
        }
    }

    /// <summary>
    /// The change set of another transaction is pending when an index build ends, as it can be when the DDL
    /// lock is lost. The step that publishes the index, or the drop of an index whose build failed, waits for
    /// that change set: neither leaves a half-built index behind.
    /// </summary>
    [Test]
    public async Task IndexBuildMeetsAPendingChangeSet_WaitsForItsEnd([Values] bool buildFails)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        DatabaseDescriptor leader = scenario.Leader.Database!;

        TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task commit = Task.CompletedTask;

        (SchemaChangeCoordinator coordinator, SchemaChangeJob job, IndexBuildInfo index) = IndexBuildOn(scenario, async _ =>
        {
            KvTransaction tx = await BeginSessionAsync(scenario.Follower);
            await Ddl(scenario.Follower, tx, scenario.Db, "CREATE TABLE meanwhile (id int64 PRIMARY KEY)");

            CommitterOf(tx).TestAtStep = step => step == ChangeSetStep.Announced ? released.Task : Task.CompletedTask;
            commit = scenario.Follower.Database!.Transactions.CommitAsync(tx);

            await EventuallyAsync(() => Task.FromResult(leader.Schema.PendingChangeSet is not null), () => "the change set is not announced");

            if (buildFails)
                throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, "the build failed");
        });

        Task build = Task.Run(async () =>
        {
            await leader.DdlGate.EnterAsync("other", null);
            try
            {
                await coordinator.RunJobAsync(leader, job, indexBuildInfo: index, gateHeldByCaller: true);
            }
            catch (CamusDBException ex)
            {
                await coordinator.TakeBackIndexAsync(leader, job, index, coordinator.LeaderTerm, ex);
                throw;
            }
            finally
            {
                leader.DdlGate.Exit();
            }
        });

        await EventuallyAsync(() => Task.FromResult(leader.Schema.PendingChangeSet is not null || build.IsCompleted), () => "the change set is not announced");
        await Task.Delay(300);
        bool endedEarly = build.IsCompleted;
        released.SetResult();

        // The change set ends by its commit, or by the resolver when the driver was too slow.
        try { await commit.WaitAsync(StatementLimit); }
        catch (CamusDBException ex) when (IsRetryable(ex)) { }

        if (buildFails)
        {
            CamusDBException failed = Assert.CatchAsync<CamusDBException>(async () => await build.WaitAsync(StatementLimit))!;
            Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, failed.Code, failed.Message);
        }
        else
        {
            await build.WaitAsync(StatementLimit);
        }

        Assert.IsFalse(endedEarly, "the build must wait for the pending change set");
        await AssertIndexEndsAsync(scenario, buildFails ? null : SchemaElementState.Public);
    }

    /// <summary>
    /// A build that failed could not drop its index, so the record of the job targets its removal. The resume
    /// drops the index, and does not finish the build.
    /// </summary>
    [Test]
    public async Task FailedIndexBuildCouldNotDropItsIndex_TheResumeDropsIt()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        DatabaseDescriptor leader = scenario.Leader.Database!;
        CamusDBException cause = new(CamusDBErrorCodes.DuplicateUniqueKeyValue, "the build failed");

        (SchemaChangeCoordinator coordinator, SchemaChangeJob job, IndexBuildInfo index) = IndexBuildOn(scenario, _ => throw cause);

        Assert.CatchAsync<CamusDBException>(async () => await coordinator.RunJobAsync(leader, job, indexBuildInfo: index));
        Assert.AreEqual(SchemaElementState.WriteOnly, IndexStateOn(scenario.Leader));

        // A degraded schema subsystem refuses the drop after the take-back recorded its intent.
        await leader.DdlGate.EnterAsync("other", null);
        leader.MarkSchemaSubsystemDegraded();
        try
        {
            await coordinator.TakeBackIndexAsync(leader, job, index, coordinator.LeaderTerm, cause);
            Assert.AreEqual(SchemaElementState.WriteOnly, IndexStateOn(scenario.Leader), "the drop must be refused");
        }
        finally
        {
            leader.ClearSchemaSubsystemDegraded();
            leader.DdlGate.Exit();
        }

        // The resume of the sweeper can have run already, so only the end is checked.
        await AssertIndexEndsAsync(scenario, expected: null);

        await AutocommitAsync(scenario.Leader, scenario.Db, "CREATE INDEX beside_v ON other (v)", ddl: true);
        await AssertIndexEndsAsync(scenario, SchemaElementState.Public);
    }

    /// <summary>
    /// A failed build left its removal record, and another index has its name by now, as a rename can give it.
    /// The resume finds the index by its id, so it deletes the record and keeps the other index.
    /// </summary>
    [Test]
    public async Task RemovalRecordOfAFailedIndexBuild_KeepsAnotherIndexWithItsName()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        DatabaseDescriptor leader = scenario.Leader.Database!;
        (SchemaChangeCoordinator coordinator, SchemaChangeJob job, IndexBuildInfo index) = IndexBuildOn(scenario, _ => Task.CompletedTask);

        await AutocommitAsync(scenario.Leader, scenario.Db, "CREATE INDEX beside_v ON other (v)", ddl: true);
        await AssertIndexEndsAsync(scenario, SchemaElementState.Public);

        await scenario.Leader.Executor.Catalogs.PersistCoordinatorJobAsync(leader, new PersistedCoordinatorJob
        {
            TableName = job.TableName,
            TableId = job.TableId,
            ElementName = job.ElementName,
            ElementKind = SchemaElementKind.Index,
            TargetState = SchemaElementState.Absent,
            IndexId = index.IndexId,
            IndexColumnIds = index.ColumnIds,
            IndexType = index.IndexType,
            LeaderTerm = leader.Kahuna.SchemaLeaderTerm(leader.Id),
        });

        await coordinator.ResumeJobsAsync(leader);

        await AssertIndexEndsAsync(scenario, SchemaElementState.Public);
    }

    /// <summary>
    /// The schema leadership goes away during an index build, and the driver learns it before a newer term is
    /// visible to it. A build that a transaction owns ends with the retryable code; any other build is
    /// answered from the outcome of the next leader. Neither reports a bare cancellation.
    /// </summary>
    [Test]
    public async Task LeadershipIsLostDuringAnIndexBuild_BeforeTheNewTermIsVisible([Values] bool owned)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        DatabaseDescriptor leader = scenario.Leader.Database!;
        long term = leader.Kahuna.SchemaLeaderTerm(leader.Id);

        (SchemaChangeCoordinator coordinator, SchemaChangeJob job, IndexBuildInfo index) = IndexBuildOn(scenario, async cancellationToken =>
        {
            _ = cluster.TransferSchemaLeadershipAsync(scenario.Db, scenario.Follower);
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }, owned ? Guid.NewGuid().ToString("N") : null);

        coordinator.ReadLeaderTermForTesting = _ => term;

        Task build = coordinator.RunJobAsync(leader, job, indexBuildInfo: index);

        if (!owned)
        {
            await build.WaitAsync(StatementLimit);
            await AssertIndexEndsAsync(scenario, SchemaElementState.Public);
            return;
        }

        CamusDBException refused = Assert.CatchAsync<CamusDBException>(async () => await build.WaitAsync(StatementLimit))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused.Code, refused.Message);

        // The owner never announced a change set, so the resume removes the index.
        await AssertIndexEndsAsync(scenario, expected: null);
    }

    /// <summary>
    /// The client goes away while its COMMIT is at each step: its request is cancelled, nothing retries it,
    /// and the reaper of idle transactions meets what it left.
    /// </summary>
    [Test]
    public async Task ClientGoesAway_TheChangeEndsAsTheKvCommitDecided(
        [Values(Open, nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced), nameof(ChangeSetStep.RowsCommitted), nameof(ChangeSetStep.Published))] string at,
        [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);

        HttpTransactionCoordinator coordinator = new(driver.Executor);
        KvTransaction tx = await coordinator.StartAsync(
            scenario.Db, isolationLevel: null, transactionMode: null, sessionOwned: true, cancellationToken: CancellationToken.None);
        await RunMigrationAsync(driver, tx, scenario.Db);

        if (at != Open)
        {
            ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);
            using CancellationTokenSource request = new();
            ChangeSetCommitter committer = CommitterOf(tx);

            committer.TestAtStep = reached =>
            {
                if (reached == step)
                    request.Cancel();

                return Task.CompletedTask;
            };

            try
            {
                await coordinator.CommitAsync(driver.Database!, tx, request.Token);
            }
            catch (Exception ex) when (ex is CamusDBException or OperationCanceledException)
            {
                await TestContext.Progress.WriteLineAsync($"the cancelled COMMIT failed: {ex.GetType().Name} {ex.Message}");
            }
            finally
            {
                committer.TestAtStep = null;
            }

            Assert.IsTrue(request.IsCancellationRequested, $"the driver must reach {at}");
        }

        await coordinator.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);

        bool committed = tx.Status == KvTransactionStatus.Committed;
        await AssertEndsAsync(scenario, applied: committed);
    }

    /// <summary>
    /// The driver is slow: it stops at a step for longer than the lease of the keys it staged, and then goes
    /// on. Whatever was decided meanwhile, its late COMMIT is refused for certain, and nothing of it stays.
    /// </summary>
    [Test]
    public async Task DriverIsSlow_ItsLateCommitIsRefused(
        [Values(nameof(ChangeSetStep.Staged), nameof(ChangeSetStep.Announced))] string at,
        [Values] bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            node.Kahuna.SchemaAckWaitTimeout = TimeSpan.FromSeconds(5);

        Scenario scenario = await PrepareAsync(cluster);
        InProcessSchemaCluster.Node driver = scenario.Driver(onLeader);
        ChangeSetStep step = Enum.Parse<ChangeSetStep>(at);

        KvTransaction tx = await BeginSessionAsync(driver);
        await RunMigrationAsync(driver, tx, scenario.Db);

        bool slow = false;
        CommitterOf(tx).TestAtStep = async reached =>
        {
            if (reached != step || slow)
                return;

            slow = true;
            await Task.Delay(ChangeSetCommitter.StagedKeyLeaseMs(driver.Database!) + 3_000);
        };

        CamusDBException refused = Assert.CatchAsync<CamusDBException>(async () => await driver.Database!.Transactions.CommitAsync(tx))!;
        Assert.IsTrue(slow, $"the driver must reach {at}");
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, refused.Code, refused.Message);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);

        await AssertEndsAsync(scenario, applied: false);
    }

    /// <summary>
    /// Runs the COMMIT or the ROLLBACK of <paramref name="tx"/> as a client does, and returns whether the rows
    /// committed. A COMMIT whose outcome is not resolved is retried on the same transaction. With
    /// <paramref name="sessionCanBeLost"/>, a COMMIT that answers that its session is gone ends with the
    /// outcome that the rows show once the schema settled.
    /// </summary>
    private static async Task<bool> FinalizeAsync(
        InProcessSchemaCluster.Node driver, KvTransaction tx, ChangeSetStep step, InProcessSchemaCluster? sessionCanBeLost = null)
    {
        KvTransactionsManager transactions = driver.Database!.Transactions;

        if (!IsCommitStep(step))
        {
            await transactions.RollbackAsync(tx).WaitAsync(StatementLimit);
            return false;
        }

        return await CommitAsClientAsync(transactions, tx, driver: driver, sessionCanBeLost: sessionCanBeLost);
    }

    /// <summary>
    /// Ends a COMMIT as a client does and returns whether the transaction committed. A COMMIT whose outcome is
    /// not resolved is retried on the same transaction; a refusal that the client can retry is a definite
    /// non-commit. <paramref name="started"/> is a COMMIT that the caller already started, or null. With
    /// <paramref name="sessionCanBeLost"/>, a COMMIT that answers that its session is gone ends with the
    /// outcome that the rows on <paramref name="driver"/> show once the schema settled.
    /// </summary>
    private static async Task<bool> CommitAsClientAsync(
        KvTransactionsManager transactions, KvTransaction tx, Task? started = null,
        InProcessSchemaCluster.Node? driver = null, InProcessSchemaCluster? sessionCanBeLost = null)
    {
        Stopwatch timer = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await (started ?? transactions.CommitAsync(tx).WaitAsync(StatementLimit));
                return true;
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved && timer.ElapsedMilliseconds < EndsWithinMs)
            {
                await TestContext.Progress.WriteLineAsync($"COMMIT is not resolved: {ex.Message}");
                started = null;
                await Task.Delay(500);
            }
            catch (CamusDBException ex) when (IsRetryable(ex))
            {
                await TestContext.Progress.WriteLineAsync($"COMMIT was refused: {ex.Code} {ex.Message}");
                return false;
            }
            catch (CamusDBException ex) when (sessionCanBeLost is not null && driver is not null && ex.Code == CamusDBErrorCodes.TransactionAlreadyCompleted)
            {
                await TestContext.Progress.WriteLineAsync($"COMMIT has an unknown outcome: {ex.Code} {ex.Message}");
                return await ReadOutcomeAsync(driver, sessionCanBeLost);
            }
        }
    }

    /// <summary>Waits until no live node has a change set pending, then answers whether the row of the migration is there.</summary>
    private static async Task<bool> ReadOutcomeAsync(InProcessSchemaCluster.Node driver, InProcessSchemaCluster cluster)
    {
        Stopwatch timer = Stopwatch.StartNew();

        while (cluster.LiveNodes.Any(n => n.Database!.Schema.PendingChangeSet is not null))
        {
            Assert.Less(timer.ElapsedMilliseconds, EndsWithinMs, "a change set is still pending");
            await Task.Delay(200);
        }

        return (await ReadAsync(driver, driver.Database!.Name, "SELECT id FROM base WHERE id = 100")).Count == 1;
    }
}
