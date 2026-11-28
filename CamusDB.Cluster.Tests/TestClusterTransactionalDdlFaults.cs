/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Models;
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

    private static Task<InProcessSchemaCluster> StartClusterAsync() =>
        InProcessSchemaCluster.StartAsync(
            nodeCount: 3,
            partitions: 3,
            wireLeaderForwarder: true,
            options: CamusDBOptions.Default with
            {
                TransactionalDdlEnabled = true,
                TransactionalDdlResolveAfterMs = 1_000,
                CoordinatorJobSweepIntervalMs = 2_000,
            });

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

        foreach (InProcessSchemaCluster.Node node in scenario.Cluster.LiveNodes)
            await AssertRowsAsync(node, scenario.Db, applied);

        await AssertAcceptsSchemaChangesAsync(scenario);
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
            Task commit = driver.Database!.Transactions.CommitAsync(tx);

            await Task.Delay(1_500);
            Assert.IsFalse(commit.IsCompleted, "the COMMIT must wait at the schema leader");

            await cluster.TransferSchemaLeadershipAsync(scenario.Db, driver);
            applied = await CommitAsClientAsync(driver.Database!.Transactions, tx, commit.WaitAsync(TimeSpan.FromSeconds(30)));
        }
        finally
        {
            scenario.Leader.Database!.SchemaDdlSemaphore.Release();
        }

        await AssertEndsAsync(scenario, applied);
    }

    /// <summary>
    /// The schema leader dies while a driver on a follower is at each step. The survivors end the change set
    /// as the rows did, and a node that joins afterwards gets the same schema.
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

        bool applied = await FinalizeAsync(driver, tx, step);
        Assert.IsTrue(died, $"the driver must reach {at}");

        await AssertEndsAsync(scenario, applied);

        InProcessSchemaCluster.Node joined = await cluster.JoinNodeAsync();
        await cluster.OpenDatabaseOnNodeAsync(joined.Index, scenario.Db);

        Assert.AreEqual(SchemaShape.Of(driver.Database!), SchemaShape.Of(joined.Database!), "the node that joined has the schema of the survivors");
        Assert.AreEqual(applied ? 4 : 3, (await ReadAsync(joined, scenario.Db, "SELECT * FROM base")).Count);
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
    /// committed. A COMMIT whose outcome is not resolved is retried on the same transaction.
    /// </summary>
    private static async Task<bool> FinalizeAsync(InProcessSchemaCluster.Node driver, KvTransaction tx, ChangeSetStep step)
    {
        KvTransactionsManager transactions = driver.Database!.Transactions;

        if (!IsCommitStep(step))
        {
            await transactions.RollbackAsync(tx).WaitAsync(StatementLimit);
            return false;
        }

        return await CommitAsClientAsync(transactions, tx);
    }

    /// <summary>
    /// Ends a COMMIT as a client does and returns whether the transaction committed. A COMMIT whose outcome is
    /// not resolved is retried on the same transaction; a refusal that the client can retry is a definite
    /// non-commit. <paramref name="started"/> is a COMMIT that the caller already started, or null.
    /// </summary>
    private static async Task<bool> CommitAsClientAsync(KvTransactionsManager transactions, KvTransaction tx, Task? started = null)
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
        }
    }
}
