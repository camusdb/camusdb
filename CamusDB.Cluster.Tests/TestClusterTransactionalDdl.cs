
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Cluster;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Schema DDL inside an explicit transaction with <c>transactional_ddl_enabled</c> on, in a three-node
/// cluster: a transaction on a follower and one on the schema leader both commit their schema changes
/// with their rows, and roll both back, and every node agrees after <c>COMMIT</c> returns.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterTransactionalDdl
{
    private static Task<InProcessSchemaCluster> StartClusterAsync(CamusDBOptions? options = null) =>
        InProcessSchemaCluster.StartAsync(
            nodeCount: 3,
            wireLeaderForwarder: true,
            options: options ?? CamusDBOptions.Default with { TransactionalDdlEnabled = true });

    private static async Task<KvTransaction> BeginSessionAsync(InProcessSchemaCluster.Node node)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    private static Task Ddl(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));

    private static Task Write(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string sql) =>
        node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task AutocommitAsync(InProcessSchemaCluster.Node node, string db, string sql, bool ddl)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await Ddl(node, tx, db, sql);
            else
                await Write(node, tx, db, sql);

            await node.Database!.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> QueryAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row));

            return rows;
        }
        finally
        {
            await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<(InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower)> PrepareAsync(
        InProcessSchemaCluster cluster, string db)
    {
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        await AutocommitAsync(leader, db, "CREATE TABLE base (id int64 PRIMARY KEY, name string)", ddl: true);
        await AutocommitAsync(leader, db, "INSERT INTO base (id, name) VALUES (1, 'one')", ddl: false);
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);

        return (leader, follower);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Commit_PublishesTheChangeSetOnEveryNode(bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);
        InProcessSchemaCluster.Node driver = onLeader ? leader : follower;

        KvTransaction tx = await BeginSessionAsync(driver);
        await Ddl(driver, tx, db, "CREATE TABLE fresh (id int64 PRIMARY KEY, v int64)");
        await Write(driver, tx, db, "INSERT INTO fresh (id, v) VALUES (1, 10)");
        await Write(driver, tx, db, "INSERT INTO base (id, name) VALUES (2, 'two')");
        await Ddl(driver, tx, db, "ALTER TABLE base ADD COLUMN w int64");
        await Write(driver, tx, db, "INSERT INTO base (id, name, w) VALUES (3, 'three', 30)");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsFalse(node.Database!.Schema.Tables.ContainsKey("fresh"), $"node {node.Index} must not see the table before the commit");

        await driver.Database!.Transactions.CommitAsync(tx);

        // The publish waits for every live node's ack before COMMIT returns.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("fresh"), $"node {node.Index} sees the new table");
            Assert.IsNull(node.Database!.Schema.PendingChangeSet, $"node {node.Index} has nothing pending");

            List<Dictionary<string, ColumnValue>> rows = await QueryAsync(node, db, "SELECT id, w FROM base ORDER BY id");
            Assert.AreEqual(3, rows.Count, $"node {node.Index} reads all rows of base");
            Assert.AreEqual(30, rows[2]["w"].LongValue, $"node {node.Index} decodes the row written with the new layout");

            Assert.AreEqual(1, (await QueryAsync(node, db, "SELECT * FROM fresh")).Count, $"node {node.Index} reads the new table's row");
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Rollback_LeavesNothingOnAnyNode(bool onLeader)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);
        InProcessSchemaCluster.Node driver = onLeader ? leader : follower;
        long versionBefore = leader.Database!.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(driver);
        await Ddl(driver, tx, db, "CREATE TABLE fresh (id int64 PRIMARY KEY)");
        await Ddl(driver, tx, db, "ALTER TABLE base ADD COLUMN w int64");
        await Ddl(driver, tx, db, "DROP TABLE base");
        await driver.Database!.Transactions.RollbackAsync(tx);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.AreEqual(versionBefore, node.Database!.Schema.SchemaVersion, $"node {node.Index}: a rollback before COMMIT writes no schema entry");
            Assert.IsFalse(node.Database!.Schema.Tables.ContainsKey("fresh"));
            Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("base"));
            Assert.IsFalse(node.Database!.Schema.Tables["base"].Columns!.Exists(c => c.Name == "w"));
            Assert.AreEqual(1, (await QueryAsync(node, db, "SELECT * FROM base")).Count);
        }

        // The DDL lock is free again: another node's DDL transaction runs at once.
        InProcessSchemaCluster.Node other = onLeader ? follower : leader;
        KvTransaction next = await BeginSessionAsync(other);
        await Ddl(other, next, db, "CREATE TABLE later (id int64 PRIMARY KEY)").WaitAsync(TimeSpan.FromSeconds(10));
        await other.Database!.Transactions.CommitAsync(next);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("later"), $"node {node.Index} sees the next transaction's table");
    }

    [Test]
    public async Task ParentDeleteOnAFollower_DuringThePendingWindow_MeetsTheNewForeignKey()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);

        KvTransaction tx = await BeginSessionAsync(leader);
        await Ddl(leader, tx, db, "CREATE TABLE children (id int64 PRIMARY KEY, base_id int64 REFERENCES base (id))");
        await Write(leader, tx, db, "INSERT INTO children (id, base_id) VALUES (10, 1)");

        // The rows commit in KV; the driver stops before the publish.
        CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay overlay =
            (CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, System.Threading.CancellationToken.None);
        await leader.Database!.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, System.Threading.CancellationToken.None);
        overlay.OutcomeUnknown = true;
        await overlay.CloseAsync();

        Assert.IsNotNull(follower.Database!.Schema.PendingChangeSet, "the follower applied the pending entry before the KV commit");

        Task deleteOnFollower = AutocommitAsync(follower, db, "DELETE FROM base WHERE id = 1", ddl: false);

        await Task.Delay(300);
        Assert.IsFalse(deleteOnFollower.IsCompleted, "the follower holds the statement on the fenced parent");

        Assert.AreEqual(true, await leader.Executor.ResolvePendingChangeSetForTests(leader.Database!));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await deleteOnFollower.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, refused!.Code);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(1, (await QueryAsync(node, db, "SELECT * FROM base")).Count, $"node {node.Index} keeps the parent row");
    }

    /// <summary>
    /// Stops the driver of <paramref name="tx"/> after its pending entry, the way a crashed node or a lost
    /// client leaves it. With <paramref name="kvCommitted"/> the rows commit in KV first and only the publish
    /// is missing; without it the KV transaction ends without a commit. Either way the driver no longer
    /// ends the change set, and its DDL lock is free.
    /// </summary>
    private static async Task StopDriverAfterThePendingEntryAsync(InProcessSchemaCluster.Node driver, KvTransaction tx, bool kvCommitted)
    {
        CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay overlay =
            (CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, System.Threading.CancellationToken.None);

        overlay.OutcomeUnknown = true;

        if (kvCommitted)
        {
            await driver.Database!.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, System.Threading.CancellationToken.None);
            await overlay.CloseAsync();
        }
        else
        {
            await driver.Database!.Transactions.RollbackAsync(tx);
        }
    }

    private static async Task WaitUntilNothingIsPendingAsync(InProcessSchemaCluster cluster, int timeoutMs = 30_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (cluster.Nodes.Any(n => n.Database!.Schema.PendingChangeSet is not null) && Environment.TickCount64 < deadline)
            await Task.Delay(100);
    }

    /// <summary>
    /// Checks the end state of a change set that created the table <paramref name="table"/> with one row:
    /// nothing is pending on any node, and every node has the table and its row exactly when the
    /// transaction's KV commit landed.
    /// </summary>
    private static async Task AssertResolvedOnEveryNodeAsync(InProcessSchemaCluster cluster, string db, string table, bool kvCommitted)
    {
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsNull(node.Database!.Schema.PendingChangeSet, $"node {node.Index} has nothing pending");
            Assert.AreEqual(kvCommitted, node.Database!.Schema.Tables.ContainsKey(table),
                $"node {node.Index}: the change set follows the KV outcome of its transaction");

            if (kvCommitted)
                Assert.AreEqual(1, (await QueryAsync(node, db, $"SELECT * FROM {table}")).Count, $"node {node.Index} reads the row of the transaction");
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task LostDriver_IsResolvedOnEveryNodeWithTheCoordinatorSweepOff(bool kvCommitted)
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync(CamusDBOptions.Default with
        {
            TransactionalDdlEnabled = true,
            CoordinatorJobSweepIntervalMs = 0,
            TransactionalDdlResolveAfterMs = 0,
        });
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);

        KvTransaction tx = await BeginSessionAsync(follower);
        await Ddl(follower, tx, db, "CREATE TABLE lost (id int64 PRIMARY KEY)");
        await Write(follower, tx, db, "INSERT INTO lost (id) VALUES (1)");

        // The leader's watcher reads the decision key: it publishes after the KV commit and aborts
        // without one. A watcher run that meets the transaction's lock on the key tries again later.
        await StopDriverAfterThePendingEntryAsync(follower, tx, kvCommitted);

        await WaitUntilNothingIsPendingAsync(cluster);
        await AssertResolvedOnEveryNodeAsync(cluster, db, "lost", kvCommitted);
    }

    /// <summary>
    /// The schema leadership moves while a change set is pending. The old leader stops acting on it, and
    /// the watcher of the new leader ends it from the decision key.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task LeaderMovesWhileAChangeSetIsPending_TheNewLeaderResolvesIt(bool kvCommitted)
    {
        // Long enough that no watcher acts before the move ends; the move takes well under a second.
        const int ResolveAfterMs = 8_000;

        await using InProcessSchemaCluster cluster = await StartClusterAsync(CamusDBOptions.Default with
        {
            TransactionalDdlEnabled = true,
            CoordinatorJobSweepIntervalMs = 0,
            TransactionalDdlResolveAfterMs = ResolveAfterMs,
        });
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);

        KvTransaction tx = await BeginSessionAsync(leader);
        await Ddl(leader, tx, db, "CREATE TABLE moved (id int64 PRIMARY KEY)");
        await Write(leader, tx, db, "INSERT INTO moved (id) VALUES (1)");

        long stoppedAt = Environment.TickCount64;
        await StopDriverAfterThePendingEntryAsync(leader, tx, kvCommitted);

        await cluster.TransferSchemaLeadershipAsync(db, follower);
        long moveMs = Environment.TickCount64 - stoppedAt;

        // Still pending on every node, so the old leader did not end it before the move.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsNotNull(node.Database!.Schema.PendingChangeSet,
                $"node {node.Index}: the change set must still be pending after the move ({moveMs} ms of {ResolveAfterMs} ms)");

        await WaitUntilNothingIsPendingAsync(cluster, timeoutMs: ResolveAfterMs + 30_000);
        await AssertResolvedOnEveryNodeAsync(cluster, db, "moved", kvCommitted);

        // The schema accepts changes again, and the DDL lock is free.
        await AutocommitAsync(leader, db, "CREATE TABLE after_move (id int64 PRIMARY KEY)", ddl: true)
            .WaitAsync(TimeSpan.FromSeconds(30));
        Assert.IsTrue(follower.Database!.Schema.Tables.ContainsKey("after_move"));
    }

    /// <summary>
    /// The lock order: an autocommit DDL statement takes the DDL lock before the DDL gate. A DDL transaction
    /// on a follower holds the DDL lock, and its <c>COMMIT</c> needs the DDL gate of the leader for its
    /// pending entry. A statement on the leader that held the gate while it waited for the DDL lock would
    /// stop that <c>COMMIT</c> until the statement gave up after its 30 s lock wait.
    /// </summary>
    [Test]
    public async Task AutocommitDdlOnTheLeader_WaitsForAFollowersDdlTransaction_WithoutHoldingTheGate()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);

        KvTransaction tx = await BeginSessionAsync(follower);
        await Ddl(follower, tx, db, "CREATE TABLE from_transaction (id int64 PRIMARY KEY)");
        await Write(follower, tx, db, "INSERT INTO from_transaction (id) VALUES (1)");

        Task statementOnLeader = AutocommitAsync(leader, db, "CREATE TABLE from_leader (id int64 PRIMARY KEY)", ddl: true);

        await Task.Delay(500);
        Assert.IsFalse(statementOnLeader.IsCompleted, "the statement on the leader waits for the DDL lock of the transaction");

        // Far below the 30 s lock wait: the commit does not queue behind the waiting statement.
        await follower.Database!.Transactions.CommitAsync(tx).WaitAsync(TimeSpan.FromSeconds(10));

        await statementOnLeader.WaitAsync(TimeSpan.FromSeconds(30));
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("from_transaction"), $"node {node.Index} sees the table of the transaction");
            Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("from_leader"), $"node {node.Index} sees the table of the statement");
        }
    }

    /// <summary>Counts the stored keys under <paramref name="prefix"/> in one key space, with a plain read of the latest committed keys.</summary>
    private static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, System.Threading.CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal) && entry.Value is not null)
                count++;
        }

        return count;
    }

    /// <summary>
    /// The committing node publishes a change set that drops an index and stops before it removes the
    /// entries. The schema leadership moves, and the coordinator-job sweep of the new leader removes them
    /// from the purge record the transaction committed. The old leader's sweep does nothing.
    /// </summary>
    [Test]
    public async Task DroppedIndexEntries_LeftByTheCommittingNode_AreRemovedByTheNewSchemaLeader()
    {
        await using InProcessSchemaCluster cluster = await StartClusterAsync(CamusDBOptions.Default with
        {
            TransactionalDdlEnabled = true,
            CoordinatorJobSweepIntervalMs = 0,
        });
        string db = cluster.NextSchemaLogDatabaseName();
        (InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await PrepareAsync(cluster, db);

        await AutocommitAsync(leader, db, "CREATE INDEX name_idx ON base (name)", ddl: true);
        await AutocommitAsync(leader, db, "INSERT INTO base (id, name) VALUES (2, 'two'), (3, 'three')", ddl: false);

        TableSchema table = leader.Database!.Schema.Tables["base"];
        TableIndexSchema index = table.Indexes!.First(i => i.Name == "name_idx");
        string indexSpace = KvKeyBuilder.IndexSpaceOf(leader.Database!.Id, table.EffectiveStorageId, index.KvId);
        string purgePrefix = MetaKeys.PurgeKeyPrefix(leader.Database!.Id);
        string metaBucket = MetaKeys.MetaBucketPrefix(leader.Database!.Id);

        Assert.AreEqual(3, await CountKeysAsync(leader.Database!, indexSpace, indexSpace + "/"));

        leader.Executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;

        KvTransaction tx = await BeginSessionAsync(leader);
        await Ddl(leader, tx, db, "DROP INDEX name_idx ON base");
        await leader.Database!.Transactions.CommitAsync(tx);

        Assert.AreEqual(3, await CountKeysAsync(leader.Database!, indexSpace, indexSpace + "/"), "the committing node stopped before its removal");
        Assert.AreEqual(1, await CountKeysAsync(leader.Database!, metaBucket, purgePrefix));

        await cluster.TransferSchemaLeadershipAsync(db, follower);

        await leader.Executor.SweepCoordinatorJobsForTests();
        Assert.AreEqual(3, await CountKeysAsync(leader.Database!, indexSpace, indexSpace + "/"), "a node that does not lead the schema log removes nothing");

        Assert.AreEqual(1, await follower.Executor.SweepCoordinatorJobsForTests(), "the new leader sweeps the database");

        Assert.AreEqual(0, await CountKeysAsync(follower.Database!, indexSpace, indexSpace + "/"), "the new leader removed the entries");
        Assert.AreEqual(0, await CountKeysAsync(follower.Database!, metaBucket, purgePrefix), "and the record");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(3, (await QueryAsync(node, db, "SELECT * FROM base")).Count, $"node {node.Index} keeps every row");
    }
}
