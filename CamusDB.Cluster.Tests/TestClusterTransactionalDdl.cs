
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Cluster;

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

    [Test]
    public async Task LostDriver_IsResolvedOnEveryNodeWithTheCoordinatorSweepOff()
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

        CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay overlay =
            (CamusDB.Core.Catalogs.Transactional.TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, System.Threading.CancellationToken.None);

        // The driver loses track of its commit, and its KV transaction ends without committing.
        overlay.OutcomeUnknown = true;
        await follower.Database!.Transactions.RollbackAsync(tx);

        long deadline = Environment.TickCount64 + 30_000;
        while (cluster.Nodes.Any(n => n.Database!.Schema.PendingChangeSet is not null) && Environment.TickCount64 < deadline)
            await Task.Delay(100);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsNull(node.Database!.Schema.PendingChangeSet, $"node {node.Index}: the leader's watcher aborted the change set");
            Assert.IsFalse(node.Database!.Schema.Tables.ContainsKey("lost"));
        }
    }
}
