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
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The cluster and concurrency matrix of views and materialized views. Each case runs a statement
/// while a competing one runs elsewhere, through the deterministic seams —
/// <see cref="MaterializedViewRefresher.AfterStagingForTesting"/>,
/// <see cref="MaterializedViewRefresher.FaultInjectionForTesting"/> and
/// <see cref="ViewCreator.BeforeProposeForTesting"/> — so the competing statement lands inside exactly
/// the window a race occupies, with no sleeps.
///
/// <para>View DDL, materialized-view creation and the refresh's publication are proposed by the node
/// that runs them, and only the schema leader can propose. They run on the leader here; table and
/// index DDL from a follower is forwarded to it. Reads, and the refusals that the refresh fence
/// decides, run on every node.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterViews
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    [TearDown]
    public void ClearSeams()
    {
        MaterializedViewRefresher.AfterStagingForTesting = null;
        MaterializedViewRefresher.FaultInjectionForTesting = null;
        ViewCreator.BeforeProposeForTesting = null;
    }

    private static async Task Ddl(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null)).WaitAsync(TimeSpan.FromSeconds(60));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Write(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null)).WaitAsync(TimeSpan.FromSeconds(60));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> Ids(InProcessSchemaCluster.Node node, string db, string sql, KvTransaction? tx = null)
    {
        KvTransaction transaction = tx ?? await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(transaction, db, sql, null));
            List<long> ids = (await cursor.ToListAsync()).Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToList();
            if (tx is null)
                await node.Database!.Transactions.CommitAsync(transaction);
            return ids;
        }
        finally
        {
            if (tx is null)
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static async Task Converge(InProcessSchemaCluster cluster, InProcessSchemaCluster.Node from, string db)
        => await cluster.WaitForSchemaConvergenceAsync(db, from.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

    private static void AssertNoStaging(InProcessSchemaCluster cluster)
    {
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            CollectionAssert.IsEmpty(node.Database!.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList(),
                $"node {node.Index} still holds a staging relation");
    }

    private static readonly List<long> OldContents = [1, 2, 3];

    private static readonly List<long> NewContents = [1, 2, 3, 6];

    private static async Task<(InProcessSchemaCluster, string, InProcessSchemaCluster.Node, InProcessSchemaCluster.Node)> StartAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            wireLeaderForwarder: true,
            options: CamusDBOptions.Default with { MaterializedViewRefreshChunkRows = 2 });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        await Ddl(leader, db, "CREATE TABLE orders (id int64 PRIMARY KEY, status string(16))");
        await Write(leader, db, "INSERT INTO orders (id, status) VALUES (1, 'open'), (2, 'open'), (3, 'open'), (4, 'closed')");
        await Ddl(leader, db, "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");
        await Write(leader, db, "INSERT INTO orders (id, status) VALUES (6, 'open')");
        await Converge(cluster, leader, db);

        return (cluster, db, leader, follower);
    }

    /// <summary>
    /// View DDL is visible on every node without a restart, a refresh reaches followers whose reads
    /// were already warm, and a rename followed by a new view under the old name leaves each name with
    /// its own body on every node.
    /// </summary>
    [Test]
    public async Task ViewDdlAndRefreshesReachEveryNode()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            await Ddl(leader, db, "CREATE VIEW open_v AS SELECT id FROM orders WHERE status = 'open'");
            await Converge(cluster, leader, db);

            await Ddl(leader, db, "CREATE VIEW closed_v AS SELECT id FROM orders WHERE status = 'closed'");
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_v"), $"node {node.Index}");
                Assert.AreEqual(new List<long> { 4 }, await Ids(node, db, "SELECT id FROM closed_v"), $"node {node.Index}");

                // Warm every node's read of the materialized view before it changes.
                Assert.AreEqual(OldContents, await Ids(node, db, "SELECT id FROM open_orders"), $"node {node.Index}");
            }

            await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders");
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_orders"), $"node {node.Index} answered from retired contents");

            // The rename-then-recreate of the old name: the renamed view keeps its body, the new one has its own.
            await Ddl(leader, db, "ALTER VIEW open_v RENAME TO open_v2");
            await Ddl(leader, db, "CREATE VIEW open_v AS SELECT id FROM orders WHERE id = 1");
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_v2"), $"node {node.Index}");
                Assert.AreEqual(new List<long> { 1 }, await Ids(node, db, "SELECT id FROM open_v"), $"node {node.Index}");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A rebuild running on the leader holds a cluster-wide fence: a refresh or a drop of the same view on
    /// another node is refused; index DDL from another node wins and the rebuild is refused; and every
    /// node ends with the same contents and no staging relation.
    /// </summary>
    [Test]
    public async Task ARebuildIsFencedAgainstStatementsOnOtherNodes()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            CamusDBException? refreshOnFollower = null, dropOnFollower = null;

            MaterializedViewRefresher.AfterStagingForTesting = async () =>
            {
                MaterializedViewRefresher.AfterStagingForTesting = null;

                try { await Write(follower, db, "REFRESH MATERIALIZED VIEW open_orders"); }
                catch (CamusDBException e) { refreshOnFollower = e; }

                try { await Ddl(follower, db, "DROP MATERIALIZED VIEW open_orders"); }
                catch (CamusDBException e) { dropOnFollower = e; }
            };

            await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders");
            await Converge(cluster, leader, db);

            Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, refreshOnFollower?.Code, refreshOnFollower?.Message);
            Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, dropOnFollower?.Code, dropOnFollower?.Message);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_orders"), $"node {node.Index}");

            // Index DDL from another node wins; the rebuild is refused rather than publishing over it.
            await Write(leader, db, "INSERT INTO orders (id, status) VALUES (7, 'open')");

            MaterializedViewRefresher.AfterStagingForTesting = async () =>
            {
                MaterializedViewRefresher.AfterStagingForTesting = null;
                await Ddl(follower, db, "CREATE INDEX open_orders_id2 ON open_orders (id)");
            };

            CamusDBException? conflicted = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders"));
            Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, conflicted!.Code);
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_orders"), $"node {node.Index} kept the previous contents");
                CollectionAssert.Contains(node.Database!.Schema.Tables["open_orders"].Indexes!.Select(i => i.Name).ToList(), "open_orders_id2");
            }

            AssertNoStaging(cluster);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A rename forwarded from a follower while the leader rebuilds the view: the rebuild publishes onto
    /// the view under its new name, or is refused and leaves the previous contents. Either way every
    /// node has the view only under the new name, holding one whole version, and no staging relation.
    /// </summary>
    [Test]
    public async Task ARenameFromAnotherNodeDuringARebuildLeavesOneWholeView()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            MaterializedViewRefresher.AfterStagingForTesting = async () =>
            {
                MaterializedViewRefresher.AfterStagingForTesting = null;
                await Ddl(follower, db, "ALTER MATERIALIZED VIEW open_orders RENAME TO open_orders_v2");
            };

            try
            {
                await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders");
            }
            catch (CamusDBException refused)
            {
                Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused.Code, refused.Message);
            }

            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.IsFalse(node.Database!.Schema.Tables.ContainsKey("open_orders"), $"node {node.Index} kept the old name");

                List<long> contents = await Ids(node, db, "SELECT id FROM open_orders_v2");
                Assert.IsTrue(contents.SequenceEqual(OldContents) || contents.SequenceEqual(NewContents),
                    $"node {node.Index} holds a mixture: {string.Join(",", contents)}");
            }

            AssertNoStaging(cluster);

            await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders_v2");
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_orders_v2"), $"node {node.Index}");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A transaction that read the materialized view before the swap never sees a mixture after it:
    /// it reads one whole version, or it is refused with a retryable error because the contents it
    /// pinned were replaced. A reader that starts after the swap sees the new contents whole.
    /// </summary>
    [Test]
    public async Task AReaderPinnedBeforeTheSwapNeverSeesAMixture()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            KvTransaction pinned = await follower.Database!.Transactions.BeginAsync();
            try
            {
                Assert.AreEqual(OldContents, await Ids(follower, db, "SELECT id FROM open_orders", pinned));

                await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders");
                await Converge(cluster, leader, db);

                try
                {
                    List<long> again = await Ids(follower, db, "SELECT id FROM open_orders", pinned);
                    Assert.IsTrue(again.SequenceEqual(OldContents) || again.SequenceEqual(NewContents),
                        $"a pinned reader saw a mixture: {string.Join(",", again)}");
                }
                catch (CamusDBException e)
                {
                    Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.TransactionConflict)
                        .Or.EqualTo(CamusDBErrorCodes.SnapshotPrecedesContentsGeneration), e.Message);
                }
            }
            finally
            {
                await follower.Database.Transactions.RollbackIfNotCompletedAsync(pinned);
            }

            Assert.AreEqual(NewContents, await Ids(follower, db, "SELECT id FROM open_orders"));
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Replace against replace, and a dependent created against a drop, each from a second session that
    /// commits inside the first one's window: the statement validated against the older definition is
    /// refused, and no node keeps a broken definition or an edge to a dropped view.
    /// </summary>
    [Test]
    public async Task CompoundViewDdlNeverBreaksADefinitionOnAnyNode()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            await Ddl(leader, db, "CREATE TABLE t (id int64 PRIMARY KEY, a int64, b int64)");
            await Ddl(leader, db, "CREATE VIEW v AS SELECT id FROM t");
            await Converge(cluster, leader, db);

            ViewCreator.BeforeProposeForTesting = async name =>
            {
                ViewCreator.BeforeProposeForTesting = null;
                await Ddl(leader, db, "CREATE OR REPLACE VIEW v AS SELECT id, a FROM t");
            };

            CamusDBException? replaced = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Ddl(leader, db, "CREATE OR REPLACE VIEW v AS SELECT id, b FROM t"));
            Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, replaced!.Code);
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                CollectionAssert.AreEqual(new[] { "id", "a" },
                    node.Database!.Schema.Views["v"].Definition!.Columns!.Select(c => c.Name).ToArray(), $"node {node.Index}");

            ViewCreator.BeforeProposeForTesting = async name =>
            {
                ViewCreator.BeforeProposeForTesting = null;
                await Ddl(leader, db, "DROP VIEW v");
            };

            CamusDBException? dependent = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Ddl(leader, db, "CREATE VIEW over_v AS SELECT id FROM v"));
            Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, dependent!.Code);
            await Converge(cluster, leader, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.IsFalse(node.Database!.Schema.Views.ContainsKey("v"), $"node {node.Index}");
                Assert.IsFalse(node.Database.Schema.Views.ContainsKey("over_v"), $"node {node.Index} kept a view over a dropped one");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A rebuild interrupted on one node is restarted by the sweep on another, after schema leadership
    /// moved there, and every node ends with the new contents and nothing left behind.
    /// </summary>
    [Test]
    public async Task AnInterruptedRebuildIsFinishedByAnotherNode()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader, InProcessSchemaCluster.Node follower) = await StartAsync();
        try
        {
            MaterializedViewRefresher.FaultInjectionForTesting = point =>
            {
                if (point != MaterializedViewRefresher.FaultPoint.AfterChunk)
                    return Task.CompletedTask;

                MaterializedViewRefresher.FaultInjectionForTesting = null;
                throw new MaterializedViewRefresher.SimulatedCrashForTesting(point);
            };

            Assert.ThrowsAsync<MaterializedViewRefresher.SimulatedCrashForTesting>(async () =>
                await Write(leader, db, "REFRESH MATERIALIZED VIEW open_orders"));

            // The node that ran the rebuild is still up, so leadership is handed over rather than lost.
            await cluster.TransferSchemaLeadershipAsync(db, follower);

            Assert.AreEqual(1, await follower.Executor.ReclaimAbandonedRefreshesForTesting(db));
            await Converge(cluster, follower, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(NewContents, await Ids(node, db, "SELECT id FROM open_orders"), $"node {node.Index}");

            AssertNoStaging(cluster);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
