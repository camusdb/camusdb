/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using System.IO;
using System.Text;
using System.Text.Json;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kommander;

using CamusDB.App.Controllers;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The control plane under replica placement: more nodes than the replication factor, so each Raft
/// partition is hosted by a strict subset of the nodes. Before this coverage existed every cluster
/// test ran as many nodes as the replication factor, where every node hosts every partition, and
/// the control plane silently assumed exactly that: the schema log, the cluster-settings log and
/// the singleton sweeps asked Raft about partitions the local node did not host and got
/// <c>PartitionNotHostedException</c> back.
///
/// <para>Five nodes, two partitions, replication factor 3: every partition has exactly three
/// replicas and two nodes that do not host it, so each scenario below has a node outside the
/// schema partition's replica set to drive from. The placement rebalancer is off; the initial
/// placement at the replication factor applies regardless, and nothing moves replicas during a
/// test.</para>
///
/// <para>What a node outside a partition's replica set cannot do is receive that partition's log.
/// Such a node follows the durable checkpoint instead (the fast unhosted probe), acknowledges each
/// version it reaches, and routes DDL and setting changes to a replica through the placement view.
/// These tests check the observable result: DDL from any node commits and converges on every
/// node, a setting changed from any node applies on every node, exactly one node leads a keyed
/// singleton, and no not-hosted error reaches a log at warning level or above.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterPartialPlacement
{
    private const int NodeCount = 5;

    private const int Partitions = 2;

    private const int ReplicationFactor = 3;

    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(20);

    /// <param name="leaderHints">
    /// False keeps the gossiped leader hints from ever forming: the load reports that carry them are
    /// published once an hour. A node outside a partition's replica set then names the first voter
    /// of the replica set for every operation on it, which is the state of a cluster in its first
    /// seconds, and the state the ack-routing scenario needs to hold for its whole run.
    /// </param>
    private static async Task<(InProcessSchemaCluster cluster, CapturingLoggerProvider logs)> StartClusterAsync(bool leaderHints = true)
    {
        CapturingLoggerProvider logs = new();

        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddFilter("Camus", LogLevel.Warning).AddConsole().AddProvider(logs));

        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: NodeCount,
            partitions: Partitions,
            loggerFactory: loggerFactory,
            logger: loggerFactory.CreateLogger<ICamusDB>(),
            wireLeaderForwarder: true,
            configureNode: node =>
            {
                node.ReplicationFactor = ReplicationFactor;
                node.EnablePlacementRebalancer = false;

                // Leader hints come from gossiped load reports; a short report cadence lets the
                // placement view settle in a test-sized window instead of the 5 s production default.
                node.LeaderBalancerReportInterval = leaderHints ? TimeSpan.FromMilliseconds(500) : TimeSpan.FromHours(1);
                node.LeaderBalancerReportTtl = leaderHints ? TimeSpan.FromSeconds(10) : TimeSpan.FromHours(2);
            });

        return (cluster, logs);
    }

    /// <summary>
    /// Waits until every node that does not host a partition knows that partition's leader through
    /// the gossiped hint. Kahuna routes an operation on a non-hosted partition to the hint when one
    /// exists, else to the first voter of the committed replica set; a sequence operation that lands
    /// on a voter that is not the leader answers <c>MustRetry</c> and is not forwarded further, so
    /// until the hint forms a table-id allocation from such a node can fail for its whole retry
    /// budget. A hint reaches a node only when the leader's gossip round picks that node directly
    /// (Kommander gossips every 5 s to 2 random peers), so the wait allows several rounds. Only the
    /// DDL scenario needs it: a CREATE TABLE allocates its table id from the store-wide sequence.
    /// </summary>
    private static async Task WaitForLeaderHintsAsync(InProcessSchemaCluster cluster)
    {
        long deadline = Environment.TickCount64 + 90_000;

        while (true)
        {
            List<string> missing = [];

            for (int partition = 1; partition <= Partitions; partition++)
            {
                foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                {
                    if (node.Kahuna.Raft.HostsPartition(partition))
                        continue;

                    if (string.IsNullOrEmpty(node.Kahuna.Raft.GetPartitionLeaderHint(partition)))
                        missing.Add($"{node.Kahuna.Raft.GetLocalNodeName()}:p{partition}");
                }
            }

            if (missing.Count == 0)
                return;

            if (Environment.TickCount64 >= deadline)
                Assert.Fail("Leader hints did not form for: " + string.Join(", ", missing));

            await Task.Delay(100);
        }
    }

    /// <summary>
    /// Splits the cluster into the nodes that host <paramref name="partitionId"/> and the ones that
    /// do not, and asserts the split is the one the configuration promises: the scenario is
    /// vacuous unless some node is outside the replica set.
    /// </summary>
    private static (List<InProcessSchemaCluster.Node> hosting, List<InProcessSchemaCluster.Node> notHosting) SplitByHosting(
        InProcessSchemaCluster cluster, int partitionId)
    {
        List<InProcessSchemaCluster.Node> hosting = [];
        List<InProcessSchemaCluster.Node> notHosting = [];

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (node.Kahuna.Raft.HostsPartition(partitionId))
                hosting.Add(node);
            else
                notHosting.Add(node);
        }

        Assert.AreEqual(ReplicationFactor, hosting.Count,
            $"Partition {partitionId} must be hosted by exactly the replication factor of nodes");
        Assert.AreEqual(NodeCount - ReplicationFactor, notHosting.Count,
            $"Partition {partitionId} must leave {NodeCount - ReplicationFactor} nodes outside its replica set");

        return (hosting, notHosting);
    }

    private static async Task WaitForConvergenceAsync(InProcessSchemaCluster cluster, string db)
    {
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        long version = leader.Database!.Schema.SchemaVersion;
        await cluster.WaitForSchemaConvergenceAsync(db, version, ConvergenceTimeout);
    }

    private static void AssertNoNotHostedErrors(CapturingLoggerProvider logs)
    {
        List<string> offending = logs.Entries
            .Where(e => e.Level >= LogLevel.Warning && e.MentionsNotHosted)
            .Select(e => $"[{e.Level}] {e.Category}: {e.Message} {e.Exception?.GetType().Name}")
            .ToList();

        Assert.IsEmpty(offending,
            "A not-hosted partition error reached a log at warning level or above:\n" + string.Join("\n", offending));
    }

    [Test]
    public async Task DdlFromAnyNode_CommitsAndConvergesOnEveryNode()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync();

        try
        {
            await WaitForLeaderHintsAsync(cluster);

            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            int schemaPartition = cluster.Nodes[0].Database!.SchemaLogPartition;
            (List<InProcessSchemaCluster.Node> hosting, List<InProcessSchemaCluster.Node> notHosting) =
                SplitByHosting(cluster, schemaPartition);

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            Assert.IsTrue(hosting.Contains(leader), "The schema leader must be one of the partition's replicas");

            InProcessSchemaCluster.Node hostingFollower = hosting.First(node => node != leader);
            InProcessSchemaCluster.Node outsiderA = notHosting[0];
            InProcessSchemaCluster.Node outsiderB = notHosting[1];

            // A node outside the replica set has no Raft group to ask about the leader; it must still
            // name a replica to forward to, never throw.
            foreach (InProcessSchemaCluster.Node outsider in notHosting)
            {
                Assert.IsFalse(outsider.Kahuna.HostsSchemaLog(outsider.Database!.Id));
                Assert.IsFalse(await outsider.Kahuna.AmISchemaLeaderAsync(outsider.Database!.Id));

                string resolved = await outsider.Kahuna.WaitForSchemaLeaderAsync(outsider.Database!.Id);
                IReadOnlyList<string> replicas = outsider.Kahuna.SchemaLogReplicaEndpoints(outsider.Database!.Id);
                Assert.That(replicas, Does.Contain(resolved),
                    "A node outside the replica set must route schema DDL to a replica of the schema partition");
            }

            // CREATE TABLE from a node that does not host the schema partition.
            CreateTableResult created = await outsiderA.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id",   ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(ConvergenceTimeout);

            Assert.IsTrue(created.Success);
            Assert.IsTrue(outsiderA.Database!.Schema.Tables.ContainsKey("robots"),
                "The forwarding node must see its own DDL before the call returns");
            await WaitForConvergenceAsync(cluster, db);

            // CREATE INDEX from the other outsider: three staged versions, each gated on every node's
            // acknowledgement, including the two nodes that only follow the checkpoint.
            await outsiderB.Executor.AlterIndex(new AlterIndexTicket(
                databaseName: db,
                tableName: "robots",
                indexName: "name_idx",
                columns: [new ColumnIndexInfo("name", OrderType.Ascending)],
                operation: AlterIndexOperation.AddIndex
            )).WaitAsync(TimeSpan.FromSeconds(60));

            await WaitForConvergenceAsync(cluster, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TableSchema robots = node.Database!.Schema.Tables["robots"];
                Assert.IsTrue(
                    robots.Indexes?.Any(ix => ix.Name == "name_idx" && ix.State == SchemaElementState.Public) == true,
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must see the public index");
            }

            // ADD COLUMN from a follower inside the replica set, the classic forwarding path.
            await hostingFollower.Executor.AlterTable(new AlterTableTicket(
                databaseName: db,
                tableName: "robots",
                column: new ColumnInfo("score", ColumnType.Integer64, notNull: false,
                    defaultValue: new ColumnValue(ColumnType.Integer64, 42L)),
                operation: AlterTableOperation.AddColumn
            )).WaitAsync(TimeSpan.FromSeconds(60));

            await WaitForConvergenceAsync(cluster, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TableSchema robots = node.Database!.Schema.Tables["robots"];
                Assert.IsTrue(
                    robots.Columns?.Any(c => c.Name == "score" && c.State == SchemaElementState.Public) == true,
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must see the public column");
            }

            // DML through the outsiders against the schema they learned from the checkpoint.
            await InsertRobotAsync(outsiderA, db, "r1");
            await InsertRobotAsync(outsiderB, db, "r2");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(2, await CountRobotsAsync(node, db),
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must read both rows");
                Assert.AreEqual(1, await CountRobotsByNameAsync(node, db, "r2"),
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must find the row through the index");
            }

            // DROP TABLE from an outsider.
            await outsiderA.Executor.DropTable(new DropTableTicket(db, "robots", ifExists: false))
                .WaitAsync(ConvergenceTimeout);

            await WaitForConvergenceAsync(cluster, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.IsFalse(node.Database!.Schema.Tables.ContainsKey("robots"),
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must no longer see the dropped table");

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task KeyedSingletonLeadership_IsHeldByExactlyOneNode_AndNeverThrows()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync();

        try
        {
            // Enough keys to land on both partitions; each key's partition has two non-hosting nodes.
            string[] keys = ["sweeps/a", "sweeps/b", "sweeps/c", "sweeps/d"];

            foreach (string key in keys)
            {
                int partition = cluster.Nodes[0].Kahuna.Raft.GetPrefixPartitionKey(key);
                (_, List<InProcessSchemaCluster.Node> notHosting) = SplitByHosting(cluster, partition);

                // Leadership belief settles after an election; the count is retried, the "no throw"
                // part is asserted on every pass.
                int leaders = 0;
                for (int attempt = 0; attempt < 50; attempt++)
                {
                    leaders = 0;
                    foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                    {
                        if (await node.Kahuna.AmILeaderForKeyAsync(key))
                            leaders++;
                    }

                    if (leaders == 1)
                        break;

                    await Task.Delay(100);
                }

                Assert.AreEqual(1, leaders, $"Exactly one node must lead the singleton keyed on '{key}'");

                foreach (InProcessSchemaCluster.Node outsider in notHosting)
                {
                    Assert.IsFalse(await outsider.Kahuna.AmILeaderForKeyAsync(key),
                        "A node outside the replica set can never lead the partition");

                    // Nothing to give up; must be a no-op rather than an error.
                    await outsider.Kahuna.StepDownForKeyAsync(key);
                }
            }

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task ClusterSettingChange_FromNodeOutsideSettingsPartition_AppliesOnEveryNode()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync();

        Dictionary<string, ClusterSettingsService> services = new(StringComparer.Ordinal);
        Dictionary<string, CamusDBOptionsHolder> holders = new(StringComparer.Ordinal);

        try
        {
            InProcessSettingsForwarder forwarder = new(services);
            string dataDir = CamusDBOptions.Default.DataDirectory;

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                string endpoint = node.Kahuna.Raft.GetLocalEndpoint();
                CamusDBOptionsHolder holder = new(CamusDBOptions.Default);
                ClusterSettingsService service = new(
                    node.Kahuna,
                    holder,
                    () => new ConfigDefinition { DataDir = dataDir },
                    postResolve: null,
                    isClusterMode: true,
                    forwarder,
                    logs.CreateLogger("Camus.Settings." + endpoint));

                holders[endpoint] = holder;
                services[endpoint] = service;
            }

            foreach (ClusterSettingsService service in services.Values)
                await service.StartAsync().WaitAsync(TimeSpan.FromSeconds(30));

            int settingsPartition = cluster.Nodes[0].Kahuna.ClusterSettingsLogPartition();
            (_, List<InProcessSchemaCluster.Node> notHosting) = SplitByHosting(cluster, settingsPartition);

            InProcessSchemaCluster.Node outsider = notHosting[0];
            Assert.IsFalse(outsider.Kahuna.HostsClusterSettingsLog());
            Assert.IsFalse(await outsider.Kahuna.AmIClusterSettingsLeaderAsync());

            ClusterSettingsService outsiderService = services[outsider.Kahuna.Raft.GetLocalEndpoint()];
            await outsiderService.SetAsync("stats_flush_interval_ms", "7777").WaitAsync(TimeSpan.FromSeconds(30));

            Assert.AreEqual(7777, holders[outsider.Kahuna.Raft.GetLocalEndpoint()].Current.StatsFlushIntervalMs,
                "The submitting node must see its own change before the call returns");

            await WaitForSettingOnEveryNodeAsync(holders, options => options.StatsFlushIntervalMs == 7777);

            // And back, from the other outsider, so the reset path crosses the same boundary.
            ClusterSettingsService otherOutsider = services[notHosting[1].Kahuna.Raft.GetLocalEndpoint()];
            await otherOutsider.ResetAsync("stats_flush_interval_ms").WaitAsync(TimeSpan.FromSeconds(30));

            int defaultValue = new ConfigDefinition().StatsFlushIntervalMs;
            await WaitForSettingOnEveryNodeAsync(holders, options => options.StatsFlushIntervalMs == defaultValue);

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            foreach (ClusterSettingsService service in services.Values)
                await service.DisposeAsync();

            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// The inter-node DDL endpoint judges leadership by the schema log of the database <em>id</em>,
    /// which is the partition the engine proposes on, and not by the partition the database
    /// <em>name</em> hashes to. The two differ for about every second name, and under leader balancing
    /// and replica placement they have different leaders, so a name-keyed check refused the real
    /// leader and accepted a node that then re-forwarded the ticket to itself and deadlocked on its
    /// own DDL semaphore until the HTTP client timed out: that is where a 5-node CREATE TABLE spent
    /// 43 s per attempt. Here the two partitions are made to have different leaders on purpose, and
    /// the endpoint is driven on both.
    /// </summary>
    [Test]
    public async Task ForwardedDdl_IsJudgedByTheSchemaLogOfTheDatabaseId_NotByItsName()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync();

        try
        {
            await WaitForLeaderHintsAsync(cluster);

            // A database whose name and id hash to different schema-log partitions. The id exists only
            // once the database does, so candidates are created until one qualifies.
            string db;
            string id;
            for (int candidate = 0; ; candidate++)
            {
                db = cluster.NextSchemaLogDatabaseName();
                await cluster.OpenDatabaseOnAllNodesAsync(db);
                id = cluster.Nodes[0].Database!.Id;

                if (cluster.Nodes[0].Kahuna.SchemaLogPartition(db) != cluster.Nodes[0].Kahuna.SchemaLogPartition(id))
                    break;

                Assert.Less(candidate, 40, "No database name hashed to a different partition than its id");
            }

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            InProcessSchemaCluster.Node nameLeader = await WaitForNodeLeadingAsync(cluster, db);

            // The name's partition must be led by a node that does not lead the schema log, so the two
            // answers differ. The leader of the name's partition is moved off the schema leader when
            // the two coincide.
            if (nameLeader.Index == leader.Index)
            {
                int namePartition = leader.Kahuna.SchemaLogPartition(db);
                InProcessSchemaCluster.Node target = cluster.Nodes.First(node =>
                    node.Index != leader.Index && node.Kahuna.Raft.HostsPartition(namePartition));

                await leader.Kahuna.TransferSchemaLeadershipAsync(db, target.Kahuna.Raft.GetLocalEndpoint());
                nameLeader = await WaitForNodeLeadingAsync(cluster, db, expected: target);
            }

            Assert.AreNotEqual(leader.Index, nameLeader.Index);
            Assert.IsTrue(await leader.Kahuna.AmISchemaLeaderAsync(id), "The schema leader must lead the id's partition");
            Assert.IsFalse(await nameLeader.Kahuna.AmISchemaLeaderAsync(id), "The name's leader must not lead the id's partition");
            Assert.IsTrue(await nameLeader.Kahuna.AmISchemaLeaderAsync(db), "The name's leader must lead the name's partition");

            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                Columns =
                [
                    new ColumnInfoRequest { Name = "id", Type = ColumnType.Id },
                    new ColumnInfoRequest { Name = "name", Type = ColumnType.String, NotNull = true },
                ],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    },
                ],
                IfNotExists = false,
            }, ForwardJson);

            // The node that leads the name's partition, but not the schema log, must bounce the ticket
            // without running it: a forwarded ticket that is forwarded again is the deadlock.
            JsonResult refused = await BuildForwardController(nameLeader, body).ForwardCreateTable();
            SchemaDdlForwardResponse refusal = (SchemaDdlForwardResponse)refused.Value!;
            Assert.AreEqual("not-leader", refusal.Status, refusal.Message);
            Assert.IsFalse(nameLeader.Database!.Schema.Tables.ContainsKey("robots"),
                "A node that does not lead the schema log must not apply a forwarded ticket");

            // The schema leader must accept and run it.
            JsonResult accepted = await BuildForwardController(leader, body).ForwardCreateTable();
            SchemaDdlForwardResponse acceptance = (SchemaDdlForwardResponse)accepted.Value!;
            Assert.AreEqual("ok", acceptance.Status, acceptance.Message);
            Assert.IsTrue(leader.Database!.Schema.Tables.ContainsKey("robots"));

            await WaitForConvergenceAsync(cluster, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.IsTrue(node.Database!.Schema.Tables.ContainsKey("robots"),
                    $"{node.Kahuna.Raft.GetLocalNodeName()} must see the forwarded table");

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A node outside the schema partition's replica set has no Raft view of its leader. Before a
    /// gossiped hint forms it names the first voter of the replica set, and an ack recorded there in
    /// silence never reached the leader, whose gate then skipped the node for every later DDL. The
    /// replica that is not the leader must refuse the ack, and the sender must walk on to the leader.
    /// Load reports, which carry the hints, stop after the one each node publishes at startup, and
    /// leadership is moved to a replica no outsider names, so the first delivery is misrouted by
    /// construction.
    /// </summary>
    [Test]
    public async Task AckFromANodeOutsideTheReplicaSet_ReachesTheSchemaLeader_BeforeAnyLeaderHintForms()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync(leaderHints: false);

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();

            // Created and opened on one node only, so the id and the replica set are known before any
            // node outside the replica set opens it and acknowledges its loaded version.
            InProcessSchemaCluster.Node first = cluster.Nodes[0];
            await first.Executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: true));
            first.Database = await first.Executor.OpenDatabase(db);

            string id = first.Database!.Id;
            int schemaPartition = first.Database!.SchemaLogPartition;
            (List<InProcessSchemaCluster.Node> hosting, List<InProcessSchemaCluster.Node> notHosting) =
                SplitByHosting(cluster, schemaPartition);

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

            // Each outsider names the leader from its placement view: the one report every node
            // publishes at startup may have left a hint, else it is the first voter. Leadership is
            // moved to a replica that no outsider names, so every first delivery is misrouted by
            // construction, and it stays misrouted because no further report is ever published.
            HashSet<string> named = new(StringComparer.Ordinal);
            foreach (InProcessSchemaCluster.Node outsider in notHosting)
                named.Add(await outsider.Kahuna.WaitForSchemaLeaderAsync(id));

            InProcessSchemaCluster.Node target = hosting.First(node => !named.Contains(node.Kahuna.Raft.GetLocalEndpoint()));

            if (target.Index != leader.Index)
            {
                await cluster.TransferSchemaLeadershipAsync(db, target);
                leader = target;
            }

            foreach (InProcessSchemaCluster.Node outsider in notHosting)
            {
                Assert.AreNotEqual(leader.Kahuna.Raft.GetLocalEndpoint(), await outsider.Kahuna.WaitForSchemaLeaderAsync(id),
                    "The scenario needs an outsider whose placement view names a replica other than the leader");

                Assert.IsNull(leader.Kahuna.GetRecordedSchemaAck(id, outsider.Kahuna.Raft.GetLocalEndpoint()));
            }

            // Opening the database on a node outside the replica set acknowledges its loaded version
            // through the ack sender, which can only name a replica from the placement view.
            foreach (InProcessSchemaCluster.Node outsider in notHosting)
                outsider.Database = await outsider.Executor.OpenDatabase(db);

            foreach (InProcessSchemaCluster.Node outsider in notHosting)
            {
                string endpoint = outsider.Kahuna.Raft.GetLocalEndpoint();
                long deadline = Environment.TickCount64 + 15_000;

                while (leader.Kahuna.GetRecordedSchemaAck(id, endpoint) is null)
                {
                    if (Environment.TickCount64 >= deadline)
                        Assert.Fail($"The schema leader never recorded an ack from {endpoint}, which does not host the schema partition");

                    await Task.Delay(50);
                }
            }

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A follower that forwards a ticket holds the database's DDL semaphore while it waits for the
    /// answer. A forward to its own node re-enters the same path and waits on that semaphore: a
    /// deadlock that only the transport timeout ends. So the walk over the replica set after a
    /// refusal must skip the local node, even though it is a replica. Every target refuses here, so
    /// the walk visits every candidate it is willing to try.
    /// </summary>
    [Test]
    public async Task DdlForwarder_NeverSendsATicketToItsOwnNode()
    {
        (InProcessSchemaCluster cluster, CapturingLoggerProvider logs) = await StartClusterAsync();

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            (List<InProcessSchemaCluster.Node> hosting, _) = SplitByHosting(cluster, leader.Database!.SchemaLogPartition);
            InProcessSchemaCluster.Node follower = hosting.First(node => node.Index != leader.Index);

            RefusingForwarder forwarder = new();
            DdlForwardingCoordinator coordinator = new(forwarder, isClusterMode: true);

            CreateTableTicket ticket = new(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            );

            CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() =>
                coordinator.TryForwardCreateTableAsync(follower.Database!, ticket).WaitAsync(TimeSpan.FromSeconds(30)));

            Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, refused!.Code);

            string self = follower.Kahuna.Raft.GetLocalEndpoint();
            Assert.That(forwarder.Targets, Does.Not.Contain(self), "A node must never forward a ticket to itself");
            Assert.That(forwarder.Targets, Does.Contain(leader.Kahuna.Raft.GetLocalEndpoint()), "The schema leader must have been tried");

            AssertNoNotHostedErrors(logs);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    private static readonly JsonSerializerOptions ForwardJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// The inter-node DDL endpoint of <paramref name="node"/>, driven without an HTTP pipeline: the
    /// request body is <paramref name="jsonBody"/> and the service provider exposes the node's
    /// <see cref="EmbeddedKahuna"/>, which is what the endpoint consults for leadership.
    /// </summary>
    private static SchemaDdlForwardController BuildForwardController(InProcessSchemaCluster.Node node, string jsonBody)
    {
        SchemaDdlForwardController controller = new(
            node.Executor,
            new HttpTransactionCoordinator(node.Executor),
            NullLogger<ICamusDB>.Instance,
            CamusDBOptions.Default);

        byte[] bytes = Encoding.UTF8.GetBytes(jsonBody);
        DefaultHttpContext httpContext = new() { RequestServices = new NodeServiceProvider(node.Kahuna) };
        httpContext.Request.Body = new MemoryStream(bytes);
        httpContext.Request.ContentLength = bytes.Length;

        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };
        return controller;
    }

    /// <summary>
    /// The node that leads the partition <paramref name="schemaKey"/> hashes to, by the engine's own
    /// leadership question. With <paramref name="expected"/> set, waits until that node leads it.
    /// </summary>
    private static async Task<InProcessSchemaCluster.Node> WaitForNodeLeadingAsync(
        InProcessSchemaCluster cluster, string schemaKey, InProcessSchemaCluster.Node? expected = null)
    {
        long deadline = Environment.TickCount64 + 20_000;

        while (true)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                if (expected is not null && node.Index != expected.Index)
                    continue;

                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
                try
                {
                    if (await node.Kahuna.AmISchemaLeaderAsync(schemaKey, cts.Token))
                        return node;
                }
                catch (OperationCanceledException) { }
                catch (RaftException) { }
            }

            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"No node leads the partition of '{schemaKey}'");

            await Task.Delay(100);
        }
    }

    private sealed class NodeServiceProvider(EmbeddedKahuna kahuna) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(EmbeddedKahuna) ? kahuna : null;
    }

    /// <summary>
    /// A forwarder every target of which answers not-leader, recording the targets in the order they
    /// were tried. It is what the coordinator sees when its placement view is stale on every attempt.
    /// </summary>
    private sealed class RefusingForwarder : ISchemaDdlForwarder
    {
        public List<string> Targets { get; } = [];

        private Task<bool?> Refuse(string leader)
        {
            lock (Targets)
                Targets.Add(leader);

            return Task.FromResult<bool?>(null);
        }

        public Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
        public Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken) => Refuse(leader);
    }

    private static async Task WaitForSettingOnEveryNodeAsync(
        Dictionary<string, CamusDBOptionsHolder> holders, Func<CamusDBOptions, bool> applied)
    {
        long deadline = Environment.TickCount64 + 10_000;

        while (true)
        {
            List<string> lagging = holders.Where(kv => !applied(kv.Value.Current)).Select(kv => kv.Key).ToList();
            if (lagging.Count == 0)
                return;

            if (Environment.TickCount64 >= deadline)
                Assert.Fail("The setting did not apply on every node: " + string.Join(", ", lagging));

            await Task.Delay(50);
        }
    }

    private static async Task InsertRobotAsync(InProcessSchemaCluster.Node node, string db, string name)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db,
            sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), '{name}')", parameters: null));
        await node.Database.Transactions.CommitAsync(tx);
    }

    private static Task<int> CountRobotsAsync(InProcessSchemaCluster.Node node, string db)
        => CountAsync(node, db, "SELECT id FROM robots");

    private static Task<int> CountRobotsByNameAsync(InProcessSchemaCluster.Node node, string db, string name)
        => CountAsync(node, db, $"SELECT id FROM robots WHERE name = '{name}'");

    private static async Task<int> CountAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));

            return (await cursor.ToListAsync()).Count;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// In-process twin of the HTTP settings forwarder: runs the change on the service of the node
    /// the caller resolved as leader, and answers null (not-leader) when that node declines, which
    /// is the signal the caller re-resolves on.
    /// </summary>
    private sealed class InProcessSettingsForwarder : IClusterSettingsForwarder
    {
        private readonly Dictionary<string, ClusterSettingsService> services;

        public InProcessSettingsForwarder(Dictionary<string, ClusterSettingsService> services)
        {
            this.services = services;
        }

        public async Task<bool?> ForwardChangeAsync(string leader, string key, string? value, CancellationToken cancellationToken)
        {
            if (!services.TryGetValue(leader, out ClusterSettingsService? target))
                return null;

            return await target.TryApplyAsLeaderAsync(new ClusterSettingChange(key, value), cancellationToken)
                ? true
                : null;
        }
    }

    /// <summary>
    /// Records every log entry the cluster emits so a test can assert on what was logged, not only
    /// on what happened. The Kahuna and Kommander loggers come from the same factory, so a
    /// not-hosted error raised anywhere in the stack lands here.
    /// </summary>
    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ConcurrentQueue<Entry> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Logger(categoryName, Entries);

        public void Dispose() { }

        public sealed record Entry(string Category, LogLevel Level, string Message, Exception? Exception)
        {
            public bool MentionsNotHosted =>
                Exception is PartitionNotHostedException ||
                Exception?.InnerException is PartitionNotHostedException ||
                Message.Contains("not hosted on this node", StringComparison.Ordinal) ||
                Message.Contains(nameof(PartitionNotHostedException), StringComparison.Ordinal);
        }

        private sealed class Logger : ILogger
        {
            private readonly string category;
            private readonly ConcurrentQueue<Entry> entries;

            public Logger(string category, ConcurrentQueue<Entry> entries)
            {
                this.category = category;
                this.entries = entries;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel))
                    return;

                entries.Enqueue(new Entry(category, logLevel, formatter(state, exception), exception));
            }
        }
    }
}
