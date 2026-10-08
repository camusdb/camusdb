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

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kommander;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
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

    private static async Task<(InProcessSchemaCluster cluster, CapturingLoggerProvider logs)> StartClusterAsync()
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
                node.LeaderBalancerReportInterval = TimeSpan.FromMilliseconds(500);
                node.LeaderBalancerReportTtl = TimeSpan.FromSeconds(10);
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
