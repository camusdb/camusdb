/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kommander.Data;
using Kommander.System;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>SHOW PARTITIONS</c> on a cluster under replica placement: three nodes, a replication factor of
/// two, so each data partition has one node that does not host it.
///
/// <para>The statement is node-local. Each node lists every partition of the committed map, and
/// reports the leader, the term and the commit index as it sees them. The tests check what must agree
/// once the leaders are stable: exactly one node claims each partition, every node names that node
/// as the leader, and a node that does not host a partition reports NULL for the values only a
/// replica has. A leadership transfer must move the local-leader flag from the old leader to the new
/// one.</para>
///
/// <para>Leader values change during an election, so each check polls until the cluster settles.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterShowPartitions
{
    private const int NodeCount = 3;

    private const int Partitions = 3;

    private const int ReplicationFactor = 2;

    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(60);

    private static async Task<InProcessSchemaCluster> StartClusterAsync()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

        return await InProcessSchemaCluster.StartAsync(
            nodeCount: NodeCount,
            partitions: Partitions,
            loggerFactory: loggerFactory,
            logger: loggerFactory.CreateLogger<ICamusDB>(),
            configureNode: node =>
            {
                node.ReplicationFactor = ReplicationFactor;
                node.EnablePlacementRebalancer = false;

                // A node that does not host a partition learns its leader from gossiped load
                // reports. A short report cadence lets that happen in a test-sized window.
                node.LeaderBalancerReportInterval = TimeSpan.FromMilliseconds(500);
                node.LeaderBalancerReportTtl = TimeSpan.FromSeconds(10);
            });
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> ShowPartitionsAsync(InProcessSchemaCluster.Node node)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "SHOW PARTITIONS", parameters: null));

        List<IReadOnlyDictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row.Row);

        return rows;
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>[]> ShowPartitionsOnEveryNodeAsync(InProcessSchemaCluster cluster)
    {
        InProcessSchemaCluster.Node[] nodes = cluster.Nodes;
        List<IReadOnlyDictionary<string, ColumnValue>>[] views = new List<IReadOnlyDictionary<string, ColumnValue>>[nodes.Length];

        for (int i = 0; i < nodes.Length; i++)
            views[i] = await ShowPartitionsAsync(nodes[i]);

        return views;
    }

    private static string Endpoint(InProcessSchemaCluster.Node node) => node.Kahuna.Raft.GetLocalEndpoint();

    private static bool IsNull(IReadOnlyDictionary<string, ColumnValue> row, string column) => row[column].Type == ColumnType.Null;

    private static string? Str(IReadOnlyDictionary<string, ColumnValue> row, string column) =>
        IsNull(row, column) ? null : row[column].StrValue;

    private static IReadOnlyDictionary<string, ColumnValue> RowOf(List<IReadOnlyDictionary<string, ColumnValue>> view, int partitionId) =>
        view.First(r => r["partition_id"].LongValue == partitionId);

    /// <summary>
    /// Waits until every data partition has a leader that has held for a moment, so that the checks
    /// after it do not race an election.
    /// </summary>
    private static async Task WaitForStableLeadersAsync(InProcessSchemaCluster cluster)
    {
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            InProcessSchemaCluster.Node hosting = cluster.Nodes.First(n => n.Kahuna.Raft.HostsPartition(partitionId));

            await hosting.Kahuna.Raft.WaitForLeaderStableAsync(partitionId, TimeSpan.FromMilliseconds(500), CancellationToken.None)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    /// <summary>
    /// What the nodes must agree on once the leaders are stable, as a description of the first
    /// difference, or null when there is none:
    /// <list type="bullet">
    ///   <item>every node lists partition 0 and every data partition, in order, under its own endpoint;</item>
    ///   <item>exactly one node reports <c>leader_is_local</c> for each partition, and every node names
    ///         that node as the leader;</item>
    ///   <item>the replication factor of nodes hosts each data partition;</item>
    ///   <item>the leader of each data partition reports a follower lag.</item>
    /// </list>
    /// </summary>
    private static string? FindDisagreement(InProcessSchemaCluster cluster, List<IReadOnlyDictionary<string, ColumnValue>>[] views)
    {
        InProcessSchemaCluster.Node[] nodes = cluster.Nodes;
        long[] expectedIds = Enumerable.Range(0, Partitions + 1).Select(i => (long)i).ToArray();

        for (int i = 0; i < nodes.Length; i++)
        {
            long[] ids = views[i].Select(r => r["partition_id"].LongValue).ToArray();
            if (!ids.SequenceEqual(expectedIds))
                return $"node {i} lists partitions [{string.Join(",", ids)}]";

            foreach (IReadOnlyDictionary<string, ColumnValue> row in views[i])
            {
                if (row["node"].StrValue != Endpoint(nodes[i]))
                    return $"node {i} labels its rows '{row["node"].StrValue}'";
            }
        }

        for (int partitionId = 0; partitionId <= Partitions; partitionId++)
        {
            List<int> claimants = [];
            int hostingCount = 0;

            for (int i = 0; i < nodes.Length; i++)
            {
                IReadOnlyDictionary<string, ColumnValue> row = RowOf(views[i], partitionId);

                if (row["leader_is_local"].BoolValue)
                    claimants.Add(i);

                if (row["hosted_locally"].BoolValue)
                    hostingCount++;
            }

            if (claimants.Count != 1)
                return $"partition {partitionId}: nodes [{string.Join(",", claimants)}] claim the leadership";

            string leader = Endpoint(nodes[claimants[0]]);

            for (int i = 0; i < nodes.Length; i++)
            {
                string? reported = Str(RowOf(views[i], partitionId), "leader");
                if (reported != leader)
                    return $"partition {partitionId}: node {i} names '{reported ?? "NULL"}' as the leader, the leader is '{leader}'";
            }

            int expectedHosting = partitionId == 0 ? NodeCount : ReplicationFactor;
            if (hostingCount != expectedHosting)
                return $"partition {partitionId}: {hostingCount} nodes host it, expected {expectedHosting}";

            if (partitionId > 0 && IsNull(RowOf(views[claimants[0]], partitionId), "max_follower_lag"))
                return $"partition {partitionId}: the leader reports no follower lag yet";
        }

        return null;
    }

    /// <summary>
    /// Runs the statement on every node until <see cref="FindDisagreement"/> finds nothing, and returns
    /// those views. Fails with the last disagreement when the cluster does not settle in time.
    /// </summary>
    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>[]> WaitForAgreementAsync(InProcessSchemaCluster cluster)
    {
        long deadline = Environment.TickCount64 + (long)SettleTimeout.TotalMilliseconds;

        while (true)
        {
            List<IReadOnlyDictionary<string, ColumnValue>>[] views = await ShowPartitionsOnEveryNodeAsync(cluster);
            string? disagreement = FindDisagreement(cluster, views);

            if (disagreement is null)
                return views;

            if (Environment.TickCount64 >= deadline)
                Assert.Fail("SHOW PARTITIONS did not settle: " + disagreement);

            await Task.Delay(200);
        }
    }

    [Test]
    public async Task EveryNodeListsEveryPartition_AndExactlyOneNodeLeadsEach()
    {
        InProcessSchemaCluster cluster = await StartClusterAsync();

        try
        {
            await WaitForStableLeadersAsync(cluster);

            List<IReadOnlyDictionary<string, ColumnValue>>[] views = await WaitForAgreementAsync(cluster);
            InProcessSchemaCluster.Node[] nodes = cluster.Nodes;

            for (int i = 0; i < nodes.Length; i++)
            {
                IReadOnlyDictionary<string, ColumnValue> system = RowOf(views[i], 0);
                Assert.AreEqual("system", system["kind"].StrValue);
                Assert.IsTrue(system["hosted_locally"].BoolValue, $"node {i}: every node hosts the system partition");
                Assert.AreEqual(0, system["replication_factor"].LongValue);
                Assert.AreEqual("", system["voters"].StrValue);

                for (int partitionId = 1; partitionId <= Partitions; partitionId++)
                {
                    IReadOnlyDictionary<string, ColumnValue> row = RowOf(views[i], partitionId);
                    string where = $"node {i}, partition {partitionId}";

                    Assert.AreEqual("data", row["kind"].StrValue, where);
                    Assert.AreEqual("active", row["state"].StrValue, where);
                    Assert.AreEqual(ReplicationFactor, row["replication_factor"].LongValue, where);
                    Assert.IsFalse(row["awaiting_placement"].BoolValue, where);

                    // The committed replica set, read from the same node's Raft, is what the row lists.
                    string expectedVoters = string.Join(",", nodes[i].Kahuna.Raft.GetPartitionReplicas(partitionId)
                        .Where(r => r.Role == RaftReplicaRole.Voter)
                        .Select(r => r.Endpoint));
                    Assert.AreEqual(expectedVoters, row["voters"].StrValue, where);
                    Assert.AreEqual(ReplicationFactor, expectedVoters.Split(',').Length, where);

                    bool hosted = row["hosted_locally"].BoolValue;
                    Assert.AreEqual(nodes[i].Kahuna.Raft.HostsPartition(partitionId), hosted, where);
                    Assert.AreEqual(hosted, expectedVoters.Split(',').Contains(Endpoint(nodes[i])),
                        $"{where}: a node hosts a partition exactly when it is a voter of it");

                    if (hosted)
                    {
                        // A replica has its own term and commit frontier, and its own leader belief.
                        Assert.IsFalse(IsNull(row, "term"), where);
                        Assert.IsFalse(IsNull(row, "commit_index"), where);
                        Assert.AreEqual("local", row["leader_source"].StrValue, where);
                    }
                    else
                    {
                        // A node outside the replica set has no term or commit index for the
                        // partition. Raft gives -1, which must reach the client as NULL.
                        Assert.IsTrue(IsNull(row, "term"), $"{where}: term");
                        Assert.IsTrue(IsNull(row, "commit_index"), $"{where}: commit_index");
                        Assert.IsFalse(row["leader_is_local"].BoolValue, where);
                        Assert.AreEqual("gossip", row["leader_source"].StrValue, where);
                    }

                    // Only the leader records what its followers acknowledged.
                    if (row["leader_is_local"].BoolValue)
                    {
                        Assert.IsFalse(IsNull(row, "max_follower_lag"), where);
                        Assert.GreaterOrEqual(row["max_follower_lag"].LongValue, 0, where);
                        Assert.AreEqual(0, row["stalled_followers"].LongValue, $"{where}: no follower disk is stalled");
                    }
                    else
                    {
                        Assert.IsTrue(IsNull(row, "max_follower_lag"), $"{where}: max_follower_lag");
                        Assert.IsTrue(IsNull(row, "stalled_followers"), $"{where}: stalled_followers");
                    }
                }
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A leadership transfer moves the flag: the old leader reports <c>leader_is_local = false</c> and
    /// names the new leader, the new leader reports <c>true</c>, and the cluster settles on it.
    /// </summary>
    [Test]
    public async Task LeadershipTransfer_MovesTheLocalLeaderFlag()
    {
        InProcessSchemaCluster cluster = await StartClusterAsync();

        try
        {
            await WaitForStableLeadersAsync(cluster);

            const int partitionId = 1;
            List<IReadOnlyDictionary<string, ColumnValue>>[] views = await WaitForAgreementAsync(cluster);
            InProcessSchemaCluster.Node[] nodes = cluster.Nodes;

            int oldLeader = Array.FindIndex(views, v => RowOf(v, partitionId)["leader_is_local"].BoolValue);
            int target = Enumerable.Range(0, nodes.Length)
                .First(i => i != oldLeader && RowOf(views[i], partitionId)["hosted_locally"].BoolValue);

            string targetEndpoint = Endpoint(nodes[target]);

            RaftOperationStatus status = await nodes[oldLeader].Kahuna.Raft
                .TransferLeadershipAsync(partitionId, targetEndpoint)
                .WaitAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(RaftOperationStatus.Success, status, "the transfer must be accepted");

            await nodes[target].Kahuna.Raft.WaitForLeaderStableAsync(partitionId, TimeSpan.FromMilliseconds(500), CancellationToken.None)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(30));

            long deadline = Environment.TickCount64 + (long)SettleTimeout.TotalMilliseconds;
            string state = "";

            while (true)
            {
                IReadOnlyDictionary<string, ColumnValue> onOld = RowOf(await ShowPartitionsAsync(nodes[oldLeader]), partitionId);
                IReadOnlyDictionary<string, ColumnValue> onTarget = RowOf(await ShowPartitionsAsync(nodes[target]), partitionId);

                if (!onOld["leader_is_local"].BoolValue
                    && Str(onOld, "leader") == targetEndpoint
                    && onTarget["leader_is_local"].BoolValue
                    && Str(onTarget, "leader") == targetEndpoint)
                {
                    break;
                }

                state = $"old leader: leader_is_local={onOld["leader_is_local"].BoolValue} leader={Str(onOld, "leader") ?? "NULL"}; " +
                        $"target: leader_is_local={onTarget["leader_is_local"].BoolValue} leader={Str(onTarget, "leader") ?? "NULL"}";

                if (Environment.TickCount64 >= deadline)
                    Assert.Fail($"the transfer to {targetEndpoint} did not show in SHOW PARTITIONS: {state}");

                await Task.Delay(200);
            }

            // Every node, the one outside the replica set too, settles on the new leader.
            List<IReadOnlyDictionary<string, ColumnValue>>[] after = await WaitForAgreementAsync(cluster);
            for (int i = 0; i < nodes.Length; i++)
                Assert.AreEqual(targetEndpoint, Str(RowOf(after[i], partitionId), "leader"), $"node {i}");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
