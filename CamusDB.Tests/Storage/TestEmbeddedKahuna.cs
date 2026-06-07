
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// T2.1 — EmbeddedKahuna wrapper.
///
/// Verifies that the wrapper correctly:
///   1. Constructs and starts with default (in-memory) options.
///   2. Exposes a usable IKahuna after StartAsync + WaitForLeaderAsync.
///   3. Performs a Set/Get round-trip through the exposed IKahuna.
///   4. Disposes cleanly via IAsyncDisposable.
/// </summary>
[TestFixture]
public sealed class TestEmbeddedKahuna
{
    // Dynamic port base — incremented atomically per test so that nodes from a prior test
    // whose DisposeAsync is still draining callbacks on the thread pool can't collide with
    // ports chosen by the next test.
    private static int nextPortBase = 9100;
    [Test]
    public async Task ConstructStartDisposeWithDefaultOptions()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);

        Assert.IsNotNull(kahuna.Kahuna);
        Assert.IsNotNull(kahuna.Raft);
    }

    [Test]
    public async Task KahunaIsUsableAfterStart()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        await kahuna.WaitForLeaderAsync("test/warmup", CancellationToken.None);

        const string key = "test/k1";
        byte[] value = Encoding.UTF8.GetBytes("hello-camusdb");

        (KeyValueResponseType setType, _, _) = await kahuna.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            value,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Set, setType);

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            key,
            -1,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, getType);
        Assert.IsNotNull(entry);
        Assert.AreEqual(value, entry!.Value);
    }

    [Test]
    public async Task MultipleSetGetRoundTrips()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        await kahuna.WaitForLeaderAsync("t1/warmup", CancellationToken.None);

        for (int i = 0; i < 5; i++)
        {
            string key = $"t1/row/{i}";
            byte[] value = Encoding.UTF8.GetBytes($"value-{i}");

            (KeyValueResponseType setType, _, _) = await kahuna.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                key,
                value,
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                CancellationToken.None
            );

            Assert.AreEqual(KeyValueResponseType.Set, setType, $"Set failed for key {key}");
        }

        for (int i = 0; i < 5; i++)
        {
            string key = $"t1/row/{i}";
            byte[] expected = Encoding.UTF8.GetBytes($"value-{i}");

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                key,
                -1,
                KeyValueDurability.Persistent,
                CancellationToken.None
            );

            Assert.AreEqual(KeyValueResponseType.Get, getType, $"Get failed for key {key}");
            Assert.IsNotNull(entry);
            Assert.AreEqual(expected, entry!.Value, $"Value mismatch for key {key}");
        }
    }

    [Test]
    public async Task WaitForLeaderReturnsNonEmptyLeader()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        string leader = await kahuna.WaitForLeaderAsync("any/key", CancellationToken.None);

        Assert.IsFalse(string.IsNullOrEmpty(leader), "Leader node name must not be empty");
    }

    [Test]
    public async Task SchemaReplication_LeaderCommitsFollowerReturnsNotLeaderAndApplyRuns()
    {
        string db = $"db_{Guid.NewGuid():N}";
        byte[] payload = Encoding.UTF8.GetBytes("schema-change-1");

        int portBase = Interlocked.Add(ref nextPortBase, 10);
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNode = new();

        EmbeddedKahuna node1 = CreateClusterNode("node1", 1, portBase + 1, [new($"localhost:{portBase + 2}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node2 = CreateClusterNode("node2", 2, portBase + 2, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node3 = CreateClusterNode("node3", 3, portBase + 3, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 2}")], raftCommunication, interNode);
        EmbeddedKahuna[] nodes = [node1, node2, node3];

        try
        {
            raftCommunication.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Raft));
            interNode.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Kahuna));

            ConcurrentBag<(string node, int partition, string data)> applied = [];

            foreach (EmbeddedKahuna node in nodes)
            {
                string nodeName = node.Raft.GetLocalNodeName();
                node.RegisterSchemaApply(
                    (partition, bytes) =>
                    {
                        applied.Add((nodeName, partition, Encoding.UTF8.GetString(bytes)));
                        return Task.FromResult(true);
                    },
                    (_, _) => Task.FromResult(true)
                );
            }

            foreach (EmbeddedKahuna node in nodes)
                await node.Raft.UpdateNodes();

            await Task.WhenAll(nodes.Select(x => x.StartAsync(CancellationToken.None)))
                .WaitAsync(TimeSpan.FromSeconds(10));

            int partitionId = node1.SchemaLogPartition(db);
            await node1.Raft.WaitForLeader(partitionId, CancellationToken.None);

            EmbeddedKahuna leader = await WaitForLeaderNode(nodes, partitionId);
            EmbeddedKahuna follower = nodes.First(x => x != leader);

            SchemaReplicationResult followerResult = await follower.ReplicateSchemaChangeAsync(db, payload, CancellationToken.None);
            Assert.AreEqual(SchemaReplicationOutcome.NotLeader, followerResult.Outcome);

            SchemaReplicationResult result = await leader.ReplicateSchemaChangeAsync(db, payload, CancellationToken.None);
            Assert.AreEqual(SchemaReplicationOutcome.Committed, result.Outcome);
            Assert.AreEqual(partitionId, result.PartitionId);

            await WaitUntilAsync(() => applied.Count > 0, TimeSpan.FromSeconds(10));

            foreach ((_, int partition, string data) in applied)
            {
                Assert.AreEqual(partitionId, partition);
                Assert.AreEqual("schema-change-1", data);
            }
        }
        finally
        {
            foreach (EmbeddedKahuna node in nodes)
                await node.DisposeAsync();
        }
    }

    [Test]
    public async Task SchemaReplication_FollowerForwardsToLeaderAndAppliesLocally()
    {
        string db = $"db_{Guid.NewGuid():N}";
        byte[] payload = Encoding.UTF8.GetBytes("schema-change-forwarded");

        int portBase = Interlocked.Add(ref nextPortBase, 10);
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNode = new();

        EmbeddedKahuna node1 = CreateClusterNode("node1", 1, portBase + 1, [new($"localhost:{portBase + 2}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node2 = CreateClusterNode("node2", 2, portBase + 2, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node3 = CreateClusterNode("node3", 3, portBase + 3, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 2}")], raftCommunication, interNode);
        EmbeddedKahuna[] nodes = [node1, node2, node3];

        try
        {
            raftCommunication.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Raft));
            interNode.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Kahuna));

            InMemorySchemaReplicationForwarder forwarder = new(nodes);
            foreach (EmbeddedKahuna node in nodes)
                node.SetSchemaReplicationForwarder(forwarder);

            ConcurrentBag<(string node, int partition, string data)> applied = [];

            foreach (EmbeddedKahuna node in nodes)
            {
                string nodeName = node.Raft.GetLocalNodeName();
                node.RegisterSchemaApply(
                    (partition, bytes) =>
                    {
                        applied.Add((nodeName, partition, Encoding.UTF8.GetString(bytes)));
                        return Task.FromResult(true);
                    },
                    (_, _) => Task.FromResult(true)
                );
            }

            foreach (EmbeddedKahuna node in nodes)
                await node.Raft.UpdateNodes();

            await Task.WhenAll(nodes.Select(x => x.StartAsync(CancellationToken.None)))
                .WaitAsync(TimeSpan.FromSeconds(10));

            int partitionId = node1.SchemaLogPartition(db);
            await node1.Raft.WaitForLeader(partitionId, CancellationToken.None);

            EmbeddedKahuna leader = await WaitForLeaderNode(nodes, partitionId);
            EmbeddedKahuna follower = nodes.First(x => x != leader);
            string followerName = follower.Raft.GetLocalNodeName();

            SchemaReplicationResult result = await follower.ReplicateSchemaChangeAsync(db, payload, CancellationToken.None);

            Assert.AreEqual(SchemaReplicationOutcome.Committed, result.Outcome);
            Assert.AreEqual(partitionId, result.PartitionId);

            await WaitUntilAsync(
                () => applied.Any(x => x.node == followerName && x.data == "schema-change-forwarded"),
                TimeSpan.FromSeconds(10)
            );
        }
        finally
        {
            foreach (EmbeddedKahuna node in nodes)
                await node.DisposeAsync();
        }
    }

    /// <summary>
    /// E1: the ack gate sources live membership from Raft (<c>GetNodes()</c> + local endpoint)
    /// rather than a manual register set. This test verifies that the gate blocks while at
    /// least one Raft member has not yet acked, and unblocks once all members have acked.
    /// </summary>
    [Test]
    public async Task SchemaAckGate_WaitsForAllRaftMembers()
    {
        string db = $"db_{Guid.NewGuid():N}";

        int portBase = Interlocked.Add(ref nextPortBase, 10);
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNode = new();

        EmbeddedKahuna node1 = CreateClusterNode("ack-node1", 1, portBase + 1, [new($"localhost:{portBase + 2}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node2 = CreateClusterNode("ack-node2", 2, portBase + 2, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 3}")], raftCommunication, interNode);
        EmbeddedKahuna node3 = CreateClusterNode("ack-node3", 3, portBase + 3, [new($"localhost:{portBase + 1}"), new($"localhost:{portBase + 2}")], raftCommunication, interNode);
        EmbeddedKahuna[] nodes = [node1, node2, node3];

        try
        {
            raftCommunication.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Raft));
            interNode.SetNodes(nodes.ToDictionary(x => x.Raft.GetLocalEndpoint(), x => x.Kahuna));

            foreach (EmbeddedKahuna node in nodes)
                await node.Raft.UpdateNodes();

            await Task.WhenAll(nodes.Select(x => x.StartAsync(CancellationToken.None)))
                .WaitAsync(TimeSpan.FromSeconds(10));

            int partitionId = node1.SchemaLogPartition(db);
            await node1.Raft.WaitForLeader(partitionId, CancellationToken.None);
            EmbeddedKahuna leader = await WaitForLeaderNode(nodes, partitionId);

            // Wait until the leader's Raft membership view includes all peers. WaitForLeader
            // only guarantees a term exists; GetNodes() may still return 0 peers if the
            // membership exchange hasn't completed. Recording the partial ack before all
            // peers are visible would cause WaitForSchemaAcksAsync to see 1/1 and pass early.
            await WaitForFullMembershipAsync(leader, nodes.Length - 1);

            // Seed all nodes at version 0, simulating RecordAndPublishSchemaApplied.
            // Each node records locally; followers also relay to the leader's per-instance
            // tracker via RecordRemoteSchemaAck so the gate knows about them (without the
            // relay the leader's tracker would have no record for followers and the
            // "hasn't-opened" fast-path would silently skip them, allowing the gate to pass
            // with only 1/N acks, which is what the old shared-static tracker masked).
            foreach (EmbeddedKahuna node in nodes)
            {
                node.RecordLocalSchemaApplied(db, 0);
                if (node != leader)
                    leader.RecordRemoteSchemaAck(db, node.Raft.GetLocalEndpoint(), 0);
            }

            // Only the leader acks version 1 — gate must block while the 2 followers are still at 0.
            leader.RecordLocalSchemaApplied(db, 1);

            bool partialAck = await leader.WaitForSchemaAcksAsync(
                db, 1, TimeSpan.FromMilliseconds(100), TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.IsFalse(partialAck, "Gate must block until all Raft members ack");

            // Deliver follower acks to the leader's per-instance tracker via RecordRemoteSchemaAck
            // (the path that RecordAndPublishSchemaApplied + InProcessSchemaAckRelay would take in
            // the full fixture). The leader records its own ack; followers relay to the leader.
            foreach (EmbeddedKahuna node in nodes)
            {
                if (node == leader)
                    continue;
                leader.RecordRemoteSchemaAck(db, node.Raft.GetLocalEndpoint(), 1);
            }

            bool allAcked = await leader.WaitForSchemaAcksAsync(
                db, 1, TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.IsTrue(allAcked, "Gate must pass once all Raft members ack");
        }
        finally
        {
            foreach (EmbeddedKahuna node in nodes)
                await node.DisposeAsync();
        }
    }

    private static EmbeddedKahuna CreateClusterNode(
        string nodeName,
        int nodeId,
        int port,
        List<RaftNode> peers,
        InMemoryCommunication raftCommunication,
        MemoryInterNodeCommmunication interNode)
    {
        return new(
            new EmbeddedKahunaOptions
            {
                NodeName = nodeName,
                NodeId = nodeId,
                Host = "localhost",
                Port = port,
                Storage = "memory",
                WalStorage = "memory",
                InitialPartitions = 3
            },
            interNode,
            raftCommunication,
            new StaticDiscovery(peers)
        );
    }

    private static async Task<EmbeddedKahuna> WaitForLeaderNode(EmbeddedKahuna[] nodes, int partitionId)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);

        while (DateTime.UtcNow < deadline)
        {
            foreach (EmbeddedKahuna node in nodes)
            {
                if (await node.Raft.AmILeaderQuick(partitionId))
                    return node;
            }

            await Task.Delay(25);
        }

        throw new AssertionException($"No leader found for partition {partitionId}");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(25);
        }

        Assert.Fail("Timed out waiting for condition");
    }

    /// <summary>
    /// Polls until <paramref name="leader"/>'s Raft membership view contains at least
    /// <paramref name="expectedPeerCount"/> peers. Must be called after WaitForLeader so that
    /// WaitForSchemaAcksAsync sources the full cluster membership rather than just the local node.
    /// </summary>
    private static async Task WaitForFullMembershipAsync(EmbeddedKahuna leader, int expectedPeerCount)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);

        while (DateTime.UtcNow < deadline)
        {
            if (leader.Raft.GetNodes().Count >= expectedPeerCount)
                return;

            await Task.Delay(25);
        }

        Assert.Fail($"Raft membership did not reach {expectedPeerCount} peers within 5 s");
    }

    private sealed class InMemorySchemaReplicationForwarder : ISchemaReplicationForwarder
    {
        private readonly Dictionary<string, EmbeddedKahuna> nodes;

        public InMemorySchemaReplicationForwarder(IEnumerable<EmbeddedKahuna> nodes)
        {
            this.nodes = new();

            foreach (EmbeddedKahuna node in nodes)
            {
                this.nodes[node.Raft.GetLocalNodeName()] = node;
                this.nodes[node.Raft.GetLocalEndpoint()] = node;
            }
        }

        public Task<SchemaReplicationResult?> ForwardSchemaChangeAsync(
            string leader,
            string db,
            byte[] entry,
            CancellationToken cancellationToken
        )
        {
            return nodes.TryGetValue(leader, out EmbeddedKahuna? node)
                ? ForwardToNode(node, db, entry, cancellationToken)
                : Task.FromResult<SchemaReplicationResult?>(null);
        }

        private static async Task<SchemaReplicationResult?> ForwardToNode(
            EmbeddedKahuna node,
            string db,
            byte[] entry,
            CancellationToken cancellationToken
        )
        {
            return await node.ReplicateSchemaChangeAsync(db, entry, cancellationToken);
        }
    }
}
