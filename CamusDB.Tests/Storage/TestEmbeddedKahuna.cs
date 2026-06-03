
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
[NonParallelizable]
public sealed class TestEmbeddedKahuna
{
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

        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNode = new();

        EmbeddedKahuna node1 = CreateClusterNode("node1", 1, 9101, [new("localhost:9102"), new("localhost:9103")], raftCommunication, interNode);
        EmbeddedKahuna node2 = CreateClusterNode("node2", 2, 9102, [new("localhost:9101"), new("localhost:9103")], raftCommunication, interNode);
        EmbeddedKahuna node3 = CreateClusterNode("node3", 3, 9103, [new("localhost:9101"), new("localhost:9102")], raftCommunication, interNode);
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

            await WaitUntilAsync(() => applied.Count > 0, TimeSpan.FromSeconds(5));

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

        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNode = new();

        EmbeddedKahuna node1 = CreateClusterNode("node1", 1, 9201, [new("localhost:9202"), new("localhost:9203")], raftCommunication, interNode);
        EmbeddedKahuna node2 = CreateClusterNode("node2", 2, 9202, [new("localhost:9201"), new("localhost:9203")], raftCommunication, interNode);
        EmbeddedKahuna node3 = CreateClusterNode("node3", 3, 9203, [new("localhost:9201"), new("localhost:9202")], raftCommunication, interNode);
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
                TimeSpan.FromSeconds(5)
            );
        }
        finally
        {
            foreach (EmbeddedKahuna node in nodes)
                await node.DisposeAsync();
        }
    }

    [Test]
    public async Task SchemaAckGate_WaitsForEveryLiveNode()
    {
        string db = $"db_{Guid.NewGuid():N}";

        await using EmbeddedKahuna node1 = CreateUnstartedNode("ack-node1", 9301);
        await using EmbeddedKahuna node2 = CreateUnstartedNode("ack-node2", 9302);

        node1.RegisterLocalSchemaAckNode(db);
        node2.RegisterLocalSchemaAckNode(db);

        node1.RecordLocalSchemaApplied(db, 1);

        bool beforeFollowerAck = await node1.WaitForSchemaAcksAsync(
            db,
            1,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        Assert.IsFalse(beforeFollowerAck);

        node2.RecordLocalSchemaApplied(db, 1);

        bool afterFollowerAck = await node1.WaitForSchemaAcksAsync(
            db,
            1,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        Assert.IsTrue(afterFollowerAck);
    }

    [Test]
    public async Task SchemaAckGate_ExpiredNodeDoesNotBlockProgress()
    {
        string db = $"db_{Guid.NewGuid():N}";

        await using EmbeddedKahuna node1 = CreateUnstartedNode("lease-node1", 9401);
        await using EmbeddedKahuna node2 = CreateUnstartedNode("lease-node2", 9402);

        node1.RegisterLocalSchemaAckNode(db);
        node2.RegisterLocalSchemaAckNode(db);
        node1.RecordLocalSchemaApplied(db, 2);

        await Task.Delay(50);

        bool acked = await node1.WaitForSchemaAcksAsync(
            db,
            2,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(10),
            CancellationToken.None
        );

        Assert.IsTrue(acked);
    }

    [Test]
    public async Task SchemaAckGate_UnregisteringLastNodeDropsDatabaseState()
    {
        string db = $"db_{Guid.NewGuid():N}";

        await using EmbeddedKahuna node = CreateUnstartedNode("cleanup-node", 9501);

        node.RegisterLocalSchemaAckNode(db);
        node.RecordLocalSchemaApplied(db, 3);
        node.UnregisterLocalSchemaAckNode(db);

        node.RegisterLocalSchemaAckNode(db);

        bool inheritedOldAck = await node.WaitForSchemaAcksAsync(
            db,
            3,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        Assert.IsFalse(inheritedOldAck);
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

    private static EmbeddedKahuna CreateUnstartedNode(string nodeName, int port)
    {
        return new(
            new EmbeddedKahunaOptions
            {
                NodeName = nodeName,
                NodeId = port,
                Host = "localhost",
                Port = port,
                Storage = "memory",
                WalStorage = "memory",
                InitialPartitions = 1
            }
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
