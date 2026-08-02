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

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Test fixture for distributed schema tests that need distinct CamusDB/Kahuna nodes
/// sharing one in-process Raft transport.
/// </summary>
public sealed class InProcessSchemaCluster : IAsyncDisposable
{
    private static int nextPortBase = 10000;

    private readonly FaultInjectingCommunication faultComm;

    private InProcessSchemaCluster(Node[] nodes, FaultInjectingCommunication faultComm)
    {
        Nodes = nodes;
        this.faultComm = faultComm;
    }

    public Node[] Nodes { get; }

    public static async Task<InProcessSchemaCluster> StartAsync(
        int nodeCount = 3,
        int partitions = 1,
        ILoggerFactory? loggerFactory = null,
        ILogger<ICamusDB>? logger = null,
        bool wireLeaderForwarder = false,
        CamusDBOptions? options = null
    )
    {
        // Every node in the cluster is built with the same configuration, fixed when the cluster
        // starts. A test wanting different settings starts its own cluster.
        CamusDBOptions effective = options ?? CamusDBOptions.Default;

        if (nodeCount < 1)
            throw new ArgumentOutOfRangeException(nameof(nodeCount), "Node count must be positive");

        loggerFactory ??= LoggerFactory.Create(builder => builder.AddFilter("Camus", LogLevel.Warning).AddConsole());
        logger ??= loggerFactory.CreateLogger<ICamusDB>();

        // Cluster bring-up (election + partition warmup) occasionally stalls under accumulated
        // in-process load across many sequential cluster tests. Retry the whole bring-up with a
        // fresh transport/ports rather than letting a transient election stall fail the test;
        // the previous attempt's nodes are fully disposed first so they stop competing for the
        // thread pool.
        const int maxStartAttempts = 3;

        for (int attempt = 1; ; attempt++)
        {
            FaultInjectingCommunication faultComm = new();
            MemoryInterNodeCommmunication interNode = new();

            int portBase = Interlocked.Add(ref nextPortBase, nodeCount + 10);
            string clusterId = Guid.NewGuid().ToString("N");

            EmbeddedKahuna[] kahunaNodes = new EmbeddedKahuna[nodeCount];
            for (int i = 0; i < nodeCount; i++)
            {
                int port = portBase + i + 1;
                List<RaftNode> peers = [];

                for (int peer = 0; peer < nodeCount; peer++)
                {
                    if (peer == i)
                        continue;

                    peers.Add(new($"localhost:{portBase + peer + 1}"));
                }

                kahunaNodes[i] = CreateClusterNode(
                    nodeName: $"schema-{clusterId}-{i + 1}",
                    nodeId: i + 1,
                    port: port,
                    peers: peers,
                    partitions: partitions,
                    raftCommunication: faultComm,
                    interNode: interNode,
                    loggerFactory: loggerFactory
                );
            }

            faultComm.SetNodes(kahunaNodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Raft));
            interNode.SetNodes(kahunaNodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Kahuna));

            try
            {
                foreach (EmbeddedKahuna kahuna in kahunaNodes)
                    await kahuna.Raft.UpdateNodes().ConfigureAwait(false);

                await Task.WhenAll(kahunaNodes.Select(node => node.StartAsync(CancellationToken.None)))
                    .WaitAsync(TimeSpan.FromSeconds(30))
                    .ConfigureAwait(false);

                await WaitForAllPartitionLeadersAsync(kahunaNodes[0], partitions).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < maxStartAttempts && ex is TimeoutException or AssertionException)
            {
                TestContext.Progress.WriteLine(
                    $"Cluster bring-up attempt {attempt} failed ({ex.GetType().Name}); disposing and retrying");
                await DisposeKahunaNodesAsync(kahunaNodes).ConfigureAwait(false);
                await Task.Delay(500 * attempt).ConfigureAwait(false);
                continue;
            }

            // DS10.5: opt-in forwarder that routes a follower's DDL ticket to the current schema
            // leader node. Shared across nodes (it resolves the leader dynamically); only
            // followers invoke it (the leader handles DDL locally). Off by default so the rest of
            // the suite keeps the "follower DDL throws leader-required" behaviour that
            // RunOnSchemaLeaderAsync relies on.
            ClusterLeaderForwarder? forwarder = wireLeaderForwarder ? new ClusterLeaderForwarder() : null;

            // E3: in-process ack relay — routes each follower's RecordAndPublishSchemaApplied
            // notification to the current leader's RecordRemoteSchemaAck, replacing the
            // co-location side-effect of the old static SchemaAckTracker. Every node gets the
            // relay so the leader's per-instance tracker receives real follower acks.
            InProcessSchemaAckRelay ackRelay = new(kahunaNodes);
            foreach (EmbeddedKahuna kahuna in kahunaNodes)
                kahuna.SetSchemaAckSender(ackRelay);

            Node[] nodes = kahunaNodes
                .Select((kahuna, index) =>
                {
                    CommandValidator validator = new(effective);
                    CatalogsManager catalogs = new(logger);
                    CommandExecutor executor = new(
                        validator,
                        catalogs,
                        logger, effective,
                        sharedNode: kahuna,
                        schemaDdlForwarder: forwarder,
                        isClusterMode: true
                    );

                    return new Node(index, kahuna, executor);
                })
                .ToArray();

            InProcessSchemaCluster cluster = new(nodes, faultComm);
            if (forwarder is not null)
                forwarder.Cluster = cluster;
            return cluster;
        }
    }

    // E3: in-process equivalent of the HTTP schema-ack transport. Delivers a follower's applied
    // version directly to the target leader node's RecordRemoteSchemaAck, replacing the
    // co-location side effect of the old static SchemaAckTracker. Synchronous delivery ensures
    // the leader's tracker is updated before the next WaitForSchemaAcksAsync poll fires.
    internal sealed class InProcessSchemaAckRelay : ISchemaAckSender
    {
        private readonly Dictionary<string, EmbeddedKahuna> nodeMap;

        public InProcessSchemaAckRelay(EmbeddedKahuna[] nodes)
        {
            nodeMap = nodes.ToDictionary(n => n.Raft.GetLocalEndpoint(), StringComparer.Ordinal);
        }

        public Task SendSchemaAckAsync(
            string leaderEndpoint,
            string database,
            string nodeEndpoint,
            long schemaVersion,
            CancellationToken cancellationToken)
        {
            if (nodeMap.TryGetValue(leaderEndpoint, out EmbeddedKahuna? leader))
                leader.RecordRemoteSchemaAck(database, nodeEndpoint, schemaVersion);
            return Task.CompletedTask;
        }
    }

    // DS10.5: in-process equivalent of HttpSchemaDdlForwarder — re-runs a forwarded DDL ticket on
    // the current schema-leader node's executor (the normal leader replicated path) and returns
    // its applied result. No HTTP, no op-id dedup (tests don't retry); the leader executor's own
    // AmISchemaLeader check prevents any re-forward loop.
    private sealed class ClusterLeaderForwarder : ISchemaDdlForwarder
    {
        public InProcessSchemaCluster? Cluster;

        private async Task<Node> LeaderAsync(string db)
            => await Cluster!.WaitForSchemaLeaderNodeAsync(db).ConfigureAwait(false);

        public async Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken ct)
            => (await (await LeaderAsync(ticket.DatabaseName)).Executor.CreateTable(ticket).ConfigureAwait(false)).Success;

        public async Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken ct)
            => await (await LeaderAsync(ticket.DatabaseName)).Executor.AlterTable(ticket).ConfigureAwait(false);

        public async Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken ct)
            => await (await LeaderAsync(ticket.DatabaseName)).Executor.AlterIndex(ticket).ConfigureAwait(false);

        public async Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken ct)
            => await (await LeaderAsync(ticket.DatabaseName)).Executor.DropTable(ticket).ConfigureAwait(false);

        public async Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken ct)
            => await (await LeaderAsync(ticket.DatabaseName)).Executor.RelinkTable(ticket).ConfigureAwait(false);

        public async Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken ct)
            => await (await LeaderAsync(ticket.DatabaseName)).Executor.RenameTable(ticket).ConfigureAwait(false);

        public async Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken ct)
            => (await (await LeaderAsync(ticket.DatabaseName)).Executor.AlterConstraint(ticket).ConfigureAwait(false)).Success;

        public async Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken ct)
            => (await (await LeaderAsync(ticket.DatabaseName)).Executor.Comment(ticket).ConfigureAwait(false)).Success;
    }

    public string NextSchemaLogDatabaseName()
    {
        for (int i = 0; i < 100; i++)
        {
            string db = $"db_{Guid.NewGuid():N}";
            try
            {
                _ = Nodes[0].Kahuna.SchemaLogPartition(db);
                return db;
            }
            catch (CamusDBException)
            {
            }
        }

        throw new AssertionException("Could not generate a database name whose schema log partition is not reserved");
    }

    public async Task OpenDatabaseOnAllNodesAsync(string databaseName)
    {
        // Every Open requires the database to exist in the registry. Schema-log test names
        // (NextSchemaLogDatabaseName) are never registered via CREATE
        // DATABASE, so we register them here via CreateDatabase(ifNotExists: true) on node 0;
        // the registry entry is replicated through the shared Kahuna KV, so the subsequent
        // Open on every node resolves it via TryResolveIdAsync.
        await Nodes[0].Executor.CreateDatabase(new CreateDatabaseTicket(databaseName, ifNotExists: true))
            .ConfigureAwait(false);

        foreach (Node node in Nodes)
        {
            node.Database = await node.Executor.OpenDatabase(databaseName).ConfigureAwait(false);
        }
    }

    public async Task<Node> WaitForSchemaLeaderNodeAsync(string databaseName, TimeSpan? timeout = null)
    {
        int partitionId = Nodes[0].Kahuna.SchemaLogPartition(databaseName);
        await Nodes[0].Kahuna.Raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);

        // Best-effort settle: let the schema-partition leader hold continuously before we resolve
        // it, so the caller does not race a still-churning election (the common cluster-load flake).
        // If it can't stabilise quickly, fall through to the polling loop rather than failing here.
        try
        {
            await Nodes[0].Kahuna.Raft.WaitForLeaderStableAsync(partitionId, TimeSpan.FromMilliseconds(300), CancellationToken.None)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(5))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Could not confirm stability in time; the polling loop below still resolves a leader.
        }

        DateTime deadline = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromSeconds(15));

        while (DateTime.UtcNow < deadline)
        {
            foreach (Node node in Nodes)
            {
                if (await node.Kahuna.AmISchemaLeaderAsync(databaseName, CancellationToken.None).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new AssertionException($"No schema leader found for partition {partitionId}");
    }

    public async Task WaitForSchemaConvergenceAsync(
        string databaseName,
        long version,
        TimeSpan? timeout = null
    )
    {
        // Acks are keyed by database.Id (the opaque UUID), not the user-facing name.
        // Use the Id when available so WaitForSchemaAcksAsync matches the
        // tokens that SchemaReplicator publishes via RecordAndPublishSchemaApplied.
        string dbKey = Nodes[0].Database?.Id ?? databaseName;

        TimeSpan waitTimeout = timeout ?? TimeSpan.FromSeconds(10);
        bool acked = await Nodes[0].Kahuna.WaitForSchemaAcksAsync(
            dbKey,
            version,
            waitTimeout,
            liveNodeLease: Timeout.InfiniteTimeSpan,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (acked)
            return;

        string versions = string.Join(", ", Nodes.Select(node =>
            $"{node.Kahuna.Raft.GetLocalNodeName()}={node.Database?.Schema.SchemaVersion.ToString() ?? "<closed>"}"));

        throw new AssertionException(
            $"Timed out waiting for database '{databaseName}' schema convergence to version {version}. Versions: {versions}"
        );
    }

    /// <summary>
    /// Runs a DDL action against the current schema leader, re-resolving the leader and
    /// retrying if leadership moves between resolution and execution (Raft can re-elect at any
    /// time, and without a production forwarder a non-leader rejects DDL). This keeps the
    /// cluster tests deterministic without depending on leadership never changing.
    /// </summary>
    public async Task RunOnSchemaLeaderAsync(
        string databaseName,
        Func<Node, Task> action,
        int maxAttempts = 5,
        TimeSpan? leaderTimeout = null
    )
    {
        for (int attempt = 1; ; attempt++)
        {
            Node leader = await WaitForSchemaLeaderNodeAsync(databaseName, leaderTimeout).ConfigureAwait(false);
            try
            {
                await action(leader).ConfigureAwait(false);
                return;
            }
            catch (CamusDBException ex) when (attempt < maxAttempts && IsLeadershipMoved(ex))
            {
                // Leadership changed under us; back off briefly and re-resolve the leader.
                await Task.Delay(150 * attempt).ConfigureAwait(false);
            }
        }
    }

    // Matches the message thrown by CommandExecutor.TryForwardDdlAsync when a non-leader is
    // asked to run DDL and no production forwarder is wired (the cluster-fixture case).
    private static bool IsLeadershipMoved(CamusDBException ex)
        => ex.Message.Contains("must be executed by schema leader", StringComparison.Ordinal);

    // ── Cluster helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Closes then reopens the database on the given node. Useful after a node has been
    /// isolated and later reconnected: <c>LoadMetaAsync</c> reads the schema checkpoint via
    /// <c>LocateAndTryGetValue</c>, which routes through the (unblocked)
    /// <see cref="MemoryInterNodeCommmunication"/> to the KV-partition leader and returns the
    /// latest committed schema — so the node catches up without a Raft WAL replay.
    /// </summary>
    public async Task ReopenDatabaseAsync(int nodeIndex, string databaseName)
    {
        ValidateNodeIndex(nodeIndex);
        Node node = Nodes[nodeIndex];

        await node.Executor.CloseDatabase(new CloseDatabaseTicket(databaseName)).ConfigureAwait(false);

        node.Database = await node.Executor.OpenDatabase(databaseName).ConfigureAwait(false);
    }

    // ── A3: Fault-injection transport hooks ──────────────────────────────────

    /// <summary>
    /// Fully isolates the given node at the Raft transport layer — blocks both its inbound
    /// and outbound messages. The node stops receiving AppendLogs so it lags behind the cluster,
    /// and it cannot send RequestVotes so it cannot disrupt the current leader with spurious
    /// elections. The remaining nodes continue to commit with quorum.
    /// </summary>
    public void PauseDelivery(int nodeIndex)
    {
        ValidateNodeIndex(nodeIndex);
        string endpoint = Nodes[nodeIndex].Kahuna.Raft.GetLocalEndpoint();
        faultComm.BlockSender(endpoint);
        faultComm.BlockRecipient(endpoint);
    }

    /// <summary>
    /// Re-connects the previously paused node to the Raft transport. Kommander will
    /// resume delivering entries to it and replay any missed ones via OnReplicationReceived.
    /// </summary>
    public void ResumeDelivery(int nodeIndex)
    {
        ValidateNodeIndex(nodeIndex);
        string endpoint = Nodes[nodeIndex].Kahuna.Raft.GetLocalEndpoint();
        faultComm.UnblockSender(endpoint);
        faultComm.UnblockRecipient(endpoint);
    }

    /// <summary>
    /// Kills the node at <paramref name="nodeIndex"/>: blocks all inbound AND outbound
    /// messages for it at the transport layer, then disposes its Kahuna instance.
    /// The remaining nodes retain quorum for a 3-node cluster.
    /// </summary>
    public async Task KillNodeAsync(int nodeIndex)
    {
        ValidateNodeIndex(nodeIndex);
        string endpoint = Nodes[nodeIndex].Kahuna.Raft.GetLocalEndpoint();

        // Block transport before disposal so in-flight messages are not forwarded.
        faultComm.BlockSender(endpoint);
        faultComm.BlockRecipient(endpoint);

        try
        {
            await Nodes[nodeIndex].Kahuna.DisposeAsync().AsTask()
                .WaitAsync(TimeSpan.FromSeconds(5))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            TestContext.Progress.WriteLine($"KillNodeAsync timed out disposing {endpoint}");
        }
    }

    /// <summary>
    /// Transfers schema-log leadership for <paramref name="databaseName"/> to
    /// <paramref name="targetNode"/> and waits until that node has actually become the
    /// schema leader.  Uses Kommander's <c>TransferLeadershipAsync</c> so the current leader
    /// actively hands off rather than the target campaigning via a forced election.
    /// All nodes remain live; no transport blocking.
    /// </summary>
    public async Task TransferSchemaLeadershipAsync(
        string databaseName,
        Node targetNode,
        TimeSpan? timeout = null)
    {
        TimeSpan searchTimeout = timeout ?? TimeSpan.FromSeconds(15);

        Node currentLeader = await WaitForSchemaLeaderNodeAsync(databaseName, searchTimeout).ConfigureAwait(false);
        string targetEndpoint = targetNode.Kahuna.Raft.GetLocalEndpoint();

        if (currentLeader.Index == targetNode.Index)
            return; // already there

        await currentLeader.Kahuna.TransferSchemaLeadershipAsync(databaseName, targetEndpoint, CancellationToken.None)
            .ConfigureAwait(false);

        // Wait until the target node has won the transfer.
        DateTime deadline = DateTime.UtcNow.Add(searchTimeout);
        while (DateTime.UtcNow < deadline)
        {
            using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(200));
            try
            {
                if (await targetNode.Kahuna.AmISchemaLeaderAsync(databaseName, cts.Token).ConfigureAwait(false))
                    return;
            }
            catch (OperationCanceledException) { }

            await Task.Delay(50).ConfigureAwait(false);
        }

        throw new AssertionException(
            $"Node {targetNode.Index} did not become schema leader for '{databaseName}' within {searchTimeout.TotalSeconds}s");
    }

    /// <summary>
    /// Forces a leadership change on the schema partition for <paramref name="databaseName"/> by
    /// blocking all outbound messages from the current leader so followers time out and elect a
    /// new one. Returns the new leader node. The old leader's outbound remains blocked until the
    /// caller explicitly calls <see cref="ResumeDelivery"/> or <see cref="KillNodeAsync"/>.
    /// </summary>
    public async Task<Node> ForceLeaderChangeAsync(string databaseName, TimeSpan? timeout = null)
    {
        TimeSpan searchTimeout = timeout ?? TimeSpan.FromSeconds(15);
        Node currentLeader = await WaitForSchemaLeaderNodeAsync(databaseName, searchTimeout).ConfigureAwait(false);
        string blockedEndpoint = currentLeader.Kahuna.Raft.GetLocalEndpoint();

        // Block outbound from the current leader so followers stop receiving heartbeats and
        // trigger a new election. The inbound is also blocked so the old leader cannot rejoin
        // as leader via stale heartbeats once unblocked by the caller.
        faultComm.BlockSender(blockedEndpoint);
        faultComm.BlockRecipient(blockedEndpoint);

        DateTime deadline = DateTime.UtcNow.Add(searchTimeout);
        while (DateTime.UtcNow < deadline)
        {
            foreach (Node node in Nodes)
            {
                if (node.Kahuna.Raft.GetLocalEndpoint() == blockedEndpoint)
                    continue;

                using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(200));
                try
                {
                    if (await node.Kahuna.AmISchemaLeaderAsync(databaseName, cts.Token).ConfigureAwait(false))
                        return node;
                }
                catch (OperationCanceledException) { }
            }

            await Task.Delay(100).ConfigureAwait(false);
        }

        throw new AssertionException(
            $"No new schema leader elected for '{databaseName}' after {searchTimeout.TotalSeconds}s. " +
            $"Blocked endpoint: {blockedEndpoint}"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        // Dispose executors first (they may hold open database descriptors), then the Kahuna
        // nodes. Both phases run in PARALLEL across nodes and use a generous budget: each
        // EmbeddedKahunaNode.DisposeAsync internally runs GracefulShutdownAll(5s) + LeaveCluster,
        // so a 5s cap here would abandon a still-draining actor system and leave zombie Raft
        // loops that starve the thread pool and slow the NEXT test's leader election (the root
        // cause of the suite-only startup-timeout flakes). 20s lets shutdown actually finish.
        await Task.WhenAll(Nodes.Select(DisposeExecutorAsync)).ConfigureAwait(false);
        await Task.WhenAll(Nodes.Select(DisposeKahunaAsync)).ConfigureAwait(false);
    }

    private static async Task DisposeExecutorAsync(Node node)
    {
        try
        {
            await node.Executor.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            TestContext.Progress.WriteLine($"executor dispose timed out for {node.Kahuna.Raft.GetLocalNodeName()}");
        }
    }

    private static async Task DisposeKahunaAsync(Node node)
    {
        try
        {
            await node.Kahuna.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            TestContext.Progress.WriteLine($"kahuna dispose timed out for {node.Kahuna.Raft.GetLocalNodeName()}");
        }
    }

    // Disposes raw Kahuna nodes created during a failed bring-up attempt (before executors exist).
    private static async Task DisposeKahunaNodesAsync(EmbeddedKahuna[] nodes)
    {
        await Task.WhenAll(nodes.Where(n => n is not null).Select(async n =>
        {
            try
            {
                await n.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                TestContext.Progress.WriteLine("kahuna dispose timed out during bring-up retry cleanup");
            }
        })).ConfigureAwait(false);
    }

    private static EmbeddedKahuna CreateClusterNode(
        string nodeName,
        int nodeId,
        int port,
        List<RaftNode> peers,
        int partitions,
        FaultInjectingCommunication raftCommunication,
        MemoryInterNodeCommmunication interNode,
        ILoggerFactory loggerFactory
    )
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
                InitialPartitions = partitions,
                // The embedded defaults (500/1500ms) are far too short for an in-process cluster
                // running several Raft groups across nodeCount nodes on the shared .NET thread pool:
                // under that contention a follower routinely misses a heartbeat inside 500ms and
                // starts a spurious election, churning partition leadership. That churn misroutes
                // schema-applied acks (a follower acks the leader it currently sees, which may no
                // longer be the one awaiting acks), so CreateTable's 30s schema-ack convergence wait
                // times out — the dominant cluster-test flake. Production-like timeouts with a wide
                // randomised spread (the increments stagger nodes to avoid split votes) keep
                // leadership stable. Matches/exceeds the real server's 2000/4000.
                StartElectionTimeout = 3000,
                EndElectionTimeout = 6000,
                StartElectionTimeoutIncrement = 200,
                EndElectionTimeoutIncrement = 400
            },
            interNode,
            raftCommunication,
            new StaticDiscovery(peers),
            loggerFactory
        );
    }

    private static async Task WaitForAllPartitionLeadersAsync(EmbeddedKahuna node, int partitions)
    {
        // Stable-leader settle window. WaitForLeaderAsync returns as soon as *an* election
        // produces a leader, but under accumulated in-process load that leader can immediately
        // re-elect — so a test that proceeds right away can race a still-churning partition and
        // time out later in WaitForSchemaLeaderNodeAsync. Gating bring-up on a continuously-stable
        // leader (Kommander WaitForLeaderStableAsync) lets the cluster actually settle first. A
        // partition that cannot stabilise inside the timeout throws, which the bring-up loop
        // retries on fresh ports (maxStartAttempts).
        TimeSpan minStableFor = TimeSpan.FromMilliseconds(500);

        HashSet<int> seenPartitions = [];

        for (int i = 0; seenPartitions.Count < partitions && i < 200; i++)
        {
            string key = $"{i}/warmup";
            int partition = node.Raft.GetPartitionKey(key);
            if (!seenPartitions.Add(partition))
                continue;

            await node.WaitForLeaderAsync(key, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(15))
                .ConfigureAwait(false);

            await node.Raft.WaitForLeaderStableAsync(partition, minStableFor, CancellationToken.None)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(15))
                .ConfigureAwait(false);
        }

        if (seenPartitions.Count < partitions)
            throw new AssertionException($"Failed to find all {partitions} partitions after 200 attempts");
    }

    private void ValidateNodeIndex(int nodeIndex)
    {
        if ((uint)nodeIndex >= (uint)Nodes.Length)
            throw new ArgumentOutOfRangeException(nameof(nodeIndex), $"Node index {nodeIndex} is out of range [0, {Nodes.Length})");
    }

    public sealed class Node
    {
        internal Node(int index, EmbeddedKahuna kahuna, CommandExecutor executor)
        {
            Index = index;
            Kahuna = kahuna;
            Executor = executor;
        }

        public int Index { get; }

        public EmbeddedKahuna Kahuna { get; }

        public CommandExecutor Executor { get; }

        public DatabaseDescriptor? Database { get; internal set; }
    }

    /// <summary>
    /// Wraps <see cref="InMemoryCommunication"/> with per-endpoint delivery filters.
    /// Blocking a recipient drops all Raft messages sent TO that endpoint.
    /// Blocking a sender drops all Raft messages sent FROM that endpoint.
    /// Used by the A3 fault-injection methods on <see cref="InProcessSchemaCluster"/>.
    /// </summary>
    internal sealed class FaultInjectingCommunication : ICommunication
    {
        private readonly InMemoryCommunication inner = new();

        private readonly ConcurrentDictionary<string, bool> blockedRecipients = new();
        private readonly ConcurrentDictionary<string, bool> blockedSenders = new();

        private static readonly Task<HandshakeResponse> emptyHandshake = Task.FromResult(new HandshakeResponse());
        private static readonly Task<RequestVotesResponse> emptyVotes = Task.FromResult(new RequestVotesResponse());
        private static readonly Task<VoteResponse> emptyVote = Task.FromResult(new VoteResponse());
        private static readonly Task<AppendLogsResponse> emptyAppend = Task.FromResult(new AppendLogsResponse());
        private static readonly Task<CompleteAppendLogsResponse> emptyComplete = Task.FromResult(new CompleteAppendLogsResponse());

        public void SetNodes(Dictionary<string, IRaft> nodes) => inner.SetNodes(nodes);

        public void BlockRecipient(string endpoint) => blockedRecipients[endpoint] = true;
        public void UnblockRecipient(string endpoint) => blockedRecipients.TryRemove(endpoint, out _);
        public void BlockSender(string endpoint) => blockedSenders[endpoint] = true;
        public void UnblockSender(string endpoint) => blockedSenders.TryRemove(endpoint, out _);

        private bool IsBlocked(RaftManager manager, RaftNode targetNode)
        {
            if (blockedSenders.ContainsKey(manager.GetLocalEndpoint())) return true;
            if (blockedRecipients.ContainsKey(targetNode.Endpoint)) return true;
            return false;
        }

        public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
        {
            if (IsBlocked(manager, node)) return emptyHandshake;
            return inner.Handshake(manager, node, request);
        }

        public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
        {
            if (IsBlocked(manager, node)) return emptyVotes;
            return inner.RequestVotes(manager, node, request);
        }

        public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
        {
            if (IsBlocked(manager, node)) return emptyVote;
            return inner.Vote(manager, node, request);
        }

        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
        {
            if (IsBlocked(manager, node)) return emptyAppend;
            return inner.AppendLogs(manager, node, request);
        }

        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
        {
            if (IsBlocked(manager, node)) return emptyComplete;
            return inner.CompleteAppendLogs(manager, node, request);
        }

        public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new BatchRequestsResponse());
            return inner.BatchRequests(manager, node, request);
        }

        public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new JoinResponse(false));
            return inner.SendJoin(manager, node, request);
        }

        public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new LeaveResponse(false));
            return inner.SendLeave(manager, node, request, cancellationToken);
        }
    }
}
