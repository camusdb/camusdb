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

    /// <summary>The in-process fragment channel shared by every node; set by StartAsync.</summary>
    internal RecordingFragmentTransport? FragmentTransport { get; set; }

    public static async Task<InProcessSchemaCluster> StartAsync(
        int nodeCount = 3,
        int partitions = 1,
        ILoggerFactory? loggerFactory = null,
        ILogger<ICamusDB>? logger = null,
        bool wireLeaderForwarder = false,
        CamusDBOptions? options = null,
        Action<EmbeddedKahunaOptions>? configureNode = null
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
                    loggerFactory: loggerFactory,
                    configureNode: configureNode
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

                await WaitForAllPartitionLeadersAsync(kahunaNodes, partitions).ConfigureAwait(false);
            }
            // A RaftException from StartAsync is the embedded node's own "leader not decided in time"
            // during bring-up: the same transient election stall as the timeout, surfaced by Kahuna
            // rather than by the harness wait, and more likely with more nodes and partitions.
            catch (Exception ex) when (attempt < maxStartAttempts && ex is TimeoutException or AssertionException or RaftException)
            {
                TestContext.Progress.WriteLine(
                    $"Cluster bring-up attempt {attempt} failed ({ex.GetType().Name}); disposing and retrying");
                await DisposeKahunaNodesAsync(kahunaNodes).ConfigureAwait(false);
                await Task.Delay(500 * attempt).ConfigureAwait(false);
                continue;
            }

            // The shipped in-process transports address a peer by its Raft endpoint, so they need
            // the address book of this cluster. It is filled below, once each node has an executor;
            // nothing resolves through it before the first statement runs.
            InProcessClusterNodes registry = new();

            // Assigned at the end of this attempt, and read only when a forwarded ticket or an ack
            // resolves a leader — long after the assignment.
            InProcessSchemaCluster? created = null;

            // The shipped forwarder trusts the leader endpoint its caller resolved. This harness
            // waits for a settled schema leader instead: a cluster under test load re-elects often
            // enough that the caller's endpoint can already be stale, and the tests expect the
            // ticket to reach whoever leads now.
            InProcessSchemaDdlForwarder schemaTransport = new(
                registry,
                async (_, databaseName, _) => (await created!.WaitForSchemaLeaderNodeAsync(databaseName).ConfigureAwait(false)).Executor
            );

            // Routes each follower's RecordAndPublishSchemaApplied notification to the current
            // leader's RecordRemoteSchemaAck, replacing the co-location side-effect of the old
            // static SchemaAckTracker. Every node gets it so the leader's per-instance tracker
            // receives real follower acks.
            foreach (EmbeddedKahuna kahuna in kahunaNodes)
                kahuna.SetSchemaAckForwarder(schemaTransport);

            // DDL forwarding itself is opt-in: off by default so the rest of the suite keeps the
            // "follower DDL throws leader-required" behaviour that RunOnSchemaLeaderAsync relies on.
            InProcessSchemaDdlForwarder? forwarder = wireLeaderForwarder ? schemaTransport : null;

            // In-process fragment channel: every node can execute span fragments on its peers.
            // Wired unconditionally — it only engages when a plan actually fragments (the
            // distribution flag is off in most fixtures, so this is inert for them).
            RecordingFragmentTransport fragmentTransport = new(new InProcessQueryFragmentTransport(registry));

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
                        isClusterMode: true,
                        fragmentTransport: fragmentTransport
                    );

                    registry.Register(kahuna, executor);

                    return new Node(index, kahuna, executor);
                })
                .ToArray();

            InProcessSchemaCluster cluster = new(nodes, faultComm);
            cluster.FragmentTransport = fragmentTransport;
            created = cluster;
            return cluster;
        }
    }

    /// <summary>
    /// Wraps the shipped <see cref="InProcessQueryFragmentTransport"/> with the bookkeeping the
    /// distributed-query tests need: how many fragments ran, how many survivor rows they shipped,
    /// and an injected failure for the coordinator's local-fallback path.
    ///
    /// <para>Only the bookkeeping lives here. The request encoding and the frame round trip
    /// through both wire codecs are the shipped transport's, so the tests exercise the same
    /// serialization the browser playground and the HTTP fragment controller use.</para>
    /// </summary>
    internal sealed class RecordingFragmentTransport : IQueryFragmentTransport
    {
        private readonly IQueryFragmentTransport inner;

        private readonly List<CamusDB.Core.CommandsExecutor.Models.Queries.QueryFragmentRequest> executed = new();

        private int failRemaining;

        private long rowsReturned;

        internal RecordingFragmentTransport(IQueryFragmentTransport inner) => this.inner = inner;

        internal int ExecutedCount { get { lock (executed) return executed.Count; } }

        /// <summary>Executed fragments that were broadcast-join probes (request carried a join spec).</summary>
        internal int ExecutedJoinCount { get { lock (executed) return executed.Count(r => r.Join is not null); } }

        /// <summary>Total survivor rows shipped across all fragment executions since the last reset.</summary>
        internal long RowsReturned => Interlocked.Read(ref rowsReturned);

        internal void ResetExecuted()
        {
            lock (executed) executed.Clear();
            Interlocked.Exchange(ref rowsReturned, 0);
        }

        /// <summary>The next <paramref name="count"/> fragment executions throw before any row.</summary>
        internal void FailNextFragments(int count) => failRemaining = count;

        public async IAsyncEnumerable<CamusDB.Core.CommandsExecutor.Models.Queries.QueryFragmentRow> ExecuteFragmentAsync(
            string targetRaftEndpoint,
            CamusDB.Core.CommandsExecutor.Models.Queries.QueryFragmentRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            lock (executed)
                executed.Add(request);

            if (failRemaining > 0)
            {
                failRemaining--;
                throw new System.IO.IOException("Injected fragment transport failure");
            }

            await foreach (CamusDB.Core.CommandsExecutor.Models.Queries.QueryFragmentRow row in
                inner.ExecuteFragmentAsync(targetRaftEndpoint, request, cancellationToken).ConfigureAwait(false))
            {
                // Terminal stats frames are protocol bookkeeping, not shipped survivors.
                if (row.Stats is null)
                    Interlocked.Increment(ref rowsReturned);

                yield return row;
            }
        }
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

    /// <summary>
    /// The key the engine actually partitions schema state by: the database's opaque Id once
    /// the database is open on any node, else the user-facing name. Every schema-leader check
    /// in this harness must use this — the executor keys <c>AmISchemaLeaderAsync</c> by
    /// <c>database.Id</c>, and the name generally hashes to a different partition. On old
    /// Kommander versions every partition happened to share one leader, which masked
    /// name-keyed lookups; leader balancing spreads partition leaderships and unmasks them.
    /// </summary>
    private string SchemaKeyFor(string databaseName)
    {
        foreach (Node node in Nodes)
        {
            if (node.Database is { } db && string.Equals(db.Name, databaseName, StringComparison.Ordinal))
                return db.Id;
        }

        return databaseName;
    }

    public async Task<Node> WaitForSchemaLeaderNodeAsync(string databaseName, TimeSpan? timeout = null)
    {
        string schemaKey = SchemaKeyFor(databaseName);
        int partitionId = Nodes[0].Kahuna.SchemaLogPartition(schemaKey);

        // Raft answers per-partition questions only on a node that hosts the partition; under
        // replica placement node 0 may not. Ask a replica instead.
        Node hosting = NodeHosting(partitionId);
        await hosting.Kahuna.Raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);

        // Best-effort settle: let the schema-partition leader hold continuously before we resolve
        // it, so the caller does not race a still-churning election (the common cluster-load flake).
        // If it can't stabilise quickly, fall through to the polling loop rather than failing here.
        try
        {
            await hosting.Kahuna.Raft.WaitForLeaderStableAsync(partitionId, TimeSpan.FromMilliseconds(300), CancellationToken.None)
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
                if (await node.Kahuna.AmISchemaLeaderAsync(schemaKey, CancellationToken.None).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new AssertionException($"No schema leader found for partition {partitionId}");
    }

    /// <summary>
    /// Waits until every node that holds <paramref name="databaseName"/> open has applied schema
    /// version <paramref name="version"/>, or a later one.
    ///
    /// <para><b>Reads each node's own schema, never one node's ack tracker.</b> A tracker answers for
    /// the cluster only while its node leads the schema partition, because a follower sends its ack
    /// to the leader alone. On a follower the tracker's live set is the node itself, so a wait there
    /// returns as soon as that one node applied the version and says nothing about the others. On a
    /// node that led until a moment ago the live set still names every peer, but their acks now go to
    /// the new leader, so a wait there cannot complete before the liveness lease runs out. A wait
    /// pinned to one node therefore depended on which node won the election: it failed whenever the
    /// test moved leadership away from that node.</para>
    ///
    /// <para>A node counts as converged once its apply has finished, not merely once the version is
    /// visible. The apply publishes the version, then evicts the changed table's cached descriptor,
    /// all under the schema lock; <see cref="HasAppliedAsync"/> passes through that lock so a caller
    /// that queries the node next cannot open a descriptor built from the previous schema.</para>
    ///
    /// <para>A node that the test isolated or killed never applies anything, so reconnect it before
    /// the wait. A node that does not hold the database open has no schema to wait for and is
    /// skipped.</para>
    /// </summary>
    public async Task WaitForSchemaConvergenceAsync(
        string databaseName,
        long version,
        TimeSpan? timeout = null
    )
    {
        TimeSpan waitTimeout = timeout ?? TimeSpan.FromSeconds(10);
        long deadline = Environment.TickCount64 + (long)waitTimeout.TotalMilliseconds;

        while (true)
        {
            if (await HaveAllOpenNodesAppliedAsync(databaseName, version).ConfigureAwait(false))
                return;

            if (Environment.TickCount64 >= deadline)
                break;

            await Task.Delay(20).ConfigureAwait(false);
        }

        string versions = string.Join(", ", Nodes.Select(node =>
            $"{node.Kahuna.Raft.GetLocalNodeName()}={OpenDescriptor(node, databaseName)?.Schema.SchemaVersion.ToString() ?? "<closed>"}"));

        throw new AssertionException(
            $"Timed out waiting for database '{databaseName}' schema convergence to version {version}. Versions: {versions}"
        );
    }

    private static DatabaseDescriptor? OpenDescriptor(Node node, string databaseName)
        => node.Database is { } database && string.Equals(database.Name, databaseName, StringComparison.Ordinal)
            ? database
            : null;

    private async Task<bool> HaveAllOpenNodesAppliedAsync(string databaseName, long version)
    {
        bool anyOpen = false;

        foreach (Node node in Nodes)
        {
            DatabaseDescriptor? database = OpenDescriptor(node, databaseName);
            if (database is null)
                continue;

            anyOpen = true;

            if (!await HasAppliedAsync(database, version).ConfigureAwait(false))
                return false;
        }

        return anyOpen;
    }

    /// <summary>
    /// Whether the apply that brought <paramref name="database"/> to <paramref name="version"/> has
    /// finished. The version alone is not enough: it becomes visible part-way through the apply,
    /// before the changed table's cached descriptor is evicted. The apply holds the schema lock from
    /// start to end, so taking the lock once after the version is visible waits out that remainder.
    /// </summary>
    private static async Task<bool> HasAppliedAsync(DatabaseDescriptor database, long version)
    {
        if (database.Schema.SchemaVersion < version)
            return false;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        database.Schema.ReleaseLock();

        return true;
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

    // ── Fault-injection transport hooks ────────────────────────────────────

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
        string schemaKey = SchemaKeyFor(databaseName);

        Node currentLeader = await WaitForSchemaLeaderNodeAsync(databaseName, searchTimeout).ConfigureAwait(false);
        string targetEndpoint = targetNode.Kahuna.Raft.GetLocalEndpoint();

        if (currentLeader.Index == targetNode.Index)
            return; // already there

        await currentLeader.Kahuna.TransferSchemaLeadershipAsync(schemaKey, targetEndpoint, CancellationToken.None)
            .ConfigureAwait(false);

        // Wait until the target node has won the transfer.
        DateTime deadline = DateTime.UtcNow.Add(searchTimeout);
        while (DateTime.UtcNow < deadline)
        {
            using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(200));
            try
            {
                if (await targetNode.Kahuna.AmISchemaLeaderAsync(schemaKey, cts.Token).ConfigureAwait(false))
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
                    if (await node.Kahuna.AmISchemaLeaderAsync(SchemaKeyFor(databaseName), cts.Token).ConfigureAwait(false))
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
        ILoggerFactory loggerFactory,
        Action<EmbeddedKahunaOptions>? configureNode = null
    )
    {
        EmbeddedKahunaOptions nodeOptions =
            new()
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
            };

        // Applied last, so a fixture can override anything above — including the election timings,
        // which a fixture should only touch with the rationale above in mind. A node fixes its
        // configuration when it is constructed, so this is the only point at which a node-level knob
        // (the range auto-split policy, for instance) can be set at all.
        configureNode?.Invoke(nodeOptions);

        return new(
            nodeOptions,
            interNode,
            raftCommunication,
            new StaticDiscovery(peers),
            loggerFactory
        );
    }

    private static async Task WaitForAllPartitionLeadersAsync(EmbeddedKahuna[] nodes, int partitions)
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
            int partition = nodes[0].Raft.GetPartitionKey(key);
            if (!seenPartitions.Add(partition))
                continue;

            // Per-partition Raft questions are answered only by a node that hosts the partition;
            // under replica placement that is not every node, so pick a replica for each one.
            EmbeddedKahuna node = await NodeHostingAsync(nodes, partition).ConfigureAwait(false);

            await node.Raft.WaitForLeader(partition, CancellationToken.None)
                .AsTask()
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

    /// <summary>
    /// A node that hosts <paramref name="partitionId"/>. Under legacy full replication every node
    /// does; under replica placement only the committed replica set does, and Raft refuses
    /// per-partition questions elsewhere.
    /// </summary>
    private static async Task<EmbeddedKahuna> NodeHostingAsync(EmbeddedKahuna[] nodes, int partitionId)
    {
        // Under replica placement a node materializes its partitions when it applies the committed
        // map, which can trail StartAsync by a moment; give the map a short window to land.
        long deadline = Environment.TickCount64 + 15_000;

        while (true)
        {
            foreach (EmbeddedKahuna node in nodes)
            {
                if (node.Raft.HostsPartition(partitionId))
                    return node;
            }

            if (Environment.TickCount64 >= deadline)
                throw new AssertionException($"No node hosts partition {partitionId}");

            await Task.Delay(50).ConfigureAwait(false);
        }
    }

    /// <summary>The first cluster node that hosts <paramref name="partitionId"/>; see the static overload.</summary>
    public Node NodeHosting(int partitionId)
    {
        foreach (Node node in Nodes)
        {
            if (node.Kahuna.Raft.HostsPartition(partitionId))
                return node;
        }

        throw new AssertionException($"No node hosts partition {partitionId}");
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
    /// Used by the fault-injection methods on <see cref="InProcessSchemaCluster"/>.
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

        public Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default)
        {
            // A dropped role-transition request must not look like an idempotent no-op: the default
            // status on the response record is Success, so a blocked link has to say Errored explicitly
            // or the caller would treat the partition as having committed the transition.
            if (IsBlocked(manager, node)) return Task.FromResult(new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored));
            return inner.SendSetMemberRole(manager, node, request, cancellationToken);
        }

        // Gossip and the SWIM probes are forwarded too. The interface defaults drop them, which
        // leaves every peer "Suspect" and, more importantly, never delivers the load reports that
        // feed Kommander's leader hints: under replica placement a node routes an operation on a
        // partition it does not host by that hint, so without gossip the placement view the
        // production transport has never forms here. A blocked link drops them like any other
        // message, so a fault-injection test isolates a node completely.

        public Task<Kommander.Gossip.GossipAck> SendGossip(RaftManager manager, RaftNode node, Kommander.Gossip.GossipMessage digest, CancellationToken cancellationToken = default)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new Kommander.Gossip.GossipAck(0, null));
            return inner.SendGossip(manager, node, digest, cancellationToken);
        }

        public Task<Kommander.Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Kommander.Gossip.PingRequest request, CancellationToken cancellationToken = default)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new Kommander.Gossip.PingResponse(false, 0));
            return inner.SendPing(manager, node, request, cancellationToken);
        }

        public Task<Kommander.Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Kommander.Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
        {
            if (IsBlocked(manager, node)) return Task.FromResult(new Kommander.Gossip.PingReqResponse(false));
            return inner.SendPingReq(manager, node, request, cancellationToken);
        }
    }
}
