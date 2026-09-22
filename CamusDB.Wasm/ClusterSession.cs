/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text;
using System.Text.Json;

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Wasm;

/// <summary>
/// Several CamusDB nodes in one browser tab, wired into one cluster over Kahuna's and Kommander's
/// in-memory transports. Every member is a full node — its own engine, its own Raft log, its own
/// schema catalog — so the cluster elects leaders, replicates DDL through Raft and fails over the
/// way a networked one does. Only the transport is in memory, because a tab cannot open a socket.
///
/// <para>The three node-to-node seams are the shipped in-process transports: DDL is forwarded to
/// the schema leader's executor, a follower's schema ack is handed to the leader's tracker, and a
/// query fragment is run on the peer that owns the span. The cluster test suite uses the same
/// three classes, so what runs here is the path the tests cover.</para>
///
/// <para>Everything runs on the page's one event loop, so the Raft timings must leave room for
/// every node and every partition. They are Kahuna's embedded defaults, which its measurements
/// picked for exactly this case; the fast single-node timings of
/// <see cref="SingleNodeSession"/> would cause needless elections here.</para>
///
/// <para>The members are not safe against one another's lifecycle. The caller serializes every
/// call through the playground's gate, so a start or a stop never overlaps a statement.</para>
/// </summary>
internal sealed class ClusterSession : IAsyncDisposable
{
    /// <summary>The database every statement runs in. It is created at start.</summary>
    public const string DatabaseName = "playground";

    /// <summary>The page offers at most this many nodes, so one visitor cannot exhaust the tab.</summary>
    public const int MaxNodeCount = 5;

    /// <summary>The first member's port. The others count up from it; nothing binds it.</summary>
    private const int BasePort = 7000;

    /// <summary>Partitions each node hosts, not counting the meta partition 0.</summary>
    private const int DataPartitions = 1;

    /// <summary>
    /// How long a leader poll waits before it gives up. Generous next to an election timeout of
    /// 500 to 1500 ms, and short enough that a wedged cluster reports an error instead of holding
    /// the playground's gate for ever.
    /// </summary>
    private static readonly TimeSpan LeaderWaitBudget = TimeSpan.FromSeconds(60);

    private readonly MemoryInterNodeCommmunication interNode = new();

    private readonly InMemoryCommunication raftTransport = new();

    private readonly InProcessClusterNodes registry = new();

    private readonly InProcessSchemaDdlForwarder schemaTransport;

    private readonly InProcessQueryFragmentTransport fragmentTransport;

    private readonly EmbeddedKahunaOptions[] nodeOptions;

    private readonly string[] endpoints;

    private readonly Member?[] members;

    private readonly CamusDBOptions options;

    private ClusterSession(EmbeddedKahunaOptions[] nodeOptions, string[] endpoints, CamusDBOptions options)
    {
        this.nodeOptions = nodeOptions;
        this.endpoints = endpoints;
        this.options = options;

        members = new Member?[nodeOptions.Length];
        schemaTransport = new(registry);
        fragmentTransport = new(registry);
    }

    /// <summary>Members, running or stopped.</summary>
    public int NodeCount => members.Length;

    /// <summary>Partitions every node hosts, including the meta partition 0.</summary>
    public int PartitionCount => DataPartitions + 1;

    /// <summary>
    /// Builds <paramref name="nodeCount"/> nodes, starts them together, waits until every
    /// partition has a leader, and creates the <see cref="DatabaseName"/> database.
    /// </summary>
    public static async Task<ClusterSession> StartAsync(int nodeCount, CancellationToken cancellationToken = default)
    {
        if (nodeCount < 1 || nodeCount > MaxNodeCount)
            throw new ArgumentOutOfRangeException(nameof(nodeCount), nodeCount, $"The playground cluster runs 1 to {MaxNodeCount} nodes.");

        EmbeddedKahunaOptions[] nodeOptions = new EmbeddedKahunaOptions[nodeCount];
        string[] endpoints = new string[nodeCount];

        for (int i = 0; i < nodeCount; i++)
        {
            // The Raft timings are left at Kahuna's embedded defaults on purpose: a 100 ms
            // heartbeat and a 500 to 1500 ms election timeout. With three nodes sharing one
            // browser event loop the longest measured gap between two heartbeats of a leader is
            // about 275 ms, so a shorter election timeout would start elections that no fault
            // caused.
            nodeOptions[i] = new()
            {
                NodeName = $"camusdb-playground-{i + 1}",
                NodeId = i + 1,
                Host = "localhost",
                Port = BasePort + i,
                Storage = "memory",
                WalStorage = "memory",
                InitialPartitions = DataPartitions,
            };

            endpoints[i] = nodeOptions[i].Host + ":" + nodeOptions[i].Port;
        }

        // The free-disk write guard is off: the browser's in-memory file system reports 0 bytes
        // free, so every write would be refused. Nothing here is written to a disk anyway.
        CamusDBOptions options = CamusDBOptions.Default with
        {
            DataDirectory = "/camusdb",
            MinFreeDiskBytes = 0,
        };

        ClusterSession cluster = new(nodeOptions, endpoints, options);

        try
        {
            for (int i = 0; i < nodeCount; i++)
                cluster.members[i] = cluster.BuildNode(i);

            cluster.PublishRoutes();

            Task[] starts = new Task[nodeCount];
            for (int i = 0; i < nodeCount; i++)
                starts[i] = cluster.StartNodeEngineAsync(i, cancellationToken);

            await Task.WhenAll(starts).ConfigureAwait(false);

            for (int i = 0; i < nodeCount; i++)
                cluster.AttachExecutor(i);

            for (int partitionId = 0; partitionId < cluster.PartitionCount; partitionId++)
                await cluster.LeaderIndexAsync(partitionId, cancellationToken).ConfigureAwait(false);

            // One node creates it; the registry entry is replicated through the shared Kahuna KV,
            // so every other node resolves the same database.
            await cluster.MemberAt(0).Executor
                .CreateDatabase(new CreateDatabaseTicket(name: DatabaseName, ifNotExists: true))
                .ConfigureAwait(false);

            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// The member at <paramref name="index"/>.
    /// </summary>
    /// <exception cref="CamusDBException">The member is stopped.</exception>
    public Member MemberAt(int index)
    {
        CheckIndex(index);

        return members[index]
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Node {index + 1} is stopped.");
    }

    /// <summary>
    /// Runs every statement of <paramref name="script"/> on the member at
    /// <paramref name="nodeIndex"/> and returns the JSON array of outcomes. A failed statement
    /// ends the script.
    /// </summary>
    public async Task<string> ExecuteAsync(int nodeIndex, string script, string databaseName)
    {
        StatementRunner runner = MemberAt(nodeIndex).Runner;

        ArrayBufferWriter<byte> buffer = new();
        await using (Utf8JsonWriter writer = new(buffer))
        {
            writer.WriteStartArray();

            foreach (string statement in SqlScriptSplitter.Split(script))
            {
                if (!await runner.RunAsync(statement, databaseName, writer).ConfigureAwait(false))
                    break;
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// What the page draws: for each member, whether it runs, which partitions it believes it
    /// leads, and which links out of it are blocked. A leader claim is the member's own belief —
    /// right after a failover the old leader can still claim the partition until it hears the new
    /// term — so the page shows it as a hint, not as the truth.
    /// </summary>
    public async Task<string> StatusJsonAsync(CancellationToken cancellationToken = default)
    {
        ArrayBufferWriter<byte> buffer = new();
        await using (Utf8JsonWriter writer = new(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("partitionCount", PartitionCount);
            writer.WriteStartArray("nodes");

            for (int i = 0; i < members.Length; i++)
            {
                writer.WriteStartObject();
                writer.WriteNumber("index", i);
                writer.WriteString("name", nodeOptions[i].NodeName);
                writer.WriteString("endpoint", endpoints[i]);
                writer.WriteBoolean("running", members[i] is not null);

                writer.WriteStartArray("leads");
                Member? member = members[i];
                if (member is not null)
                {
                    for (int partitionId = 0; partitionId < PartitionCount; partitionId++)
                    {
                        if (await LeadsAsync(member, partitionId, cancellationToken).ConfigureAwait(false))
                            writer.WriteNumberValue(partitionId);
                    }
                }
                writer.WriteEndArray();

                // Only a link between two running members counts as cut. The transport also
                // reports every link that touches a stopped member as blocked, which is true and
                // useless to show: the member's own stopped state already says it.
                writer.WriteStartArray("blockedTo");
                if (member is not null)
                {
                    for (int to = 0; to < members.Length; to++)
                    {
                        if (to != i && members[to] is not null && raftTransport.IsDeliveryBlocked(endpoints[i], endpoints[to]))
                            writer.WriteNumberValue(to);
                    }
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// The index of a running member that believes it leads <paramref name="partitionId"/>. It
    /// polls until one does, so it also serves as the wait for a failover to finish.
    ///
    /// <para>The wait is bounded. The caller holds the playground's gate for the whole poll, so an
    /// endless one would freeze every other call and the page would look dead rather than slow.</para>
    /// </summary>
    /// <exception cref="CamusDBException">No member claimed the partition inside the budget.</exception>
    public async Task<int> LeaderIndexAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        DateTime until = DateTime.UtcNow + LeaderWaitBudget;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (int i = 0; i < members.Length; i++)
            {
                Member? member = members[i];
                if (member is not null && await LeadsAsync(member, partitionId, cancellationToken).ConfigureAwait(false))
                    return i;
            }

            if (DateTime.UtcNow > until)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"No node claimed partition {partitionId} within {LeaderWaitBudget.TotalSeconds:0} s."
                );

            await Task.Delay(TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stops the member at <paramref name="index"/> as if the host it ran on had crashed: its
    /// traffic is cut, it leaves both transports and the address book, and its engine is disposed.
    /// The others elect new leaders for the partitions it led. A majority must survive, so the
    /// last member that a quorum needs cannot be stopped.
    /// </summary>
    public async Task StopNodeAsync(int index)
    {
        CheckIndex(index);

        Member member = members[index]
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Node {index + 1} is already stopped.");

        int running = 0;
        foreach (Member? other in members)
        {
            if (other is not null)
                running++;
        }

        if (running - 1 < members.Length / 2 + 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Stopping node {index + 1} would leave {running - 1} of {members.Length} nodes, which is less than a majority. The cluster would stop committing."
            );

        // Cut the member off before it is disposed, so no peer calls into a node that is shutting
        // down, and so a statement addressed to it fails as a crashed host rather than reaching a
        // disposed executor.
        raftTransport.PartitionNode(endpoints[index]);
        registry.Unregister(endpoints[index]);
        members[index] = null;
        PublishRoutes();

        await member.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Starts a stopped member again under the same endpoint, with an empty log and empty storage.
    /// It rejoins through the static roster and catches up from the others.
    /// </summary>
    public async Task StartNodeAsync(int index, CancellationToken cancellationToken = default)
    {
        CheckIndex(index);

        if (members[index] is not null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Node {index + 1} is already running.");

        members[index] = BuildNode(index);
        PublishRoutes();
        raftTransport.HealPartition(endpoints[index]);

        try
        {
            await StartNodeEngineAsync(index, cancellationToken).ConfigureAwait(false);
            AttachExecutor(index);
        }
        catch
        {
            Member? failed = members[index];
            raftTransport.PartitionNode(endpoints[index]);
            registry.Unregister(endpoints[index]);
            members[index] = null;
            PublishRoutes();

            if (failed is not null)
                await failed.DisposeAsync().ConfigureAwait(false);

            throw;
        }
    }

    /// <summary>
    /// Drops the traffic from one member to another on both transports, which is how the page
    /// shows a network partition. The members keep running, so a member cut off from the rest
    /// keeps its timers and keeps campaigning.
    /// </summary>
    public void BlockLink(int from, int to)
    {
        CheckIndex(from);
        CheckIndex(to);

        if (from == to)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A member has no link to itself.");

        raftTransport.BlockLink(endpoints[from], endpoints[to]);
        interNode.BlockLink(endpoints[from], endpoints[to]);
    }

    /// <summary>
    /// Restores every blocked link. A stopped member stays stopped and stays unreachable: cutting
    /// a member off when it stops is a separate filter in both transports, and only the link
    /// blocks are removed here.
    /// </summary>
    public void RestoreLinks()
    {
        for (int from = 0; from < members.Length; from++)
        {
            for (int to = 0; to < members.Length; to++)
            {
                if (from != to)
                    raftTransport.UnblockLink(endpoints[from], endpoints[to]);
            }
        }

        interNode.UnblockAllLinks();
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = 0; i < members.Length; i++)
        {
            Member? member = members[i];
            if (member is null)
                continue;

            members[i] = null;
            registry.Unregister(endpoints[i]);
            await member.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A member's local answer to "do I lead this partition?". A node that is rejoining, that does
    /// not host the partition, or that was disposed under this poll leads nothing, and each of
    /// those says so by throwing.
    /// </summary>
    private static async Task<bool> LeadsAsync(Member member, int partitionId, CancellationToken cancellationToken)
    {
        try
        {
            return await member.Node.Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RaftException or ObjectDisposedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Builds the member's engine, with its own view of the shared inter-node transport so that
    /// the transport knows the sender of every call and can drop the traffic of a blocked link.
    /// The executor comes later, in <see cref="AttachExecutor"/>: it can only be built once
    /// the node runs.
    /// </summary>
    private Member BuildNode(int index)
    {
        List<RaftNode> peers = new(endpoints.Length - 1);
        for (int i = 0; i < endpoints.Length; i++)
        {
            if (i != index)
                peers.Add(new(endpoints[i]));
        }

        EmbeddedKahuna node = new(
            nodeOptions[index],
            interNode.ForNode(endpoints[index]),
            raftTransport,
            new StaticDiscovery(peers),
            NullLoggerFactory.Instance
        );

        return new Member(node);
    }

    private async Task StartNodeEngineAsync(int index, CancellationToken cancellationToken)
    {
        EmbeddedKahuna node = members[index]!.Node;

        await node.Raft.UpdateNodes().ConfigureAwait(false);
        await node.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gives the running member its executor, publishes it in the address book, and wires the ack
    /// transport. Ordering matters: the node is in the address book only once its executor exists,
    /// so a forwarded ticket never resolves to a half-built member.
    /// </summary>
    private void AttachExecutor(int index)
    {
        Member member = members[index]!;

        CommandExecutor executor = new(
            new CommandValidator(options),
            new CatalogsManager(NullLogger<ICamusDB>.Instance),
            NullLogger<ICamusDB>.Instance,
            options,
            sharedNode: member.Node,
            schemaDdlForwarder: schemaTransport,
            isClusterMode: true,
            fragmentTransport: fragmentTransport
        );

        member.Attach(executor, new StatementRunner(executor, options));
        registry.Register(member.Node, executor);
        member.Node.SetSchemaAckForwarder(schemaTransport);
    }

    /// <summary>Publishes the running members to both transports; a stopped member is unreachable.</summary>
    private void PublishRoutes()
    {
        Dictionary<string, IKahuna> kahunaRoutes = new(members.Length);
        Dictionary<string, IRaft> raftRoutes = new(members.Length);

        for (int i = 0; i < members.Length; i++)
        {
            Member? member = members[i];
            if (member is null)
                continue;

            kahunaRoutes[endpoints[i]] = member.Node.Kahuna;
            raftRoutes[endpoints[i]] = member.Node.Raft;
        }

        interNode.SetNodes(kahunaRoutes);
        raftTransport.SetNodes(raftRoutes);
    }

    private void CheckIndex(int index)
    {
        if ((uint)index >= (uint)members.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"There is no node {index + 1} in a {members.Length}-node cluster.");
    }

    /// <summary>One member: its embedded engine, and the executor and runner built over it.</summary>
    internal sealed class Member : IAsyncDisposable
    {
        private CommandExecutor? executor;

        private StatementRunner? runner;

        internal Member(EmbeddedKahuna node) => Node = node;

        public EmbeddedKahuna Node { get; }

        public CommandExecutor Executor => executor
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "The node is still starting.");

        public StatementRunner Runner => runner
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "The node is still starting.");

        internal void Attach(CommandExecutor executor, StatementRunner runner)
        {
            this.executor = executor;
            this.runner = runner;
        }

        public async ValueTask DisposeAsync()
        {
            if (executor is not null)
                await executor.DisposeAsync().ConfigureAwait(false);

            await Node.DisposeAsync().ConfigureAwait(false);
        }
    }
}
