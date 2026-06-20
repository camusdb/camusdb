
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Discovery;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Process-level handle to the embedded Kahuna KV engine.
///
/// Owns a single <see cref="EmbeddedKahunaNode"/> and exposes the <see cref="IKahuna"/>
/// interface so the rest of CamusDB (KvTableStore, transaction layer) can perform KV
/// operations without depending on the Kahuna bootstrap details.
///
/// One instance per CamusDB process. This class will replace <c>StorageManager</c> on
/// <c>DatabaseDescriptor</c> in Phase 4 of the Kahuna refactor.
/// </summary>
public sealed class EmbeddedKahuna : IAsyncDisposable
{
    public const string SchemaChangeLogType = "SchemaChange";

    private readonly SchemaAckTracker schemaAcks = new();

    private ISchemaAckSender? schemaAckSender;

    private readonly EmbeddedKahunaNode node;

    // True when this instance was constructed with an explicit IInterNodeCommunication (cluster mode).
    // False for standalone embedded nodes that use phantom EmbeddedRaftCommunication.Witnesses — those
    // witness nodes must not appear in the schema ack gate because they are never real schema participants.
    private readonly bool isClusterMode;

    private readonly object schemaSubscriptionsSync = new();
    private readonly List<SchemaApplySubscription> schemaSubscriptions = new();
    private ISchemaReplicationForwarder? schemaReplicationForwarder;

    // F1b late-subscriber buffer: WAL restore events (OnLogRestored / OnRestoreFinished) fire
    // during Raft.StartAsync() — before any OpenDatabase → RegisterSchemaApply call registers
    // a subscriber. To deliver those events to late subscribers, we buffer schema-log entries
    // per-partition until the first RegisterSchemaApply call for that partition consumes them.
    //
    // Correctness assumption: OnRestoreFinished fires synchronously within StartAsync so that
    // WaitForLeaderAsync (which follows StartAsync) returns only after all partitions are fully
    // restored. OpenDatabase is always called after WaitForLeaderAsync, so by the time
    // RegisterSchemaApply runs, _walRestoreCompletedPartitions already contains the partition.
    // If restore were ever made async-after-StartAsync, a subscriber registered mid-restore
    // would find alreadyCompleted==false, the already-buffered early entries would never replay,
    // and RestoreAsync's gap check would throw loudly rather than silently diverge.
    //
    // Drain-once invariant: _walRestoreDrainedPartitions prevents a second open of the same
    // database from re-firing onRestoreFinished (and thus PersistFullSchemaCheckpointAsync) on
    // an already-current checkpoint. Without it alreadyCompleted stays true forever (the set is
    // never cleared), so every OpenDatabase would trigger a needless full re-persist.
    //
    // Thread safety: all reads/writes protected by _walRestoreBufferLock.
    // Concurrency note: between buffer replay entries the schema semaphore is released, so a
    // live OnReplicationReceived delta could interleave. Both ApplyAsync and RestoreAsync are
    // idempotent for already-applied versions, so the worst-case outcome is a benign skip or
    // a loud "out of order" throw rather than silent corruption.
    private readonly object _walRestoreBufferLock = new();
    private readonly Dictionary<int, List<byte[]>> _walRestoreBuffer = new();
    private readonly HashSet<int> _walRestoreCompletedPartitions = new();
    private readonly HashSet<int> _walRestoreDrainedPartitions = new();

    // Stored so DisposeAsync can unregister them from Raft's event chains.
    private Func<int, RaftLog, Task<bool>>? _walRestoreLogHandler;
    private Action<int>? _walRestoreFinishedHandler;

    /// <summary>
    /// The Kahuna KV API. Used by KvTableStore and the transaction layer.
    /// </summary>
    public IKahuna Kahuna => node.Kahuna;

    /// <summary>
    /// The Raft consensus handle. Exposed for partition/leader queries and HLC clock access.
    /// </summary>
    public IRaft Raft => node.Raft;

    /// <summary>
    /// Maximum time replicated DDL waits for live schema-apply acknowledgements.
    /// </summary>
    public TimeSpan SchemaAckWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Live-node expiry used by the schema ack gate. A peer the schema leader has not
    /// heard from (via Raft activity, <see cref="Kommander.IRaft.GetActiveNodes"/>) within this
    /// window is presumed dead and dropped from the ack gate, so a node death does not freeze
    /// subsequent DDL on the strict pre-proposal gate.
    ///
    /// <para>
    /// Default is 30 s — comfortably above the Raft heartbeat interval, so a healthy-but-idle
    /// follower (heard from many times per second) is never false-evicted, while a genuinely dead
    /// node is shed within the window. Set to <see cref="Timeout.InfiniteTimeSpan"/> (config
    /// <c>-1</c>) to restore the strict "every configured node must ack" behaviour, at the cost of
    /// DDL liveness under a node failure. Note the lease and <see cref="SchemaAckWaitTimeout"/> are
    /// both 30 s by default, so the first DDL issued within the lease window of a node death may
    /// still time out once before the dead node ages out of the active set; tune the lease below
    /// the wait timeout if faster eviction is needed.
    /// </para>
    /// </summary>
    public TimeSpan SchemaAckLiveNodeLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// <b>Quorum backstop.</b> How long the schema-ack gate tries to achieve
    /// <em>full</em> convergence (every live node acked) before falling back to
    /// <em>quorum</em> convergence (⌊N/2⌋+1 of live nodes acked).
    ///
    /// <para>
    /// <b>Liveness guarantee:</b> DDL completes within <see cref="SchemaAckWaitTimeout"/>
    /// whenever a majority of the cluster applies the schema delta, regardless of what the
    /// minority does. Specifically, a single slow or unreachable follower stalls DDL for at
    /// most <c>SchemaAckQuorumBackstopDelay</c> rather than the full 30-second timeout.
    /// </para>
    ///
    /// <para>
    /// Default is 10 s — generous enough to clear a Raft leadership election (configured at
    /// 3–6 s) before the backstop fires, so a transient election is never mistaken for a
    /// permanently-slow follower. Set to <see cref="Timeout.InfiniteTimeSpan"/> to restore the
    /// original strict behaviour where every configured node must ack before DDL proceeds.
    /// </para>
    /// </summary>
    public TimeSpan SchemaAckQuorumBackstopDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Records the outcome of the most recent <see cref="WaitForSchemaAcksAsync"/> call on this
    /// node. Useful in fault-injection tests to assert whether DDL proceeded via full convergence
    /// or via the quorum backstop. Not meaningful in production (production code checks
    /// the boolean return value, not this property).
    /// </summary>
    internal SchemaAckOutcome LastGateOutcome { get; private set; }

    /// <summary>
    /// The live endpoints that had not acked the target version when the most recent
    /// <see cref="WaitForSchemaAcksAsync"/> resolved via <see cref="SchemaAckOutcome.QuorumBackstop"/>
    /// or <see cref="SchemaAckOutcome.Timeout"/> — i.e. the lagging nodes. Empty on full convergence.
    /// Lets the DDL warning name who lagged instead of "one or more live nodes".
    /// </summary>
    internal IReadOnlyList<string> LastGateLaggards { get; private set; } = [];

    /// <summary>
    /// Constructs the embedded engine with the provided options.
    /// </summary>
    public EmbeddedKahuna(EmbeddedKahunaOptions options, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        node = new EmbeddedKahunaNode(options, loggerFactory);
        isClusterMode = false;
        WireWalRestoreBuffer();
    }

    /// <summary>
    /// Constructs the embedded engine with externally supplied communication implementations.
    /// Use for cluster mode where real gRPC transports replace in-process fakes.
    /// </summary>
    public EmbeddedKahuna(
        EmbeddedKahunaOptions options,
        IInterNodeCommunication interNode,
        ICommunication raftComm,
        IDiscovery discovery,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        node = new EmbeddedKahunaNode(options, interNode, raftComm, discovery, loggerFactory);
        isClusterMode = true;
        WireWalRestoreBuffer();
    }

    /// <summary>
    /// Creates a cluster-mode engine backed by SQLite, wired with real gRPC communications.
    /// </summary>
    public static EmbeddedKahuna CreateCluster(
        EmbeddedKahunaOptions options,
        IEnumerable<string> peers,
        ILoggerFactory? loggerFactory = null)
    {
        List<RaftNode> peerNodes = [.. peers.Select(p => new RaftNode(p))];

        KahunaConfiguration kahunaConfig = new();
        ILogger<GrpcInterNodeCommunication> grpcLogger = (loggerFactory ?? LoggerFactory.Create(_ => { }))
            .CreateLogger<GrpcInterNodeCommunication>();

        return new EmbeddedKahuna(
            options,
            new GrpcInterNodeCommunication(kahunaConfig, grpcLogger),
            new GrpcCommunication(),
            new StaticDiscovery(peerNodes),
            loggerFactory
        );
    }

    /// <summary>
    /// Convenience constructor: in-memory storage, single partition, no persistence.
    /// Suitable for tests and in-process dev scenarios.
    /// </summary>
    public EmbeddedKahuna(ILoggerFactory? loggerFactory = null)
        : this(DefaultOptions(), loggerFactory)
    {
    }

    /// <summary>
    /// Constructs the embedded engine backed by RocksDB / SQLite at <paramref name="dataPath"/>.
    /// </summary>
    public EmbeddedKahuna(string dataPath, ILoggerFactory? loggerFactory = null)
        : this(PersistentOptions(dataPath), loggerFactory)
    {
    }

    /// <summary>
    /// Constructs the embedded engine backed by SQLite at <paramref name="dataPath"/>.
    /// Suitable for single-process embedded use and tests that require persistence across close/reopen.
    /// </summary>
    public static EmbeddedKahuna CreateSqlite(string dataPath, ILoggerFactory? loggerFactory = null)
        => new(EmbeddedKahunaOptionsBuilder.BuildStandalone(dataPath, CamusDBConfig.Kahuna), loggerFactory);

    /// <summary>
    /// Starts the Raft cluster and waits for the initial partition to elect a leader.
    /// Must be called once before any KV operations.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
        => node.StartAsync(cancellationToken);

    /// <summary>
    /// Blocks until the partition that owns <paramref name="key"/> has an elected leader.
    /// Call this after <see cref="StartAsync"/> before issuing writes on a key.
    /// </summary>
    public Task<string> WaitForLeaderAsync(string key, CancellationToken cancellationToken = default)
        => node.WaitForLeaderForKeyAsync(key, cancellationToken);

    /// <summary>
    /// Flushes all pending dirty writes queued during WAL restore to the persistence backend.
    /// Must be called after <see cref="WaitForLeaderAsync"/> and before reading persisted data.
    /// </summary>
    public Task FlushAsync() => node.FlushAsync();

    /// <summary>
    /// Resolves the single Raft partition that carries <i>all</i> schema-log traffic for a
    /// database. Uses <c>GetPrefixPartitionKey</c> (hashes the whole <c>{db}/meta</c> string)
    /// so every schema delta for the database lands on one partition and is therefore totally
    /// ordered. Throws if it resolves to the reserved partition 0. See the architecture documentation.
    /// </summary>
    public int SchemaLogPartition(string db)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(db);

        int partitionId = Raft.GetPrefixPartitionKey($"{db}/meta");
        if (partitionId == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema log partition for database '{db}' resolved to reserved partition 0"
            );

        return partitionId;
    }

    /// <summary>
    /// Installs the internal raw-entry forwarder used by in-memory cluster tests.
    /// Production DDL forwarding must use the command-layer ticket forwarder.
    /// </summary>
    internal void SetSchemaReplicationForwarder(ISchemaReplicationForwarder? forwarder)
    {
        schemaReplicationForwarder = forwarder;
    }

    /// <summary>
    /// Installs the ack transport used to deliver this node's applied-schema notifications to the
    /// current schema-partition leader. Called once after DI is fully wired (production) or by
    /// the in-process cluster fixture. A <c>null</c> value disables the ack transport — the node
    /// still records acks locally but does not forward them; the gate falls back to its timeout.
    /// </summary>
    internal void SetSchemaAckSender(ISchemaAckSender? sender)
    {
        schemaAckSender = sender;
    }

    /// <summary>
    /// Production entry point: wires the <see cref="ISchemaAckSender"/> from the DDL forwarder.
    /// <see cref="CamusDB.Core.CommandsExecutor.HttpSchemaDdlForwarder"/> implements both
    /// <c>ISchemaDdlForwarder</c> and <c>ISchemaAckSender</c>; this method performs the cast so
    /// <c>Program.cs</c> (which cannot access the internal <c>ISchemaAckSender</c>) can wire it.
    /// </summary>
    public void SetSchemaAckForwarder(CamusDB.Core.CommandsExecutor.ISchemaDdlForwarder? forwarder)
    {
        schemaAckSender = forwarder as ISchemaAckSender;
    }

    /// <summary>
    /// Triggers this node to start an immediate election for the schema-log partition of
    /// <paramref name="db"/>.  The node still has to satisfy Raft log-freshness and quorum
    /// rules, so it is not guaranteed to win — but if it has an up-to-date log it will.
    /// Intended for testing only.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> ForceSchemaLeaderForTestingAsync(
        string db,
        CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        return Raft.ForceLeaderForTestingAsync(partitionId, cancellationToken);
    }

    public async ValueTask<bool> AmISchemaLeaderAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        return await Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<string> WaitForSchemaLeaderAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        return await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the current leader endpoint for <paramref name="partitionId"/>, waiting up to
    /// the Raft election timeout if no leader is currently elected.
    /// </summary>
    public async ValueTask<string> GetPartitionLeaderAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        return await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Voluntarily steps down from schema-partition leadership for <paramref name="db"/>.
    /// The node remains online as a follower and can vote in the next election.
    /// Called on F1a persist exhaustion so a healthy peer can take over.
    /// </summary>
    public async Task StepDownSchemaPartitionAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        await Raft.StepDownAsync(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Transfers schema-partition leadership for <paramref name="db"/> to <paramref name="targetEndpoint"/>.
    /// The local node must currently be the leader and the target must have an up-to-date log.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public async Task TransferSchemaLeadershipAsync(
        string db,
        string targetEndpoint,
        CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        await Raft.TransferLeadershipAsync(partitionId, targetEndpoint, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Suspends outbound heartbeats for the schema partition of <paramref name="db"/>.
    /// Followers will time out and elect a new leader. Used in fault-injection tests.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public async Task SuspendSchemaHeartbeatsAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        await Raft.SuspendHeartbeatsAsync(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resumes outbound heartbeats for the schema partition of <paramref name="db"/>.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public async Task ResumeSchemaHeartbeatsAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        await Raft.ResumeHeartbeatsAsync(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits until the schema partition for <paramref name="db"/> has had a stable leader
    /// for at least <paramref name="minStableFor"/>. Useful after a forced step-down or
    /// leadership transfer to confirm the cluster has re-stabilized.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public async ValueTask<string> WaitForSchemaLeaderStableAsync(
        string db,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        return await Raft.WaitForLeaderStableAsync(partitionId, minStableFor, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a serialized <see cref="Catalogs.Models.SchemaChangeLogEntry"/> for a database.
    /// If this node is the schema leader it proposes → commits → fans the entry out to local
    /// apply subscribers (<c>autoCommit: false</c> so apply runs only after the quorum commit).
    /// If it is not the leader it forwards the raw entry through the internal test-only
    /// <c>ISchemaReplicationForwarder</c> (production uses the command-layer ticket forwarder),
    /// then applies locally on a committed result. See the architecture documentation.
    /// </summary>
    public async Task<SchemaReplicationResult> ReplicateSchemaChangeAsync(
        string db,
        byte[] entry,
        CancellationToken cancellationToken = default
    )
    {
        int partitionId = SchemaLogPartition(db);

        for (int attempt = 0; attempt < 3; attempt++)
        {
            string leader = await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);

            if (await Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
                return await ReplicateSchemaChangeAsLeaderAsync(db, partitionId, leader, entry, cancellationToken).ConfigureAwait(false);

            ISchemaReplicationForwarder? forwarder = schemaReplicationForwarder;
            if (forwarder is null)
                return new(SchemaReplicationOutcome.NotLeader, partitionId, leader: leader);

            SchemaReplicationResult? forwarded =
                await forwarder.ForwardSchemaChangeAsync(leader, db, entry, cancellationToken).ConfigureAwait(false);

            if (forwarded is null)
                return new(SchemaReplicationOutcome.NotLeader, partitionId, leader: leader);

            if (forwarded.Outcome == SchemaReplicationOutcome.Committed)
            {
                await InvokeLocalSchemaApplyAsync(partitionId, entry).ConfigureAwait(false);
                return forwarded;
            }

            if (forwarded.Outcome != SchemaReplicationOutcome.NotLeader)
                return forwarded;
        }

        string lastLeader = await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        return new(SchemaReplicationOutcome.NotLeader, partitionId, leader: lastLeader);
    }

    public void RecordLocalSchemaApplied(string db, long schemaVersion)
    {
        schemaAcks.RecordApplied(db, Raft.GetLocalEndpoint(), schemaVersion);
    }

    /// <summary>
    /// Records a schema-apply ack received from a remote follower node. Called by the
    /// ack transport endpoint when a follower posts its applied version to the leader.
    /// </summary>
    public void RecordRemoteSchemaAck(string db, string nodeEndpoint, long version)
    {
        Diagnostics.SchemaDiag.Log($"REMOTE-ACK leader={Raft.GetLocalEndpoint()} from={nodeEndpoint} db={db} ver={version}");
        schemaAcks.RecordApplied(db, nodeEndpoint, version);
    }

    /// <summary>
    /// Records the local apply ack and, if an <see cref="ISchemaAckSender"/> is wired, fires
    /// a best-effort notification to the current schema-partition leader so it can observe this
    /// follower's progress. The send is fire-and-forget; the gate's timeout is the backstop.
    /// </summary>
    internal void RecordAndPublishSchemaApplied(string db, long schemaVersion)
    {
        string localEndpoint = Raft.GetLocalEndpoint();
        schemaAcks.RecordApplied(db, localEndpoint, schemaVersion);

        ISchemaAckSender? sender = schemaAckSender;
        if (sender is null)
            return;

        _ = Task.Run(() => PublishSchemaAckWithRetryAsync(sender, db, localEndpoint, schemaVersion));
    }

    /// <summary>
    /// Delivers this follower's applied-version notification to the current schema-partition leader,
    /// <b>retrying with backoff</b> until it succeeds or a deadline (under the gate's ack timeout)
    /// elapses. A single dropped notification must not strand the leader's ack gate for its full
    /// timeout — the dominant multi-node DDL flake. Confirmed mechanism: a follower applies the new
    /// schema version but its one fire-and-forget ack is lost (a transient <see cref="IRaft.WaitForLeader"/>
    /// that momentarily resolves this node as the leader and takes the "I am the leader" early-out, or
    /// a transient send), and because the gate waits for an explicit ack from every live node and never
    /// evicts a Raft-alive follower, the lost ack is only recovered by re-sending. Re-resolving the
    /// leader each attempt also rides through a real leadership change.
    /// </summary>
    private async Task PublishSchemaAckWithRetryAsync(
        ISchemaAckSender sender, string db, string localEndpoint, long schemaVersion)
    {
        long deadline = Environment.TickCount64 + 25_000;
        int attempt = 0;

        while (Environment.TickCount64 < deadline)
        {
            try
            {
                int partitionId = SchemaLogPartition(db);
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(5));
                string leader = await Raft.WaitForLeader(partitionId, cts.Token).ConfigureAwait(false);
                Diagnostics.SchemaDiag.Log(
                    $"ACK-SEND node={localEndpoint} db={db} ver={schemaVersion} attempt={attempt} part={partitionId} resolvedLeader={leader}");

                if (string.Equals(leader, localEndpoint, StringComparison.Ordinal))
                {
                    // We currently see ourselves as the schema leader, so our local record suffices —
                    // BUT this view can be transient/stale. If we are genuinely the leader the gate runs
                    // here and is satisfied locally; if our view is wrong, retry until it corrects and we
                    // send to the real leader. Stop re-checking once we are stably the leader.
                    if (await Raft.AmILeader(partitionId, cts.Token).ConfigureAwait(false))
                    {
                        Diagnostics.SchemaDiag.Log($"ACK-SELF node={localEndpoint} db={db} ver={schemaVersion} (we are the leader; local record suffices)");
                        return;
                    }
                }
                else
                {
                    await sender.SendSchemaAckAsync(leader, db, localEndpoint, schemaVersion, cts.Token).ConfigureAwait(false);
                    Diagnostics.SchemaDiag.Log($"ACK-SENT node={localEndpoint} to={leader} db={db} ver={schemaVersion}");
                    return; // delivered to the leader
                }
            }
            catch (Exception ex)
            {
                Diagnostics.SchemaDiag.Log($"ACK-ERR node={localEndpoint} db={db} ver={schemaVersion} attempt={attempt} ex={ex.GetType().Name}:{ex.Message}");
                // Transient (leader churn, transport, timeout) — fall through to backoff and retry.
            }

            int delay = Math.Min(500, 50 * (1 << Math.Min(attempt, 4)));
            attempt++;
            try { await Task.Delay(delay).ConfigureAwait(false); }
            catch { return; }
        }

        Diagnostics.SchemaDiag.Log($"ACK-GIVEUP node={localEndpoint} db={db} ver={schemaVersion} (deadline reached without delivering)");
    }

    /// <summary>
    /// Returns the live schema membership as a set of endpoint strings.
    /// In cluster mode: local endpoint plus every peer from <c>Raft.GetNodes()</c>.
    /// In standalone mode: only the local endpoint — <c>GetNodes()</c> returns phantom witness
    /// nodes used by the embedded Raft for quorum and must not appear in the ack gate.
    /// </summary>
    /// <summary>
    /// The set of nodes the schema ack gate must wait on. Always includes the local node.
    /// <para>
    /// <b>E1 (infinite lease, opt-in via config <c>-1</c>):</b> every configured Raft peer — the
    /// gate waits for every member, so a crashed-but-configured node freezes DDL until the ack
    /// timeout. Strictest (never false-evicts) but not live under a node failure.
    /// </para>
    /// <para>
    /// <b>E2 (finite lease, the default — 30 s):</b> live peers are sourced from the leader's real Raft activity view
    /// (<see cref="Kommander.IRaft.GetActiveNodes"/>) within the lease window. A peer the leader
    /// has not heard from within the lease is presumed dead and excluded from the gate, so DDL
    /// completes without it; a slow-but-alive peer (still answering Raft, even if not applying
    /// schema deltas) stays active and must still ack — no false eviction. The gate runs on the
    /// schema leader (the proposer), so <c>GetActiveNodes</c> reflects its follower reachability.
    /// </para>
    /// </summary>
    private IReadOnlyCollection<string> GetLiveSchemaNodes()
    {
        if (!isClusterMode)
            return [Raft.GetLocalEndpoint()];

        List<string> members = [Raft.GetLocalEndpoint()];

        if (SchemaAckLiveNodeLease == Timeout.InfiniteTimeSpan)
        {
            foreach (RaftNode peer in Raft.GetNodes())
                members.Add(peer.Endpoint);
        }
        else
        {
            members.AddRange(Raft.GetActiveNodes(SchemaAckLiveNodeLease));
        }

        return members;
    }

    /// <summary>
    /// Waits until <paramref name="schemaVersion"/> has been applied on all live schema nodes,
    /// with an H5 quorum backstop as a liveness escape hatch.
    /// </summary>
    /// <param name="enforceFullConvergence">
    /// When <see langword="true"/> the quorum backstop is disabled — the gate waits for every
    /// live node to ack, regardless of <see cref="SchemaAckQuorumBackstopDelay"/>. Use this
    /// for the <em>pre-proposal</em> gate that enforces the two-version safety invariant:
    /// allowing quorum-only convergence there lets a proposer advance N→N+1 while a minority
    /// sits at N−1, breaking the invariant and risking schema mis-decode on those nodes. The
    /// quorum backstop is appropriate only for the <em>post-commit</em> ack gate (after the
    /// log entry is already durable), where it bounds DDL latency without affecting safety.
    /// </param>
    public async Task<bool> WaitForSchemaAcksAsync(
        string db,
        long schemaVersion,
        TimeSpan timeout,
        TimeSpan? liveNodeLease = null,
        bool enforceFullConvergence = false,
        CancellationToken cancellationToken = default
    )
    {
        TimeSpan backstopDelay = enforceFullConvergence
            ? Timeout.InfiniteTimeSpan
            : SchemaAckQuorumBackstopDelay;

        IReadOnlyCollection<string> liveMembers = GetLiveSchemaNodes();

        if (Diagnostics.SchemaDiag.Enabled)
            Diagnostics.SchemaDiag.Log(
                $"GATE-WAIT leader={Raft.GetLocalEndpoint()} db={db} ver={schemaVersion} " +
                $"timeoutMs={(long)timeout.TotalMilliseconds} " +
                $"backstopMs={(backstopDelay == Timeout.InfiniteTimeSpan ? "∞" : ((long)backstopDelay.TotalMilliseconds).ToString())} " +
                $"liveMembers=[{string.Join(",", liveMembers)}]");

        SchemaAckOutcome outcome = await schemaAcks.WaitForAllLiveAsync(
            db,
            schemaVersion,
            timeout,
            GetLiveSchemaNodes,
            // Liveness is carried by membership (GetLiveSchemaNodes filters dead peers out of
            // the set via Raft activity, E2). The tracker must NOT also expire members on its
            // apply-derived LastSeen — that would false-evict a Raft-alive node that is merely slow
            // to apply a schema delta. So the tracker waits for every live member to ack, with the
            // quorum backstop as the liveness escape hatch (post-commit gate only).
            Timeout.InfiniteTimeSpan,
            backstopDelay,
            cancellationToken
        ).ConfigureAwait(false);

        if (Diagnostics.SchemaDiag.Enabled)
            Diagnostics.SchemaDiag.Log(
                $"GATE-{outcome.ToString().ToUpperInvariant()} leader={Raft.GetLocalEndpoint()} db={db} ver={schemaVersion} " +
                $"liveMembers=[{string.Join(",", GetLiveSchemaNodes())}]");

        LastGateOutcome = outcome;

        // Capture who lagged (for the DDL warning) and bump the always-on metric. On full
        // convergence there are no laggards and no backstop activation.
        if (outcome == SchemaAckOutcome.FullConvergence)
        {
            LastGateLaggards = [];
        }
        else
        {
            LastGateLaggards = schemaAcks.GetLaggingNodes(db, schemaVersion, GetLiveSchemaNodes());

            if (outcome == SchemaAckOutcome.QuorumBackstop)
                Diagnostics.SchemaMetrics.RecordQuorumBackstop();
        }

        return outcome != SchemaAckOutcome.Timeout;
    }

    /// <summary>
    /// Registers a callback that fires when THIS node becomes the schema leader for
    /// <paramref name="db"/>. Useful for coordinator resume on leader change.
    /// Returns an <see cref="IDisposable"/> that unhooks the handler on dispose.
    /// </summary>
    public IDisposable RegisterSchemaLeaderCallback(string db, Func<Task> onBecameLeader)
    {
        ArgumentNullException.ThrowIfNull(onBecameLeader);

        int schemaPartition = SchemaLogPartition(db);
        string localEndpoint = Raft.GetLocalEndpoint();

        Func<int, string, Task<bool>> handler = (partitionId, leaderEndpoint) =>
        {
            if (partitionId == schemaPartition &&
                string.Equals(leaderEndpoint, localEndpoint, StringComparison.Ordinal))
            {
                // Fire on the thread pool so the Raft actor is not blocked while the
                // coordinator reads from KV and replicates schema changes back through Raft.
                _ = Task.Run(onBecameLeader);
            }
            return Task.FromResult(true);
        };

        Raft.OnLeaderChanged += handler;
        return new LeaderCallbackSubscription(Raft, handler);
    }

    public IDisposable RegisterSchemaApply(
        Func<int, byte[], Task<bool>> onApply,
        Func<int, byte[], Task<bool>> onRestore,
        string? db = null,
        Func<Task>? onRestoreFinished = null
    )
    {
        ArgumentNullException.ThrowIfNull(onApply);
        ArgumentNullException.ThrowIfNull(onRestore);

        // Call onApply / onRestore directly rather than via Task.Run. Kommander awaits
        // the returned Task from its Raft state machine thread (a Nixie actor thread that is
        // already a thread pool thread). Dispatching another Task.Run from that context adds
        // a redundant thread pool hop and causes starvation when many partitions commit
        // concurrently: all state machines block waiting for Task.Run slots that other
        // blocked state machines are consuming, matching the thread pool injector rate (~1/s).
        // Calling inline is safe because onApply/onRestore are async and yield on their own
        // internal awaits (semaphores, KV writes) without ever blocking the thread pool.
        Func<int, RaftLog, Task<bool>> applyHandler = async (partitionId, log) =>
        {
            if (log.LogType != SchemaChangeLogType)
                return true;

            return await onApply(partitionId, log.LogData ?? []).ConfigureAwait(false);
        };

        Func<int, RaftLog, Task<bool>> restoreHandler = async (partitionId, log) =>
        {
            if (log.LogType != SchemaChangeLogType)
                return true;

            return await onRestore(partitionId, log.LogData ?? []).ConfigureAwait(false);
        };

        Raft.OnReplicationReceived += applyHandler;
        Raft.OnLogRestored += restoreHandler;

        // Wire the restore-finished callback. Two cases:
        //
        // A) Restore not yet complete when we subscribe (mid-restore or pre-restore):
        //    The subscriber's OnLogRestored handler receives live entries as they arrive.
        //    When restore completes, OnRestoreFinished fires → drain any entries that were
        //    buffered before this subscriber registered (mid-restore early entries), replay
        //    them, then call onRestoreFinished. Drain-once guard prevents a second open from
        //    re-firing onRestoreFinished on an already-current checkpoint.
        //
        // B) Restore already complete when we subscribe (post-StartAsync open, the common case):
        //    Handled below after subscription registration — drain buffer + fire callback there.
        Action<int>? restoreFinishedHandler = null;
        if (onRestoreFinished is not null && db is not null)
        {
            int schemaPartition = SchemaLogPartition(db);
            restoreFinishedHandler = (partitionId) =>
            {
                if (partitionId != schemaPartition)
                    return;

                // Drain buffered entries that arrived before this subscriber registered
                // (mid-restore case: subscriber saw only the tail via OnLogRestored).
                List<byte[]>? buffered;
                bool shouldFire;
                lock (_walRestoreBufferLock)
                {
                    shouldFire = !_walRestoreDrainedPartitions.Contains(schemaPartition);
                    if (shouldFire)
                    {
                        _walRestoreBuffer.TryGetValue(schemaPartition, out buffered);
                        _walRestoreBuffer.Remove(schemaPartition);
                        _walRestoreDrainedPartitions.Add(schemaPartition);
                    }
                    else
                    {
                        buffered = null;
                    }
                }

                if (!shouldFire)
                    return;

                _ = Task.Run(async () =>
                {
                    if (buffered is not null)
                        foreach (byte[] entry in buffered)
                            await onRestore(schemaPartition, entry).ConfigureAwait(false);
                    await onRestoreFinished().ConfigureAwait(false);
                });
            };
            Raft.OnRestoreFinished += restoreFinishedHandler;
        }

        SchemaApplySubscription subscription = new(this, Raft, applyHandler, restoreHandler, restoreFinishedHandler);
        lock (schemaSubscriptionsSync)
            schemaSubscriptions.Add(subscription);

        // F1b late-subscriber replay (case B): if the schema partition's WAL restore already
        // completed before this subscriber registered (the common cluster startup order where
        // OpenDatabase runs after WaitForLeaderAsync), drain the buffer and fire onRestoreFinished
        // so the subscriber catches up to the WAL head.
        // Drain-once guard: if a previous open already consumed this partition's buffer, skip.
        if (db is not null)
        {
            int schemaPartition = SchemaLogPartition(db);
            List<byte[]>? buffered;
            bool shouldDrain;
            lock (_walRestoreBufferLock)
            {
                shouldDrain = _walRestoreCompletedPartitions.Contains(schemaPartition) &&
                              !_walRestoreDrainedPartitions.Contains(schemaPartition);
                if (shouldDrain)
                {
                    _walRestoreBuffer.TryGetValue(schemaPartition, out buffered);
                    _walRestoreBuffer.Remove(schemaPartition);
                    _walRestoreDrainedPartitions.Add(schemaPartition);
                }
                else
                {
                    buffered = null;
                }
            }

            if (shouldDrain)
            {
                // Replay buffered WAL restore entries then fire restore-finished.  Runs on a
                // separate task so RegisterSchemaApply returns promptly and the caller (e.g.
                // DatabaseOpener.LoadDatabase) can proceed.  The schema semaphore inside
                // RestoreAsync serialises each entry; a concurrent live OnReplicationReceived
                // delta that interleaves is handled by ApplyAsync's idempotency checks
                // (benign skip or loud "out of order" throw — not silent corruption).
                _ = Task.Run(async () =>
                {
                    if (buffered is not null)
                        foreach (byte[] entry in buffered)
                            await onRestore(schemaPartition, entry).ConfigureAwait(false);

                    if (onRestoreFinished is not null)
                        await onRestoreFinished().ConfigureAwait(false);
                });
            }
        }

        return subscription;
    }

    /// <summary>
    /// Subscribes internal buffer handlers so that schema-log WAL restore entries that fire
    /// before any <see cref="RegisterSchemaApply"/> subscriber is registered are captured and
    /// replayed to the late subscriber by <see cref="RegisterSchemaApply"/>. Called once from
    /// each constructor immediately after the <see cref="EmbeddedKahunaNode"/> is created.
    /// </summary>
    private void WireWalRestoreBuffer()
    {
        _walRestoreLogHandler = (partitionId, log) =>
        {
            if (log.LogType != SchemaChangeLogType || log.LogData is null)
                return Task.FromResult(true);

            lock (_walRestoreBufferLock)
            {
                // Stop buffering once a subscriber has already drained this partition
                // (_walRestoreDrainedPartitions is set when the buffer is consumed).
                if (_walRestoreDrainedPartitions.Contains(partitionId))
                    return Task.FromResult(true);

                if (!_walRestoreBuffer.TryGetValue(partitionId, out List<byte[]>? list))
                    _walRestoreBuffer[partitionId] = list = new();

                list.Add(log.LogData);
            }

            return Task.FromResult(true);
        };

        _walRestoreFinishedHandler = (partitionId) =>
        {
            lock (_walRestoreBufferLock)
                _walRestoreCompletedPartitions.Add(partitionId);
        };

        Raft.OnLogRestored += _walRestoreLogHandler;
        Raft.OnRestoreFinished += _walRestoreFinishedHandler;
    }

    public async ValueTask DisposeAsync()
    {
        // Unregister all event handlers before stopping the node so Raft's delegate chains
        // release references to this EmbeddedKahuna instance and its captured state. Without
        // this, the Raft object (inside EmbeddedKahunaNode) holds the handler delegates alive
        // via its event fields, keeping EmbeddedKahuna (and its _walRestoreBuffer / schema
        // subscriptions) reachable — and any SQLite connections they reference uncloseable.
        if (_walRestoreLogHandler is not null)
        {
            Raft.OnLogRestored -= _walRestoreLogHandler;
            _walRestoreLogHandler = null;
        }

        if (_walRestoreFinishedHandler is not null)
        {
            Raft.OnRestoreFinished -= _walRestoreFinishedHandler;
            _walRestoreFinishedHandler = null;
        }

        // Dispose all schema-apply subscriptions (unregisters OnReplicationReceived /
        // OnLogRestored / OnRestoreFinished handlers that capture DatabaseDescriptor objects).
        List<SchemaApplySubscription> subs;
        lock (schemaSubscriptionsSync)
        {
            subs = [.. schemaSubscriptions];
            schemaSubscriptions.Clear();
        }
        foreach (SchemaApplySubscription sub in subs)
            sub.Dispose();

        // Release WAL restore buffer memory.
        lock (_walRestoreBufferLock)
        {
            _walRestoreBuffer.Clear();
            _walRestoreCompletedPartitions.Clear();
            _walRestoreDrainedPartitions.Clear();
        }

        await node.DisposeAsync().ConfigureAwait(false);
    }

    // -----------------------------------------------------------------------

    private static SchemaReplicationOutcome ToOutcome(RaftOperationStatus status)
    {
        return status switch
        {
            RaftOperationStatus.NodeIsNotLeader or RaftOperationStatus.LeaderInOldTerm => SchemaReplicationOutcome.NotLeader,
            RaftOperationStatus.ProposalTimeout => SchemaReplicationOutcome.Timeout,
            _ => SchemaReplicationOutcome.Failed
        };
    }

    private async Task<SchemaReplicationResult> ReplicateSchemaChangeAsLeaderAsync(
        string db,
        int partitionId,
        string leader,
        byte[] entry,
        CancellationToken cancellationToken
    )
    {
        RaftReplicationResult proposal = await Raft.ReplicateLogs(
            partitionId,
            SchemaChangeLogType,
            entry,
            autoCommit: false,
            cancellationToken: cancellationToken
        ).ConfigureAwait(false);

        if (!proposal.Success)
            return new(ToOutcome(proposal.Status), partitionId, proposal.LogIndex, leader, proposal.Status.ToString());

        (bool committed, RaftOperationStatus status, long commitLogId) =
            await Raft.CommitLogs(partitionId, proposal.TicketId).ConfigureAwait(false);

        if (committed && status == RaftOperationStatus.Success)
        {
            await InvokeLocalSchemaApplyAsync(partitionId, entry).ConfigureAwait(false);

            return new(SchemaReplicationOutcome.Committed, partitionId, commitLogId, leader, status.ToString());
        }

        await Raft.RollbackLogs(partitionId, proposal.TicketId).ConfigureAwait(false);
        return new(ToOutcome(status), partitionId, proposal.LogIndex, leader, status.ToString());
    }

    private async Task InvokeLocalSchemaApplyAsync(int partitionId, byte[] entry)
    {
        List<SchemaApplySubscription> subscriptions;
        lock (schemaSubscriptionsSync)
            subscriptions = [.. schemaSubscriptions];

        foreach (SchemaApplySubscription subscription in subscriptions)
            await subscription.Apply(partitionId, entry).ConfigureAwait(false);
    }

    private void RemoveSchemaSubscription(SchemaApplySubscription subscription)
    {
        lock (schemaSubscriptionsSync)
            schemaSubscriptions.Remove(subscription);
    }

    private sealed class LeaderCallbackSubscription : IDisposable
    {
        private readonly IRaft raft;
        private readonly Func<int, string, Task<bool>> handler;
        private bool disposed;

        public LeaderCallbackSubscription(IRaft raft, Func<int, string, Task<bool>> handler)
        {
            this.raft = raft;
            this.handler = handler;
        }

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            raft.OnLeaderChanged -= handler;
        }
    }

    private sealed class SchemaApplySubscription : IDisposable
    {
        private readonly EmbeddedKahuna owner;
        private readonly IRaft raft;
        private readonly Func<int, RaftLog, Task<bool>> applyHandler;
        private readonly Func<int, RaftLog, Task<bool>> restoreHandler;
        private readonly Action<int>? restoreFinishedHandler;
        private bool disposed;

        public SchemaApplySubscription(
            EmbeddedKahuna owner,
            IRaft raft,
            Func<int, RaftLog, Task<bool>> applyHandler,
            Func<int, RaftLog, Task<bool>> restoreHandler,
            Action<int>? restoreFinishedHandler = null
        )
        {
            this.owner = owner;
            this.raft = raft;
            this.applyHandler = applyHandler;
            this.restoreHandler = restoreHandler;
            this.restoreFinishedHandler = restoreFinishedHandler;
        }

        public Task<bool> Apply(int partitionId, byte[] entry)
            => applyHandler(partitionId, new() { LogType = SchemaChangeLogType, LogData = entry });

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            owner.RemoveSchemaSubscription(this);
            raft.OnReplicationReceived -= applyHandler;
            raft.OnLogRestored -= restoreHandler;
            if (restoreFinishedHandler is not null)
                raft.OnRestoreFinished -= restoreFinishedHandler;
        }
    }

    private static EmbeddedKahunaOptions DefaultOptions() => new()
    {
        NodeName = "camusdb-embedded",
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    private static EmbeddedKahunaOptions PersistentOptions(string dataPath) => new()
    {
        NodeName = "camusdb-embedded",
        Storage = "rocksdb",
        StoragePath = System.IO.Path.Combine(dataPath, "kv"),
        WalStorage = "sqlite",
        WalPath = System.IO.Path.Combine(dataPath, "wal"),
        InitialPartitions = 1
    };

    private static EmbeddedKahunaOptions SqliteOptions(string dataPath) => new()
    {
        NodeName = "camusdb-embedded",
        Storage = "sqlite",
        StoragePath = System.IO.Path.Combine(dataPath, "kv"),
        StorageRevision = "v1",
        WalStorage = "sqlite",
        WalPath = System.IO.Path.Combine(dataPath, "wal"),
        WalRevision = "v1",
        InitialPartitions = 1
    };
}
