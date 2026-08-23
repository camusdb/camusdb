
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using CamusDB.Core.Config.Models;
using Kahuna.Shared.Communication.Rest;
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
    private const string SchemaChangeLogType = "SchemaChange";

    /// <summary>
    /// Raft log type carrying replicated cluster-setting changes. A separate consumer log type on
    /// the same generic <c>OnReplicationReceived</c> hook the schema log rides — dispatch is by
    /// <see cref="RaftLog.LogType"/>, so the two never see each other's entries.
    /// </summary>
    private const string ClusterSettingsLogType = "ClusterSettings";

    /// <summary>
    /// Prefix whose hash selects the single Raft partition that carries every cluster-settings log
    /// entry, mirroring how <c>{db}/meta</c> selects a database's schema-log partition. One
    /// partition means Raft commit order totally orders concurrent changes, which is what makes two
    /// conflicting <c>SET</c>s converge identically on every node.
    /// </summary>
    private const string ClusterSettingsPrefix = "_system/settings";

    private readonly SchemaAckTracker schemaAcks = new();

    private ISchemaAckSender? schemaAckSender;

    private readonly EmbeddedKahunaNode node;

    // True when this instance was constructed with an explicit IInterNodeCommunication (cluster mode).
    // False for standalone embedded nodes that use phantom EmbeddedRaftCommunication.Witnesses — those
    // witness nodes must not appear in the schema ack gate because they are never real schema participants.
    private readonly bool isClusterMode;

    private readonly Lock schemaSubscriptionsSync = new();
    private readonly List<SchemaApplySubscription> schemaSubscriptions = new();
    private ISchemaReplicationForwarder? schemaReplicationForwarder;

    // Late-subscriber buffer: WAL restore events (OnLogRestored / OnRestoreFinished) fire
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
    private readonly Lock _walRestoreBufferLock = new();
    private readonly Dictionary<int, List<byte[]>> _walRestoreBuffer = new();
    private readonly HashSet<int> _walRestoreCompletedPartitions = new();
    private readonly HashSet<int> _walRestoreDrainedPartitions = new();

    // Stored so DisposeAsync can unregister them from Raft's event chains.
    private Func<int, RaftLog, Task<bool>>? _walRestoreLogHandler;
    private Action<int>? _walRestoreFinishedHandler;

    // Settings-log twin of the schema WAL-restore buffer above, deliberately its own structures
    // rather than a rekeying of the schema buffer: the schema buffer filters hard on
    // SchemaChangeLogType, so without this a settings entry replayed from the WAL during restore —
    // exactly the window a node that was down during a change catches up in — would be silently
    // dropped. All settings traffic lives on one partition, so the buffer needs no per-partition
    // keying; completion is tracked for the settings partition alone.
    private readonly Lock _settingsRestoreLock = new();
    private readonly List<byte[]> _settingsRestoreBuffer = new();
    private bool _settingsRestoreCompleted;
    private bool _settingsRestoreDrained;
    private Func<int, RaftLog, Task<bool>>? _settingsRestoreLogHandler;
    private Action<int>? _settingsRestoreFinishedHandler;

    private readonly Lock settingsSubscriptionsSync = new();
    private readonly List<ClusterSettingsSubscription> settingsSubscriptions = [];

    // Completes when StartAsync has finished electing leaders for every partition (system + all data
    // partitions). Background startup work fired from constructors — the database-registry load, the
    // snapshot-hold renewer, and the orphan-branch scrub — awaits this before issuing any KV
    // operation: a hosted service can eagerly construct CommandExecutor during host startup, before
    // Program.cs calls StartAsync, and a request routed to a not-yet-created partition throws
    // Kommander's "Invalid partition". RunContinuationsAsynchronously so a resumed waiter never runs
    // inline on the thread that completes StartAsync.
    private readonly TaskCompletionSource startedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// The Kahuna KV API. Used by KvTableStore and the transaction layer.
    /// </summary>
    public IKahuna Kahuna => node.Kahuna;

    /// <summary>
    /// The Raft consensus handle. Exposed for partition/leader queries and HLC clock access.
    /// </summary>
    public IRaft Raft => node.Raft;

    /// <summary>
    /// True when this node was built through the cluster constructor (real inter-node
    /// transports and peers); false for the standalone embedded node. Placement and
    /// distributed-execution decisions branch on this — a standalone node never reads the
    /// range map or Raft placement because there is nothing to ask.
    /// </summary>
    public bool IsClusterMode => isClusterMode;

    /// <summary>
    /// How long a cached <see cref="TablePlacement"/> stays fresh. Short on purpose: placement
    /// is advisory (execution re-resolves through the locator), so the only cost of staleness
    /// is a mis-costed plan or an extra hop, and the read itself is a cheap in-memory compose.
    /// </summary>
    private const long PlacementCacheTtlMs = 2_000;

    /// <summary>Per-key-space placement snapshots; see <see cref="GetPlacement"/>.</summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, TablePlacement> placementCache = new();

    /// <summary>
    /// Returns the current placement of <paramref name="keySpace"/> (a bucket prefix with no
    /// trailing slash, e.g. <c>{dbId}:{tableId}:r</c>): its spans, owning partitions, and this
    /// node's best-effort view of each partition's leader and replica set.
    ///
    /// <para>Standalone nodes short-circuit to a single local span without touching Kahuna or
    /// Raft. Cluster nodes compose <c>IKahuna.GetRangeMap</c> (node-local applied snapshot)
    /// with <c>IRaft</c> placement reads — all cheap, synchronous, in-memory — and cache the
    /// result for <see cref="PlacementCacheTtlMs"/>. Callers that observe a generation fence
    /// miss or <c>MustRetry</c> at execution time should call <see cref="InvalidatePlacement"/>
    /// before re-resolving.</para>
    ///
    /// <para>Hash-routed key spaces resolve to exactly one partition, computed with the same
    /// hash Kahuna's data router uses: <c>1 + InversePrefixedHash(prefix + "/", '/',
    /// InitialPartitions)</c>. <c>IRaft.GetPrefixPartitionKey</c> is a different hash over a
    /// different partition map and must never be used for data keys.</para>
    /// </summary>
    public TablePlacement GetPlacement(string keySpace)
    {
        if (!isClusterMode)
            return placementCache.GetOrAdd(keySpace, static ks => TablePlacement.Local(ks));

        if (placementCache.TryGetValue(keySpace, out TablePlacement? cached)
            && Environment.TickCount64 - cached.CapturedAtTicks < PlacementCacheTtlMs)
            return cached;

        TablePlacement fresh = BuildPlacement(keySpace);
        placementCache[keySpace] = fresh;
        return fresh;
    }

    /// <summary>
    /// Drops the cached placement for <paramref name="keySpace"/> so the next
    /// <see cref="GetPlacement"/> rebuilds from the live range map — for callers that just
    /// observed evidence the cache is stale (generation fence miss, leader-moved retry).
    /// </summary>
    public void InvalidatePlacement(string keySpace) => placementCache.TryRemove(keySpace, out _);

    private TablePlacement BuildPlacement(string keySpace)
    {
        string localEndpoint = Raft.GetLocalEndpoint();

        // Filtered form on purpose: the unfiltered call enumerates every registered key space.
        KahunaRangeMapResponse map = node.Kahuna.GetRangeMap(keySpace);

        KahunaKeySpaceRangesResponse? space = null;
        foreach (KahunaKeySpaceRangesResponse candidate in map.KeySpaces)
        {
            if (string.Equals(candidate.KeySpace, keySpace, StringComparison.Ordinal))
            {
                space = candidate;
                break;
            }
        }

        bool isKeyRange = space is not null
            && string.Equals(space.RoutingMode, "KeyRange", StringComparison.Ordinal)
            && space.Descriptors.Count > 0;

        List<PlacementSpan> spans;

        if (isKeyRange)
        {
            spans = new(space!.Descriptors.Count);
            foreach (KahunaRangeDescriptorResponse descriptor in space.Descriptors)
                spans.Add(BuildSpan(localEndpoint, descriptor.StartKey, descriptor.EndKey, descriptor.PartitionId, descriptor.Generation));
        }
        else
        {
            // Hash routing — or a key-range space with no seeded descriptors, which the router
            // also serves through the hash path. One partition owns the whole bucket. The
            // trailing slash matters: the data router hashes the key-space slice before the
            // LAST '/', so appending one reproduces what routing does for keys in this bucket.
            int partitionId = 1 + (int)HashUtils.InversePrefixedHash(keySpace + "/", '/', Raft.Configuration.InitialPartitions);
            spans = [BuildSpan(localEndpoint, startKey: null, endKey: null, partitionId, generation: 0L)];
        }

        return new TablePlacement(keySpace, isKeyRange, spans, Environment.TickCount64);
    }

    private PlacementSpan BuildSpan(string localEndpoint, string? startKey, string? endKey, int partitionId, long generation)
    {
        bool hosted = Raft.HostsPartition(partitionId);
        string? leaderHint = Raft.GetPartitionLeaderHint(partitionId);

        // Empty replica list = legacy full replication (every roster node hosts the partition);
        // non-empty = the complete hosting set under per-partition replica placement. Nodes
        // being removed no longer count as dispatch targets.
        IReadOnlyList<Kommander.System.RaftReplica> replicas = Raft.GetPartitionReplicas(partitionId);
        List<string>? endpoints = null;

        foreach (Kommander.System.RaftReplica replica in replicas)
        {
            if (replica.Role != Kommander.System.RaftReplicaRole.Removing)
                (endpoints ??= new(replicas.Count)).Add(replica.Endpoint);
        }

        return new PlacementSpan(
            startKey,
            endKey,
            partitionId,
            generation,
            leaderHint,
            endpoints ?? (IReadOnlyList<string>)[],
            LeaderIsLocal: hosted && string.Equals(leaderHint, localEndpoint, StringComparison.Ordinal),
            HostedLocally: hosted);
    }

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
        WireSettingsRestoreBuffer();
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
        WireSettingsRestoreBuffer();
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
    public static EmbeddedKahuna CreateSqlite(string dataPath, CamusDBOptions options, ILoggerFactory? loggerFactory = null)
        => new(EmbeddedKahunaOptionsBuilder.BuildStandalone(dataPath, options.Kahuna, options), loggerFactory);

    /// <summary>
    /// Constructs the embedded engine backed by RocksDB for both KV and WAL at <paramref name="dataPath"/>.
    /// Suitable for production standalone deployments that need higher write throughput than SQLite.
    /// </summary>
    public static EmbeddedKahuna CreateRocksDb(string dataPath, CamusDBOptions options, ILoggerFactory? loggerFactory = null)
        => new(EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, options.Kahuna, options), loggerFactory);

    /// <summary>
    /// Starts the Raft cluster and waits for every partition (system + all data partitions) to elect
    /// a leader. Must be called once before any KV operations. On completion it signals
    /// <see cref="WaitUntilStartedAsync"/> so background startup work kicked off eagerly during
    /// construction can proceed; if start fails, that failure is propagated to those waiters too.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await node.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            startedSignal.TrySetException(ex);
            throw;
        }

        startedSignal.TrySetResult();
    }

    /// <summary>
    /// Completes once <see cref="StartAsync"/> has elected leaders for all partitions — or faults
    /// with the same exception StartAsync threw. Background startup tasks that issue KV operations
    /// (the database-registry load, the snapshot-hold renewer, the orphan-branch scrub) must await
    /// this first: they can begin before StartAsync is called, and issuing a request against a
    /// partition that does not exist yet throws Kommander's "Invalid partition". Returns immediately
    /// once the node has started.
    /// </summary>
    public Task WaitUntilStartedAsync(CancellationToken cancellationToken = default)
        => startedSignal.Task.WaitAsync(cancellationToken);

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
    /// True when the underlying node was started with a backup directory configured
    /// (<c>kahuna.backup_dir</c> → <see cref="EmbeddedKahunaOptions.BackupDir"/>). When false, every
    /// backup/PITR call below throws inside Kahuna, so callers gate on this and surface
    /// <see cref="CamusDBErrorCodes.BackupNotConfigured"/> instead of leaking a raw exception.
    /// </summary>
    public bool IsBackupConfigured => node.Kahuna.IsBackupConfigured;

    /// <summary>
    /// True when restore is permitted on this node: a server-owned restore root is configured (so
    /// destinations are confined to it) or an explicit unconfined opt-in is set. False by default —
    /// restore is administrative and denied unless deliberately enabled via <c>kahuna.restore_root</c>
    /// (or <c>kahuna.allow_unconfined_remote_restore</c>). The backup/restore admin surface gates on this.
    /// </summary>
    public bool IsRemoteRestoreAllowed => node.Kahuna.IsRemoteRestoreAllowed;

    /// <summary>
    /// Takes a full backup of the whole node now: a base image of the storage engine plus a manifest
    /// recording per-partition WAL coverage and checksums. Online — safe while the node serves traffic.
    /// Backups are node-wide (every CamusDB database lives in this one shared node), not per-database.
    /// </summary>
    public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken cancellationToken = default)
        => node.Kahuna.TakeFullBackupAsync(cancellationToken);

    /// <summary>
    /// Takes an incremental backup: the slice of WAL committed since <paramref name="parentBackupId"/>,
    /// linked to that parent to form a chain. Fails if the parent has fallen below the retention floor
    /// (a fresh full backup is then required). Online.
    /// </summary>
    public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken cancellationToken = default)
        => node.Kahuna.TakeIncrementalBackupAsync(parentBackupId, cancellationToken);

    /// <summary>
    /// Takes a cluster-wide coordinated full backup: every partition is capped at one consistent HLC
    /// cut. For the embedded single node this is effectively equivalent to a plain full backup (its
    /// quorum is formed with phantom witnesses); it matters for real multi-node clusters. Online.
    /// </summary>
    public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken cancellationToken = default)
        => node.Kahuna.TakeCoordinatedBackupAsync(cancellationToken);

    /// <summary>
    /// Lists every backup recorded in the configured backup directory's catalog. Online.
    /// </summary>
    public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken cancellationToken = default)
        => node.Kahuna.ListBackupsAsync(cancellationToken);

    /// <summary>
    /// Resolves and validates the backup chain ending at <paramref name="leafBackupId"/> (must start at
    /// a full backup, be contiguous, unbroken, and acyclic) and returns it root-first. Online.
    /// </summary>
    public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken cancellationToken = default)
        => node.Kahuna.GetBackupChainAsync(leafBackupId, cancellationToken);

    /// <summary>
    /// Offline restore: validates the chain ending at <paramref name="leafBackupId"/>, copies its base
    /// image into <paramref name="targetDir"/>, and replays WAL up to <paramref name="targetTimeMs"/>
    /// (Unix epoch milliseconds; <c>0</c> = chain max). Non-destructive to the live node — it only
    /// writes <paramref name="targetDir"/>; the operator then boots a fresh node whose storage path
    /// points at the restored image (see the backup/restore runbook for the RocksDB path shape).
    /// </summary>
    public Task<KahunaRestoreResponse> RestoreToAsync(Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken = default)
        => node.Kahuna.RestoreToAsync(leafBackupId, targetDir, targetTimeMs, cancellationToken);

    /// <summary>
    /// Reclaims backup disk: sweeps orphaned/leftover artifacts and enforces the retention policy. With
    /// <paramref name="dryRun"/> true it returns the inventory of what would be reclaimed without deleting
    /// anything. The same work also runs automatically after each backup and on the periodic GC tick;
    /// this is the on-demand operator entry point.
    /// </summary>
    public Task<KahunaBackupGcResult> RunBackupGarbageCollectionAsync(bool dryRun, CancellationToken cancellationToken = default)
        => node.Kahuna.RunBackupGarbageCollectionAsync(dryRun, cancellationToken);

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

    /// <summary>
    /// Returns true when this node is the Raft leader of the partition that owns <paramref name="key"/>.
    /// Used to elect a single node for process-wide singleton background work keyed on a stable KV key
    /// (e.g. the branch snapshot-hold renewer, elected on the database-registry key's partition), so
    /// the sweep runs on exactly one node and fails over with leadership. A standalone node leads every
    /// partition and always returns true.
    /// </summary>
    public async ValueTask<bool> AmILeaderForKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        int partitionId = Raft.GetPrefixPartitionKey(key);
        return await Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<string> WaitForSchemaLeaderAsync(string db, CancellationToken cancellationToken = default)
    {
        int partitionId = SchemaLogPartition(db);
        return await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Voluntarily steps down from leadership of the partition that owns <paramref name="key"/>,
    /// staying online as a follower. Companion to <see cref="AmILeaderForKeyAsync"/> for the same
    /// key-to-partition routing. Used to hand off ownership of a keyed responsibility (e.g. the
    /// registry-bucket key that gates background sweeps) without isolating the node's transport, so
    /// its in-flight reads keep working while its leadership check flips to false.
    /// </summary>
    public async Task StepDownForKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        int partitionId = Raft.GetPrefixPartitionKey(key);
        await Raft.StepDownAsync(partitionId, cancellationToken).ConfigureAwait(false);
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
    /// Called on persist exhaustion so a healthy peer can take over.
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
    /// Releases the schema-ack state for a database this node has stopped holding open, so the tracker
    /// tracks the databases in use rather than every database ever touched. See
    /// <see cref="SchemaAckTracker.Forget"/> for why this is safe to discard and re-derive.
    /// </summary>
    internal void ForgetSchemaAcks(string db) => schemaAcks.Forget(db);

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
    /// The set of nodes the schema ack gate must wait on. Always includes the local node.
    /// In cluster mode: local endpoint plus every active peer. In standalone mode: only the
    /// local endpoint — <c>GetNodes()</c> returns phantom witness nodes used by the embedded
    /// Raft for quorum and must not appear in the ack gate.
    /// <para>
    /// <b>Infinite lease (opt-in via config <c>-1</c>):</b> every configured Raft peer — the
    /// gate waits for every member, so a crashed-but-configured node freezes DDL until the ack
    /// timeout. Strictest (never false-evicts) but not live under a node failure.
    /// </para>
    /// <para>
    /// <b>Finite lease (the default — 30 s):</b> live peers are sourced from the leader's real Raft activity view
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
            // the set via Raft activity). The tracker must NOT also expire members on its
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

        // Late-subscriber replay (case B): if the schema partition's WAL restore already
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

    // ── Cluster-settings log ──────────────────────────────────────────────────────────────────
    //
    // A second consumer log type on the same generic Raft hooks the schema log rides. The shape
    // mirrors the schema plumbing (propose-with-explicit-commit, local fan-out after quorum,
    // late-subscriber WAL-restore buffer) but the machinery is deliberately separate: settings
    // traffic lives on one fixed partition, has exactly one subscriber (the cluster-settings
    // service), and must never perturb the heavily-exercised schema paths.

    /// <summary>
    /// Resolves the single Raft partition that carries every cluster-settings log entry — the same
    /// prefix-hash routing the schema log uses, so ordering comes from one partition's commit
    /// order. Unlike <see cref="SchemaLogPartition"/>, a resolution to the reserved partition 0
    /// cannot throw here: the settings partition must exist in every deployment, so 0 is remapped
    /// deterministically to partition 1 (which every layout has). The remap is a pure function of
    /// values all nodes share — the fixed prefix and the shared partition layout — so every node
    /// proposes onto the same partition.
    /// </summary>
    public int ClusterSettingsLogPartition()
    {
        int partitionId = Raft.GetPrefixPartitionKey(ClusterSettingsPrefix);
        return partitionId == 0 ? 1 : partitionId;
    }

    /// <summary>
    /// Whether this node currently leads the cluster-settings partition — the node that may
    /// propose setting changes. Followers forward to the leader instead.
    /// </summary>
    public async Task<bool> AmIClusterSettingsLeaderAsync(CancellationToken cancellationToken = default)
        => await Raft.AmILeader(ClusterSettingsLogPartition(), cancellationToken).ConfigureAwait(false);

    /// <summary>Resolves the current leader endpoint of the cluster-settings partition.</summary>
    public async Task<string> WaitForClusterSettingsLeaderAsync(CancellationToken cancellationToken = default)
        => await Raft.WaitForLeader(ClusterSettingsLogPartition(), cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Proposes one cluster-settings entry on the settings partition and applies it locally after
    /// the quorum commit. Leader-only: callers that are not the settings leader get
    /// <see cref="SchemaReplicationOutcome.NotLeader"/> back and must forward the request at the
    /// command layer (over the authenticated internal route), mirroring how DDL forwards — there
    /// is deliberately no raw-entry forwarding here.
    /// </summary>
    public async Task<SchemaReplicationResult> ReplicateClusterSettingsChangeAsync(
        byte[] entry, 
        CancellationToken cancellationToken = default
    )
    {
        int partitionId = ClusterSettingsLogPartition();
        string leader = await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);

        if (!await Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return new(SchemaReplicationOutcome.NotLeader, partitionId, leader: leader);

        RaftReplicationResult proposal = await Raft.ReplicateLogs(
            partitionId,
            ClusterSettingsLogType,
            entry,
            autoCommit: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!proposal.Success)
            return new(ToOutcome(proposal.Status), partitionId, proposal.LogIndex, leader, proposal.Status.ToString());

        (bool committed, RaftOperationStatus status, long commitLogId) =
            await Raft.CommitLogs(partitionId, proposal.TicketId, cancellationToken).ConfigureAwait(false);

        if (committed && status == RaftOperationStatus.Success)
        {
            await InvokeLocalClusterSettingsApplyAsync(partitionId, entry).ConfigureAwait(false);
            return new(SchemaReplicationOutcome.Committed, partitionId, commitLogId, leader, status.ToString());
        }

        await Raft.RollbackLogs(partitionId, proposal.TicketId, cancellationToken).ConfigureAwait(false);
        return new(ToOutcome(status), partitionId, proposal.LogIndex, leader, status.ToString());
    }

    /// <summary>
    /// Subscribes the cluster-settings service to committed and WAL-restored settings entries,
    /// with the same late-subscriber catch-up the schema log has: entries restored before the
    /// service registered are buffered by <see cref="WireSettingsRestoreBuffer"/> and replayed
    /// here, then <paramref name="onRestoreFinished"/> fires exactly once. Without that replay a
    /// node that was down during a change and received it during WAL restore would silently miss
    /// it — the exact window the settings feature's catch-up guarantee covers.
    /// </summary>
    public IDisposable RegisterClusterSettingsApply(
        Func<int, byte[], Task<bool>> onApply,
        Func<int, byte[], Task<bool>> onRestore,
        Func<Task>? onRestoreFinished = null)
    {
        ArgumentNullException.ThrowIfNull(onApply);
        ArgumentNullException.ThrowIfNull(onRestore);

        // Called inline rather than via Task.Run for the same starvation reason RegisterSchemaApply
        // documents: Kommander awaits these from its Nixie actor thread.
        Func<int, RaftLog, Task<bool>> applyHandler = async (partitionId, log) =>
        {
            if (log.LogType != ClusterSettingsLogType)
                return true;

            return await onApply(partitionId, log.LogData ?? []).ConfigureAwait(false);
        };

        Func<int, RaftLog, Task<bool>> restoreHandler = async (partitionId, log) =>
        {
            if (log.LogType != ClusterSettingsLogType)
                return true;

            return await onRestore(partitionId, log.LogData ?? []).ConfigureAwait(false);
        };

        Raft.OnReplicationReceived += applyHandler;
        Raft.OnLogRestored += restoreHandler;

        int settingsPartition = ClusterSettingsLogPartition();

        // Mid-restore case: restore has not finished when the service subscribes; drain when it does.
        Action<int>? restoreFinishedHandler = null;
        if (onRestoreFinished is not null)
        {
            restoreFinishedHandler = (partitionId) =>
            {
                if (partitionId != settingsPartition)
                    return;

                List<byte[]>? buffered = DrainSettingsRestoreBuffer();
                if (buffered is null)
                    return;

                _ = Task.Run(async () =>
                {
                    foreach (byte[] entry in buffered)
                        await onRestore(settingsPartition, entry).ConfigureAwait(false);
                    await onRestoreFinished().ConfigureAwait(false);
                });
            };
            Raft.OnRestoreFinished += restoreFinishedHandler;
        }

        ClusterSettingsSubscription subscription = new(this, Raft, applyHandler, restoreHandler, restoreFinishedHandler);
        lock (settingsSubscriptionsSync)
            settingsSubscriptions.Add(subscription);

        // Post-restore case (the common startup order): the settings partition already finished
        // restoring — drain the buffer now and fire onRestoreFinished so the subscriber catches up.
        bool alreadyCompleted;
        lock (_settingsRestoreLock)
            alreadyCompleted = _settingsRestoreCompleted && !_settingsRestoreDrained;

        if (alreadyCompleted)
        {
            List<byte[]>? buffered = DrainSettingsRestoreBuffer();
            if (buffered is not null)
            {
                _ = Task.Run(async () =>
                {
                    foreach (byte[] entry in buffered)
                        await onRestore(settingsPartition, entry).ConfigureAwait(false);

                    if (onRestoreFinished is not null)
                        await onRestoreFinished().ConfigureAwait(false);
                });
            }
        }

        return subscription;
    }

    /// <summary>
    /// Consumes the buffered settings-restore entries exactly once; null when another subscriber
    /// already drained them (the drain-once guard that keeps a re-subscribe from replaying).
    /// </summary>
    private List<byte[]>? DrainSettingsRestoreBuffer()
    {
        lock (_settingsRestoreLock)
        {
            if (_settingsRestoreDrained)
                return null;

            _settingsRestoreDrained = true;
            List<byte[]> buffered = [.. _settingsRestoreBuffer];
            _settingsRestoreBuffer.Clear();
            return buffered;
        }
    }

    /// <summary>
    /// Buffers cluster-settings WAL-restore entries that fire before the settings service has
    /// subscribed, exactly as <see cref="WireWalRestoreBuffer"/> does for the schema log. Wired
    /// once from each constructor.
    /// </summary>
    private void WireSettingsRestoreBuffer()
    {
        _settingsRestoreLogHandler = (partitionId, log) =>
        {
            if (log.LogType != ClusterSettingsLogType || log.LogData is null)
                return Task.FromResult(true);

            lock (_settingsRestoreLock)
            {
                if (_settingsRestoreDrained)
                    return Task.FromResult(true);

                _settingsRestoreBuffer.Add(log.LogData);
            }

            return Task.FromResult(true);
        };

        // Completion is recorded for every partition rather than resolving the settings partition
        // here: partition routing is not queryable until the node starts, and recording a boolean
        // per event is harmless — the drain paths check the settings partition themselves.
        _settingsRestoreFinishedHandler = (partitionId) =>
        {
            lock (_settingsRestoreLock)
                _settingsRestoreCompleted = true;
        };

        Raft.OnLogRestored += _settingsRestoreLogHandler;
        Raft.OnRestoreFinished += _settingsRestoreFinishedHandler;
    }

    /// <summary>
    /// Fans one committed settings entry out to the local subscribers, exactly as
    /// <see cref="InvokeLocalSchemaApplyAsync"/> does for schema deltas: the proposer applies
    /// locally only after the quorum commit, through the same filtered handler live replication
    /// uses.
    /// </summary>
    private async Task InvokeLocalClusterSettingsApplyAsync(int partitionId, byte[] entry)
    {
        List<ClusterSettingsSubscription> subscriptions;
        lock (settingsSubscriptionsSync)
            subscriptions = [.. settingsSubscriptions];

        foreach (ClusterSettingsSubscription subscription in subscriptions)
            await subscription.Apply(partitionId, entry).ConfigureAwait(false);
    }

    internal void RemoveSettingsSubscription(ClusterSettingsSubscription subscription)
    {
        lock (settingsSubscriptionsSync)
            settingsSubscriptions.Remove(subscription);
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

        if (_settingsRestoreLogHandler is not null)
        {
            Raft.OnLogRestored -= _settingsRestoreLogHandler;
            _settingsRestoreLogHandler = null;
        }

        if (_settingsRestoreFinishedHandler is not null)
        {
            Raft.OnRestoreFinished -= _settingsRestoreFinishedHandler;
            _settingsRestoreFinishedHandler = null;
        }

        // Same reference-release reasoning as the schema subscriptions below.
        List<ClusterSettingsSubscription> settingsSubs;
        lock (settingsSubscriptionsSync)
        {
            settingsSubs = [.. settingsSubscriptions];
            settingsSubscriptions.Clear();
        }
        foreach (ClusterSettingsSubscription sub in settingsSubs)
            sub.Dispose();

        lock (_settingsRestoreLock)
            _settingsRestoreBuffer.Clear();

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
            await Raft.CommitLogs(partitionId, proposal.TicketId, cancellationToken).ConfigureAwait(false);

        if (committed && status == RaftOperationStatus.Success)
        {
            await InvokeLocalSchemaApplyAsync(partitionId, entry).ConfigureAwait(false);

            return new(SchemaReplicationOutcome.Committed, partitionId, commitLogId, leader, status.ToString());
        }

        await Raft.RollbackLogs(partitionId, proposal.TicketId, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Settings-log twin of <see cref="SchemaApplySubscription"/>: unhooks the Raft event handlers
    /// on dispose, and lets the proposer's local apply reuse the same filtered handler live
    /// replication uses by synthesizing a <see cref="RaftLog"/> of the settings log type.
    /// </summary>
    internal sealed class ClusterSettingsSubscription : IDisposable
    {
        private readonly EmbeddedKahuna owner;
        private readonly IRaft raft;
        private readonly Func<int, RaftLog, Task<bool>> applyHandler;
        private readonly Func<int, RaftLog, Task<bool>> restoreHandler;
        private readonly Action<int>? restoreFinishedHandler;
        private bool disposed;

        public ClusterSettingsSubscription(
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
            => applyHandler(partitionId, new() { LogType = ClusterSettingsLogType, LogData = entry });

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            owner.RemoveSettingsSubscription(this);
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
        StorageRevision = "v1",
        WalStorage = "rocksdb",
        WalPath = System.IO.Path.Combine(dataPath, "wal"),
        WalRevision = "v1",
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
