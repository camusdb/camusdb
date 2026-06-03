
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

    private static readonly SchemaAckTracker SchemaAcks = new();

    private readonly EmbeddedKahunaNode node;

    private readonly object schemaSubscriptionsSync = new();
    private readonly List<SchemaApplySubscription> schemaSubscriptions = new();
    private ISchemaReplicationForwarder? schemaReplicationForwarder;

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
    /// Live-node expiry used by the schema ack gate. The default is non-expiring because
    /// this layer does not yet receive real heartbeat/membership failure signals.
    /// </summary>
    public TimeSpan SchemaAckLiveNodeLease { get; set; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// Constructs the embedded engine with the provided options.
    /// </summary>
    public EmbeddedKahuna(EmbeddedKahunaOptions options, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        node = new EmbeddedKahunaNode(options, loggerFactory);
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

        return new EmbeddedKahuna(
            options,
            new GrpcInterNodeCommunication(new KahunaConfiguration()),
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
        => new(SqliteOptions(dataPath), loggerFactory);

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
    /// ordered. Throws if it resolves to the reserved partition 0. See architecture doc §5.1.
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
    /// Replicates a serialized <see cref="Catalogs.Models.SchemaChangeLogEntry"/> for a database.
    /// If this node is the schema leader it proposes → commits → fans the entry out to local
    /// apply subscribers (<c>autoCommit: false</c> so apply runs only after the quorum commit).
    /// If it is not the leader it forwards the raw entry through the internal test-only
    /// <c>ISchemaReplicationForwarder</c> (production uses the command-layer ticket forwarder),
    /// then applies locally on a committed result. See architecture doc §5.2 / §5.3.
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

    public void RegisterLocalSchemaAckNode(string db)
    {
        SchemaAcks.Register(db, Raft.GetLocalNodeName());
    }

    public void UnregisterLocalSchemaAckNode(string db)
    {
        SchemaAcks.Unregister(db, Raft.GetLocalNodeName());
    }

    public void RecordLocalSchemaApplied(string db, long schemaVersion)
    {
        SchemaAcks.RecordApplied(db, Raft.GetLocalNodeName(), schemaVersion);
    }

    public Task<bool> WaitForSchemaAcksAsync(
        string db,
        long schemaVersion,
        TimeSpan timeout,
        TimeSpan? liveNodeLease = null,
        CancellationToken cancellationToken = default
    )
    {
        return SchemaAcks.WaitForAllLiveAsync(
            db,
            schemaVersion,
            timeout,
            liveNodeLease ?? SchemaAckLiveNodeLease,
            cancellationToken
        );
    }

    public IDisposable RegisterSchemaApply(
        Func<int, byte[], Task<bool>> onApply,
        Func<int, byte[], Task<bool>> onRestore
    )
    {
        ArgumentNullException.ThrowIfNull(onApply);
        ArgumentNullException.ThrowIfNull(onRestore);

        Func<int, RaftLog, Task<bool>> applyHandler = (partitionId, log) =>
        {
            if (log.LogType != SchemaChangeLogType)
                return Task.FromResult(true);

            return Task.Run(() => onApply(partitionId, log.LogData ?? []));
        };

        Func<int, RaftLog, Task<bool>> restoreHandler = (partitionId, log) =>
        {
            if (log.LogType != SchemaChangeLogType)
                return Task.FromResult(true);

            return Task.Run(() => onRestore(partitionId, log.LogData ?? []));
        };

        Raft.OnReplicationReceived += applyHandler;
        Raft.OnLogRestored += restoreHandler;

        SchemaApplySubscription subscription = new(this, Raft, applyHandler, restoreHandler);
        lock (schemaSubscriptionsSync)
            schemaSubscriptions.Add(subscription);

        return subscription;
    }

    public ValueTask DisposeAsync() => node.DisposeAsync();

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

    private sealed class SchemaApplySubscription : IDisposable
    {
        private readonly EmbeddedKahuna owner;
        private readonly IRaft raft;
        private readonly Func<int, RaftLog, Task<bool>> applyHandler;
        private readonly Func<int, RaftLog, Task<bool>> restoreHandler;
        private bool disposed;

        public SchemaApplySubscription(
            EmbeddedKahuna owner,
            IRaft raft,
            Func<int, RaftLog, Task<bool>> applyHandler,
            Func<int, RaftLog, Task<bool>> restoreHandler
        )
        {
            this.owner = owner;
            this.raft = raft;
            this.applyHandler = applyHandler;
            this.restoreHandler = restoreHandler;
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
