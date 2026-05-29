
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
    private readonly EmbeddedKahunaNode node;

    /// <summary>
    /// The Kahuna KV API. Used by KvTableStore and the transaction layer.
    /// </summary>
    public IKahuna Kahuna => node.Kahuna;

    /// <summary>
    /// The Raft consensus handle. Exposed for partition/leader queries and HLC clock access.
    /// </summary>
    public IRaft Raft => node.Raft;

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

    public ValueTask DisposeAsync() => node.DisposeAsync();

    // -----------------------------------------------------------------------

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
