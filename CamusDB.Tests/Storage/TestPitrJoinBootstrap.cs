/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Seeding a joining node from a backup chain, which lets it present to the leader at the restored
/// index instead of at index 0. Drives the operator runbook up to the join: back up a RocksDB node,
/// restore the chain into a data root, check that root, and run the bootstrap on a node built over it.
/// The join itself, and the leader sending a delta instead of a snapshot, is Kahuna's behaviour and
/// is covered there; this fixture proves CamusDB reaches it with the right inputs and refuses every
/// state in which the seed would describe a store that is not on disk.
/// </summary>
[TestFixture]
// Serial: boots RocksDB-backed nodes, which are too heavy to run alongside other node-booting fixtures.
[NonParallelizable]
public sealed class TestPitrJoinBootstrap
{
    private const int Partitions = 2;

    private string root = "";

    [SetUp]
    public void SetUp()
    {
        root = Path.Combine(Path.GetTempPath(), "camus-pitrjoin-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(root);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }

    private const string ClusterId = "pitrjoin-cluster";

    private string MacKeyFile()
    {
        string path = Path.Combine(root, "mac.key");
        if (!File.Exists(path))
            File.WriteAllBytes(path, System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
        return path;
    }

    private static EmbeddedKahunaOptions RocksDbOptions(
        string dataDir, string backupDir, string restoreRoot = "", string macKeyFile = "", string clusterId = "") => new EmbeddedKahunaOptions
    {
        BackupMacKeyFile = macKeyFile,
        BackupClusterId = clusterId,
        NodeName = "pitrjoin-" + Guid.NewGuid().ToString("n"),
        Host = "localhost",
        Port = 7070,
        InitialPartitions = Partitions,
        Storage = "rocksdb",
        StoragePath = Path.Combine(dataDir, "kv"),
        StorageRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision,
        WalStorage = "rocksdb",
        WalPath = Path.Combine(dataDir, "wal"),
        WalRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision,
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        BackupDir = backupDir,
        RestoreRoot = restoreRoot,
    }.WithTestNodeDefaults();

    private static EmbeddedKahunaOptions MemoryJoinerOptions(string backupDir) => new EmbeddedKahunaOptions
    {
        NodeName = "pitrjoin-" + Guid.NewGuid().ToString("n"),
        Host = "localhost",
        Port = 7071,
        InitialPartitions = Partitions,
        Storage = "memory",
        WalStorage = "memory",
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        BackupDir = backupDir,
        JoinExistingSeeds = ["localhost:7070"],
    }.WithTestNodeDefaults();

    /// <summary>
    /// A node built with the cluster constructor, as a joiner is in production. It is never started
    /// here: starting would contact the seed, and the bootstrap's whole point is to run before that.
    /// </summary>
    private static EmbeddedKahuna Joiner(EmbeddedKahunaOptions options)
    {
        options.JoinExistingSeeds ??= ["localhost:7070"];
        return new EmbeddedKahuna(
            options,
            new MemoryInterNodeCommmunication(),
            new InMemoryCommunication(),
            new StaticDiscovery([new RaftNode("localhost:7070")]));
    }

    private static async Task WriteKeyAsync(EmbeddedKahuna node, string key, string value)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type);
        await node.FlushAsync();
    }

    /// <summary>
    /// Backs up a RocksDB node (a full backup and an incremental on top of it) and restores the chain
    /// into a data root exactly as <c>POST /v1/restore</c> lays it out. Returns the data root, the leaf
    /// backup id, and the highest WAL index the source reached on each data partition.
    /// </summary>
    private async Task<(string DataRoot, Guid Leaf, Dictionary<int, long> SourceMaxLog)> BackUpAndRestoreAsync(
        string backupDir, string macKeyFile, string clusterId)
    {
        string restoreRoot = Path.Combine(root, "restores");
        Directory.CreateDirectory(restoreRoot);

        await using EmbeddedKahuna source = new(RocksDbOptions(Path.Combine(root, "source"), backupDir, restoreRoot, macKeyFile, clusterId));
        await source.StartAsync(CancellationToken.None);
        await source.WaitForLeaderAsync("warmup", CancellationToken.None);
        await source.FlushAsync();

        for (int i = 0; i < 20; i++)
            await WriteKeyAsync(source, $"pitrjoin/before/{i}", "v" + i);

        KahunaBackupInfo full = await source.TakeFullBackupAsync();

        for (int i = 0; i < 20; i++)
            await WriteKeyAsync(source, $"pitrjoin/after/{i}", "w" + i);

        KahunaBackupInfo incremental = await source.TakeIncrementalBackupAsync(full.BackupId);

        Dictionary<int, long> sourceMaxLog = [];
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            sourceMaxLog[partitionId] = source.Raft.WalAdapter.GetMaxLog(partitionId);

        string dataRoot = Path.Combine(restoreRoot, "restored");
        await source.RestoreToAsync(incremental.BackupId, Path.Combine(dataRoot, "kv"), 0);
        Directory.CreateDirectory(Path.Combine(dataRoot, "wal"));

        return (dataRoot, incremental.BackupId, sourceMaxLog);
    }

    [Test]
    public async Task RestoredDataRoot_IsSeededAtTheRestoredIndex_AndRefusedOnceUsed()
    {
        // Signed and stamped manifests, read back by a joiner with the same key and cluster id: the
        // configuration a production cluster with authenticated backups runs.
        string backupDir = Path.Combine(root, "backups");
        (string dataRoot, Guid leaf, Dictionary<int, long> sourceMaxLog) = await BackUpAndRestoreAsync(backupDir, MacKeyFile(), ClusterId);

        EmbeddedKahunaOptions options = RocksDbOptions(dataRoot, backupDir, macKeyFile: MacKeyFile(), clusterId: ClusterId);

        // The restored root holds the image and an empty WAL, so the directory check passes.
        EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options);

        List<long> checkpoints = [];
        await using (EmbeddedKahuna joiner = Joiner(options))
        {
            for (int partitionId = 0; partitionId <= Partitions; partitionId++)
                Assert.That(joiner.Raft.WalAdapter.GetMaxLog(partitionId), Is.EqualTo(0),
                    "a restored data root presents at index 0 before the bootstrap");

            await joiner.BootstrapFromBackupAsync(leaf, 0);

            // The seed is one committed checkpoint per partition at the index the chain reached, which
            // is what Raft reports to the leader when the node joins.
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            {
                List<RaftLog> logs = joiner.Raft.WalAdapter.ReadLogs(partitionId);
                long seededIndex = joiner.Raft.WalAdapter.GetMaxLog(partitionId);

                Assert.That(logs.All(l => l.Type == RaftLogType.CommittedCheckpoint), Is.True,
                    $"partition {partitionId} must hold only the seeded checkpoint");
                Assert.That(seededIndex, Is.LessThanOrEqualTo(sourceMaxLog[partitionId]),
                    $"partition {partitionId} must not be seeded past the index the source reached");
                checkpoints.Add(seededIndex);
            }

            Assert.That(checkpoints.Any(c => c > 0), Is.True,
                "the bootstrap must seed a checkpoint at the restored index, or the node joins at 0 and is re-seeded by snapshot");

            CamusDBException again = Assert.ThrowsAsync<CamusDBException>(() => joiner.BootstrapFromBackupAsync(leaf, 0))!;
            Assert.That(again.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        }

        // The same root, now with a seeded WAL, is what a restart with the key left in config finds.
        CamusDBException used = Assert.Throws<CamusDBException>(() => EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options))!;
        Assert.That(used.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("empty WAL", used.Message);
    }

    [Test]
    public async Task ChainOfAnotherCluster_Refused_AndTheNodeDoesNotStart()
    {
        string backupDir = Path.Combine(root, "backups");
        (string dataRoot, Guid leaf, _) = await BackUpAndRestoreAsync(backupDir, macKeyFile: "", clusterId: "another-cluster");

        EmbeddedKahunaOptions options = RocksDbOptions(dataRoot, backupDir, clusterId: ClusterId);
        EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options);

        await using EmbeddedKahuna joiner = Joiner(options);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => joiner.BootstrapFromBackupAsync(leaf, 0))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains(nameof(KahunaBackupOutcome.CorruptChain), ex.Message);

        for (int partitionId = 0; partitionId <= Partitions; partitionId++)
            Assert.That(joiner.Raft.WalAdapter.GetMaxLog(partitionId), Is.EqualTo(0),
                "a refused chain must leave the WAL as the restore left it");

        CamusDBException start = Assert.ThrowsAsync<CamusDBException>(() => joiner.StartAsync(CancellationToken.None))!;
        Assert.That(start.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
    }

    [Test]
    public void DataRootWithoutTheRestoredImage_Refused()
    {
        EmbeddedKahunaOptions options = RocksDbOptions(Path.Combine(root, "fresh"), Path.Combine(root, "backups"));

        CamusDBException missing = Assert.Throws<CamusDBException>(() => EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options))!;
        Assert.That(missing.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("missing or empty", missing.Message);

        Directory.CreateDirectory(Path.Combine(options.StoragePath, options.StorageRevision));
        CamusDBException empty = Assert.Throws<CamusDBException>(() => EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options))!;
        StringAssert.Contains("missing or empty", empty.Message);
    }

    [Test]
    public void DataRootWithAUsedWal_Refused()
    {
        EmbeddedKahunaOptions options = RocksDbOptions(Path.Combine(root, "used"), Path.Combine(root, "backups"));

        string store = Path.Combine(options.StoragePath, options.StorageRevision);
        Directory.CreateDirectory(store);
        File.WriteAllText(Path.Combine(store, "CURRENT"), "");

        string wal = Path.Combine(options.WalPath, options.WalRevision);
        Directory.CreateDirectory(wal);
        File.WriteAllText(Path.Combine(wal, "CURRENT"), "");

        CamusDBException ex = Assert.Throws<CamusDBException>(() => EmbeddedKahuna.EnsureDataRootReadyForBootstrap(options))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("empty WAL", ex.Message);
    }

    [Test]
    public void NonRocksDbDataRoot_Refused()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => EmbeddedKahuna.EnsureDataRootReadyForBootstrap(MemoryJoinerOptions(Path.Combine(root, "backups"))))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("RocksDB", ex.Message);
    }

    [Test]
    public async Task NodeThatDoesNotJoin_Refused()
    {
        EmbeddedKahunaOptions options = MemoryJoinerOptions(Path.Combine(root, "backups"));
        options.JoinExistingSeeds = null;

        await using EmbeddedKahuna standalone = new(options);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => standalone.BootstrapFromBackupAsync(Guid.NewGuid(), 0))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("join", ex.Message);
    }

    [Test]
    public async Task WalWithEntries_Refused_AndTheNodeDoesNotStart()
    {
        await using EmbeddedKahuna joiner = Joiner(MemoryJoinerOptions(Path.Combine(root, "backups")));

        RaftOperationStatus written = joiner.Raft.WalAdapter.Write(
            [(1, [new RaftLog { Id = 5, Term = 1, Type = RaftLogType.Committed, Time = HLCTimestamp.Zero }])]);
        Assert.That(written, Is.EqualTo(RaftOperationStatus.Success));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => joiner.BootstrapFromBackupAsync(Guid.NewGuid(), 0))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        StringAssert.Contains("empty WAL", ex.Message);

        CamusDBException start = Assert.ThrowsAsync<CamusDBException>(() => joiner.StartAsync(CancellationToken.None))!;
        Assert.That(start.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
    }

    [Test]
    public async Task FailedBootstrap_BlocksStart()
    {
        string backupDir = Path.Combine(root, "backups");
        Directory.CreateDirectory(backupDir);

        await using EmbeddedKahuna joiner = Joiner(MemoryJoinerOptions(backupDir));

        // No backup with this id exists in the catalog, so the chain cannot be resolved.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => joiner.BootstrapFromBackupAsync(Guid.NewGuid(), 0))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));

        // Start is refused without contacting the seed, and waiters see the same failure.
        CamusDBException start = Assert.ThrowsAsync<CamusDBException>(() => joiner.StartAsync(CancellationToken.None))!;
        Assert.That(start.Code, Is.EqualTo(CamusDBErrorCodes.PitrBootstrapFailed));
        Assert.ThrowsAsync<CamusDBException>(() => joiner.WaitUntilStartedAsync(CancellationToken.None));
    }
}
