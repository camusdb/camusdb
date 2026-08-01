/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Exercises the backup/PITR passthrough on <see cref="EmbeddedKahuna"/> that CamusDB's admin API is
/// built on: the not-configured gate, and a full round trip (take full → list → validate chain →
/// offline restore into a fresh directory).
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestEmbeddedKahunaBackup
{
    private static string NewTempDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), "camusdb-backup-test", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static EmbeddedKahuna NewNode(string? backupDir)
        => new(new EmbeddedKahunaOptions
        {
            NodeName = "camusdb-backup-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            BackupDir = backupDir ?? "",
        }.WithFastTestTimers());

    // Persistent node laid out exactly as CamusDB's standalone builder does for a given data root:
    // storage under {dataRoot}/kv, WAL under {dataRoot}/wal. Used to prove a restored data root actually
    // boots. Uses the SQLite backend so two nodes can open sequentially within one test process — RocksDB
    // holds a per-directory OS lock for the process lifetime, which only a separate restart process (the
    // real operator flow) releases; the {dataRoot}/kv layout under test is identical for both backends.
    private static EmbeddedKahuna NewPersistentNode(string dataRoot, string? backupDir)
        => new(new EmbeddedKahunaOptions
        {
            NodeName = "camusdb-backup-test",
            Storage = "sqlite",
            StoragePath = Path.Combine(dataRoot, "kv"),
            StorageRevision = "v1",
            WalStorage = "sqlite",
            WalPath = Path.Combine(dataRoot, "wal"),
            WalRevision = "v1",
            InitialPartitions = 1,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            BackupDir = backupDir ?? "",
        }.WithFastTestTimers());

    [Test]
    public async Task IsBackupConfigured_FalseWhenNoBackupDir_AndCallsThrow()
    {
        await using EmbeddedKahuna node = NewNode(backupDir: null);
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);

        Assert.IsFalse(node.IsBackupConfigured, "Backups must be disabled when no backup dir is set");
        Assert.ThrowsAsync<InvalidOperationException>(async () => await node.TakeFullBackupAsync());
    }

    [Test]
    public async Task FullBackup_List_Chain_Restore_RoundTrip()
    {
        string backupDir = NewTempDir();
        string restoreDir = NewTempDir();
        Directory.Delete(restoreDir, recursive: true); // 0.9.3 restore refuses an existing/overlapping destination

        await using (EmbeddedKahuna node = NewNode(backupDir))
        {
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None);

            Assert.IsTrue(node.IsBackupConfigured, "Backups must be enabled when a backup dir is set");

            // Write something so the backup has data to cover.
            (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "backup/k1", Encoding.UTF8.GetBytes("v1"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Set, setType);

            await node.FlushAsync();

            KahunaBackupInfo full = await node.TakeFullBackupAsync();
            Assert.AreNotEqual(Guid.Empty, full.BackupId);

            IReadOnlyList<KahunaBackupInfo> all = await node.ListBackupsAsync();
            Assert.IsTrue(all.Any(b => b.BackupId == full.BackupId), "Full backup must appear in the catalog");

            IReadOnlyList<KahunaBackupInfo> chain = await node.GetBackupChainAsync(full.BackupId);
            Assert.IsNotEmpty(chain);
            Assert.AreEqual(full.BackupId, chain[0].BackupId, "Chain must start at the full backup");

            KahunaRestoreResponse restore = await node.RestoreToAsync(full.BackupId, restoreDir, targetTimeMs: 0);
            Assert.AreEqual(restoreDir, restore.TargetDir);
            Assert.IsNotEmpty(restore.Chain);
        }

        // The restore wrote into a fresh directory, separate from the live backup dir.
        Assert.IsTrue(Directory.Exists(restoreDir));
    }

    /// <summary>
    /// End-to-end proof of the restore data-root layout: back up a persistent RocksDB node, restore into
    /// a fresh data root laid out the way <c>BackupManager.Restore</c> does ({dataRoot}/kv for storage +
    /// an empty {dataRoot}/wal), then boot a brand-new node with that data root and read the value back.
    /// This is the check that would have caught returning Kahuna's inner checkpoint path (unusable as a
    /// data_dir) instead of a bootable data root.
    /// </summary>
    [Test]
    public async Task RestoredDataRoot_BootsAFreshNode()
    {
        string liveRoot = NewTempDir();
        string backupDir = NewTempDir();
        string dataRoot = NewTempDir();
        Directory.Delete(dataRoot, recursive: true); // must be fresh/empty for the restore

        const string key = "backup/boot-k1";
        byte[] value = Encoding.UTF8.GetBytes("survives-restart");

        await using (EmbeddedKahuna live = NewPersistentNode(liveRoot, backupDir))
        {
            await live.StartAsync(CancellationToken.None);
            await live.WaitForLeaderAsync("warmup", CancellationToken.None);

            (KeyValueResponseType setType, _, _) = await live.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Set, setType);

            await live.FlushAsync();

            KahunaBackupInfo full = await live.TakeFullBackupAsync();

            // Mirror BackupManager.Restore's layout: restore into {dataRoot}/kv and create {dataRoot}/wal.
            await live.RestoreToAsync(full.BackupId, Path.Combine(dataRoot, "kv"), targetTimeMs: 0);
            Directory.CreateDirectory(Path.Combine(dataRoot, "wal"));
        }

        // Boot a brand-new node with data_dir == dataRoot and confirm the restored value is readable.
        await using EmbeddedKahuna restored = NewPersistentNode(dataRoot, backupDir: null);
        await restored.StartAsync(CancellationToken.None);
        await restored.WaitForLeaderAsync("warmup", CancellationToken.None);

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await restored.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);

        Assert.AreEqual(KeyValueResponseType.Get, getType);
        Assert.IsNotNull(entry);
        Assert.AreEqual(value, entry!.Value, "Restored data root must boot with the backed-up value present");
    }
}
