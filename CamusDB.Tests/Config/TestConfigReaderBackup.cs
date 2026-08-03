/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Verifies the backup/PITR <c>kahuna:</c> knobs (<c>backup_dir</c>, <c>pitr_window_seconds</c>,
/// <c>base_snapshot_interval_seconds</c>) are parsed and validated, and — critically — that the
/// snapshot-interval-vs-window cross-check fires on the <b>effective</b> pair, so a one-sided override
/// that conflicts with the other side's default is rejected at startup rather than at Kahuna boot.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestConfigReaderBackup
{
    [Test]
    public void BackupKeys_AcceptedByReader_ValuesApplied()
    {
        const string yml = """
            kahuna:
              backup_dir: /opt/camusdb/backups
              pitr_window_seconds: 7200
              base_snapshot_interval_seconds: 900
            """;

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual("/opt/camusdb/backups", config.Kahuna.BackupDir);
        Assert.AreEqual(7200, config.Kahuna.PitrWindowSeconds);
        Assert.AreEqual(900, config.Kahuna.BaseSnapshotIntervalSeconds);
    }

    [Test]
    public void BlankBackupDir_Rejected()
    {
        const string yml = """
            kahuna:
              backup_dir: "   "
            """;

        Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yml));
    }

    [Test]
    public void PitrWindow_AboveSixHours_Rejected()
    {
        const string yml = """
            kahuna:
              pitr_window_seconds: 21601
            """;

        Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yml));
    }

    [Test]
    public void OneSidedWindowBelowDefaultSnapshot_RejectedByEffectiveCrossCheck()
    {
        // Only the window is set, below the 1800s default base-snapshot interval. The effective pair
        // (window=600, snapshot=1800 default) is invalid and must be caught by CamusDB, not deferred to
        // Kahuna startup.
        const string yml = """
            kahuna:
              pitr_window_seconds: 600
            """;

        CamusDBException ex = Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yml))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    public void RestoreAndRetentionKeys_AcceptedByReader_ValuesApplied()
    {
        const string yml = """
            kahuna:
              restore_root: /opt/camusdb/restores
              allow_unconfined_remote_restore: false
              backup_retention_max_chains: 10
              backup_retention_max_age_seconds: 86400
              backup_retention_max_bytes: 1073741824
              backup_gc_interval_seconds: 1800
              backup_restore_throttle_bytes_per_sec: 52428800
            """;

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual("/opt/camusdb/restores", config.Kahuna.RestoreRoot);
        Assert.AreEqual(false, config.Kahuna.AllowUnconfinedRemoteRestore);
        Assert.AreEqual(10, config.Kahuna.BackupRetentionMaxChains);
        Assert.AreEqual(86400, config.Kahuna.BackupRetentionMaxAgeSeconds);
        Assert.AreEqual(1073741824, config.Kahuna.BackupRetentionMaxBytes);
        Assert.AreEqual(1800, config.Kahuna.BackupGcIntervalSeconds);
        Assert.AreEqual(52428800, config.Kahuna.BackupRestoreThrottleBytesPerSec);
    }

    [Test]
    public void BlankRestoreRoot_Rejected()
    {
        const string yml = """
            kahuna:
              restore_root: "  "
            """;

        Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yml));
    }

    [Test]
    public void ClusterIdentityAndMacKeys_AcceptedByReader_ValuesApplied()
    {
        const string yml = """
            kahuna:
              backup_cluster_id: prod-cluster-a
              backup_mac_key_file: /etc/camusdb/backup.key
            """;

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual("prod-cluster-a", config.Kahuna.BackupClusterId);
        Assert.AreEqual("/etc/camusdb/backup.key", config.Kahuna.BackupMacKeyFile);
    }

    [Test]
    public void BlankMacKeyFile_Rejected()
    {
        const string yml = """
            kahuna:
              backup_mac_key_file: "  "
            """;

        Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yml));
    }

    [Test]
    public void OneSidedWindowAboveDefaultSnapshot_Accepted()
    {
        // window=3000 >= default snapshot 1800 → effective pair is valid.
        const string yml = """
            kahuna:
              pitr_window_seconds: 3000
            """;

        Assert.DoesNotThrow(() => new ConfigReader().Read(yml));
    }
}
