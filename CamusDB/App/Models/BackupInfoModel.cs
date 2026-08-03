/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

/// <summary>
/// JSON view of a single backup. Decouples the wire shape from the Core <see cref="BackupInfo"/> record
/// so the API surface does not depend on Kahuna transport types. The three cluster-snapshot fields carry
/// the coordinated-cut HLC and are non-null only for a coordinated backup.
/// </summary>
public sealed class BackupInfoModel
{
    public string BackupId { get; set; } = "";

    public int FormatVersion { get; set; }

    public string Type { get; set; } = "";

    public DateTime CreatedAtUtc { get; set; }

    public string? ParentBackupId { get; set; }

    public int PartitionCount { get; set; }

    public int? ClusterSnapshotNode { get; set; }

    public long? ClusterSnapshotPhysical { get; set; }

    public uint? ClusterSnapshotCounter { get; set; }

    /// <summary>The kind the caller asked for; differs from <see cref="ActualKind"/> only on a substitution.</summary>
    public string? RequestedKind { get; set; }

    /// <summary>The kind actually produced.</summary>
    public string? ActualKind { get; set; }

    /// <summary>Non-null when the requested kind was substituted (e.g. incremental fell back to full); explains why.</summary>
    public string? SubstitutionReason { get; set; }

    /// <summary>True when this entry's manifest could not be read; only <see cref="BackupId"/> is meaningful.</summary>
    public bool IsInvalid { get; set; }

    public string? InvalidReason { get; set; }

    /// <summary>Chain recoverable coverage (physical-ms), set only on a resolved chain's head; a target in
    /// [<see cref="MinRecoverablePhysicalMs"/>, <see cref="MaxRecoverablePhysicalMs"/>] is restorable.</summary>
    public long? MinRecoverablePhysicalMs { get; set; }

    public long? MaxRecoverablePhysicalMs { get; set; }

    /// <summary>Cluster identity the backup was taken under (foreign-cluster chains are rejected on restore); null on a standalone/pre-cluster backup.</summary>
    public string? ClusterId { get; set; }

    /// <summary>The node that coordinated the backup; null on a standalone/pre-cluster backup.</summary>
    public string? CoordinatorNode { get; set; }

    public static BackupInfoModel From(BackupInfo info) => new()
    {
        BackupId = info.BackupId.ToString(),
        FormatVersion = info.FormatVersion,
        Type = info.Type,
        CreatedAtUtc = info.CreatedAtUtc,
        ParentBackupId = info.ParentBackupId?.ToString(),
        PartitionCount = info.PartitionCount,
        ClusterSnapshotNode = info.ClusterSnapshotNode,
        ClusterSnapshotPhysical = info.ClusterSnapshotPhysical,
        ClusterSnapshotCounter = info.ClusterSnapshotCounter,
        RequestedKind = info.RequestedKind,
        ActualKind = info.ActualKind,
        SubstitutionReason = info.SubstitutionReason,
        IsInvalid = info.IsInvalid,
        InvalidReason = info.InvalidReason,
        MinRecoverablePhysicalMs = info.MinRecoverablePhysicalMs,
        MaxRecoverablePhysicalMs = info.MaxRecoverablePhysicalMs,
        ClusterId = info.ClusterId,
        CoordinatorNode = info.CoordinatorNode,
    };
}
