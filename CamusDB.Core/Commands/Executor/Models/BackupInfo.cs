/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// CamusDB-side description of a single node-wide backup artifact. Mirrors the fields Kahuna returns
/// for a backup, decoupled from the Kahuna transport DTO so the command surface and the HTTP layer do
/// not depend on <c>Kahuna.Shared</c> types.
///
/// <para><see cref="ClusterSnapshotNode"/>/<see cref="ClusterSnapshotPhysical"/>/
/// <see cref="ClusterSnapshotCounter"/> together carry the coordinated-cut HLC and are non-null only
/// for a coordinated backup. <see cref="RequestedKind"/>/<see cref="ActualKind"/>/
/// <see cref="SubstitutionReason"/> make an incremental→full substitution observable rather than
/// silent. <see cref="IsInvalid"/>/<see cref="InvalidReason"/> mark a manifest that could not be read
/// (listings surface these instead of hiding them). <see cref="MinRecoverablePhysicalMs"/>/
/// <see cref="MaxRecoverablePhysicalMs"/> are the chain's recoverable HLC coverage (physical-ms), set
/// only on the head of a resolved chain — a restore target is valid inside that range regardless of
/// wall-clock age.</para>
/// </summary>
public sealed record BackupInfo(
    Guid BackupId,
    int FormatVersion,
    string Type,
    DateTime CreatedAtUtc,
    Guid? ParentBackupId,
    int PartitionCount,
    int? ClusterSnapshotNode,
    long? ClusterSnapshotPhysical,
    uint? ClusterSnapshotCounter,
    string? RequestedKind,
    string? ActualKind,
    string? SubstitutionReason,
    bool IsInvalid,
    string? InvalidReason,
    long? MinRecoverablePhysicalMs,
    long? MaxRecoverablePhysicalMs,
    string? ClusterId,
    string? CoordinatorNode);
