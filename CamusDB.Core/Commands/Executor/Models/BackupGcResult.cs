/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// CamusDB-side result of a backup garbage-collection pass — what was (or, for a dry run, would be)
/// reclaimed: whole backup chains deleted by the retention policy and orphaned/leftover artifacts swept.
/// Decoupled from the Kahuna transport DTO so the command surface does not leak <c>Kahuna.Shared</c>
/// types. Orphan entries surface only the artifact name, never an absolute server path.
/// </summary>
public sealed record BackupGcResult(
    bool Applied,
    long BytesReclaimed,
    IReadOnlyList<BackupGcDeletion> RetentionDeletions,
    IReadOnlyList<BackupGcOrphan> OrphanReclamations);

/// <summary>A backup deleted (or slated for deletion) by the retention policy.</summary>
public sealed record BackupGcDeletion(Guid BackupId, string Type, DateTime CreatedAtUtc, long Bytes, string Reason);

/// <summary>An orphaned/leftover artifact reclaimed (or slated for reclaim) by the sweep.</summary>
public sealed record BackupGcOrphan(string Name, bool IsDirectory, string Reason);
