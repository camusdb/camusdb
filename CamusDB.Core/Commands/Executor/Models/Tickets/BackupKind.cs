/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Which flavour of node-wide backup a <see cref="TakeBackupTicket"/> requests. All three are online
/// (safe while the node serves traffic) and cover every database, since they share one Kahuna node.
/// </summary>
public enum BackupKind
{
    /// <summary>A complete base image of the storage engine plus a manifest — the root of a chain.</summary>
    Full,

    /// <summary>
    /// Only the WAL committed since a parent backup (<see cref="TakeBackupTicket.ParentBackupId"/> is
    /// required), linked to that parent. Cheap and proportional to the changes, not the dataset size.
    /// </summary>
    Incremental,

    /// <summary>
    /// A cluster-wide full backup taken at one consistent HLC cut across all partitions. Equivalent to
    /// <see cref="Full"/> for the embedded single node; the production recommendation for real clusters.
    /// </summary>
    Coordinated,
}
