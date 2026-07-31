/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Body for the incremental-backup endpoint. <see cref="ParentBackupId"/> is the backup the increment
/// chains onto (required for incremental; the full and coordinated endpoints take no body).
/// </summary>
public sealed class TakeBackupRequest
{
    public string? ParentBackupId { get; set; }
}
