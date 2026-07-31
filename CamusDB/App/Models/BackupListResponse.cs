/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Response for listing backups or resolving a validated chain: status envelope plus the backups on
/// success. For a chain the list is ordered root-first (full backup, then incrementals).
/// </summary>
public sealed class BackupListResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public List<BackupInfoModel>? Backups { get; set; }

    public BackupListResponse(string status)
    {
        Status = status;
    }

    public BackupListResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
