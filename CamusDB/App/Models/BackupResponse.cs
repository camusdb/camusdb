/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>Response for taking a single backup: status envelope plus the created backup on success.</summary>
public sealed class BackupResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public BackupInfoModel? Backup { get; set; }

    public BackupResponse(string status)
    {
        Status = status;
    }

    public BackupResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
