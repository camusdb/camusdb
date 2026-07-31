/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

/// <summary>
/// Response for an offline restore. <see cref="DataRoot"/> is a ready-to-boot CamusDB data directory:
/// start a fresh server with <c>data_dir = DataRoot</c> to run on the restored image (no manual file
/// moves). See <see cref="RestoreResult.DataRoot"/>.
/// </summary>
public sealed class RestoreResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public string? DataRoot { get; set; }

    public int PartitionsRestored { get; set; }

    public long EntriesApplied { get; set; }

    public long LastAppliedPhysicalMs { get; set; }

    public long MinRecoverablePhysicalMs { get; set; }

    public long MaxRecoverablePhysicalMs { get; set; }

    public List<BackupInfoModel>? Chain { get; set; }

    public RestoreResponse(string status)
    {
        Status = status;
    }

    public RestoreResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }

    public static RestoreResponse Ok(RestoreResult result) => new("ok")
    {
        DataRoot = result.DataRoot,
        PartitionsRestored = result.PartitionsRestored,
        EntriesApplied = result.EntriesApplied,
        LastAppliedPhysicalMs = result.LastAppliedPhysicalMs,
        MinRecoverablePhysicalMs = result.MinRecoverablePhysicalMs,
        MaxRecoverablePhysicalMs = result.MaxRecoverablePhysicalMs,
        Chain = result.Chain.Select(BackupInfoModel.From).ToList(),
    };
}
