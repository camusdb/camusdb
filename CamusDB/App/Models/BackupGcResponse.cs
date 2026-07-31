/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

/// <summary>Response for a backup garbage-collection run/preview: status envelope plus what was (or would be) reclaimed.</summary>
public sealed class BackupGcResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    /// <summary>False for a dry-run preview; true when the pass was applied.</summary>
    public bool Applied { get; set; }

    public long BytesReclaimed { get; set; }

    public List<BackupGcDeletionModel>? RetentionDeletions { get; set; }

    public List<BackupGcOrphanModel>? OrphanReclamations { get; set; }

    public BackupGcResponse(string status)
    {
        Status = status;
    }

    public BackupGcResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }

    public static BackupGcResponse Ok(BackupGcResult result) => new("ok")
    {
        Applied = result.Applied,
        BytesReclaimed = result.BytesReclaimed,
        RetentionDeletions = result.RetentionDeletions
            .Select(d => new BackupGcDeletionModel
            {
                BackupId = d.BackupId.ToString(),
                Type = d.Type,
                CreatedAtUtc = d.CreatedAtUtc,
                Bytes = d.Bytes,
                Reason = d.Reason,
            }).ToList(),
        OrphanReclamations = result.OrphanReclamations
            .Select(o => new BackupGcOrphanModel { Name = o.Name, IsDirectory = o.IsDirectory, Reason = o.Reason })
            .ToList(),
    };
}

public sealed class BackupGcDeletionModel
{
    public string BackupId { get; set; } = "";
    public string Type { get; set; } = "";
    public DateTime CreatedAtUtc { get; set; }
    public long Bytes { get; set; }
    public string Reason { get; set; } = "";
}

public sealed class BackupGcOrphanModel
{
    public string Name { get; set; } = "";
    public bool IsDirectory { get; set; }
    public string Reason { get; set; } = "";
}
