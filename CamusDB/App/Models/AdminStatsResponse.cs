/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.Diagnostics;

namespace CamusDB.App.Models;

/// <summary>
/// Body of <c>GET /v1/admin/stats</c>: the node stats answer (see <see cref="NodeStats"/>) in the usual
/// <c>status</c> envelope. On success the sections are present and <c>code</c>/<c>message</c> are
/// omitted; on failure only <c>status</c>, <c>code</c> and <c>message</c> are written. The field names
/// are the camel-case forms of the gRPC <c>NodeStatsReply</c> fields, so one client model fits both
/// transports.
/// </summary>
public sealed class AdminStatsResponse
{
    public string Status { get; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Code { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Message { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public NodeIdentityStats? Node { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<DiskVolumeStats>? Disks { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public CpuStats? Cpu { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public MemoryStats? Memory { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<NodeStatsError>? Errors { get; init; }

    public AdminStatsResponse(string status) => Status = status;

    public static AdminStatsResponse From(NodeStats stats) => new("ok")
    {
        Node = stats.Node,
        Disks = stats.Disks,
        Cpu = stats.Cpu,
        Memory = stats.Memory,
        Errors = stats.Errors,
    };
}
