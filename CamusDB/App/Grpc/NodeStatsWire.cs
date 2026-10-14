/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

using ProtoCpu = CamusDB.Grpc.CpuStats;
using ProtoDisk = CamusDB.Grpc.DiskVolumeStats;
using ProtoError = CamusDB.Grpc.NodeStatsError;
using ProtoIdentity = CamusDB.Grpc.NodeIdentity;
using ProtoMemory = CamusDB.Grpc.MemoryStats;
using ProtoReply = CamusDB.Grpc.NodeStatsReply;

namespace CamusDB.App.Grpc;

/// <summary>
/// Copies a <see cref="NodeStats"/> onto the <c>NodeStatsReply</c> wire message. A separate file on
/// purpose: the proto messages share their names with the engine's model (<c>CpuStats</c>,
/// <c>MemoryStats</c>, ...), and the aliases above keep both readable in one place instead of fully
/// qualifying every name inside the service.
/// </summary>
internal static class NodeStatsWire
{
    public static ProtoReply ToReply(NodeStats stats)
    {
        ProtoReply reply = new()
        {
            Node = new ProtoIdentity
            {
                NodeId = stats.Node.NodeId,
                Endpoint = stats.Node.Endpoint,
                Version = stats.Node.Version,
                ClusterMode = stats.Node.ClusterMode,
                UptimeSeconds = stats.Node.UptimeSeconds,
                SampledAtUnixMs = stats.Node.SampledAtUnixMs,
            },
            Cpu = new ProtoCpu
            {
                ProcessCpuPercent = stats.Cpu.ProcessCpuPercent,
                ProcessCpuWindowMs = stats.Cpu.ProcessCpuWindowMs,
                ProcessorCount = stats.Cpu.ProcessorCount,
                LoadAverage1M = stats.Cpu.LoadAverage1m,
                LoadAverage5M = stats.Cpu.LoadAverage5m,
                LoadAverage15M = stats.Cpu.LoadAverage15m,
            },
            Memory = new ProtoMemory
            {
                ProcessWorkingSetBytes = stats.Memory.ProcessWorkingSetBytes,
                GcHeapBytes = stats.Memory.GcHeapBytes,
                GcCommittedBytes = stats.Memory.GcCommittedBytes,
                GcMemoryLimitBytes = stats.Memory.GcMemoryLimitBytes,
                HostTotalBytes = stats.Memory.HostTotalBytes,
                HostAvailableBytes = stats.Memory.HostAvailableBytes,
            },
        };

        foreach (DiskVolumeStats disk in stats.Disks)
        {
            reply.Disks.Add(new ProtoDisk
            {
                Role = disk.Role,
                Path = disk.Path,
                MountPoint = disk.MountPoint,
                TotalBytes = disk.TotalBytes,
                FreeBytes = disk.FreeBytes,
                AvailableBytes = disk.AvailableBytes,
                UsedBytes = disk.UsedBytes,
                MinFreeDiskBytes = disk.MinFreeDiskBytes,
                BelowWatermark = disk.BelowWatermark,
                EngineBytes = disk.EngineBytes,
            });
        }

        foreach (NodeStatsError error in stats.Errors)
            reply.Errors.Add(new ProtoError { Field = error.Field, Message = error.Message });

        return reply;
    }
}
