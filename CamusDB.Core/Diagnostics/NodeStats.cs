/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// One node's own resource state, as the node stats call returns it over gRPC
/// (<c>GetNodeStats</c>) and REST (<c>GET /v1/admin/stats</c>). Built by
/// <see cref="NodeStatsCollector"/>; see <c>docs/node-stats.md</c> for the operator view.
///
/// <para><b>The -1 rule.</b> Every size is bytes and every number is plain. A value the node cannot
/// read is <c>-1</c>, never <c>0</c>, and gets an entry in <see cref="Errors"/> naming the field and
/// the reason. One failed probe never fails the whole call: a monitor that loses its disk gauge
/// because the load average was unreadable has lost the thing it came for.</para>
///
/// <para>In cluster mode this answers for the node that received the call, not the cluster.</para>
/// </summary>
public sealed record NodeStats
{
    public required NodeIdentityStats Node { get; init; }

    /// <summary>
    /// The data volume first. A second entry, with <see cref="DiskVolumeStats.Role"/> <c>wal</c>, is
    /// present only when the Raft WAL directory sits on a different volume.
    /// </summary>
    public required IReadOnlyList<DiskVolumeStats> Disks { get; init; }

    public required CpuStats Cpu { get; init; }

    public required MemoryStats Memory { get; init; }

    /// <summary>One entry per value reported as -1, in no particular order.</summary>
    public required IReadOnlyList<NodeStatsError> Errors { get; init; }
}

/// <summary>Which node answered, and when.</summary>
public sealed record NodeIdentityStats
{
    /// <summary>The node's Raft node id as a string; empty early in boot before the node has one.</summary>
    public string NodeId { get; init; } = "";

    /// <summary>The node's Raft endpoint (<c>host:port</c>); empty early in boot.</summary>
    public string Endpoint { get; init; } = "";

    /// <summary>The server's informational version.</summary>
    public string Version { get; init; } = "";

    public bool ClusterMode { get; init; }

    /// <summary>Whole seconds since the process started.</summary>
    public long UptimeSeconds { get; init; } = -1;

    /// <summary>When this answer was built, unix epoch milliseconds (UTC).</summary>
    public long SampledAtUnixMs { get; init; }
}

/// <summary>
/// One volume the engine writes to. Sizes come from one <c>statfs</c>-class probe, cached for two
/// seconds and shared with the write-admission gate.
/// </summary>
public sealed record DiskVolumeStats
{
    /// <summary>The volume role this entry answers for.</summary>
    public const string DataRole = "data";

    /// <summary>The volume role of a Raft WAL directory on its own volume.</summary>
    public const string WalRole = "wal";

    /// <summary><see cref="DataRole"/> or <see cref="WalRole"/>.</summary>
    public string Role { get; init; } = DataRole;

    /// <summary>The configured directory this entry measures (the data directory, or the WAL directory).</summary>
    public string Path { get; init; } = "";

    /// <summary>The mount point that holds <see cref="Path"/>; empty when it is not known yet.</summary>
    public string MountPoint { get; init; } = "";

    public long TotalBytes { get; init; } = -1;

    /// <summary>Raw free space of the volume.</summary>
    public long FreeBytes { get; init; } = -1;

    /// <summary>Free space this server's user may still write — what the watermark compares.</summary>
    public long AvailableBytes { get; init; } = -1;

    public long UsedBytes { get; init; } = -1;

    /// <summary>
    /// The write-admission watermark (<c>min_free_disk_bytes</c>) that applies to this volume. The gate
    /// watches the data volume only, so a WAL entry reports 0 — the configuration's own value for
    /// "no gate".
    /// </summary>
    public long MinFreeDiskBytes { get; init; }

    /// <summary>
    /// True when the write gate refuses new writes because of this volume. On the data volume this is
    /// the gate's own answer, from the same cached probe; on a WAL entry it is always false.
    /// </summary>
    public bool BelowWatermark { get; init; }

    /// <summary>
    /// Bytes of the storage engine's own files on this volume: the key/value store, plus the Raft WAL
    /// when it shares the volume. Measured by a cached scan of the engine's directories, refreshed off
    /// the request path; 0 for an in-memory engine.
    /// </summary>
    public long EngineBytes { get; init; } = -1;
}

/// <summary>CPU of the server process and of the host.</summary>
public sealed record CpuStats
{
    /// <summary>
    /// Process CPU as a share of the node's whole capacity (all <see cref="ProcessorCount"/>
    /// processors), 0–100, averaged over <see cref="ProcessCpuWindowMs"/>.
    /// </summary>
    public double ProcessCpuPercent { get; init; } = -1;

    /// <summary>
    /// The window <see cref="ProcessCpuPercent"/> is averaged over, in milliseconds: up to a minute of
    /// background samples, or the process's whole lifetime before the first sample.
    /// </summary>
    public long ProcessCpuWindowMs { get; init; } = -1;

    /// <summary>Processors as the runtime sees them, with container CPU limits applied.</summary>
    public int ProcessorCount { get; init; } = -1;

    /// <summary>The host's 1-minute load average (Linux, macOS); a host figure, not the container's.</summary>
    public double LoadAverage1m { get; init; } = -1;

    public double LoadAverage5m { get; init; } = -1;

    public double LoadAverage15m { get; init; } = -1;
}

/// <summary>Memory of the server process, its managed heap, and the host.</summary>
public sealed record MemoryStats
{
    public long ProcessWorkingSetBytes { get; init; } = -1;

    /// <summary>
    /// Managed heap size as of the last garbage collection; before the first collection, the
    /// runtime's estimate of the bytes allocated so far.
    /// </summary>
    public long GcHeapBytes { get; init; } = -1;

    /// <summary>
    /// Memory the garbage collector holds committed as of the last collection, which can stay far
    /// above the heap size after load. -1 (with an error entry) until the first collection completes.
    /// </summary>
    public long GcCommittedBytes { get; init; } = -1;

    /// <summary>
    /// The managed heap hard limit in force: an explicit <c>GCHeapHardLimit</c> /
    /// <c>GCHeapHardLimitPercent</c>, or the runtime's default inside a memory-limited container.
    /// <c>0</c> means no limit is in force — the heap may grow until the host runs out — which is how
    /// a reader tells "high and limited" from "high and unbounded".
    /// </summary>
    public long GcMemoryLimitBytes { get; init; } = -1;

    /// <summary>Memory the node may use in total: the cgroup limit when one applies, else the host's physical memory.</summary>
    public long HostTotalBytes { get; init; } = -1;

    /// <summary>Of <see cref="HostTotalBytes"/>, how much is still available.</summary>
    public long HostAvailableBytes { get; init; } = -1;
}

/// <summary>Why one field is -1.</summary>
/// <param name="Field">The field's path in the reply, e.g. <c>cpu.load_average_1m</c> or <c>disks[wal].engine_bytes</c>.</param>
/// <param name="Message">The reason, as the probe reported it.</param>
public sealed record NodeStatsError(string Field, string Message);
