/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Builds the node stats answer (<see cref="NodeStats"/>) for the gRPC <c>GetNodeStats</c> call and
/// <c>GET /v1/admin/stats</c>, so both transports return the same fields from one place.
///
/// <para><b>Cost.</b> The call is meant to run every minute from several monitors, so nothing here
/// probes, scans or sleeps beyond two bounded caches. The data volume comes from the engine's shared
/// <see cref="DiskSpaceMonitor"/> — the write gate's own cache, at most one <c>statfs</c> per two
/// seconds — and the WAL volume, when it has one, from a second monitor with the same cache. CPU, load,
/// host memory, the heap limit and the engine's size come from <see cref="NodeResourceSampler"/>'s
/// last tick. Working set and heap figures are values the runtime already keeps.</para>
///
/// <para><b>Agreement with the write gate.</b> <c>below_watermark</c> is
/// <see cref="DiskSpaceMonitor.IsBelowWithReading"/> on the same monitor instance, against the same
/// live <c>min_free_disk_bytes</c> snapshot the gate reads, so a monitor alerting on it sees exactly
/// what writes see.</para>
/// </summary>
public sealed class NodeStatsCollector
{
    private readonly CommandExecutor executor;

    private readonly NodeResourceSampler sampler;

    private readonly string version;

    private readonly bool clusterMode;

    private readonly Func<string> nodeId;

    private readonly Func<string> endpoint;

    /// <summary>Production constructor: identity from the node's Raft instance.</summary>
    public NodeStatsCollector(CommandExecutor executor, EmbeddedKahuna node, NodeResourceSampler sampler, string version)
        : this(
            executor,
            sampler,
            version,
            node.IsClusterMode,
            () => node.Raft.GetLocalNodeId().ToString(System.Globalization.CultureInfo.InvariantCulture),
            () => node.Raft.GetLocalEndpoint())
    {
    }

    /// <summary>Test seam: identity supplied directly.</summary>
    internal NodeStatsCollector(
        CommandExecutor executor,
        NodeResourceSampler sampler,
        string version,
        bool clusterMode,
        Func<string> nodeId,
        Func<string> endpoint)
    {
        this.executor = executor;
        this.sampler = sampler;
        this.version = version;
        this.clusterMode = clusterMode;
        this.nodeId = nodeId;
        this.endpoint = endpoint;
    }

    /// <summary>
    /// Refuses a caller who may not read node stats: with authentication on, anyone who is neither a
    /// superuser nor a holder of <see cref="Privilege.Monitor"/> on <c>*.*</c>. With authentication off
    /// everyone may, like the dashboard. The single copy of the rule both transports call.
    /// </summary>
    public static void EnsureAuthorized(Principal? principal, bool authenticationEnabled)
    {
        if (!authenticationEnabled)
            return;

        if (principal is null || !principal.CanMonitorNode)
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                "Reading node stats requires a superuser or the MONITOR privilege (GRANT MONITOR ON *.* TO <user>)");
    }

    /// <summary>Builds the answer. Never throws for an unreadable value: that value is -1 with an <c>errors</c> entry.</summary>
    public NodeStats Collect()
    {
        List<NodeStatsError> errors = new();
        CamusDBOptions options = executor.Options;

        ResourceSnapshot? resources = sampler.Resources;
        StorageSnapshot? storage = sampler.Storage;

        return new NodeStats
        {
            Node = CollectIdentity(errors),
            Disks = CollectDisks(options, storage, errors),
            Cpu = CollectCpu(resources, errors),
            Memory = CollectMemory(resources, errors),
            Errors = errors,
        };
    }

    private NodeIdentityStats CollectIdentity(List<NodeStatsError> errors)
    {
        DateTime now = DateTime.UtcNow;

        return new NodeIdentityStats
        {
            NodeId = Try(nodeId, "node.node_id", errors),
            Endpoint = Try(endpoint, "node.endpoint", errors),
            Version = version,
            ClusterMode = clusterMode,
            UptimeSeconds = Math.Max(0, (long)(now - sampler.ProcessStartUtc).TotalSeconds),
            SampledAtUnixMs = new DateTimeOffset(now).ToUnixTimeMilliseconds(),
        };
    }

    /// <summary>
    /// The node's identity can be missing early in boot, before Raft has an endpoint; that is an empty
    /// string with an error entry, not a failed call.
    /// </summary>
    private static string Try(Func<string> read, string field, List<NodeStatsError> errors)
    {
        try
        {
            return read() ?? "";
        }
        catch (Exception ex)
        {
            errors.Add(new NodeStatsError(field, ex.Message));
            return "";
        }
    }

    private List<DiskVolumeStats> CollectDisks(CamusDBOptions options, StorageSnapshot? storage, List<NodeStatsError> errors)
    {
        List<DiskVolumeStats> disks = new(2);

        bool walSeparate = storage?.WalOnSeparateVolume == true;
        long minFree = options.MinFreeDiskBytes;

        DiskSpaceMonitor dataMonitor = executor.DiskSpace;
        bool below = dataMonitor.IsBelowWithReading(minFree, out DiskVolumeReading reading, out string? dataError);
        AddReadingErrors("data", reading, dataError, errors);

        // The data volume's engine bytes are the key/value store plus the WAL when the WAL shares the
        // volume; a WAL on its own volume is counted on its own entry instead.
        (long engineBytes, string? engineError) = storage is null
            ? (-1, "the engine's size is not measured yet")
            : walSeparate
                ? (storage.KvBytes, storage.KvError)
                : Sum(storage.KvBytes, storage.KvError, storage.WalBytes, storage.WalError);

        if (engineError is not null)
            errors.Add(new NodeStatsError("disks[data].engine_bytes", engineError));

        if (storage?.MountError is not null)
            errors.Add(new NodeStatsError("disks[data].mount_point", storage.MountError));

        disks.Add(new DiskVolumeStats
        {
            Role = DiskVolumeStats.DataRole,
            Path = options.DataDirectory,
            MountPoint = storage?.DataMountPoint ?? "",
            TotalBytes = reading.TotalBytes,
            FreeBytes = reading.FreeBytes,
            AvailableBytes = reading.AvailableBytes,
            UsedBytes = reading.UsedBytes,
            MinFreeDiskBytes = minFree,
            BelowWatermark = below,
            EngineBytes = engineBytes,
        });

        if (walSeparate && sampler.WalVolume is DiskSpaceMonitor walMonitor)
        {
            // The write gate does not watch this volume: a disabled watermark (0) reads the volume
            // without judging it.
            walMonitor.IsBelowWithReading(0, out DiskVolumeReading walReading, out string? walError);
            AddReadingErrors("wal", walReading, walError, errors);

            if (storage!.WalError is not null)
                errors.Add(new NodeStatsError("disks[wal].engine_bytes", storage.WalError));

            disks.Add(new DiskVolumeStats
            {
                Role = DiskVolumeStats.WalRole,
                Path = sampler.Paths.WalPath,
                MountPoint = storage.WalMountPoint,
                TotalBytes = walReading.TotalBytes,
                FreeBytes = walReading.FreeBytes,
                AvailableBytes = walReading.AvailableBytes,
                UsedBytes = walReading.UsedBytes,
                MinFreeDiskBytes = 0,
                BelowWatermark = false,
                EngineBytes = storage.WalBytes,
            });
        }

        return disks;
    }

    private static (long Bytes, string? Error) Sum(long a, string? aError, long b, string? bError)
    {
        if (aError is not null)
            return (-1, aError);

        if (bError is not null)
            return (-1, bError);

        return (a + b, null);
    }

    /// <summary>One <c>errors</c> entry per unknown field of a volume reading, all carrying the probe's reason.</summary>
    private static void AddReadingErrors(string role, DiskVolumeReading reading, string? probeError, List<NodeStatsError> errors)
    {
        string reason = probeError ?? "the volume probe did not report this value";

        if (reading.TotalBytes < 0)
            errors.Add(new NodeStatsError($"disks[{role}].total_bytes", reason));
        if (reading.FreeBytes < 0)
            errors.Add(new NodeStatsError($"disks[{role}].free_bytes", reason));
        if (reading.AvailableBytes < 0)
            errors.Add(new NodeStatsError($"disks[{role}].available_bytes", reason));
        if (reading.UsedBytes < 0)
            errors.Add(new NodeStatsError($"disks[{role}].used_bytes", reason));
    }

    private static CpuStats CollectCpu(ResourceSnapshot? resources, List<NodeStatsError> errors)
    {
        int processors = Environment.ProcessorCount;

        if (resources is null)
        {
            const string notSampled = "the resource sampler has not run yet";
            errors.Add(new NodeStatsError("cpu.process_cpu_percent", notSampled));
            errors.Add(new NodeStatsError("cpu.load_average_1m", notSampled));
            errors.Add(new NodeStatsError("cpu.load_average_5m", notSampled));
            errors.Add(new NodeStatsError("cpu.load_average_15m", notSampled));
            return new CpuStats { ProcessorCount = processors };
        }

        if (resources.CpuError is not null)
            errors.Add(new NodeStatsError("cpu.process_cpu_percent", resources.CpuError));

        if (resources.LoadError is not null)
        {
            errors.Add(new NodeStatsError("cpu.load_average_1m", resources.LoadError));
            errors.Add(new NodeStatsError("cpu.load_average_5m", resources.LoadError));
            errors.Add(new NodeStatsError("cpu.load_average_15m", resources.LoadError));
        }

        return new CpuStats
        {
            ProcessCpuPercent = resources.CpuPercent,
            ProcessCpuWindowMs = resources.CpuWindowMs,
            ProcessorCount = processors,
            LoadAverage1m = resources.LoadAverage1m,
            LoadAverage5m = resources.LoadAverage5m,
            LoadAverage15m = resources.LoadAverage15m,
        };
    }

    private static MemoryStats CollectMemory(ResourceSnapshot? resources, List<NodeStatsError> errors)
    {
        GCMemoryInfo gc = GC.GetGCMemoryInfo();

        // The committed figure exists only as of a completed collection. A fresh node under Server GC
        // can run for a while before its first one, and 0 would read as "nothing committed".
        bool collected = gc.Index > 0;
        if (!collected)
            errors.Add(new NodeStatsError("memory.gc_committed_bytes", "no garbage collection has completed yet"));

        long hostTotal = -1;
        long hostAvailable = -1;
        long gcLimit = -1;

        if (resources is null)
        {
            const string notSampled = "the resource sampler has not run yet";
            errors.Add(new NodeStatsError("memory.gc_memory_limit_bytes", notSampled));
            errors.Add(new NodeStatsError("memory.host_total_bytes", notSampled));
            errors.Add(new NodeStatsError("memory.host_available_bytes", notSampled));
        }
        else
        {
            gcLimit = resources.GcHeapHardLimitBytes;
            if (resources.GcHeapHardLimitError is not null)
                errors.Add(new NodeStatsError("memory.gc_memory_limit_bytes", resources.GcHeapHardLimitError));

            hostTotal = resources.HostTotalBytes;
            hostAvailable = resources.HostAvailableBytes;
            if (resources.HostMemoryError is not null)
            {
                if (hostTotal < 0)
                    errors.Add(new NodeStatsError("memory.host_total_bytes", resources.HostMemoryError));
                if (hostAvailable < 0)
                    errors.Add(new NodeStatsError("memory.host_available_bytes", resources.HostMemoryError));
            }
        }

        return new MemoryStats
        {
            ProcessWorkingSetBytes = Environment.WorkingSet,
            // Before the first collection the last-GC figures are all zero; the allocated-bytes
            // estimate is the honest heap size then.
            GcHeapBytes = collected ? gc.HeapSizeBytes : GC.GetTotalMemory(forceFullCollection: false),
            GcCommittedBytes = collected ? gc.TotalCommittedBytes : -1,
            GcMemoryLimitBytes = gcLimit,
            HostTotalBytes = hostTotal,
            HostAvailableBytes = hostAvailable,
        };
    }
}
