/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Kahuna;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Storage;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Where the engine keeps its files: the data directory the write gate watches, and the key/value
/// store and Raft WAL directories with their storage kinds (<c>memory</c> engines have no files).
/// </summary>
public sealed record NodeStoragePaths(string DataDirectory, string KvPath, string KvStorage, string WalPath, string WalStorage)
{
    /// <summary>The paths of a running node, from its engine options and its embedded Kahuna options.</summary>
    public static NodeStoragePaths From(CamusDBOptions options, EmbeddedKahunaOptions kahuna)
        => new(options.DataDirectory, kahuna.StoragePath, kahuna.Storage, kahuna.WalPath, kahuna.WalStorage);
}

/// <summary>
/// Samples, in the background, the node figures that are either a rate (process CPU) or too costly
/// for the request path (a scan of the engine's files), and publishes them for
/// <see cref="NodeStatsCollector"/> to read with one volatile load.
///
/// <para><b>Why a background tick.</b> A CPU percentage is a rate, so it needs two readings some time
/// apart. Measuring it inside the call would mean sleeping on the request path, and measuring "since
/// the previous call" would make the window depend on how many monitors happen to be asking. Instead
/// a tick every <see cref="TickInterval"/> records the process CPU time into a ring, and the reported
/// percentage is the average over the last minute of ticks (a shorter window until a minute of history
/// exists, and the process's lifetime average before the second tick). The same tick refreshes the
/// load averages, the host memory and the managed heap limit, which are cheap file or runtime reads,
/// and — every <see cref="StorageRefreshInterval"/> — the engine's on-disk size and the volume layout,
/// which need a directory scan and the mount table.</para>
///
/// <para><b>Ownership.</b> The host runs <see cref="RunAsync"/> from a hosted service for the life of
/// the process; cancellation at shutdown ends the loop. A probe that fails records its reason in the
/// published snapshot and never stops the loop. Nothing here writes, locks, or touches the engine,
/// so a sampler that stalls cannot affect a statement.</para>
///
/// <para>Thread-safe: <see cref="Sample"/> serializes on a private lock, and readers only load the
/// published immutable snapshots.</para>
/// </summary>
public sealed class NodeResourceSampler
{
    /// <summary>How often the loop samples CPU, load, host memory and the heap limit.</summary>
    public static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

    /// <summary>How often the engine's on-disk size and the volume layout are re-measured.</summary>
    public static readonly TimeSpan StorageRefreshInterval = TimeSpan.FromSeconds(60);

    /// <summary>CPU ring length: twelve tick intervals of history plus the current reading, one minute in all.</summary>
    private const int CpuRingLength = 13;

    private readonly NodeStoragePaths paths;

    private readonly ILogger logger;

    private readonly Func<string, string?> readFile;

    private readonly Func<TimeSpan> processCpuTime;

    private readonly Func<long> timestamp;

    private readonly Func<IReadOnlyList<string>> mountPoints;

    private readonly Lock sampleSync = new();

    /// <summary>Monotonic timestamps (<see cref="Stopwatch"/> ticks) of the CPU ring entries.</summary>
    private readonly long[] ringTimestamps = new long[CpuRingLength];

    /// <summary>Process CPU time (<see cref="TimeSpan"/> ticks) of the CPU ring entries.</summary>
    private readonly long[] ringCpuTicks = new long[CpuRingLength];

    private int ringCount;

    private int ringNext;

    private long lastStorageRefresh;

    private ResourceSnapshot? resources;

    private StorageSnapshot? storage;

    private DiskSpaceMonitor? walVolume;

    /// <summary>Production constructor: real <c>/proc</c> and cgroup files, the runtime's CPU time, and the OS mount table.</summary>
    public NodeResourceSampler(NodeStoragePaths paths, ILogger logger)
        : this(paths, logger, MachineMemory.TryReadFile, ReadProcessCpuTime, Stopwatch.GetTimestamp, ReadMountPoints)
    {
    }

    /// <summary>
    /// Test seam: every source the sampler reads, so a test can drive CPU time and the clock, supply a
    /// synthetic <c>/proc</c>, and fake the mount table.
    /// </summary>
    internal NodeResourceSampler(
        NodeStoragePaths paths,
        ILogger logger,
        Func<string, string?> readFile,
        Func<TimeSpan> processCpuTime,
        Func<long> timestamp,
        Func<IReadOnlyList<string>> mountPoints)
    {
        this.paths = paths;
        this.logger = logger;
        this.readFile = readFile;
        this.processCpuTime = processCpuTime;
        this.timestamp = timestamp;
        this.mountPoints = mountPoints;
        ProcessStartUtc = ResolveProcessStart();
    }

    /// <summary>When this process started, from the OS; the sampler's construction time when the OS will not say.</summary>
    public DateTime ProcessStartUtc { get; }

    /// <summary>The engine's storage directories this sampler measures.</summary>
    public NodeStoragePaths Paths => paths;

    /// <summary>The last tick's resource figures, or null before the first tick.</summary>
    internal ResourceSnapshot? Resources => Volatile.Read(ref resources);

    /// <summary>The last storage measurement, or null before the first one.</summary>
    internal StorageSnapshot? Storage => Volatile.Read(ref storage);

    /// <summary>
    /// The free-space monitor of the WAL directory's volume, once a storage refresh has found it on a
    /// volume of its own; null otherwise. Its cache works like the data volume's, so reading it on the
    /// request path costs at most one probe per two seconds.
    /// </summary>
    internal DiskSpaceMonitor? WalVolume => Volatile.Read(ref walVolume);

    /// <summary>
    /// Samples every tick now and publishes the result until <paramref name="cancellationToken"/> is
    /// cancelled. Returns normally on cancellation.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(TickInterval);
        try
        {
            do
            {
                try
                {
                    Sample();
                }
                catch (Exception ex)
                {
                    // Every probe already records its own failure; reaching here is a defect, and the
                    // next tick may still succeed, so log and keep sampling.
                    logger.LogWarning(ex, "Node resource sampling failed; node stats keep the previous sample");
                }
            }
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown.
        }
    }

    /// <summary>
    /// One tick: records the CPU reading, re-reads load, host memory and the heap limit, re-measures
    /// storage when <see cref="StorageRefreshInterval"/> has passed (or on the first tick), and
    /// publishes the result. Called by <see cref="RunAsync"/>; tests call it directly.
    /// </summary>
    public void Sample()
    {
        lock (sampleSync)
        {
            long now = timestamp();

            (double cpuPercent, long cpuWindowMs, string? cpuError) = SampleCpu(now);
            (double load1, double load5, double load15, string? loadError) = HostResourceProbe.ReadLoadAverages(readFile);
            (long hostTotal, long hostAvailable, string? memoryError) = HostResourceProbe.ReadMemory(readFile);
            (long gcLimit, string? gcLimitError) = ReadGcHeapHardLimit();

            Volatile.Write(ref resources, new ResourceSnapshot(
                cpuPercent, cpuWindowMs, cpuError,
                load1, load5, load15, loadError,
                hostTotal, hostAvailable, memoryError,
                gcLimit, gcLimitError));

            if (storage is null || Stopwatch.GetElapsedTime(lastStorageRefresh, now) >= StorageRefreshInterval)
            {
                RefreshStorage();
                lastStorageRefresh = now;
            }
        }
    }

    /// <summary>
    /// Adds the current CPU reading to the ring and returns the average over the ring's span. With a
    /// single reading the span is the process's lifetime, measured from <see cref="ProcessStartUtc"/>.
    /// The percentage is of the whole node (all processors), clamped to 0–100 because two clocks are
    /// involved and rounding can push an idle or saturated reading just past either end.
    /// </summary>
    private (double Percent, long WindowMs, string? Error) SampleCpu(long now)
    {
        long cpuTicks;
        try
        {
            cpuTicks = processCpuTime().Ticks;
        }
        catch (Exception ex)
        {
            return (-1, -1, ex.Message);
        }

        ringTimestamps[ringNext] = now;
        ringCpuTicks[ringNext] = cpuTicks;
        ringNext = (ringNext + 1) % CpuRingLength;
        if (ringCount < CpuRingLength)
            ringCount++;

        int processors = Math.Max(1, Environment.ProcessorCount);

        if (ringCount == 1)
        {
            TimeSpan lifetime = DateTime.UtcNow - ProcessStartUtc;
            if (lifetime <= TimeSpan.Zero)
                return (-1, -1, "the process start time is not known yet");

            return (Percent(cpuTicks, lifetime.Ticks, processors), (long)lifetime.TotalMilliseconds, null);
        }

        // The oldest entry: the slot about to be overwritten once the ring is full, else slot 0.
        int oldest = ringCount == CpuRingLength ? ringNext : 0;
        TimeSpan wall = Stopwatch.GetElapsedTime(ringTimestamps[oldest], now);
        if (wall <= TimeSpan.Zero)
            return (-1, -1, "the CPU sample window is empty");

        return (Percent(cpuTicks - ringCpuTicks[oldest], wall.Ticks, processors), (long)wall.TotalMilliseconds, null);
    }

    private static double Percent(long cpuTicks, long wallTicks, int processors)
        => Math.Clamp(cpuTicks * 100.0 / wallTicks / processors, 0, 100);

    /// <summary>
    /// The managed heap hard limit in force, from the GC's own configuration report: an explicit
    /// <c>GCHeapHardLimit</c>, the bytes a <c>GCHeapHardLimitPercent</c> resolved to, or the runtime's
    /// default inside a memory-limited container. 0 when no limit is in force.
    /// </summary>
    private static (long Bytes, string? Error) ReadGcHeapHardLimit()
    {
        try
        {
            if (!GC.GetConfigurationVariables().TryGetValue("GCHeapHardLimit", out object? value) || value is null)
                return (-1, "the runtime does not report GCHeapHardLimit");

            return (Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture), null);
        }
        catch (Exception ex)
        {
            return (-1, ex.Message);
        }
    }

    /// <summary>
    /// Re-measures the engine's on-disk size and decides whether the WAL directory is on its own
    /// volume. Runs on the sampler's tick only: the size is a recursive directory scan, which the
    /// request path must never pay.
    /// </summary>
    private void RefreshStorage()
    {
        (long kvBytes, string? kvError) = MeasureEngineDirectory(paths.KvPath, paths.KvStorage);
        (long walBytes, string? walError) = MeasureEngineDirectory(paths.WalPath, paths.WalStorage);

        string dataMount = "";
        string walMount = "";
        string? mountError = null;
        bool walSeparate = false;

        try
        {
            IReadOnlyList<string> mounts = mountPoints();
            dataMount = FindMountPoint(ResolveRealPath(paths.DataDirectory), mounts) ?? "";

            if (!IsMemory(paths.WalStorage) && !string.IsNullOrEmpty(paths.WalPath))
            {
                walMount = FindMountPoint(ResolveRealPath(paths.WalPath), mounts) ?? "";
                walSeparate = dataMount.Length > 0 && walMount.Length > 0 && !string.Equals(dataMount, walMount, StringComparison.Ordinal);
            }
        }
        catch (Exception ex)
        {
            mountError = ex.Message;
        }

        if (walSeparate && walVolume is null)
            Volatile.Write(ref walVolume, new DiskSpaceMonitor(paths.WalPath, logger));

        Volatile.Write(ref storage, new StorageSnapshot(dataMount, walMount, walSeparate, mountError, kvBytes, kvError, walBytes, walError));
    }

    /// <summary>
    /// Sum of the sizes of every file under <paramref name="path"/>. An in-memory engine keeps nothing
    /// on disk, which is a real 0, not an unknown. A file that disappears during the scan — compaction
    /// deletes files all the time — is simply not counted.
    /// </summary>
    internal static (long Bytes, string? Error) MeasureEngineDirectory(string path, string storageKind)
    {
        if (IsMemory(storageKind))
            return (0, null);

        if (string.IsNullOrEmpty(path))
            return (-1, "no directory is configured");

        try
        {
            if (!Directory.Exists(path))
                return (-1, $"directory '{path}' does not exist");

            EnumerationOptions options = new()
            {
                RecurseSubdirectories = true,
                IgnoreInaccessible = true,
                AttributesToSkip = FileAttributes.ReparsePoint,
            };

            long total = 0;
            foreach (FileInfo file in new DirectoryInfo(path).EnumerateFiles("*", options))
            {
                try
                {
                    total += file.Length;
                }
                catch (IOException)
                {
                    // Deleted between enumeration and stat.
                }
            }

            return (total, null);
        }
        catch (Exception ex)
        {
            return (-1, ex.Message);
        }
    }

    private static bool IsMemory(string storageKind) => string.Equals(storageKind, "memory", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The longest mount point that contains <paramref name="realPath"/>, or null when none does.
    /// <paramref name="realPath"/> must already have its symbolic links resolved, because the mount
    /// table names real paths.
    /// </summary>
    internal static string? FindMountPoint(string realPath, IReadOnlyList<string> mounts)
    {
        StringComparison comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        string? best = null;

        foreach (string raw in mounts)
        {
            string mount = raw.Length > 1 ? Path.TrimEndingDirectorySeparator(raw) : raw;
            // The root contains everything; any other mount contains itself and the paths below it
            // (a separator must follow the prefix, so "/data" does not contain "/database").
            bool contains = (mount.Length == 1 && mount[0] == Path.DirectorySeparatorChar)
                || string.Equals(realPath, mount, comparison)
                || (realPath.StartsWith(mount, comparison)
                    && (mount.EndsWith(Path.DirectorySeparatorChar)
                        || (realPath.Length > mount.Length && realPath[mount.Length] == Path.DirectorySeparatorChar)));

            if (contains && (best is null || mount.Length > best.Length))
                best = mount;
        }

        return best;
    }

    /// <summary>
    /// <paramref name="path"/> as an absolute path with every symbolic link resolved, component by
    /// component, so it can be matched against the mount table. A link to a link is followed; the walk
    /// stops after a fixed number of hops so a link loop cannot hang the sampler.
    /// </summary>
    internal static string ResolveRealPath(string path)
    {
        string full = Path.GetFullPath(path);

        for (int hop = 0; hop < 32; hop++)
        {
            string? next = ReplaceFirstLink(full);
            if (next is null)
                return full;

            full = next;
        }

        return full;
    }

    /// <summary>
    /// Finds the first component of <paramref name="full"/> that is a symbolic link and returns the path
    /// with that prefix replaced by the link's target, or null when no component is a link.
    /// </summary>
    private static string? ReplaceFirstLink(string full)
    {
        string? root = Path.GetPathRoot(full);
        if (string.IsNullOrEmpty(root))
            return null;

        string[] parts = full[root.Length..].Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);
        string current = root;

        for (int i = 0; i < parts.Length; i++)
        {
            current = Path.Combine(current, parts[i]);

            string? target = new FileInfo(current).LinkTarget;
            if (target is null)
                continue;

            // A relative target is relative to the directory that holds the link.
            string resolved = Path.GetFullPath(target, Path.GetDirectoryName(current) ?? root);
            return i == parts.Length - 1
                ? resolved
                : Path.Combine(resolved, string.Join(Path.DirectorySeparatorChar, parts, i + 1, parts.Length - i - 1));
        }

        return null;
    }

    private static IReadOnlyList<string> ReadMountPoints()
    {
        DriveInfo[] drives = DriveInfo.GetDrives();
        string[] names = new string[drives.Length];
        for (int i = 0; i < drives.Length; i++)
            names[i] = drives[i].Name;
        return names;
    }

    /// <summary>The process's total CPU time. The browser build has no process to measure; the sampler records the reason.</summary>
    private static TimeSpan ReadProcessCpuTime()
    {
        if (OperatingSystem.IsBrowser())
            throw new PlatformNotSupportedException("process CPU time is not available in the browser");

        return Environment.CpuUsage.TotalTime;
    }

    private static DateTime ResolveProcessStart()
    {
        if (OperatingSystem.IsBrowser())
            return DateTime.UtcNow;

        try
        {
            using Process process = Process.GetCurrentProcess();
            return process.StartTime.ToUniversalTime();
        }
        catch (Exception)
        {
            return DateTime.UtcNow;
        }
    }
}

/// <summary>
/// The figures one sampler tick published. Each group carries the reason it is -1, or null when it
/// was read.
/// </summary>
internal sealed record ResourceSnapshot(
    double CpuPercent, long CpuWindowMs, string? CpuError,
    double LoadAverage1m, double LoadAverage5m, double LoadAverage15m, string? LoadError,
    long HostTotalBytes, long HostAvailableBytes, string? HostMemoryError,
    long GcHeapHardLimitBytes, string? GcHeapHardLimitError);

/// <summary>
/// The last storage measurement: where the data and WAL directories are mounted, whether the WAL has a
/// volume of its own, and the bytes of the key/value store and WAL directories.
/// </summary>
internal sealed record StorageSnapshot(
    string DataMountPoint, string WalMountPoint, bool WalOnSeparateVolume, string? MountError,
    long KvBytes, string? KvError, long WalBytes, string? WalError);
