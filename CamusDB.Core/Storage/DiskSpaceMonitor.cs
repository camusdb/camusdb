
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Storage;

/// <summary>
/// Samples the free space of the volume that holds the engine's data directory and answers the
/// write-admission question "is free space below the watermark?". The point of the gate is to
/// refuse new mutations <em>before</em> the storage engine runs the disk to zero: a refused
/// statement is a clean, retryable error, while a memtable flush that hits a hard ENOSPC puts the
/// storage engine into a background-error state that is much slower to recover from.
///
/// <para><b>Sampling model:</b> on demand with a short cache (<see cref="SampleIntervalMs"/>),
/// not a timer thread. Callers on the DML hot path pay a volatile read plus one comparison;
/// at most one caller per interval pays the actual <c>statfs</c>-class probe. On-demand keeps the
/// class free of background threads and makes tests deterministic — the first check always
/// samples.</para>
///
/// <para><b>Failure policy — fail open:</b> when free space cannot be determined (the directory
/// does not exist yet, the volume query throws), the monitor reports "not low" and logs once at
/// warning. Blocking every write because monitoring broke would turn an observability defect into
/// an outage; the storage engine's own errors remain the backstop.</para>
///
/// <para><b>One reading, two readers.</b> A probe captures the whole volume reading (total, free,
/// available) once, and both the write gate (<see cref="IsBelow(long, out long)"/>) and the node stats
/// call (<see cref="IsBelowWithReading"/>) answer from that same cached sample. That
/// is what makes the stats call's <c>below_watermark</c> the same answer the write gate gives, rather
/// than a second probe that could disagree near the threshold.</para>
///
/// <para>Thread-safe. The engine shares one instance per node (owned by
/// <see cref="CommandsExecutor.CommandExecutor"/> and handed to every
/// <see cref="Transactions.KvTransactionsManager"/> it opens), so every database's write gate and the
/// stats call read one cache. A manager built without one creates its own, which is harmless: a probe
/// is microseconds.</para>
/// </summary>
public sealed class DiskSpaceMonitor
{
    /// <summary>Minimum milliseconds between two real free-space probes; results are cached between them.</summary>
    private const int SampleIntervalMs = 2000;

    /// <summary>
    /// One probe's outcome, published as a single immutable reference so a reader on the write path
    /// sees a consistent reading and its timestamp without taking the lock.
    /// </summary>
    private sealed record VolumeSample(long TakenAtTicks, DiskVolumeReading Reading, string? Error);

    private readonly Func<DiskVolumeReading?> volumeProvider;

    private readonly ILogger logger;

    private readonly Lock sampleSync = new();

    /// <summary>The last probe, or null before the first one.</summary>
    private VolumeSample? lastSample;

    /// <summary>True while the last check answered "below the watermark"; drives transition logs.</summary>
    private bool reportedLow;

    /// <summary>True once a probe failure was logged, so a persistently broken probe logs once, not per write.</summary>
    private bool probeFailureLogged;

    /// <summary>
    /// Production constructor: watches the volume that contains <paramref name="dataDirectory"/>.
    /// A null or empty directory produces a monitor that never reports low (nothing to watch —
    /// e.g. a purely in-memory engine).
    /// </summary>
    public DiskSpaceMonitor(string? dataDirectory, ILogger logger)
        : this(BuildVolumeProvider(dataDirectory), logger)
    {
    }

    /// <summary>
    /// Test seam: <paramref name="freeBytesProvider"/> supplies the available-byte reading directly
    /// (null = reading unavailable). Total and free bytes are unknown (-1) under this seam.
    /// </summary>
    internal DiskSpaceMonitor(Func<long?> freeBytesProvider, ILogger logger)
        : this(() => freeBytesProvider() is long available ? new DiskVolumeReading(-1, -1, available) : null, logger)
    {
    }

    /// <summary>
    /// Test seam: <paramref name="volumeProvider"/> supplies the whole volume reading (null = reading
    /// unavailable). Also used by the production constructor via <see cref="BuildVolumeProvider"/>.
    /// </summary>
    internal DiskSpaceMonitor(Func<DiskVolumeReading?> volumeProvider, ILogger logger)
    {
        this.volumeProvider = volumeProvider;
        this.logger = logger;
    }

    /// <summary>
    /// Returns true when the watched volume's free space is below <paramref name="minFreeBytes"/>.
    /// <paramref name="freeBytes"/> carries the sampled available bytes (-1 when unknown). A
    /// non-positive watermark and an unknown reading both answer false — the gate is disabled, or
    /// fails open — and a disabled gate does not probe at all. State transitions (entering/leaving the
    /// low-space condition) are logged here, on the sampling path, so the log reflects what the gate
    /// actually enforced.
    /// </summary>
    public bool IsBelow(long minFreeBytes, out long freeBytes)
    {
        if (minFreeBytes <= 0)
        {
            freeBytes = -1;
            return false;
        }

        bool low = IsBelowWithReading(minFreeBytes, out DiskVolumeReading reading, out _);
        freeBytes = reading.AvailableBytes;
        return low;
    }

    /// <summary>
    /// The same decision as <see cref="IsBelow(long, out long)"/>, from the same cached sample, but
    /// always returning the full <paramref name="reading"/> — the stats call needs the volume's numbers
    /// even when the gate is disabled (<paramref name="minFreeBytes"/> &lt;= 0, which answers false and
    /// leaves the transition state alone). <paramref name="probeError"/> says why the reading's fields
    /// are -1, from the same sample, or is null when the probe succeeded; a stats reader puts it in its
    /// <c>errors</c> list so an unknown value is never silent.
    /// </summary>
    public bool IsBelowWithReading(long minFreeBytes, out DiskVolumeReading reading, out string? probeError)
    {
        VolumeSample sample = CurrentSample();
        reading = sample.Reading;
        probeError = sample.Error;

        if (minFreeBytes <= 0)
            return false;

        bool low = IsBelowWatermark(reading.AvailableBytes, minFreeBytes);
        ReportTransition(low, reading.AvailableBytes, minFreeBytes);
        return low;
    }

    /// <summary>
    /// The watermark rule in one place: below means a known reading under a positive watermark. An
    /// unknown reading (-1) is never below — the gate fails open.
    /// </summary>
    public static bool IsBelowWatermark(long availableBytes, long minFreeBytes)
        => minFreeBytes > 0 && availableBytes >= 0 && availableBytes < minFreeBytes;

    /// <summary>
    /// The cached sample, probing first when it is missing or older than
    /// <see cref="SampleIntervalMs"/>. At most one caller per interval pays the probe; the rest read
    /// the published reference.
    /// </summary>
    private VolumeSample CurrentSample()
    {
        VolumeSample? sample = Volatile.Read(ref lastSample);
        if (sample is not null && Environment.TickCount64 - sample.TakenAtTicks < SampleIntervalMs)
            return sample;

        lock (sampleSync)
        {
            // Re-check under the lock: another caller may have sampled while we waited.
            sample = lastSample;
            if (sample is not null && Environment.TickCount64 - sample.TakenAtTicks < SampleIntervalMs)
                return sample;

            DiskVolumeReading reading = DiskVolumeReading.Unknown;
            string? error = null;
            try
            {
                DiskVolumeReading? probe = volumeProvider();
                if (probe is DiskVolumeReading value)
                    reading = value;
                else
                    error = "free space of the data volume cannot be determined (no data directory to watch)";
            }
            catch (Exception ex)
            {
                error = ex.Message;
                if (!probeFailureLogged)
                {
                    probeFailureLogged = true;
                    Log.LogDiskSpaceProbeFailed(logger, ex.Message);
                }
            }

            sample = new VolumeSample(Environment.TickCount64, reading, error);
            Volatile.Write(ref lastSample, sample);
            return sample;
        }
    }

    private void ReportTransition(bool low, long availableBytes, long minFreeBytes)
    {
        if (low == reportedLow)
            return;

        lock (sampleSync)
        {
            if (low == reportedLow)
                return;

            reportedLow = low;

            if (low)
                Log.LogDiskSpaceLow(logger, availableBytes, minFreeBytes);
            else
                Log.LogDiskSpaceRecovered(logger, availableBytes, minFreeBytes);
        }
    }

    /// <summary>
    /// Builds the real probe over <see cref="DriveInfo.AvailableFreeSpace"/> for the volume that
    /// contains <paramref name="dataDirectory"/>. The <see cref="DriveInfo"/> is constructed per
    /// probe, not cached: the directory may not exist at engine construction time, and a stale
    /// handle would keep answering for an unmounted volume. <c>AvailableFreeSpace</c> (not
    /// <c>TotalFreeSpace</c>) is deliberate — it honors per-user quotas.
    /// </summary>
    internal static Func<long?> BuildDriveProvider(string? dataDirectory)
    {
        if (string.IsNullOrWhiteSpace(dataDirectory))
            return static () => null;

        string fullPath = Path.GetFullPath(dataDirectory);

        return () => ProbeDrive(fullPath, static drive => (long?)drive.AvailableFreeSpace);
    }

    /// <summary>
    /// Builds the full-reading probe for the volume that contains <paramref name="directory"/>: total,
    /// free and available bytes. Each <see cref="DriveInfo"/> property is its own <c>statvfs</c> call, so
    /// one probe is three syscalls of a few microseconds each, paid at most once per
    /// <see cref="SampleIntervalMs"/>. Per-probe construction follows <see cref="BuildDriveProvider"/>.
    /// </summary>
    internal static Func<DiskVolumeReading?> BuildVolumeProvider(string? directory)
    {
        if (string.IsNullOrWhiteSpace(directory))
            return static () => null;

        string fullPath = Path.GetFullPath(directory);

        return () => ProbeDrive(
            fullPath,
            static drive => (DiskVolumeReading?)new DiskVolumeReading(drive.TotalSize, drive.TotalFreeSpace, drive.AvailableFreeSpace));
    }

    /// <summary>
    /// Runs <paramref name="read"/> against the volume holding <paramref name="fullPath"/>. On Unix any
    /// rooted path resolves to its containing mount; on Windows <see cref="DriveInfo"/> needs the root,
    /// so the path root is used when the directory itself is rejected.
    /// </summary>
    private static T? ProbeDrive<T>(string fullPath, Func<DriveInfo, T?> read) where T : struct
    {
        try
        {
            return read(new DriveInfo(fullPath));
        }
        catch (ArgumentException)
        {
            string? root = Path.GetPathRoot(fullPath);
            return string.IsNullOrEmpty(root) ? null : read(new DriveInfo(root));
        }
    }
}
