
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
/// <para>Thread-safe. One instance per <see cref="Transactions.KvTransactionsManager"/> — several
/// instances may watch the same volume, which is harmless because a probe is microseconds.</para>
/// </summary>
public sealed class DiskSpaceMonitor
{
    /// <summary>Minimum milliseconds between two real free-space probes; results are cached between them.</summary>
    private const int SampleIntervalMs = 2000;

    private readonly Func<long?> freeBytesProvider;

    private readonly ILogger logger;

    private readonly Lock sampleSync = new();

    /// <summary><see cref="Environment.TickCount64"/> of the last probe; 0 = never sampled.</summary>
    private long lastSampleTicks;

    /// <summary>Free bytes at the last probe; -1 = unknown (probe failed or never ran).</summary>
    private long lastFreeBytes = -1;

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
        : this(BuildDriveProvider(dataDirectory), logger)
    {
    }

    /// <summary>
    /// Test seam: <paramref name="freeBytesProvider"/> supplies the free-byte reading directly
    /// (null = reading unavailable). Also used by the production constructor via
    /// <see cref="BuildDriveProvider"/>.
    /// </summary>
    internal DiskSpaceMonitor(Func<long?> freeBytesProvider, ILogger logger)
    {
        this.freeBytesProvider = freeBytesProvider;
        this.logger = logger;
    }

    /// <summary>
    /// Returns true when the watched volume's free space is below <paramref name="minFreeBytes"/>.
    /// <paramref name="freeBytes"/> carries the sampled reading (-1 when unknown). A non-positive
    /// watermark and an unknown reading both answer false — the gate is disabled, or fails open.
    /// State transitions (entering/leaving the low-space condition) are logged here, on the
    /// sampling path, so the log reflects what the gate actually enforced.
    /// </summary>
    public bool IsBelow(long minFreeBytes, out long freeBytes)
    {
        if (minFreeBytes <= 0)
        {
            freeBytes = -1;
            return false;
        }

        long now = Environment.TickCount64;
        long sampledTicks = Volatile.Read(ref lastSampleTicks);

        if (sampledTicks == 0 || now - sampledTicks >= SampleIntervalMs)
        {
            lock (sampleSync)
            {
                // Re-check under the lock: another writer may have sampled while we waited.
                if (lastSampleTicks == 0 || Environment.TickCount64 - lastSampleTicks >= SampleIntervalMs)
                {
                    long? probe = null;
                    try
                    {
                        probe = freeBytesProvider();
                    }
                    catch (Exception ex)
                    {
                        if (!probeFailureLogged)
                        {
                            probeFailureLogged = true;
                            Log.LogDiskSpaceProbeFailed(logger, ex.Message);
                        }
                    }

                    Volatile.Write(ref lastFreeBytes, probe ?? -1);
                    Volatile.Write(ref lastSampleTicks, Environment.TickCount64);
                }
            }
        }

        freeBytes = Volatile.Read(ref lastFreeBytes);

        bool low = freeBytes >= 0 && freeBytes < minFreeBytes;

        if (low != reportedLow)
        {
            lock (sampleSync)
            {
                if (low != reportedLow)
                {
                    reportedLow = low;

                    if (low)
                        Log.LogDiskSpaceLow(logger, freeBytes, minFreeBytes);
                    else
                        Log.LogDiskSpaceRecovered(logger, freeBytes, minFreeBytes);
                }
            }
        }

        return low;
    }

    /// <summary>
    /// Builds the real probe over <see cref="DriveInfo.AvailableFreeSpace"/> for the volume that
    /// contains <paramref name="dataDirectory"/>. The <see cref="DriveInfo"/> is constructed per
    /// probe, not cached: the directory may not exist at engine construction time, and a stale
    /// handle would keep answering for an unmounted volume. <c>AvailableFreeSpace</c> (not
    /// <c>TotalFreeSpace</c>) is deliberate — it honors per-user quotas.
    /// </summary>
    private static Func<long?> BuildDriveProvider(string? dataDirectory)
    {
        if (string.IsNullOrWhiteSpace(dataDirectory))
            return static () => null;

        string fullPath = Path.GetFullPath(dataDirectory);

        return () =>
        {
            // On Unix any rooted path resolves to its containing mount; on Windows DriveInfo
            // needs the root, so walk up to the path root when the directory itself is rejected.
            try
            {
                return new DriveInfo(fullPath).AvailableFreeSpace;
            }
            catch (ArgumentException)
            {
                string? root = Path.GetPathRoot(fullPath);
                if (string.IsNullOrEmpty(root))
                    return null;

                return new DriveInfo(root).AvailableFreeSpace;
            }
        };
    }
}
