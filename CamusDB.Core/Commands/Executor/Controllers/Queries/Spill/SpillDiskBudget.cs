/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// Counts the bytes of every live spill file under one spill root and refuses a write that would
/// pass either of two limits. Spill files share the volume with the storage engine, and a storage
/// engine that hits a full disk enters an error state that a restart does not clear. So a spill
/// must fail its own statement before it can take that space.
///
/// <list type="bullet">
/// <item><b>Total bytes</b> (<see cref="CamusDBOptions.SpillMaxTotalBytes"/>): the sum over all live
///   scopes, so many concurrent spilling queries share one limit. Refused with
///   <see cref="CamusDBErrorCodes.SpillLimitExceeded"/>.</item>
/// <item><b>Free-space floor</b> (<see cref="CamusDBOptions.MinFreeDiskBytes"/>, the same watermark
///   that stops DML): a spill never takes the volume below the point where writes stop. Refused with
///   <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/>.</item>
/// </list>
///
/// <para><b>Accounting:</b> a <see cref="SpillScope"/> reserves the bytes of each write before the
/// write and releases its whole total when it is disposed, which is also when its files are deleted.
/// No spill file is deleted before its scope ends, so the count follows the bytes on disk. The count
/// is of encoded bytes; file-system block overhead is not included.</para>
///
/// <para><b>Free-space estimate:</b> a real probe of the volume runs at most every
/// <see cref="SampleIntervalMs"/>. Between probes the estimate is the last reading minus the spill
/// bytes reserved since that reading, so a fast writer cannot pass the floor inside one interval.
/// When the estimate falls below the floor, a new probe confirms it before the write is refused,
/// because other files may have been deleted since the last reading. Writes of the storage engine
/// itself between probes are not seen; the DML gate on the same watermark is the backstop for those.
/// A probe that fails reads as "unknown" and the floor check passes (fail open), as in
/// <see cref="Storage.DiskSpaceMonitor"/>: broken monitoring must not stop every spilling query.</para>
///
/// <para>Thread-safe. The reservation is one <see cref="Interlocked.Add(ref long, long)"/>; a
/// reservation that passes a limit is given back. Two writers near the limit can therefore both be
/// refused where one of them would fit. That error is on the safe side and needs no lock.</para>
/// </summary>
internal sealed class SpillDiskBudget
{
    /// <summary>Minimum milliseconds between two scheduled probes of the volume.</summary>
    internal const long SampleIntervalMs = 2000;

    /// <summary>
    /// Minimum milliseconds between two confirmation probes when the estimate is below the floor.
    /// Inside this window the estimate is trusted, so a writer at the floor does not probe per row.
    /// </summary>
    private const long RecheckIntervalMs = 50;

    /// <summary>One reading of the volume and the spill bytes reserved at that moment.</summary>
    private sealed record Sample(long Ticks, long FreeBytes, long UsedBytes);

    private readonly Func<long?> freeBytesProvider;

    /// <summary>Bytes reserved by all live scopes.</summary>
    private long usedBytes;

    /// <summary>The last reading, swapped as a whole so a reader never sees a torn pair. Null = never probed.</summary>
    private Sample? sample;

    /// <summary>Watches the volume that holds <paramref name="spillRoot"/>.</summary>
    public SpillDiskBudget(string spillRoot)
        : this(Storage.DiskSpaceMonitor.BuildDriveProvider(spillRoot))
    {
    }

    /// <summary>
    /// Test seam: <paramref name="freeBytesProvider"/> gives the free bytes of the volume directly
    /// (null = unknown).
    /// </summary>
    internal SpillDiskBudget(Func<long?> freeBytesProvider)
    {
        this.freeBytesProvider = freeBytesProvider;
    }

    /// <summary>Bytes that live spill files hold now.</summary>
    public long UsedBytes => Volatile.Read(ref usedBytes);

    /// <summary>
    /// Reserves <paramref name="bytes"/> for a write that is about to happen, or throws and reserves
    /// nothing. A limit that is zero or less is off.
    /// </summary>
    public void Reserve(long bytes, long maxTotalBytes, long minFreeDiskBytes)
    {
        long used = Interlocked.Add(ref usedBytes, bytes);

        if (maxTotalBytes > 0 && used > maxTotalBytes)
        {
            Interlocked.Add(ref usedBytes, -bytes);
            throw new CamusDBException(
                CamusDBErrorCodes.SpillLimitExceeded,
                $"Spill refused: this write needs {bytes} bytes, but spill files on this node already hold " +
                $"{used - bytes} bytes and 'spill_max_total_bytes' is {maxTotalBytes}; " +
                "run the statement when fewer queries spill, make it spill less, or raise the limit");
        }

        if (minFreeDiskBytes > 0 && IsBelowFloor(used, bytes, minFreeDiskBytes, out long estimatedFree))
        {
            Interlocked.Add(ref usedBytes, -bytes);
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientDiskSpace,
                $"Spill refused: the spill volume has about {estimatedFree} bytes free, below the " +
                $"'min_free_disk_bytes' watermark of {minFreeDiskBytes}; free disk space on this node, then retry");
        }
    }

    /// <summary>Gives back bytes whose files are deleted.</summary>
    public void Release(long bytes)
    {
        if (bytes != 0)
            Interlocked.Add(ref usedBytes, -bytes);
    }

    /// <summary>
    /// True when the estimated free space, after the reservation of <paramref name="bytes"/> that
    /// brought the total to <paramref name="used"/>, is below <paramref name="floor"/>. An unknown
    /// reading answers false.
    /// </summary>
    private bool IsBelowFloor(long used, long bytes, long floor, out long estimatedFree)
    {
        long now = Environment.TickCount64;
        Sample? current = Volatile.Read(ref sample);

        if (current is null || now - current.Ticks >= SampleIntervalMs)
            current = Probe(now, used - bytes);

        estimatedFree = Estimate(current, used);
        if (estimatedFree < 0 || estimatedFree >= floor)
            return false;

        if (now - current.Ticks >= RecheckIntervalMs)
        {
            current = Probe(now, used - bytes);
            estimatedFree = Estimate(current, used);
            if (estimatedFree < 0 || estimatedFree >= floor)
                return false;
        }

        return true;
    }

    /// <summary>
    /// The free bytes now: the reading, less the spill bytes reserved since it. -1 when the reading
    /// is unknown. A release since the reading adds bytes back, because the files of that scope are
    /// deleted.
    /// </summary>
    private static long Estimate(Sample current, long used)
    {
        if (current.FreeBytes < 0)
            return -1;

        return Math.Max(0, current.FreeBytes - (used - current.UsedBytes));
    }

    /// <summary>
    /// Reads the volume and publishes the result. <paramref name="usedOnDisk"/> is the total before
    /// the write that asks, because the reading cannot include a write that has not happened. Two
    /// threads can probe at the same time; the later one wins, and both readings are valid, so no
    /// lock is needed.
    /// </summary>
    private Sample Probe(long now, long usedOnDisk)
    {
        long? free;
        try
        {
            free = freeBytesProvider();
        }
        catch
        {
            free = null;
        }

        Sample next = new(now, free ?? -1, usedOnDisk);
        Volatile.Write(ref sample, next);
        return next;
    }
}
