/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

namespace CamusDB.Core.Config;

/// <summary>
/// The memory a node may plan its native caches against: the container's cgroup limit when one is set,
/// otherwise the machine's physical memory.
///
/// <para>Not <see cref="GC.GetGCMemoryInfo"/>'s <c>TotalAvailableMemoryBytes</c>. That figure honours a
/// GC heap hard limit (<c>DOTNET_GCHeapHardLimit</c> / <c>Percent</c>), which describes the managed heap
/// only; RocksDB's block cache and memtables are native allocations that live outside it. Sizing them
/// from the heap limit under-provisions them precisely when an operator pins the heap to keep a
/// container inside its limit: on the reference node (4,096 MB container, 60% heap limit) the Raft
/// WAL's memtable budget came out at 61 MB, below the 192 MB flush unit, and every Raft-log flush was
/// forced by the write-buffer manager (CamusDB feature 6f0e0fb7, task 5).</para>
/// </summary>
public static class HostMemory
{
    /// <summary>Test seam: when set, <see cref="TotalBytes"/> returns this instead of probing the host.</summary>
    internal static long? Override;

    /// <summary>
    /// Bytes the node may plan against: cgroup v2 <c>memory.max</c>, else cgroup v1
    /// <c>memory.limit_in_bytes</c> (both only when they are a real limit below physical memory), else
    /// <c>/proc/meminfo</c> <c>MemTotal</c>; on a platform without those, the GC's figure.
    /// </summary>
    public static long TotalBytes()
    {
        if (Override is long forced)
            return forced;

        long physical = ReadMemInfoTotal();
        long cgroup = ReadCgroupLimit();

        if (cgroup > 0 && (physical <= 0 || cgroup < physical))
            return cgroup;

        if (physical > 0)
            return physical;

        return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
    }

    private static long ReadCgroupLimit()
    {
        // cgroup v2: a plain number, or "max" for unlimited. cgroup v1: a number, with "unlimited" encoded as
        // a very large value (9223372036854771712), which the physical-memory comparison discards.
        foreach (string path in new[] { "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes" })
        {
            try
            {
                if (!File.Exists(path))
                    continue;

                string text = File.ReadAllText(path).Trim();
                if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long bytes) && bytes > 0)
                    return bytes;
            }
            catch (Exception)
            {
                // Unreadable or unavailable: fall through to the next source.
            }
        }

        return 0;
    }

    private static long ReadMemInfoTotal()
    {
        try
        {
            const string path = "/proc/meminfo";
            if (!File.Exists(path))
                return 0;

            foreach (string line in File.ReadLines(path))
            {
                if (!line.StartsWith("MemTotal:", StringComparison.Ordinal))
                    continue;

                string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 2 && long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out long kib))
                    return kib * 1024;
            }
        }
        catch (Exception)
        {
            // No /proc on this platform, or unreadable: fall through.
        }

        return 0;
    }
}
