/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please see the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Runtime.InteropServices;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Reads how much memory the process may use <b>as a whole</b> — the container's cgroup memory
/// limit when one is set, else the machine's physical RAM — for sizing native allocations such as
/// the RocksDB block cache and memtables.
///
/// <para><b>Why not <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>.</b> That value is the
/// managed heap's budget: under <c>DOTNET_GCHeapHardLimit</c> / <c>GCHeapHardLimitPercent</c> it
/// is the heap limit, and inside a memory-limited container with no explicit limit the runtime
/// already defaults the heap to 75% of the container. RocksDB memory is native and lives
/// <i>outside</i> the heap, so sizing it from the heap limit under-provisions it exactly when an
/// operator pins the heap to keep the node inside its container: a 4,096 MB container with a 60%
/// heap limit reported 2,458 MB, which sized a 61 MB memtable budget, below the Raft log's flush
/// unit. The managed caches (the key/value actor caches) are a different matter; they live inside
/// the heap and are still sized from <see cref="ManagedHeapBytes"/>.</para>
///
/// <para><b>Sources, in order.</b> On Linux, the smallest numeric cgroup limit on the path from
/// this process's cgroup up to the hierarchy root (v2 <c>memory.max</c>, or v1
/// <c>memory.limit_in_bytes</c>), capped by <c>MemTotal</c> from <c>/proc/meminfo</c>; on macOS,
/// <c>sysctl hw.memsize</c>. Anywhere a read fails, the GC's figure is the fallback, which is
/// correct whenever no heap limit is configured. The cgroup walk assumes the usual mounts at
/// <c>/sys/fs/cgroup</c> (with or without a cgroup namespace); an unusual mount layout falls back
/// to <c>MemTotal</c>, never to a smaller value.</para>
/// </summary>
internal static class MachineMemory
{
    /// <summary>A v1 cgroup reports "no limit" as a page-aligned value near <see cref="long.MaxValue"/>; anything at or above this is treated as unlimited.</summary>
    private const long UnlimitedThreshold = 1L << 62;

    private const string CgroupRoot = "/sys/fs/cgroup";

    /// <summary>
    /// The memory the whole process may use, and a short label naming where the figure came from
    /// (for the startup log). Returns the GC's figure with source <c>gc</c> when nothing better
    /// can be read, and zero only when even that is unknown.
    /// </summary>
    public static (long Bytes, string Source) ReadTotal()
    {
        try
        {
            if (OperatingSystem.IsLinux())
            {
                long physical = ParseMemTotalBytes(TryReadFile("/proc/meminfo"));
                long cgroup = ReadLinuxCgroupLimit(TryReadFile);

                if (cgroup > 0 && (physical <= 0 || cgroup < physical))
                    return (cgroup, "cgroup");

                if (physical > 0)
                    return (physical, "meminfo");
            }
            else if (OperatingSystem.IsMacOS())
            {
                long physical = ReadDarwinMemSize();
                if (physical > 0)
                    return (physical, "sysctl");
            }
        }
        catch (Exception)
        {
            // A probe that throws must never stop a node from starting; the GC figure below is a
            // correct answer whenever no heap limit is configured.
        }

        return (ManagedHeapBytes(), "gc");
    }

    /// <summary>
    /// The managed heap's budget as the GC sees it: the heap hard limit when one is in force, else
    /// the (container-aware) physical memory. Size managed caches from this, never native ones.
    /// </summary>
    public static long ManagedHeapBytes() => GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;

    /// <summary>
    /// Resolves the effective cgroup memory limit for the current process through
    /// <paramref name="readFile"/> (a seam so tests can supply a synthetic filesystem): the smallest
    /// numeric limit from the process's own cgroup directory up to the root, trying cgroup v2 first
    /// and v1 second. Returns 0 when no limit is set or none can be read.
    /// </summary>
    internal static long ReadLinuxCgroupLimit(Func<string, string?> readFile)
    {
        string? membership = readFile("/proc/self/cgroup");
        if (membership is null)
            return 0;

        string? v2Path = null;
        string? v1Path = null;

        foreach (string rawLine in membership.Split('\n'))
        {
            string line = rawLine.Trim();
            if (line.Length == 0)
                continue;

            // hierarchy-id:controller-list:path — the path itself may contain ':'.
            int first = line.IndexOf(':');
            int second = first < 0 ? -1 : line.IndexOf(':', first + 1);
            if (second < 0)
                continue;

            string controllers = line.Substring(first + 1, second - first - 1);
            string path = line.Substring(second + 1);

            if (controllers.Length == 0 && line.StartsWith("0:", StringComparison.Ordinal))
                v2Path = path;
            else if (controllers.Split(',').Contains("memory", StringComparer.Ordinal))
                v1Path = path;
        }

        if (v2Path is not null)
        {
            long limit = SmallestLimitOnPath(readFile, CgroupRoot, v2Path, "memory.max");
            if (limit > 0)
                return limit;
        }

        if (v1Path is not null)
            return SmallestLimitOnPath(readFile, CgroupRoot + "/memory", v1Path, "memory.limit_in_bytes");

        return 0;
    }

    /// <summary>
    /// Walks from <c>{mount}{cgroupPath}</c> up to <c>{mount}</c>, reading <paramref name="file"/>
    /// at each level, and returns the smallest numeric limit found (a parent's limit binds a child
    /// that sets none). Inside a cgroup namespace the process path is <c>/</c> and only the mount
    /// root is read.
    /// </summary>
    private static long SmallestLimitOnPath(Func<string, string?> readFile, string mount, string cgroupPath, string file)
    {
        long smallest = 0;

        // A path that climbs out of the namespace ("/../..") names a directory this mount cannot
        // show; read the mount root only rather than resolve it against the wrong hierarchy.
        string relative = cgroupPath.Contains("..", StringComparison.Ordinal) ? "" : cgroupPath.TrimEnd('/');

        while (true)
        {
            long limit = ParseCgroupLimit(readFile(mount + relative + "/" + file));
            if (limit > 0 && (smallest == 0 || limit < smallest))
                smallest = limit;

            if (relative.Length == 0)
                return smallest;

            int slash = relative.LastIndexOf('/');
            relative = slash <= 0 ? "" : relative[..slash];
        }
    }

    /// <summary>
    /// Parses a cgroup memory limit file. <c>max</c> (v2), a missing file, and the v1 "unlimited"
    /// sentinel all yield 0, meaning "no limit at this level".
    /// </summary>
    internal static long ParseCgroupLimit(string? content)
    {
        if (content is null)
            return 0;

        ReadOnlySpan<char> value = content.AsSpan().Trim();
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out long bytes))
            return 0;

        return bytes is > 0 and < UnlimitedThreshold ? bytes : 0;
    }

    /// <summary>Parses <c>MemTotal:  N kB</c> out of <c>/proc/meminfo</c>; 0 when absent or malformed.</summary>
    internal static long ParseMemTotalBytes(string? meminfo)
    {
        if (meminfo is null)
            return 0;

        foreach (string rawLine in meminfo.Split('\n'))
        {
            ReadOnlySpan<char> line = rawLine.AsSpan();
            if (!line.StartsWith("MemTotal:", StringComparison.Ordinal))
                continue;

            ReadOnlySpan<char> rest = line["MemTotal:".Length..].Trim();
            int space = rest.IndexOf(' ');
            ReadOnlySpan<char> number = space < 0 ? rest : rest[..space];

            return long.TryParse(number, NumberStyles.None, CultureInfo.InvariantCulture, out long kb) && kb > 0
                ? checked(kb * 1024)
                : 0;
        }

        return 0;
    }

    private static string? TryReadFile(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path) : null;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static long ReadDarwinMemSize()
    {
        long value = 0;
        nint size = sizeof(long);
        return sysctlbyname("hw.memsize", ref value, ref size, IntPtr.Zero, 0) == 0 ? value : 0;
    }

    [DllImport("libc", EntryPoint = "sysctlbyname")]
    private static extern int sysctlbyname(string name, ref long oldp, ref nint oldlenp, IntPtr newp, nint newlen);
}
