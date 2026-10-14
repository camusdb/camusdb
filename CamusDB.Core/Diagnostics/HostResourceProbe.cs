/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Runtime.InteropServices;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Reads the host-level figures the node stats call reports and the .NET runtime does not keep: the
/// load averages and the memory the host (or the container) still has available.
///
/// <para><b>Linux first, macOS second, nothing else.</b> Production runs on Linux, where everything
/// comes from <c>/proc</c> and the cgroup filesystem — plain text reads of a few microseconds, no
/// native calls. macOS is the development platform and uses <c>getloadavg</c> and <c>sysctl</c>.
/// Any other system, and any read that fails, yields <c>-1</c> with a reason, never a guessed
/// value: the stats call turns the reason into an <c>errors</c> entry.</para>
///
/// <para><b>"Available" inside a container is the container's headroom.</b> With a cgroup memory
/// limit in force, the host's <c>MemAvailable</c> describes a machine the process cannot use, so the
/// figure becomes the limit minus the cgroup's working set (usage less reclaimable inactive file
/// cache, the same subtraction <c>docker stats</c> makes), capped by the host's own
/// <c>MemAvailable</c> because an overcommitted host can run out first. The cgroup walk itself is
/// <see cref="MachineMemory.ResolveLinuxCgroupLimit"/>, shared with the engine's memory sizing.</para>
///
/// <para>Not on the request path: <see cref="NodeResourceSampler"/> calls this on its background
/// tick and the stats call reads the published result.</para>
/// </summary>
internal static class HostResourceProbe
{
    /// <summary>The three load averages, or -1 each and a reason when the system has none.</summary>
    internal static (double OneMinute, double FiveMinutes, double FifteenMinutes, string? Error) ReadLoadAverages(Func<string, string?> readFile)
    {
        try
        {
            if (OperatingSystem.IsLinux())
                return ParseLoadAverages(readFile("/proc/loadavg"));

            if (OperatingSystem.IsMacOS())
            {
                double[] samples = new double[3];
                if (getloadavg(samples, 3) == 3)
                    return (samples[0], samples[1], samples[2], null);

                return (-1, -1, -1, "getloadavg returned fewer than three samples");
            }

            return (-1, -1, -1, "load averages are not available on this operating system");
        }
        catch (Exception ex)
        {
            return (-1, -1, -1, ex.Message);
        }
    }

    /// <summary>
    /// Parses the first three fields of <c>/proc/loadavg</c> (<c>0.52 0.58 0.59 1/389 12345</c>).
    /// A missing or malformed file yields -1 each and a reason.
    /// </summary>
    internal static (double OneMinute, double FiveMinutes, double FifteenMinutes, string? Error) ParseLoadAverages(string? content)
    {
        if (content is null)
            return (-1, -1, -1, "/proc/loadavg cannot be read");

        string[] fields = content.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (fields.Length >= 3
            && double.TryParse(fields[0], NumberStyles.Float, CultureInfo.InvariantCulture, out double one)
            && double.TryParse(fields[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double five)
            && double.TryParse(fields[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double fifteen))
            return (one, five, fifteen, null);

        return (-1, -1, -1, "/proc/loadavg has an unexpected format");
    }

    /// <summary>
    /// The memory the node may use in total and how much of it is still available, in bytes — the
    /// cgroup limit and the cgroup's headroom when a limit applies, else the host's
    /// <c>MemTotal</c>/<c>MemAvailable</c>. Either value is -1 when it cannot be read, with a reason.
    /// </summary>
    internal static (long TotalBytes, long AvailableBytes, string? Error) ReadMemory(Func<string, string?> readFile)
    {
        try
        {
            if (OperatingSystem.IsLinux())
                return ReadLinuxMemory(readFile);

            if (OperatingSystem.IsMacOS())
                return ReadDarwinMemory();

            return (-1, -1, "host memory is not available on this operating system");
        }
        catch (Exception ex)
        {
            return (-1, -1, ex.Message);
        }
    }

    /// <summary>The Linux reading, through <paramref name="readFile"/> so tests can supply a synthetic <c>/proc</c> and cgroup tree.</summary>
    internal static (long TotalBytes, long AvailableBytes, string? Error) ReadLinuxMemory(Func<string, string?> readFile)
    {
        string? meminfo = readFile("/proc/meminfo");
        long hostTotal = MachineMemory.ParseMeminfoBytes(meminfo, "MemTotal:");
        long hostAvailable = MachineMemory.ParseMeminfoBytes(meminfo, "MemAvailable:");

        (long limit, string? directory, bool isV2) = MachineMemory.ResolveLinuxCgroupLimit(readFile);

        if (limit > 0 && directory is not null && (hostTotal <= 0 || limit < hostTotal))
        {
            long usage = ParseCounter(readFile(directory + (isV2 ? "/memory.current" : "/memory.usage_in_bytes")));
            if (usage < 0)
                return (limit, -1, "the cgroup's memory usage cannot be read");

            long inactiveFile = ParseStatField(readFile(directory + "/memory.stat"), isV2 ? "inactive_file" : "total_inactive_file");
            long workingSet = Math.Max(0, usage - Math.Max(0, inactiveFile));
            long headroom = Math.Max(0, limit - workingSet);

            return (limit, hostAvailable > 0 ? Math.Min(hostAvailable, headroom) : headroom, null);
        }

        if (hostTotal <= 0 || hostAvailable <= 0)
            return (hostTotal > 0 ? hostTotal : -1, hostAvailable > 0 ? hostAvailable : -1, "/proc/meminfo has no MemTotal or MemAvailable");

        return (hostTotal, hostAvailable, null);
    }

    /// <summary>
    /// macOS: physical memory from <c>hw.memsize</c>, and available memory from
    /// <c>kern.memorystatus_level</c> — the kernel's own percentage of memory available, the figure its
    /// memory-pressure logic acts on. It has a resolution of one percent of physical memory, which is
    /// enough for a development machine.
    /// </summary>
    private static (long TotalBytes, long AvailableBytes, string? Error) ReadDarwinMemory()
    {
        long total = MachineMemory.ReadDarwinSysctl("hw.memsize");
        if (total <= 0)
            return (-1, -1, "sysctl hw.memsize failed");

        long level = MachineMemory.ReadDarwinSysctl("kern.memorystatus_level");
        if (level is <= 0 or > 100)
            return (total, -1, "sysctl kern.memorystatus_level failed");

        return (total, total / 100 * level, null);
    }

    /// <summary>Parses a cgroup counter file holding one integer; -1 when absent or malformed.</summary>
    private static long ParseCounter(string? content)
    {
        if (content is null)
            return -1;

        return long.TryParse(content.AsSpan().Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out long value) ? value : -1;
    }

    /// <summary>Reads one <c>key value</c> line of a cgroup <c>memory.stat</c> file; 0 when absent, which subtracts nothing.</summary>
    internal static long ParseStatField(string? content, string key)
    {
        if (content is null)
            return 0;

        foreach (string rawLine in content.Split('\n'))
        {
            ReadOnlySpan<char> line = rawLine.AsSpan().Trim();
            if (line.Length <= key.Length || !line.StartsWith(key, StringComparison.Ordinal) || line[key.Length] != ' ')
                continue;

            return long.TryParse(line[(key.Length + 1)..].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out long value) ? value : 0;
        }

        return 0;
    }

    [DllImport("libc", EntryPoint = "getloadavg")]
    private static extern int getloadavg(double[] loadavg, int nelem);
}
