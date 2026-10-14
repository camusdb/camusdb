/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Unit tests for the pieces behind the node stats call that need no engine: the CPU window of
/// <see cref="NodeResourceSampler"/> (driven by a fake clock and a fake CPU counter), the host probe's
/// parsers over a synthetic <c>/proc</c> and cgroup tree, the mount-point match, symbolic-link
/// resolution, and the engine-size scan.
/// </summary>
[TestFixture]
internal sealed class TestNodeResourceSampler
{
    private static NodeStoragePaths MemoryPaths() =>
        new(Path.GetTempPath(), "", "memory", "", "memory");

    private static long Seconds(double seconds) => (long)(seconds * Stopwatch.Frequency);

    // ─── CPU window ───────────────────────────────────────────────────────────

    [Test]
    public void CpuPercent_IsTheAverageOverTheRing_AndTheRingCapsAtOneMinute()
    {
        int processors = Environment.ProcessorCount;
        long now = Seconds(1000);
        TimeSpan cpu = TimeSpan.FromSeconds(10);

        NodeResourceSampler sampler = new(MemoryPaths(), NullLogger.Instance,
            _ => null, () => cpu, () => now, () => ["/"]);

        // The first reading has no window of its own: it reports the lifetime average.
        sampler.Sample();
        Assert.Greater(sampler.Resources!.CpuWindowMs, 0);
        Assert.That(sampler.Resources.CpuPercent, Is.InRange(0.0, 100.0));

        // Half the node's capacity for one tick.
        now += Seconds(5);
        cpu += TimeSpan.FromSeconds(5 * processors * 0.5);
        sampler.Sample();
        Assert.AreEqual(5000, sampler.Resources!.CpuWindowMs);
        Assert.AreEqual(50.0, sampler.Resources.CpuPercent, 0.0001);

        // Eleven more ticks at 50%: the window reaches one minute.
        for (int i = 0; i < 11; i++)
        {
            now += Seconds(5);
            cpu += TimeSpan.FromSeconds(5 * processors * 0.5);
            sampler.Sample();
        }
        Assert.AreEqual(60000, sampler.Resources!.CpuWindowMs);
        Assert.AreEqual(50.0, sampler.Resources.CpuPercent, 0.0001);

        // Twelve ticks at 25%: the window stays one minute and the 50% minute has slid out of it.
        for (int i = 0; i < 12; i++)
        {
            now += Seconds(5);
            cpu += TimeSpan.FromSeconds(5 * processors * 0.25);
            sampler.Sample();
        }
        Assert.AreEqual(60000, sampler.Resources!.CpuWindowMs);
        Assert.AreEqual(25.0, sampler.Resources.CpuPercent, 0.0001);
        Assert.IsNull(sampler.Resources.CpuError);
    }

    [Test]
    public void CpuPercent_IsClampedToTheNodesCapacity()
    {
        long now = Seconds(1000);
        TimeSpan cpu = TimeSpan.Zero;
        NodeResourceSampler sampler = new(MemoryPaths(), NullLogger.Instance, _ => null, () => cpu, () => now, () => ["/"]);

        sampler.Sample();
        now += Seconds(5);
        cpu += TimeSpan.FromSeconds(5 * Environment.ProcessorCount * 1.01); // rounding across two clocks
        sampler.Sample();

        Assert.AreEqual(100.0, sampler.Resources!.CpuPercent);
    }

    [Test]
    public void CpuProbeFailure_IsMinusOneWithAReason_AndTheOtherFiguresStillPublish()
    {
        NodeResourceSampler sampler = new(MemoryPaths(), NullLogger.Instance,
            _ => null, () => throw new InvalidOperationException("no cpu counter"), () => Seconds(1), () => ["/"]);

        sampler.Sample();

        Assert.AreEqual(-1, sampler.Resources!.CpuPercent);
        Assert.AreEqual(-1, sampler.Resources.CpuWindowMs);
        Assert.AreEqual("no cpu counter", sampler.Resources.CpuError);
        Assert.IsNotNull(sampler.Storage, "a failed CPU probe must not stop the rest of the tick");
    }

    // ─── Load averages ────────────────────────────────────────────────────────

    [Test]
    public void ParseLoadAverages_ReadsTheFirstThreeFields()
    {
        (double one, double five, double fifteen, string? error) = HostResourceProbe.ParseLoadAverages("0.52 1.58 12.09 1/389 12345\n");

        Assert.IsNull(error);
        Assert.AreEqual(0.52, one, 1e-9);
        Assert.AreEqual(1.58, five, 1e-9);
        Assert.AreEqual(12.09, fifteen, 1e-9);
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("garbage")]
    [TestCase("0.5 x 0.7 1/2 3")]
    public void ParseLoadAverages_UnreadableIsMinusOneWithAReason(string? content)
    {
        (double one, double five, double fifteen, string? error) = HostResourceProbe.ParseLoadAverages(content);

        Assert.AreEqual(-1, one);
        Assert.AreEqual(-1, five);
        Assert.AreEqual(-1, fifteen);
        Assert.IsNotNull(error);
    }

    // ─── Host memory (Linux, synthetic files) ────────────────────────────────

    private const string MemInfo = "MemTotal:       16000000 kB\nMemFree:         1000000 kB\nMemAvailable:    8000000 kB\n";

    private static Func<string, string?> Files(Dictionary<string, string> files) =>
        path => files.TryGetValue(path, out string? content) ? content : null;

    [Test]
    public void LinuxMemory_WithoutACgroupLimit_IsMemTotalAndMemAvailable()
    {
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = MemInfo,
        }));

        Assert.IsNull(error);
        Assert.AreEqual(16000000L * 1024, total);
        Assert.AreEqual(8000000L * 1024, available);
    }

    [Test]
    public void LinuxMemory_UnderACgroupV2Limit_IsTheLimitAndItsHeadroom()
    {
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = MemInfo,
            ["/proc/self/cgroup"] = "0::/camus\n",
            ["/sys/fs/cgroup/camus/memory.max"] = "1073741824\n",
            ["/sys/fs/cgroup/camus/memory.current"] = "600000000\n",
            // Reclaimable file cache does not count against the headroom; the v1-only total_ key must not match.
            ["/sys/fs/cgroup/camus/memory.stat"] = "anon 400000000\ntotal_inactive_file 7\ninactive_file 100000000\n",
        }));

        Assert.IsNull(error);
        Assert.AreEqual(1073741824L, total);
        Assert.AreEqual(1073741824L - (600000000L - 100000000L), available);
    }

    [Test]
    public void LinuxMemory_ReadsUsageWhereTheBindingLimitIs()
    {
        // The process's own cgroup sets no limit ("max"); its parent does. Usage must come from the
        // parent, whose limit is the one that binds.
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = MemInfo,
            ["/proc/self/cgroup"] = "0::/pod/app\n",
            ["/sys/fs/cgroup/pod/memory.max"] = "2147483648",
            ["/sys/fs/cgroup/pod/memory.current"] = "1000000000",
            ["/sys/fs/cgroup/pod/app/memory.max"] = "max",
            ["/sys/fs/cgroup/pod/app/memory.current"] = "1",
        }));

        Assert.IsNull(error);
        Assert.AreEqual(2147483648L, total);
        Assert.AreEqual(2147483648L - 1000000000L, available);
    }

    [Test]
    public void LinuxMemory_UnderACgroupV1Limit_UsesTheV1Files()
    {
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = MemInfo,
            ["/proc/self/cgroup"] = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n",
            ["/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes"] = "536870912",
            ["/sys/fs/cgroup/memory/docker/abc/memory.usage_in_bytes"] = "300000000",
            ["/sys/fs/cgroup/memory/docker/abc/memory.stat"] = "inactive_file 1\ntotal_inactive_file 50000000\n",
        }));

        Assert.IsNull(error);
        Assert.AreEqual(536870912L, total);
        Assert.AreEqual(536870912L - 250000000L, available);
    }

    [Test]
    public void LinuxMemory_AnOvercommittedHostCapsTheContainersHeadroom()
    {
        (_, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = "MemTotal: 16000000 kB\nMemAvailable: 100000 kB\n",
            ["/proc/self/cgroup"] = "0::/camus\n",
            ["/sys/fs/cgroup/camus/memory.max"] = "8589934592",
            ["/sys/fs/cgroup/camus/memory.current"] = "1000",
        }));

        Assert.IsNull(error);
        Assert.AreEqual(100000L * 1024, available);
    }

    [Test]
    public void LinuxMemory_UnreadableUsage_KeepsTheLimitAndReportsAvailableAsUnknown()
    {
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(Files(new()
        {
            ["/proc/meminfo"] = MemInfo,
            ["/proc/self/cgroup"] = "0::/camus\n",
            ["/sys/fs/cgroup/camus/memory.max"] = "1073741824",
        }));

        Assert.AreEqual(1073741824L, total);
        Assert.AreEqual(-1, available);
        Assert.IsNotNull(error);
    }

    [Test]
    public void LinuxMemory_NoMeminfo_IsMinusOneWithAReason()
    {
        (long total, long available, string? error) = HostResourceProbe.ReadLinuxMemory(_ => null);

        Assert.AreEqual(-1, total);
        Assert.AreEqual(-1, available);
        Assert.IsNotNull(error);
    }

    // ─── Mount points and paths ───────────────────────────────────────────────

    [Test]
    public void FindMountPoint_PicksTheLongestMountThatContainsThePath()
    {
        if (OperatingSystem.IsWindows())
            Assert.Ignore("Unix paths");

        string[] mounts = ["/", "/data", "/data/wal", "/database"];

        Assert.AreEqual("/data", NodeResourceSampler.FindMountPoint("/data/camus", mounts));
        Assert.AreEqual("/data", NodeResourceSampler.FindMountPoint("/data", mounts));
        Assert.AreEqual("/data/wal", NodeResourceSampler.FindMountPoint("/data/wal/v2", mounts));
        Assert.AreEqual("/database", NodeResourceSampler.FindMountPoint("/database/x", mounts));
        Assert.AreEqual("/", NodeResourceSampler.FindMountPoint("/datab", mounts), "a prefix must end at a separator");
        Assert.AreEqual("/data", NodeResourceSampler.FindMountPoint("/data/x", ["/data/", "/"]), "a trailing separator is ignored");
        Assert.IsNull(NodeResourceSampler.FindMountPoint("/data/x", ["/other"]));
    }

    [Test]
    public void ResolveRealPath_FollowsALinkInTheMiddleOfThePath()
    {
        if (OperatingSystem.IsWindows())
            Assert.Ignore("symbolic links need privileges on Windows");

        string root = Path.Combine(Path.GetTempPath(), "camus-realpath-" + Guid.NewGuid().ToString("n"));
        string real = Path.Combine(root, "real");
        string link = Path.Combine(root, "link");
        Directory.CreateDirectory(Path.Combine(real, "sub"));

        try
        {
            Directory.CreateSymbolicLink(link, "real"); // relative target

            string resolved = NodeResourceSampler.ResolveRealPath(Path.Combine(link, "sub", "wal"));

            Assert.AreEqual(Path.Combine(NodeResourceSampler.ResolveRealPath(real), "sub", "wal"), resolved);
            StringAssert.DoesNotContain(Path.DirectorySeparatorChar + "link" + Path.DirectorySeparatorChar, resolved);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    // ─── Engine size ──────────────────────────────────────────────────────────

    [Test]
    public void MeasureEngineDirectory_SumsEveryFileBelowTheDirectory()
    {
        string root = Path.Combine(Path.GetTempPath(), "camus-engine-bytes-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path.Combine(root, "v2", "nested"));

        try
        {
            File.WriteAllBytes(Path.Combine(root, "MANIFEST"), new byte[100]);
            File.WriteAllBytes(Path.Combine(root, "v2", "000001.sst"), new byte[2000]);
            File.WriteAllBytes(Path.Combine(root, "v2", "nested", "LOG"), new byte[30]);

            Assert.AreEqual((2130L, (string?)null), NodeResourceSampler.MeasureEngineDirectory(root, "rocksdb"));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Test]
    public void MeasureEngineDirectory_InMemoryIsZero_AndAMissingDirectoryIsUnknown()
    {
        Assert.AreEqual((0L, (string?)null), NodeResourceSampler.MeasureEngineDirectory("", "memory"));
        Assert.AreEqual((0L, (string?)null), NodeResourceSampler.MeasureEngineDirectory("/does/not/matter", "Memory"));

        (long missing, string? missingError) = NodeResourceSampler.MeasureEngineDirectory(
            Path.Combine(Path.GetTempPath(), "camus-missing-" + Guid.NewGuid().ToString("n")), "rocksdb");
        Assert.AreEqual(-1, missing);
        Assert.IsNotNull(missingError);

        (long unset, string? unsetError) = NodeResourceSampler.MeasureEngineDirectory("", "rocksdb");
        Assert.AreEqual(-1, unset);
        Assert.IsNotNull(unsetError);
    }
}
