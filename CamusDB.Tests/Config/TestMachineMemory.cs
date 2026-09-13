/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using NUnit.Framework;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers <see cref="MachineMemory"/>, the probe that sizes the native RocksDB budgets from the
/// container or machine instead of the GC heap limit. The Linux cgroup and <c>/proc/meminfo</c>
/// parsing is driven through a synthetic filesystem, so the container layouts are tested on any
/// host.
/// </summary>
[TestFixture]
public sealed class TestMachineMemory
{
    private const long OneMb = 1024L * 1024;

    private static Func<string, string?> Files(Dictionary<string, string> files) =>
        path => files.TryGetValue(path, out string? content) ? content : null;

    [Test]
    public void CgroupV2_InsideANamespace_ReadsTheRootLimit()
    {
        long limit = MachineMemory.ReadLinuxCgroupLimit(Files(new()
        {
            ["/proc/self/cgroup"] = "0::/\n",
            ["/sys/fs/cgroup/memory.max"] = "4294967296\n",
        }));

        Assert.That(limit, Is.EqualTo(4096 * OneMb));
    }

    [Test]
    public void CgroupV2_NoLimit_IsZero()
    {
        long limit = MachineMemory.ReadLinuxCgroupLimit(Files(new()
        {
            ["/proc/self/cgroup"] = "0::/\n",
            ["/sys/fs/cgroup/memory.max"] = "max\n",
        }));

        Assert.That(limit, Is.Zero);
    }

    [Test]
    public void CgroupV2_WithoutANamespace_TakesTheSmallestLimitOnThePath()
    {
        // A parent's limit binds a child that sets none, and the smaller of two limits binds.
        long limit = MachineMemory.ReadLinuxCgroupLimit(Files(new()
        {
            ["/proc/self/cgroup"] = "0::/system.slice/docker-abc.scope\n",
            ["/sys/fs/cgroup/system.slice/docker-abc.scope/memory.max"] = "max",
            ["/sys/fs/cgroup/system.slice/memory.max"] = "2147483648",
        }));

        Assert.That(limit, Is.EqualTo(2048 * OneMb));
    }

    [Test]
    public void CgroupV2_PathOutsideTheNamespace_ReadsOnlyTheMountRoot()
    {
        long limit = MachineMemory.ReadLinuxCgroupLimit(Files(new()
        {
            ["/proc/self/cgroup"] = "0::/../../kubepods/pod1\n",
            ["/sys/fs/cgroup/memory.max"] = "1073741824",
        }));

        Assert.That(limit, Is.EqualTo(1024 * OneMb));
    }

    [Test]
    public void CgroupV1_ReadsTheMemoryController_AndTreatsTheSentinelAsUnlimited()
    {
        Dictionary<string, string> limited = new()
        {
            ["/proc/self/cgroup"] = "12:cpu,cpuacct:/docker/abc\n11:memory:/docker/abc\n",
            ["/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes"] = "3221225472\n",
        };
        Assert.That(MachineMemory.ReadLinuxCgroupLimit(Files(limited)), Is.EqualTo(3072 * OneMb));

        Dictionary<string, string> unlimited = new()
        {
            ["/proc/self/cgroup"] = "11:memory:/\n",
            ["/sys/fs/cgroup/memory/memory.limit_in_bytes"] = "9223372036854771712\n",
        };
        Assert.That(MachineMemory.ReadLinuxCgroupLimit(Files(unlimited)), Is.Zero);
    }

    [Test]
    public void NoCgroupFile_IsZero()
    {
        Assert.That(MachineMemory.ReadLinuxCgroupLimit(Files(new())), Is.Zero);
    }

    [Test]
    public void MemTotal_IsParsedInKibibytes()
    {
        const string meminfo = "MemTotal:        8039852 kB\nMemFree:          812344 kB\n";

        Assert.That(MachineMemory.ParseMemTotalBytes(meminfo), Is.EqualTo(8039852L * 1024));
        Assert.That(MachineMemory.ParseMemTotalBytes("MemFree: 1 kB\n"), Is.Zero);
        Assert.That(MachineMemory.ParseMemTotalBytes(null), Is.Zero);
    }

    [Test]
    public void MalformedLimits_AreIgnored()
    {
        Assert.That(MachineMemory.ParseCgroupLimit("-1"), Is.Zero);
        Assert.That(MachineMemory.ParseCgroupLimit(""), Is.Zero);
        Assert.That(MachineMemory.ParseCgroupLimit("12abc"), Is.Zero);
    }

    [Test]
    public void ThisMachine_ReportsAPositiveSize_NoSmallerThanAnUnlimitedHeap()
    {
        (long bytes, string source) = MachineMemory.ReadTotal();

        Assert.That(bytes, Is.GreaterThan(0));
        Assert.That(source, Is.Not.Empty);

        // With no GC heap limit configured the GC reports the machine (or container) itself, so the
        // probe must not report less. The test process sets no heap limit.
        if (GC.GetConfigurationVariables().TryGetValue("GCHeapHardLimit", out object? hardLimit)
            && Convert.ToUInt64(hardLimit) == 0
            && GC.GetConfigurationVariables().TryGetValue("GCHeapHardLimitPercent", out object? percent)
            && Convert.ToUInt64(percent) == 0
            && source != "cgroup")
            Assert.That(bytes, Is.GreaterThanOrEqualTo(MachineMemory.ManagedHeapBytes()));
    }
}
