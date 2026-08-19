
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core.Storage;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Unit tests for <see cref="DiskSpaceMonitor"/> through its injected free-space provider:
/// the watermark comparison, the disabled watermark, the fail-open behavior when the probe
/// cannot answer or throws, and the sampling cache that keeps the probe off the DML hot path.
/// </summary>
public sealed class TestDiskSpaceMonitor
{
    private static DiskSpaceMonitor Monitor(Func<long?> provider) =>
        new(provider, NullLogger.Instance);

    [Test]
    public void BelowWatermark_ReportsLow()
    {
        DiskSpaceMonitor monitor = Monitor(() => 10);

        Assert.IsTrue(monitor.IsBelow(minFreeBytes: 100, out long freeBytes));
        Assert.AreEqual(10, freeBytes);
    }

    [Test]
    public void AtOrAboveWatermark_ReportsNotLow()
    {
        Assert.IsFalse(Monitor(() => 100).IsBelow(minFreeBytes: 100, out _));
        Assert.IsFalse(Monitor(() => 500).IsBelow(minFreeBytes: 100, out _));
    }

    [Test]
    public void DisabledWatermark_NeverSamplesAndReportsNotLow()
    {
        int probes = 0;
        DiskSpaceMonitor monitor = Monitor(() => { probes++; return 0; });

        Assert.IsFalse(monitor.IsBelow(minFreeBytes: 0, out long freeBytes));
        Assert.IsFalse(monitor.IsBelow(minFreeBytes: -1, out _));
        Assert.AreEqual(-1, freeBytes);
        Assert.AreEqual(0, probes); // a disabled gate must cost nothing
    }

    [Test]
    public void UnknownReading_FailsOpen()
    {
        // A probe that cannot answer must never block writes: monitoring breakage is not an outage.
        Assert.IsFalse(Monitor(() => null).IsBelow(minFreeBytes: long.MaxValue, out long freeBytes));
        Assert.AreEqual(-1, freeBytes);
    }

    [Test]
    public void ThrowingProbe_FailsOpenAndKeepsWorking()
    {
        DiskSpaceMonitor monitor = Monitor(() => throw new InvalidOperationException("volume gone"));

        Assert.IsFalse(monitor.IsBelow(minFreeBytes: long.MaxValue, out _));
        Assert.IsFalse(monitor.IsBelow(minFreeBytes: long.MaxValue, out _)); // second call: no rethrow
    }

    [Test]
    public void ReadingsAreCachedBetweenSamples()
    {
        int probes = 0;
        DiskSpaceMonitor monitor = Monitor(() => { probes++; return 10; });

        for (int i = 0; i < 50; i++)
            Assert.IsTrue(monitor.IsBelow(minFreeBytes: 100, out _));

        // The first call samples; the rest ride the cache inside the sampling interval.
        Assert.AreEqual(1, probes);
    }

    [Test]
    public void WatermarkChange_TakesEffectWithoutResample()
    {
        // The threshold is compared per call against the cached reading, so a runtime
        // configuration change acts immediately even when the reading itself is cached.
        DiskSpaceMonitor monitor = Monitor(() => 50);

        Assert.IsFalse(monitor.IsBelow(minFreeBytes: 10, out _));
        Assert.IsTrue(monitor.IsBelow(minFreeBytes: 100, out _));
    }

    [Test]
    public void NullDataDirectory_NeverReportsLow()
    {
        DiskSpaceMonitor monitor = new(dataDirectory: null, NullLogger.Instance);

        Assert.IsFalse(monitor.IsBelow(minFreeBytes: long.MaxValue, out long freeBytes));
        Assert.AreEqual(-1, freeBytes);
    }

    [Test]
    public void RealDataDirectory_SamplesTheVolume()
    {
        // The temp directory exists on a real volume, so the probe must produce a real
        // reading, and a watermark above any physical disk must report low.
        DiskSpaceMonitor monitor = new(Path.GetTempPath(), NullLogger.Instance);

        Assert.IsTrue(monitor.IsBelow(minFreeBytes: long.MaxValue, out long freeBytes));
        Assert.Greater(freeBytes, 0);
    }
}
