
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Diagnostics.Metrics;

using NUnit.Framework;

using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Drives <see cref="KahunaAbortAttribution"/> through a private meter that carries Kahuna's meter
/// name, the same way <see cref="TestEngineMetricsCollector"/> drives the collector: the instruments
/// are process-wide, so the test publishes its own measurements and checks that only they are reported.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestKahunaAbortAttribution
{
    [Test]
    public void ARefusalCounterThatMovedIsReportedWithItsTags()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        Counter<long> lostLocks = meter.CreateCounter<long>("kahuna.transactions.lost_lock_aborts");

        Dictionary<string, KahunaAbortAttribution.Reading> before = KahunaAbortAttribution.Capture();

        lostLocks.Add(1, new KeyValuePair<string, object?>("detection", "lease_lapsed"));
        lostLocks.Add(1, new KeyValuePair<string, object?>("detection", "lease_lapsed"));

        string? report = KahunaAbortAttribution.Describe(before, KahunaAbortAttribution.Capture());

        Assert.That(report, Is.Not.Null);
        Assert.That(report, Does.Contain("kahuna.transactions.lost_lock_aborts"));
        Assert.That(report, Does.Contain("lease_lapsed"));
        Assert.That(report, Does.Contain(": +2"));
    }

    [Test]
    public void ACounterOutsideTheListIsReportedOnlyWhenItCountsAborts()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        Counter<long> aborts = meter.CreateCounter<long>("kahuna.kv.some_future_aborts");
        Counter<long> unrelated = meter.CreateCounter<long>("kahuna.kv.some_future_reads");

        Dictionary<string, KahunaAbortAttribution.Reading> before = KahunaAbortAttribution.Capture();

        aborts.Add(1);
        unrelated.Add(5);

        string? report = KahunaAbortAttribution.Describe(before, KahunaAbortAttribution.Capture());

        Assert.That(report, Is.Not.Null);
        Assert.That(report, Does.Contain("kahuna.kv.some_future_aborts: +1"));
        Assert.That(report, Does.Not.Contain("some_future_reads"));
    }

    [Test]
    public void NothingMovedMeansNoReport()
    {
        Dictionary<string, KahunaAbortAttribution.Reading> before = KahunaAbortAttribution.Capture();

        Assert.That(KahunaAbortAttribution.Describe(before, KahunaAbortAttribution.Capture()), Is.Null);
    }
}
