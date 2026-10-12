/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers the load generator's self-check. Its job is to stop the most expensive mistake in a
/// throughput measurement: reporting the client's own ceiling as the database's.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class ClientResourceTests
{
    [Test]
    public async Task SummarizesTheWindowItSampled()
    {
        await using ClientResourceSampler sampler = new(TimeSpan.FromMilliseconds(50));
        DateTime start = DateTime.UtcNow;
        sampler.Start();
        await Task.Delay(400).ConfigureAwait(false);
        await sampler.StopAsync().ConfigureAwait(false);

        ClientResources? resources = sampler.Summarize(
            start, measuredSeconds: 2, mode: "closed", workers: 64, connections: 8, maxInFlight: 4096,
            throughputOpsPerSec: 10, meanLatencyMs: 1);

        Assert.That(resources, Is.Not.Null);
        Assert.That(resources!.ProcessorCount, Is.GreaterThan(0));
        Assert.That(resources.MeasuredSeconds, Is.GreaterThan(0));
        Assert.That(resources.CpuSecondsUsed, Is.GreaterThanOrEqualTo(0));
        Assert.That(resources.AllocatedBytes, Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public async Task ReportsNothingRatherThanZeroWhenTheWindowCaughtNoSamples()
    {
        // A made-up zero would read as "the client was idle", which is the opposite of "not measured".
        await using ClientResourceSampler sampler = new(TimeSpan.FromMilliseconds(50));
        sampler.Start();
        await Task.Delay(120).ConfigureAwait(false);
        await sampler.StopAsync().ConfigureAwait(false);

        ClientResources? resources = sampler.Summarize(
            new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), measuredSeconds: 1, mode: "closed",
            workers: 8, connections: 1, maxInFlight: 4096, throughputOpsPerSec: 1, meanLatencyMs: 1);

        Assert.That(resources, Is.Null);
    }

    [Test]
    public async Task FlagsAnOpenLoopRunPinnedAgainstItsInFlightCap()
    {
        // 500 operations per second at 200 ms each needs 100 in flight against a cap of 100: the cap,
        // not the server, set the pace.
        ClientResources? resources = await SummarizeAsync(
            mode: "open", workers: 64, maxInFlight: 100, throughputOpsPerSec: 500, meanLatencyMs: 200);

        Assert.That(resources, Is.Not.Null);
        Assert.That(resources!.RequiredInFlight, Is.EqualTo(100).Within(0.001));
        Assert.That(resources.HeadroomAvailable, Is.False);
        Assert.That(string.Join(" ", resources.Warnings), Does.Contain("--max-in-flight"));
    }

    [Test]
    public async Task DoesNotFlagAClosedLoopRunForHoldingExactlyItsWorkerCountInFlight()
    {
        // A closed-loop run always has `workers` operations in flight; that is what closed loop means.
        // Warning about it would fire on every healthy run and teach the reader to ignore the warnings.
        ClientResources? resources = await SummarizeAsync(
            mode: "closed", workers: 8, maxInFlight: 4096, throughputOpsPerSec: 1000, meanLatencyMs: 8);

        Assert.That(resources, Is.Not.Null);
        Assert.That(resources!.RequiredInFlight, Is.EqualTo(8).Within(0.001));
        Assert.That(resources.Warnings, Is.Empty);
        Assert.That(resources.HeadroomAvailable, Is.True);
    }

    [Test]
    public async Task LeavesAnOpenLoopRunWithRoomUnflagged()
    {
        ClientResources? resources = await SummarizeAsync(
            mode: "open", workers: 64, maxInFlight: 4096, throughputOpsPerSec: 10, meanLatencyMs: 1);

        Assert.That(resources, Is.Not.Null);
        Assert.That(resources!.Warnings.Where(w => w.Contains("max-in-flight")), Is.Empty);
    }

    [Test]
    public void OneSampleOverTheBarIsNotABacklog()
    {
        // rl7 (2026-10-11): a two-hour soak with a clean generator (CPU 6%, 71 MB/s) failed the
        // headroom check on one 1-second sample of 103 against a bar of 100. One spike is the normal
        // shape of a burst of continuations, not a client holding operations back.
        List<ClientResourceSnapshot> series = Series(7_200, depth: 2);
        series[4_000] = series[4_000] with { ThreadPoolQueue = 103 };

        ClientResources resources = Summarize(series)!;

        Assert.That(resources.PeakThreadPoolQueue, Is.EqualTo(103));
        Assert.That(resources.ThreadPoolQueueP95, Is.EqualTo(2));
        Assert.That(resources.ThreadPoolBacklogSeconds, Is.EqualTo(1).Within(0.001), "an isolated spike covers one sample cadence");
        Assert.That(resources.ThreadPoolBacklogSamples, Is.EqualTo(1));
        Assert.That(resources.ThreadPoolQueueSamples, Is.EqualTo(7_200));
        Assert.That(resources.Warnings, Is.Empty);
        Assert.That(resources.HeadroomAvailable, Is.True);
    }

    [Test]
    public void AQueueThatStaysOverTheBarForTheDwellIsABacklog()
    {
        List<ClientResourceSnapshot> series = Series(600, depth: 2);
        for (int i = 300; i < 305; i++)
            series[i] = series[i] with { ThreadPoolQueue = 150 };

        ClientResources resources = Summarize(series)!;

        Assert.That(resources.ThreadPoolBacklogSeconds, Is.EqualTo(5).Within(0.001), "samples 300..304 over, 305 back under: five seconds");
        Assert.That(resources.ThreadPoolQueueP95, Is.EqualTo(2), "under 5% of the window, so the percentile alone would not have fired");
        Assert.That(resources.HeadroomAvailable, Is.False);
        Assert.That(string.Join(" ", resources.Warnings), Does.Contain("for 5 s without a break"));
    }

    [Test]
    public void FourSecondsOverTheBarIsStillUnderTheDwell()
    {
        List<ClientResourceSnapshot> series = Series(600, depth: 2);
        for (int i = 300; i < 304; i++)
            series[i] = series[i] with { ThreadPoolQueue = 150 };

        ClientResources resources = Summarize(series)!;

        Assert.That(resources.ThreadPoolBacklogSeconds, Is.EqualTo(4).Within(0.001));
        Assert.That(resources.Warnings, Is.Empty);
    }

    [Test]
    public void ManyShortSpikesAreABacklogWhenTheyCoverMoreThanFivePercentOfTheWindow()
    {
        // Every tenth sample over the bar: never five in a row, but 10% of the window. A generator
        // that is behind one second in ten is behind.
        List<ClientResourceSnapshot> series = Series(1_000, depth: 2);
        for (int i = 0; i < series.Count; i += 10)
            series[i] = series[i] with { ThreadPoolQueue = 120 };

        ClientResources resources = Summarize(series)!;

        Assert.That(resources.ThreadPoolBacklogSeconds, Is.EqualTo(1).Within(0.001));
        Assert.That(resources.ThreadPoolQueueP95, Is.EqualTo(120));
        Assert.That(resources.HeadroomAvailable, Is.False);
        Assert.That(string.Join(" ", resources.Warnings), Does.Contain("more than 5% of samples"));
    }

    [Test]
    public void ABacklogStillOpenAtTheEndOfTheWindowCountsItsLastSample()
    {
        List<ClientResourceSnapshot> series = Series(100, depth: 2);
        for (int i = 94; i < 100; i++)
            series[i] = series[i] with { ThreadPoolQueue = 500 };

        ClientResourceSampler.ThreadPoolBacklog backlog = ClientResourceSampler.MeasureThreadPoolBacklog(series);

        Assert.That(backlog.LongestSeconds, Is.EqualTo(6).Within(0.001), "samples 94..99 over, closed by one cadence past the last");
        Assert.That(backlog.Samples, Is.EqualTo(6));
    }

    [Test]
    public void BacklogOfAnEmptyWindowIsZero()
    {
        ClientResourceSampler.ThreadPoolBacklog backlog = ClientResourceSampler.MeasureThreadPoolBacklog([]);
        Assert.That(backlog, Is.EqualTo(new ClientResourceSampler.ThreadPoolBacklog(0, 0, 0)));
    }

    private static readonly DateTime SeriesStart = new(2026, 10, 11, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>A one-second series of idle readings at a constant queue depth.</summary>
    private static List<ClientResourceSnapshot> Series(int count, long depth) =>
        Enumerable.Range(0, count).Select(i => new ClientResourceSnapshot(
            AtUtc: SeriesStart.AddSeconds(i), CpuSeconds: i * 0.05, AllocatedBytes: i * 1_000_000L,
            Gen0: i, Gen1: 0, Gen2: 0, GcPauseSeconds: i * 0.001, WorkingSetBytes: 100_000_000,
            ThreadPoolThreads: 8, ThreadPoolQueue: depth)).ToList();

    private static ClientResources? Summarize(IReadOnlyList<ClientResourceSnapshot> series) =>
        ClientResourceSampler.Summarize(
            series, SeriesStart, measuredSeconds: series.Count, mode: "closed", workers: 64, connections: 8,
            maxInFlight: 4096, throughputOpsPerSec: 3_000, meanLatencyMs: 20);

    private static async Task<ClientResources?> SummarizeAsync(
        string mode, int workers, int maxInFlight, double throughputOpsPerSec, double meanLatencyMs)
    {
        await using ClientResourceSampler sampler = new(TimeSpan.FromMilliseconds(50));
        DateTime start = DateTime.UtcNow;
        sampler.Start();
        await Task.Delay(300).ConfigureAwait(false);
        await sampler.StopAsync().ConfigureAwait(false);

        return sampler.Summarize(
            start, measuredSeconds: 2, mode, workers, connections: 8, maxInFlight, throughputOpsPerSec, meanLatencyMs);
    }
}
