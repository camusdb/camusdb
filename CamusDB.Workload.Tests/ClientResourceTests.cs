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
