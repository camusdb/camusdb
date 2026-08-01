/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Measurement harness for how long a single embedded node takes to become usable — start, elect a
/// leader, flush — which is the fixed cost every node-booting test pays in its set-up.
///
/// <para>This is a benchmark, not an assertion of correctness: it prints timings rather than pinning
/// them, because the numbers move with machine load. It exists to answer "is the per-test floor the
/// Raft timer grace period?" with a measurement instead of an inference.</para>
/// </summary>
[NonParallelizable]
[Explicit("Timing measurement, not a correctness check — run deliberately by name.")]
public class TestNodeBootTiming
{
    private const int Iterations = 5;

    private static EmbeddedKahunaOptions BaselineOptions() => new()
    {
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        NodeName = $"boot-timing-{Guid.NewGuid():N}",
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    /// <summary>
    /// The same node, but with the Raft timers scaled down to what a single in-memory node actually
    /// needs. The grace period exists so a real node can restore its WAL and join a cluster before
    /// participating in elections; an in-memory single-node test has neither a WAL to replay nor peers
    /// to wait for, so it spends that time idle. Heartbeat and leader-check intervals move together
    /// with the election timeout because Kommander validates their relative ordering.
    /// </summary>
    private static EmbeddedKahunaOptions TunedOptions()
    {
        EmbeddedKahunaOptions options = BaselineOptions();

        options.TimerInitialDelay = TimeSpan.FromMilliseconds(100);
        options.StartElectionTimeout = 150;
        options.EndElectionTimeout = 300;
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(20);
        options.CheckLeaderInterval = TimeSpan.FromMilliseconds(25);
        options.VotingTimeout = TimeSpan.FromMilliseconds(300);

        return options;
    }

    private static async Task<double> TimeBootAsync(EmbeddedKahunaOptions options)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        EmbeddedKahuna node = new(options);

        try
        {
            await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None).ConfigureAwait(false);
            await node.FlushAsync().ConfigureAwait(false);

            return stopwatch.Elapsed.TotalMilliseconds;
        }
        finally
        {
            await node.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Test]
    public async Task CompareNodeBootCostAgainstTunedRaftTimers()
    {
        double[] baseline = new double[Iterations];
        double[] tuned = new double[Iterations];

        for (int i = 0; i < Iterations; i++)
            baseline[i] = await TimeBootAsync(BaselineOptions()).ConfigureAwait(false);

        for (int i = 0; i < Iterations; i++)
            tuned[i] = await TimeBootAsync(TunedOptions()).ConfigureAwait(false);

        double baselineMean = Mean(baseline);
        double tunedMean = Mean(tuned);

        Console.WriteLine($"baseline boots (ms): {string.Join(", ", Array.ConvertAll(baseline, v => v.ToString("F0")))}");
        Console.WriteLine($"tuned    boots (ms): {string.Join(", ", Array.ConvertAll(tuned, v => v.ToString("F0")))}");
        Console.WriteLine($"baseline mean = {baselineMean:F0} ms, tuned mean = {tunedMean:F0} ms, saved = {baselineMean - tunedMean:F0} ms/boot");

        Assert.Pass($"baseline {baselineMean:F0} ms vs tuned {tunedMean:F0} ms per node boot");
    }

    private static double Mean(double[] values)
    {
        double total = 0;

        foreach (double value in values)
            total += value;

        return total / values.Length;
    }
}
