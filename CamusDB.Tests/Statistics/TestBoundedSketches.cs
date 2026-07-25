
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using NUnit.Framework;
using CamusDB.Core.Statistics;

namespace CamusDB.Tests.Statistics;

/// <summary>
/// Unit tests for the bounded sketches the background auto-analyze collector uses to keep peak
/// memory independent of table size: <see cref="HyperLogLog"/> (approximate NDV) and
/// <see cref="ReservoirSampler{T}"/> (bounded histogram sample).
/// </summary>
[TestFixture]
public sealed class TestBoundedSketches
{
    [Test]
    public void HllCountsDistinctWithinTolerance()
    {
        var hll = new HyperLogLog(11); // ~2.3% standard error
        const int distinct = 50_000;
        for (int i = 0; i < distinct; i++)
            hll.Add("value-" + i);

        long estimate = hll.Estimate();
        double error = Math.Abs(estimate - distinct) / (double)distinct;

        // Allow a generous 5% band (≈2σ) to keep the test non-flaky across precisions.
        Assert.Less(error, 0.05, $"HLL estimate {estimate} too far from {distinct} (error {error:P1})");
    }

    [Test]
    public void HllIgnoresDuplicates()
    {
        var hll = new HyperLogLog(11);
        for (int i = 0; i < 10_000; i++)
            hll.Add("same-value");

        Assert.AreEqual(1, hll.Estimate(), "A single repeated value must estimate to 1 distinct");
    }

    [Test]
    public void HllIsDeterministic()
    {
        var a = new HyperLogLog(11);
        var b = new HyperLogLog(11);
        for (int i = 0; i < 5_000; i++)
        {
            a.Add("k" + i);
            b.Add("k" + i);
        }

        Assert.AreEqual(a.Estimate(), b.Estimate(), "Same input must yield identical estimates (deterministic hashing)");
    }

    [Test]
    public void HllEmptyIsZero()
    {
        Assert.AreEqual(0, new HyperLogLog(11).Estimate());
    }

    [Test]
    public void ReservoirCapsRetainedItems()
    {
        var sampler = new ReservoirSampler<int>(capacity: 100, seed: 12345);
        for (int i = 0; i < 10_000; i++)
            sampler.Add(i);

        Assert.AreEqual(100, sampler.Items.Count, "Reservoir must never exceed its capacity");
        Assert.AreEqual(10_000, sampler.Seen, "Reservoir must count every offered item");
    }

    [Test]
    public void ReservoirKeepsEverythingBelowCapacity()
    {
        var sampler = new ReservoirSampler<int>(capacity: 100, seed: 1);
        for (int i = 0; i < 40; i++)
            sampler.Add(i);

        Assert.AreEqual(40, sampler.Items.Count);
        Assert.AreEqual(40, sampler.Seen);
    }

    [Test]
    public void ReservoirIsDeterministicForSameSeed()
    {
        var a = new ReservoirSampler<int>(capacity: 50, seed: 999);
        var b = new ReservoirSampler<int>(capacity: 50, seed: 999);
        for (int i = 0; i < 5_000; i++)
        {
            a.Add(i);
            b.Add(i);
        }

        CollectionAssert.AreEqual(a.Items, b.Items, "Same seed + same stream must produce an identical sample");
    }

    [Test]
    public void ReservoirSamplesRoughlyUniformly()
    {
        // Over many trials, the fraction of retained items from the first half of the stream should
        // sit near 0.5 — a coarse check that Algorithm R isn't biased toward early/late items.
        const int trials = 200;
        const int n = 1_000;
        const int k = 100;
        long firstHalf = 0, total = 0;

        for (int t = 0; t < trials; t++)
        {
            var sampler = new ReservoirSampler<int>(k, (ulong)(t + 1) * 2654435761UL);
            for (int i = 0; i < n; i++)
                sampler.Add(i);

            foreach (int v in sampler.Items)
            {
                total++;
                if (v < n / 2) firstHalf++;
            }
        }

        double fraction = firstHalf / (double)total;
        Assert.That(fraction, Is.EqualTo(0.5).Within(0.05), $"First-half retention {fraction:P1} indicates sampling bias");
    }
}
