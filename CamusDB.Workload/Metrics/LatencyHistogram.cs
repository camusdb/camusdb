/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;

namespace CamusDB.Workload.Metrics;

/// <summary>
/// A fixed-relative-error (~2%) bucketed latency histogram. Values are stored as exponentially spaced
/// buckets in microseconds, so it preserves p50/p90/p95/p99/p99.9/max and merges with another
/// histogram by adding bucket counts — without retaining one sample object per operation (a five-minute
/// run at hundreds of ops/s would otherwise hoard hundreds of thousands of samples). Recording is
/// lock-free (per-bucket <see cref="Interlocked"/> increment) so many worker tasks can record
/// concurrently, and a snapshot can be taken mid-run for per-second intervals.
/// </summary>
public sealed class LatencyHistogram
{
    private const double Gamma = 1.02;                 // ~2% relative bucket error
    private static readonly double LogGamma = Math.Log(Gamma);
    private const int BucketCount = 1100;              // covers ~1us .. ~600s

    private readonly long[] _buckets = new long[BucketCount];
    private long _count;
    private long _maxUs;

    public long Count => Interlocked.Read(ref _count);

    public void Record(double milliseconds)
    {
        double us = milliseconds * 1000.0;
        int idx = BucketIndex(us);
        Interlocked.Increment(ref _buckets[idx]);
        Interlocked.Increment(ref _count);

        long usLong = (long)us;
        long snapshot;
        while (usLong > (snapshot = Interlocked.Read(ref _maxUs)))
        {
            if (Interlocked.CompareExchange(ref _maxUs, usLong, snapshot) == snapshot)
                break;
        }
    }

    /// <summary>Returns the value at the given percentile (0..100) in milliseconds, or 0 when empty.</summary>
    public double Percentile(double percentile)
    {
        long total = Interlocked.Read(ref _count);
        if (total == 0)
            return 0;

        long target = (long)Math.Ceiling(percentile / 100.0 * total);
        if (target < 1)
            target = 1;

        long cumulative = 0;
        for (int i = 0; i < _buckets.Length; i++)
        {
            cumulative += Interlocked.Read(ref _buckets[i]);
            if (cumulative >= target)
                return BucketValueUs(i) / 1000.0;
        }
        return MaxMilliseconds;
    }

    public double MaxMilliseconds => Interlocked.Read(ref _maxUs) / 1000.0;

    /// <summary>Adds another histogram's counts into this one (both must use identical bucketing).</summary>
    public void Merge(LatencyHistogram other)
    {
        for (int i = 0; i < _buckets.Length; i++)
        {
            long v = Interlocked.Read(ref other._buckets[i]);
            if (v != 0)
                Interlocked.Add(ref _buckets[i], v);
        }
        Interlocked.Add(ref _count, Interlocked.Read(ref other._count));

        long otherMax = Interlocked.Read(ref other._maxUs);
        long snapshot;
        while (otherMax > (snapshot = Interlocked.Read(ref _maxUs)))
        {
            if (Interlocked.CompareExchange(ref _maxUs, otherMax, snapshot) == snapshot)
                break;
        }
    }

    /// <summary>A point-in-time copy, used to derive per-second interval deltas without stopping recording.</summary>
    public LatencyHistogram Snapshot()
    {
        LatencyHistogram copy = new();
        for (int i = 0; i < _buckets.Length; i++)
            copy._buckets[i] = Interlocked.Read(ref _buckets[i]);
        copy._count = Interlocked.Read(ref _count);
        copy._maxUs = Interlocked.Read(ref _maxUs);
        return copy;
    }

    /// <summary>Difference (this − earlier) as a new histogram; used for one-second interval buckets.</summary>
    public LatencyHistogram Since(LatencyHistogram earlier)
    {
        LatencyHistogram delta = new();
        long count = 0;
        for (int i = 0; i < _buckets.Length; i++)
        {
            long d = _buckets[i] - earlier._buckets[i];
            delta._buckets[i] = d;
            count += d;
        }
        delta._count = count;
        delta._maxUs = _maxUs;
        return delta;
    }

    private static int BucketIndex(double us)
    {
        if (us <= 1.0)
            return 0;
        int idx = (int)(Math.Log(us) / LogGamma);
        if (idx < 0)
            return 0;
        return idx >= BucketCount ? BucketCount - 1 : idx;
    }

    private static double BucketValueUs(int index) => Math.Pow(Gamma, index);
}
