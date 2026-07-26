/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Threading;

namespace CamusDB.Workload.Metrics;

/// <summary>
/// Counts failures by stable error code and keeps only a bounded number of representative samples per
/// code. Emitting unbounded copies of the same failure would bloat the error artifact without adding
/// information, so this both aggregates the total and caps retained detail — a run that fails a million
/// times on one code stores a handful of examples plus the count, never a million strings.
/// </summary>
public sealed class ErrorSampler
{
    private const int MaxSamplesPerCode = 5;

    private sealed class Bucket
    {
        public long Count;
        public readonly List<string> Samples = new();
    }

    private readonly ConcurrentDictionary<string, Bucket> _byCode = new();

    public void Record(string code, string? sample)
    {
        Bucket bucket = _byCode.GetOrAdd(code, static _ => new Bucket());
        Interlocked.Increment(ref bucket.Count);
        if (sample is null)
            return;
        lock (bucket.Samples)
        {
            if (bucket.Samples.Count < MaxSamplesPerCode)
                bucket.Samples.Add(sample);
        }
    }

    public IReadOnlyDictionary<string, (long Count, IReadOnlyList<string> Samples)> Snapshot()
    {
        Dictionary<string, (long, IReadOnlyList<string>)> result = new();
        foreach ((string code, Bucket bucket) in _byCode)
        {
            lock (bucket.Samples)
                result[code] = (Interlocked.Read(ref bucket.Count), bucket.Samples.ToArray());
        }
        return result;
    }

    public long TotalCount()
    {
        long total = 0;
        foreach (Bucket bucket in _byCode.Values)
            total += Interlocked.Read(ref bucket.Count);
        return total;
    }
}
