/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Linq;
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

    /// <summary>How many distinct message shapes are kept per code. Run lk8 (2026-09-15) recorded
    /// 38,397 failures under one code whose five samples were all the first shape seen (the in-flight
    /// set at a kill); the 38,000 that followed had a different shape and a different cause, and the
    /// artifact could not say so. A shape is the message with digits and hex runs blanked, so a
    /// per-key or per-endpoint variation folds into one class.</summary>
    private const int MaxClassesPerCode = 16;

    private sealed class Bucket
    {
        public long Count;
        public readonly List<string> Samples = new();
        public readonly Dictionary<string, MessageClass> Classes = new(StringComparer.Ordinal);
        public long UnclassedCount;
    }

    private sealed class MessageClass(string sample)
    {
        public long Count;
        public readonly string Sample = sample;
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

            string shape = ShapeOf(sample);
            if (bucket.Classes.TryGetValue(shape, out MessageClass? cls))
                cls.Count++;
            else if (bucket.Classes.Count < MaxClassesPerCode)
                bucket.Classes[shape] = new MessageClass(sample) { Count = 1 };
            else
                bucket.UnclassedCount++;
        }
    }

    /// <summary>The message with every run of digits or hex blanked, so messages that differ only in
    /// a key, a revision, an endpoint or a count share one class.</summary>
    public static string ShapeOf(string sample)
        => System.Text.RegularExpressions.Regex.Replace(sample, "[0-9a-fA-F]{6,}|[0-9]+", "#");

    /// <summary>One message shape under a code: how many failures carried it and the first one seen.</summary>
    public readonly record struct ErrorClass(long Count, string Sample);

    /// <summary>The message classes per code, most frequent first, plus the count of failures that
    /// arrived after the class cap was reached (under the key <c>"…"</c>) so the classes always sum
    /// to the code's count.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<ErrorClass>> Classes()
    {
        Dictionary<string, IReadOnlyList<ErrorClass>> result = new();
        foreach ((string code, Bucket bucket) in _byCode)
        {
            lock (bucket.Samples)
            {
                List<ErrorClass> classes = bucket.Classes.Values
                    .OrderByDescending(c => c.Count)
                    .Select(c => new ErrorClass(c.Count, c.Sample))
                    .ToList();
                if (bucket.UnclassedCount > 0)
                    classes.Add(new ErrorClass(bucket.UnclassedCount, "…"));
                result[code] = classes;
            }
        }
        return result;
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
