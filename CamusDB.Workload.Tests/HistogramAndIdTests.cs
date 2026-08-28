/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using CamusDB.Workload.Util;
using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

[TestFixture]
public sealed class HistogramAndIdTests
{
    [Test]
    public void PercentilesAreWithinRelativeErrorBudget()
    {
        var h = new LatencyHistogram();
        for (int i = 1; i <= 1000; i++)
            h.Record(i); // 1..1000 ms uniform

        // ~2% bucket error; assert percentiles land near the expected value.
        Assert.That(h.Percentile(50), Is.EqualTo(500).Within(500 * 0.03));
        Assert.That(h.Percentile(99), Is.EqualTo(990).Within(990 * 0.03));
        Assert.That(h.MaxMilliseconds, Is.EqualTo(1000).Within(1000 * 0.03));
    }

    [Test]
    public void MergeSumsCountsAndSinceComputesDelta()
    {
        var a = new LatencyHistogram();
        var b = new LatencyHistogram();
        for (int i = 0; i < 100; i++) a.Record(10);
        for (int i = 0; i < 50; i++) b.Record(20);

        var before = a.Snapshot();
        a.Merge(b);
        Assert.That(a.Count, Is.EqualTo(150));

        // Since(before) should recover exactly b's 50 records.
        var delta = a.Since(before);
        Assert.That(delta.Count, Is.EqualTo(50));
        Assert.That(delta.Percentile(50), Is.EqualTo(20).Within(20 * 0.03));
    }

    [Test]
    public void RowIdsAreDeterministicUniqueAnd24Hex()
    {
        const ulong seed = 1847;
        var ids = new HashSet<string>();
        for (long i = 0; i < 5000; i++)
        {
            string id = RowIdFactory.ForRow(seed, i);
            Assert.That(id.Length, Is.EqualTo(24));
            Assert.That(id, Does.Match("^[0-9a-f]{24}$"));
            Assert.That(ids.Add(id), Is.True, $"duplicate id at index {i}");
            Assert.That(RowIdFactory.ForRow(seed, i), Is.EqualTo(id), "id must be deterministic");
        }
    }

    /// <summary>
    /// The per-row check joins a scan back to its bookkeeping by decoding the row index out of the id,
    /// so the decode must be the exact inverse of the encode and must refuse anything this seed did not
    /// produce — a wrong index would silently attribute one row's writes to another.
    /// </summary>
    [Test]
    public void RowIndexRoundTripsThroughTheId()
    {
        const ulong seed = 1847;
        foreach (long index in new long[] { 0, 1, 42, 99_999, 5_000_000, long.MaxValue })
        {
            Assert.That(RowIdFactory.TryRowIndex(seed, RowIdFactory.ForRow(seed, index), out long decoded), Is.True);
            Assert.That(decoded, Is.EqualTo(index));
        }
    }

    [Test]
    public void AnIdFromAnotherSeedOrAMalformedIdIsRejected()
    {
        const ulong seed = 1847;
        Assert.That(RowIdFactory.TryRowIndex(seed, RowIdFactory.ForRow(seed + 1, 7), out _), Is.False,
            "a different seed occupies disjoint id space and its rows are not ours to attribute");
        Assert.That(RowIdFactory.TryRowIndex(seed, null, out _), Is.False);
        Assert.That(RowIdFactory.TryRowIndex(seed, "abc", out _), Is.False, "wrong length");
        Assert.That(RowIdFactory.TryRowIndex(seed, new string('z', 24), out _), Is.False, "not hex");
    }

    [Test]
    public void DatasetRowsAndFingerprintAreDeterministic()
    {
        var d1 = new Dataset(1847, 1000, 256);
        var d2 = new Dataset(1847, 1000, 256);
        Assert.That(d1.Fingerprint(), Is.EqualTo(d2.Fingerprint()));

        for (long i = 0; i < 100; i++)
            Assert.That(d1.RowFor(i), Is.EqualTo(d2.RowFor(i)));

        var different = new Dataset(1848, 1000, 256);
        Assert.That(different.Fingerprint(), Is.Not.EqualTo(d1.Fingerprint()));
    }

    [Test]
    public void DurationParserHandlesUnits()
    {
        Assert.That(DurationParser.Parse("5m"), Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(DurationParser.Parse("30s"), Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(DurationParser.Parse("250ms"), Is.EqualTo(TimeSpan.FromMilliseconds(250)));
        Assert.That(DurationParser.Parse("1h"), Is.EqualTo(TimeSpan.FromHours(1)));
        Assert.That(DurationParser.Parse("10"), Is.EqualTo(TimeSpan.FromSeconds(10)));
    }
}
