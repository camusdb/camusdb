/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The error artifact must say what the *bulk* of a code's failures looked like, not only the first
/// five. Run lk8 (2026-09-15) had 38,397 failures under one code: the five samples were the in-flight
/// set at a leader kill, the 38,000 that followed were the surviving followers' refused forwards, and
/// the artifact could not distinguish them.
/// </summary>
[TestFixture]
public sealed class ErrorSamplerTests
{
    [Test]
    public void EveryMessageShapeIsCountedUnderItsCode_MostFrequentFirst()
    {
        ErrorSampler sampler = new();
        for (int i = 0; i < 5; i++)
            sampler.Record("CADB0000", $"Write:Indeterminate — Endpoint https://camus3:5096: Error reading next message. rev {1000 + i}");
        for (int i = 0; i < 40; i++)
            sampler.Record("CADB0000", $"Write:DomainError — Internal server error (key 4a1f{i:x4}bc)");
        sampler.Record("CADB0504", "Write:Conflict — retry from BEGIN");

        IReadOnlyDictionary<string, IReadOnlyList<ErrorSampler.ErrorClass>> classes = sampler.Classes();

        Assert.That(classes["CADB0000"].Select(c => c.Count), Is.EqualTo(new long[] { 40, 5 }));
        Assert.That(classes["CADB0000"][0].Sample, Does.StartWith("Write:DomainError"));
        Assert.That(classes["CADB0000"][1].Sample, Does.StartWith("Write:Indeterminate"));
        Assert.That(classes["CADB0504"].Single().Count, Is.EqualTo(1));
        Assert.That(sampler.Snapshot()["CADB0000"].Samples, Has.Count.EqualTo(5), "the first-five samples are unchanged");
    }

    [Test]
    public void TheShapeBlanksNumbersAndHexRuns_SoPerKeyVariantsFoldTogether()
    {
        Assert.That(ErrorSampler.ShapeOf("key 12:7|r/aa4bf0c19e at revision 2567 on https://camus2:5096"),
            Is.EqualTo("key #:#|r/# at revision # on https://camus#:#"));
    }

    [Test]
    public void PastTheClassCap_TheRestIsCountedRatherThanDropped()
    {
        ErrorSampler sampler = new();
        for (int i = 0; i < 20; i++)
            sampler.Record("X", new string((char)('a' + i), 3) + " failed");   // 20 distinct shapes, no digits

        IReadOnlyList<ErrorSampler.ErrorClass> classes = sampler.Classes()["X"];

        Assert.That(classes.Sum(c => c.Count), Is.EqualTo(20), "classes always sum to the code's count");
        Assert.That(classes.Last().Sample, Is.EqualTo("…"));
        Assert.That(classes.Last().Count, Is.EqualTo(4));
    }
}
