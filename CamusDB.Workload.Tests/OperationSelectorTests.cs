/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

[TestFixture]
public sealed class OperationSelectorTests
{
    private static OperationSelector NewSelector(ulong seed, int worker, int readPercent, int workers, long rows)
        => new(seed, worker, readPercent, rows, new WorkerShard(worker, workers, rows));

    [Test]
    public void SameSeedProducesIdenticalKindAndRowSequence()
    {
        var a = NewSelector(1847, worker: 3, readPercent: 60, workers: 8, rows: 10_000);
        var b = NewSelector(1847, worker: 3, readPercent: 60, workers: 8, rows: 10_000);

        for (int i = 0; i < 5000; i++)
        {
            (OperationKind kindA, long rowA) = a.Next();
            (OperationKind kindB, long rowB) = b.Next();
            Assert.That(kindA, Is.EqualTo(kindB), $"kind diverged at {i}");
            Assert.That(rowA, Is.EqualTo(rowB), $"row diverged at {i}");
        }
    }

    [Test]
    public void DifferentSeedsDiverge()
    {
        var a = NewSelector(1, 0, 60, 8, 10_000);
        var b = NewSelector(2, 0, 60, 8, 10_000);

        bool anyDifference = false;
        for (int i = 0; i < 200 && !anyDifference; i++)
        {
            var na = a.Next();
            var nb = b.Next();
            anyDifference = na != nb;
        }
        Assert.That(anyDifference, Is.True, "distinct seeds should not produce identical streams");
    }

    [Test]
    public void SubmittedMixConvergesToConfiguredSplit()
    {
        var selector = NewSelector(99, 0, readPercent: 60, workers: 4, rows: 100_000);
        int reads = 0, writes = 0;
        const int n = 100_000;
        for (int i = 0; i < n; i++)
        {
            if (selector.Next().Kind == OperationKind.Read) reads++; else writes++;
        }
        double readPct = 100.0 * reads / n;
        Assert.That(readPct, Is.EqualTo(60).Within(1.5), "read share should converge to ~60%");
    }

    [Test]
    public void WriteTargetsAlwaysBelongToTheWorkerShard()
    {
        const int workers = 7;      // uneven vs rows
        const long rows = 10_000;
        const int worker = 4;
        var shard = new WorkerShard(worker, workers, rows);
        var selector = new OperationSelector(555, worker, readPercent: 40, rows, shard);

        for (int i = 0; i < 20_000; i++)
        {
            (OperationKind kind, long rowIndex) = selector.Next();
            if (kind == OperationKind.Write)
                Assert.That(shard.Owns(rowIndex), Is.True, $"write row {rowIndex} outside worker {worker}'s shard");
        }
    }
}
