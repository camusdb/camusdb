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
public sealed class WorkerShardTests
{
    [TestCase(10_000, 8)]
    [TestCase(10_001, 7)]     // uneven
    [TestCase(97, 10)]        // fewer rows-per-worker, uneven
    [TestCase(5, 5)]          // one row each
    public void ShardsArePairwiseDisjointAndCoverEveryRow(long rows, int workers)
    {
        var seen = new HashSet<long>();
        long totalOwned = 0;

        for (int w = 0; w < workers; w++)
        {
            var shard = new WorkerShard(w, workers, rows);
            totalOwned += shard.OwnedCount;
            foreach (long idx in shard.OwnedRowIndexes())
            {
                Assert.That(idx, Is.InRange(0, rows - 1));
                Assert.That(seen.Add(idx), Is.True, $"row {idx} owned by more than one worker");
                Assert.That(shard.Owns(idx), Is.True);
            }
        }

        Assert.That(totalOwned, Is.EqualTo(rows), "owned counts must sum to total rows");
        Assert.That(seen.Count, Is.EqualTo(rows), "every row must be owned exactly once");
    }

    [Test]
    public void RowIndexAtAndOrdinalOfAreInverses()
    {
        var shard = new WorkerShard(workerIndex: 3, workers: 8, rows: 10_000);
        for (long ordinal = 0; ordinal < shard.OwnedCount; ordinal++)
        {
            long rowIndex = shard.RowIndexAt(ordinal);
            Assert.That(shard.OrdinalOf(rowIndex), Is.EqualTo(ordinal));
            Assert.That(shard.Owns(rowIndex), Is.True);
        }
    }
}
