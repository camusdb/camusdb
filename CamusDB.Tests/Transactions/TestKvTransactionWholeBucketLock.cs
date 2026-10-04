/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// <see cref="KvTransaction.HasWholeBucketLock"/> answers from a prefix set that
/// <see cref="KvTransaction.TrackRangeLock"/> keeps, not from a walk of every tracked lock. The
/// foreign-key path asks it once per constraint per statement, while its point locks accumulate for
/// the life of the transaction, so a walk made many small statements cost O(N²).
/// </summary>
[TestFixture]
public sealed class TestKvTransactionWholeBucketLock
{
    private const string Bucket = "db:1|i:2";
    private const string Other = "db:1|i:3";

    private static KvTransaction NewTransaction() => new(new HLCTimestamp(1, 10, 0), "tx-whole-bucket");

    [Test]
    public void PointAndBoundedLocksAreNotAWholeBucketLock()
    {
        KvTransaction tx = NewTransaction();

        Assert.IsFalse(tx.HasWholeBucketLock(Bucket));

        tx.TrackRangeLock(Bucket, "k1", true, "k1", true, KeyValueDurability.Persistent);
        tx.TrackRangeLock(Bucket, null, true, "k5", true, KeyValueDurability.Persistent);
        tx.TrackRangeLock(Bucket, "k7", false, null, true, KeyValueDurability.Persistent);

        Assert.IsFalse(tx.HasWholeBucketLock(Bucket), "A lock with either bound set covers only part of the bucket");
        Assert.IsTrue(tx.HasPointLock(Bucket, "k1"));
    }

    [Test]
    public void AWholeBucketLockInEitherModeCoversItsBucketOnly()
    {
        KvTransaction shared = NewTransaction();
        shared.TrackRangeLock(Bucket, null, true, null, true, KeyValueDurability.Persistent, RangeLockMode.Shared);

        Assert.IsTrue(shared.HasWholeBucketLock(Bucket));
        Assert.IsFalse(shared.HasWholeBucketLock(Other));

        KvTransaction exclusive = NewTransaction();
        exclusive.TrackRangeLock(Bucket, null, true, null, true, KeyValueDurability.Persistent, RangeLockMode.Exclusive);

        Assert.IsTrue(exclusive.HasWholeBucketLock(Bucket));
        Assert.IsFalse(exclusive.HasWholeBucketLock(Other));
    }

    [Test]
    public void AnExclusiveUpgradeOfASharedWholeBucketLockStillCovers()
    {
        KvTransaction tx = NewTransaction();
        tx.TrackRangeLock(Bucket, null, true, null, true, KeyValueDurability.Persistent, RangeLockMode.Shared);
        tx.TrackRangeLock(Bucket, null, true, null, true, KeyValueDurability.Persistent, RangeLockMode.Exclusive);

        Assert.IsTrue(tx.HasWholeBucketLock(Bucket));
        Assert.AreEqual(1, tx.GetAcquiredRangeLocks().Count, "The upgrade replaces the Shared entry");
    }

    /// <summary>
    /// The check does not grow with the number of point locks. 50,000 point locks with one check after
    /// each were 1.25 billion record visits with the walk (about 9 s); with the lookup they take
    /// milliseconds. The bound is loose on purpose, so a slow machine does not fail it, and a walk
    /// still exceeds it by far.
    /// </summary>
    [Test]
    public void TheCheckCostDoesNotGrowWithThePointLocksHeld()
    {
        const int Locks = 50_000;
        KvTransaction tx = NewTransaction();

        Stopwatch watch = Stopwatch.StartNew();

        for (int i = 0; i < Locks; i++)
        {
            string key = "k" + i;
            tx.TrackRangeLock(Bucket, key, true, key, true, KeyValueDurability.Persistent);
            Assert.IsFalse(tx.HasWholeBucketLock(Bucket));
        }

        watch.Stop();
        Assert.That(watch.Elapsed.TotalSeconds, Is.LessThan(2), $"{Locks} checks took {watch.Elapsed}");
    }
}
