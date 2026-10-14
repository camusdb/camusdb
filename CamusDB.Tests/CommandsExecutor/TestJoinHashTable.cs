/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The hash join build table keeps a single row per key without a list, turns the bucket into a
/// list when a second row arrives, and keeps the rows of a bucket in the order they were added:
/// the broadcast join ships the build flattened in bucket order, and the remote's match indices
/// point into that order.
/// </summary>
[TestFixture]
public sealed class TestJoinHashTable
{
    private static Dictionary<string, ColumnValue> Row(long id) =>
        new() { { "id", new ColumnValue(ColumnType.Integer64, id) } };

    private static ColumnValue[] Key(long key) => [new ColumnValue(ColumnType.Integer64, key)];

    private static long[] Ids(JoinBucket bucket)
    {
        List<long> ids = [];
        foreach (IReadOnlyDictionary<string, ColumnValue> row in bucket)
            ids.Add(row["id"].LongValue);
        return ids.ToArray();
    }

    [Test]
    public void SingleRowBucket_EnumeratesThatRow()
    {
        JoinHashTable table = new();
        table.Add(Key(1), Row(10));

        Assert.IsTrue(table.TryGetBucket(Key(1), out JoinBucket bucket));
        Assert.AreEqual(1, bucket.Count);
        Assert.AreEqual(new long[] { 10 }, Ids(bucket));
        Assert.AreEqual(1, table.RowCount);
    }

    [Test]
    public void SecondAndLaterRows_KeepInsertionOrder()
    {
        JoinHashTable table = new();
        table.Add(Key(1), Row(10));
        table.Add(Key(2), Row(20));
        table.Add(Key(1), Row(11));
        table.Add(Key(1), Row(12));

        Assert.IsTrue(table.TryGetBucket(Key(1), out JoinBucket bucket));
        Assert.AreEqual(3, bucket.Count);
        Assert.AreEqual(new long[] { 10, 11, 12 }, Ids(bucket));

        Assert.IsTrue(table.TryGetBucket(Key(2), out JoinBucket other));
        Assert.AreEqual(new long[] { 20 }, Ids(other));
        Assert.AreEqual(4, table.RowCount);
    }

    [Test]
    public void MissingKey_HasNoBucket()
    {
        JoinHashTable table = new();
        table.Add(Key(1), Row(10));

        Assert.IsFalse(table.TryGetBucket(Key(2), out _));
    }

    [Test]
    public void Buckets_FlattenEveryRowOnceInBucketOrder()
    {
        JoinHashTable table = new();
        table.Add(Key(1), Row(10));
        table.Add(Key(2), Row(20));
        table.Add(Key(1), Row(11));
        table.Add(Key(3), Row(30));
        table.Add(Key(2), Row(21));

        long[] flat = table.Buckets.SelectMany(Ids).ToArray();

        Assert.AreEqual(new long[] { 10, 11, 20, 21, 30 }, flat);
    }

    [Test]
    public void ScratchKeyArray_CanBeReusedAfterAdd()
    {
        // The table copies the key it keeps; the caller's scratch array is free afterward.
        JoinHashTable table = new();
        ColumnValue[] scratch = Key(1);
        table.Add(scratch, Row(10));

        scratch[0] = new ColumnValue(ColumnType.Integer64, 2L);
        table.Add(scratch, Row(20));

        Assert.IsTrue(table.TryGetBucket(Key(1), out JoinBucket one));
        Assert.AreEqual(new long[] { 10 }, Ids(one));
        Assert.IsTrue(table.TryGetBucket(Key(2), out JoinBucket two));
        Assert.AreEqual(new long[] { 20 }, Ids(two));
    }
}
