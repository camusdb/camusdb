
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using NUnit.Framework;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBTreeMvcc
{
    private readonly HybridLogicalClock hlc;

    public TestBTreeMvcc()
    {
        hlc = new();
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid2, BTreeCommitState.Committed, 7, 100);

        Assert.AreEqual(tree.Size(), 2);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestBasicGetTxnidSeen()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid2, BTreeCommitState.Committed, 7, 100);        

        int? values1 = await tree.Get(txnid1, 5);
        int? values2 = await tree.Get(txnid2, 5);

        Assert.NotNull(values1);
        Assert.NotNull(values2);
    }

    [Test]
    public async Task TestBasicGetTxnidCouldntSeen()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid2, BTreeCommitState.Committed, 7, 100);

        int? values1 = await tree.Get(txnid1, 5);
        int? values2 = await tree.Get(txnid2, 5);

        Assert.NotNull(values1);
        Assert.NotNull(values2);

        int? values3 = await tree.Get(txnid1, 7);
        int? values4 = await tree.Get(txnid2, 7);

        Assert.Null(values3);
        Assert.NotNull(values4);
    }

    [Test]
    public async Task TestBasicGetTxnidUncommitted()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, BTreeCommitState.Uncommitted, 5, 100);
        await tree.Put(txnid2, BTreeCommitState.Uncommitted, 7, 100);

        int? values1 = await tree.Get(txnid1, 5);
        int? values2 = await tree.Get(txnid2, 5);

        Assert.NotNull(values1);
        Assert.Null(values2);

        int? values3 = await tree.Get(txnid1, 7);
        int? values4 = await tree.Get(txnid2, 7);

        Assert.Null(values3);
        Assert.NotNull(values4);
    }

    [Test]
    public async Task TestBasicGetTxnidUncommittedThenCommit()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        BTreeMutationDeltas<int, int?> deltas1 = await tree.Put(txnid1, BTreeCommitState.Uncommitted, 5, 100);
        BTreeMutationDeltas<int, int?> deltas2 = await tree.Put(txnid2, BTreeCommitState.Uncommitted, 7, 100);

        int? values1 = await tree.Get(txnid1, 5);
        int? values2 = await tree.Get(txnid2, 5);

        Assert.NotNull(values1);
        Assert.Null(values2);

        int? values3 = await tree.Get(txnid1, 7);
        int? values4 = await tree.Get(txnid2, 7);

        Assert.Null(values3);
        Assert.NotNull(values4);

        //await tree.Put(txnid1, BTreeCommitState.Committed, 5, 100);
        //await tree.Put(txnid2, BTreeCommitState.Committed, 7, 100);

        Assert.AreEqual(1, deltas1.Entries.Count);
        Assert.AreEqual(1, deltas2.Entries.Count);

        foreach (BTreeMvccEntry<int?> x in deltas1.Entries)
            x.CommitState = BTreeCommitState.Committed;

        foreach (BTreeMvccEntry<int?> x in deltas2.Entries)
            x.CommitState = BTreeCommitState.Committed;

        int? values5 = await tree.Get(txnid1, 5);
        int? values6 = await tree.Get(txnid2, 5);

        Assert.NotNull(values5);
        Assert.NotNull(values6);

        int? values7 = await tree.Get(txnid1, 7);
        int? values8 = await tree.Get(txnid2, 7);

        Assert.Null(values7);
        Assert.NotNull(values8);
    }
}