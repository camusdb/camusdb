
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Util.Trees;
using System.Threading.Tasks;
using System.Collections.Generic;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBTree
{
    private readonly HybridLogicalClock hlc;

    public TestBTree()
    {
        hlc = new();
    }

    [Test]
    public void TestEmpty()
    {
        BTree<int, int?> tree = new(new());

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertNoSplit()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        Assert.AreEqual(tree.Size(), 6);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertSplit()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 65; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(tree.Size(), 65);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        int? values = await tree.Get(txnid, 5);        

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        int? values = await tree.Get(txnid, 11);

        Assert.Null(values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        int? values = await tree.Get(txnid, 5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = await tree.Get(txnid, 7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public async Task TestMultiInsertDeltas()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        HashSet<BTreeNode<int, int?>> deltas;

        BTree<int, int?> tree = new(new());

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 10, 105);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 11, 105);
        Assert.AreEqual(1, deltas.Count);        

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestBasicRemove()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestRemoveUnknownKey()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        (bool found, _) = await tree.Remove(10);
        Assert.IsFalse(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemove()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = await tree.Get(txnid, 5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck2()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = await tree.Get(txnid, 5);
        Assert.IsNull(search);

        search = await tree.Get(txnid, 4);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 6);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 7);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 8);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 9);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 65; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(65, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = await tree.Get(txnid, 5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck2()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 65; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(65, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(64);
        Assert.IsTrue(found);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = await tree.Get(txnid, 64);
        Assert.IsNull(search);

        search = await tree.Get(txnid, 63);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck3()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 8192; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(8192, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 8192; i += 4)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(6144, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int? search = await tree.Get(txnid, 4);
        Assert.IsNull(search);

        search = await tree.Get(txnid, 7);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 41);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 49);
        Assert.IsNotNull(search);

        search = await tree.Get(txnid, 256);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck4()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 8192; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(8192, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 8192; i += 8)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(7168, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8192; i++)
        {
            int? search = await tree.Get(txnid, i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(7168, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck5()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 8100; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(8100, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 8100; i += 7)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(6943, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8100; i++)
        {
            int? search = await tree.Get(txnid, i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(6943, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheckEmpty()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 8192; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(8192, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 8192; i++)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8192; i++)
        {
            int? search = await tree.Get(txnid, i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(0, count);
    }
}
