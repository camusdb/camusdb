
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Util.Trees;
using System.Threading.Tasks;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Models;

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
        BTree<int, int> tree = new(new(), 8);

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertNoSplit()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

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

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 9; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(tree.Size(), 9);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        int values = await tree.Get(TransactionType.ReadOnly, txnid, 5);        

        Assert.AreEqual(100, values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        int values = await tree.Get(TransactionType.ReadOnly, txnid, 11);

        Assert.AreEqual(0, values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);

        int? values = await tree.Get(TransactionType.ReadOnly, txnid, 5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = await tree.Get(TransactionType.ReadOnly, txnid, 7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public async Task TestMultiInsertDeltas()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTreeMutationDeltas<int, int> deltas;

        BTree<int, int> tree = new(new(), 8);

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 4, 100);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 6, 101);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 7, 102);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 8, 103);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 9, 104);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 10, 105);
        Assert.AreEqual(1, deltas.Nodes.Count);        

        deltas = await tree.Put(txnid, BTreeCommitState.Committed, 11, 105);
        Assert.AreEqual(3, deltas.Nodes.Count);        

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicRemove()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

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

        BTree<int, int> tree = new(new(), 8);

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

        BTree<int, int> tree = new(new(), 8);

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

        BTree<int, int> tree = new(new(), 8);

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

        int search = await tree.Get(TransactionType.ReadOnly, txnid, 5);
        Assert.AreEqual(0, search);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck2()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

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

        int search = await tree.Get(TransactionType.ReadOnly, txnid, 5);
        Assert.AreEqual(0, search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 4);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 6);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 7);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 8);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 9);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 16384; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(16384, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(16383, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int search = await tree.Get(TransactionType.ReadOnly, txnid, 5);
        Assert.AreEqual(0, search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck2()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 16; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(16, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(64);
        Assert.IsTrue(found);

        Assert.AreEqual(16, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int search = await tree.Get(TransactionType.ReadOnly, txnid, 64);
        Assert.AreEqual(0, search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 63);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck3()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 32; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(32, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 32; i += 4)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(32, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int search = await tree.Get(TransactionType.ReadOnly, txnid, 4);
        Assert.AreEqual(0, search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 7);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 41);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 49);
        Assert.IsNotNull(search);

        search = await tree.Get(TransactionType.ReadOnly, txnid, 256);
        Assert.AreEqual(0, search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck4()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 256; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(256, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 256; i += 8)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(256, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 256; i++)
        {
            int search = await tree.Get(TransactionType.ReadOnly, txnid, i);
            if (search != 0)
                count++;
        }

        Assert.AreEqual(256, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck5()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 524288; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(524288, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 524288; i += 7)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(449390, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 524288; i++)
        {
            int search = await tree.Get(TransactionType.ReadOnly, txnid, i);
            if (search != 0)
                count++;
        }

        Assert.AreEqual(449390, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheckEmpty()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<int, int> tree = new(new(), 8);

        for (int i = 0; i < 524288; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, i, 100 + i);

        Assert.AreEqual(524288, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 524288; i++)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8192; i++)
        {
            int search = await tree.Get(TransactionType.ReadOnly, txnid, i);
            if (search != 0)
                count++;
        }

        Assert.AreEqual(0, count);
    }
}
