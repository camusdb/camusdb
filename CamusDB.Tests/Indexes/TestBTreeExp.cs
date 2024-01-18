
using System.Threading.Tasks;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Trees.Experimental;
using NUnit.Framework;

namespace CamusDB.Tests.Indexes;

internal class TestBTreeExp
{
    private readonly HybridLogicalClock hlc;

    public TestBTreeExp()
    {
        hlc = new();
    }

    [Test]
    public async Task TestEmpty()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPlusTree<int, int> bpt = new(new());

        for (int i = 0; i < 40; i++)        
            await bpt.Put(txnid, BTreeCommitState.Uncommitted, System.Random.Shared.Next(0, 100), 2);        

        await bpt.Print(txnid);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPlusTree<int, int> tree = new(new());

        await tree.Put(txnid, BTreeCommitState.Committed, 5, 100);

        int values = await tree.Get(TransactionType.ReadOnly, txnid, 11);

        Assert.AreEqual(0, values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPlusTree<int, int> tree = new(new());

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

        BPlusTreeMutationDeltas<int, int> deltas;

        BPlusTree<int, int> tree = new(new());

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
        Assert.AreEqual(1, deltas.Nodes.Count);
    }
}
