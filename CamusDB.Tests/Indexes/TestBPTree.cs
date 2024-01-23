
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using NUnit.Framework;
using System.Threading.Tasks;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBPTree
{
    private readonly HybridLogicalClock hlc;

    public TestBPTree()
    {
        hlc = new();
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPTree<CompositeColumnValue, ColumnValue, int> tree = new(new(), 8);

        await tree.Put(txnid, BTreeCommitState.Committed, new CompositeColumnValue(new ColumnValue(ColumnType.Integer64, 5)), 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestBasicGet()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPTree<CompositeColumnValue, ColumnValue, int> tree = new(new(), 8);

        CompositeColumnValue key = new(new ColumnValue(ColumnType.Integer64, 5));

        await tree.Put(txnid, BTreeCommitState.Committed, key, 100);

        int? values = await tree.Get(TransactionType.ReadOnly, txnid, key);

        Assert.NotNull(values);
        Assert.AreEqual(100, values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicGetPrefix()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPTree<CompositeColumnValue, ColumnValue, int> tree = new(new(), 8);

        ColumnValue cv = new(ColumnType.Integer64, 5);
        CompositeColumnValue key = new(cv);

        await tree.Put(txnid, BTreeCommitState.Committed, key, 100);

        await foreach (int? value in tree.GetPrefix(TransactionType.ReadOnly, txnid, cv))
        {
            Assert.NotNull(value);
            Assert.AreEqual(100, value);
        }
    }

    [Test]
    public async Task TestTwoValuesGetPrefix()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPTree<CompositeColumnValue, ColumnValue, int> tree = new(new(), 8);

        ColumnValue cv = new(ColumnType.Integer64, 5);
        CompositeColumnValue key = new(cv);

        await tree.Put(txnid, BTreeCommitState.Committed, key, 100);
        await tree.Put(txnid, BTreeCommitState.Committed, key, 250);

        await foreach (int? value in tree.GetPrefix(TransactionType.ReadOnly, txnid, cv))
        {
            Assert.NotNull(value);
            Assert.True(value == 100 || value == 250);
        }
    }

    [Test]
    public async Task TestCompositeBasicGetPrefix()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BPTree<CompositeColumnValue, ColumnValue, int> tree = new(new(), 8);

        ColumnValue cv1 = new(ColumnType.Integer64, 5);
        ColumnValue cv2 = new(ColumnType.Integer64, 25);

        CompositeColumnValue key = new(new ColumnValue[] { cv1, cv2 });

        await tree.Put(txnid, BTreeCommitState.Committed, key, 100);

        await foreach (int? value in tree.GetPrefix(TransactionType.ReadOnly, txnid, cv1))
        {
            Assert.NotNull(value);
            Assert.AreEqual(100, value);
        }
    }
}

