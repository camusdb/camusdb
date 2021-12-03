
using System.IO;
using System.Text;
using CamusDB.Core;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Tests.Indexes;

public class TestBTree
{
    [Test]
    public void TestBasicInsert()
    {
        BTree<int> tree = new(0);

        tree.Put(5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertNoSplit()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        Assert.AreEqual(tree.Size(), 6);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertSplit()
    {
        BTree<int> tree = new(0);

        tree.Put(0, 100);
        tree.Put(1, 100);
        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public void TestBasicGet()
    {
        BTree<int> tree = new(0);

        tree.Put(5, 100);

        int? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public void TestBasicNullGet()
    {
        BTree<int> tree = new(0);

        tree.Put(5, 100);

        int? values = tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public void TestMultiInsertGet()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        int? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = tree.Get(7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public void TestMultiInsertDeltas()
    {
        BTreeInsertDeltas<int> deltas;

        BTree<int> tree = new(0);

        deltas = tree.Put(4, 100);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(5, 100);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(6, 101);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(7, 102);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(8, 103);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(9, 104);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(10, 105);
        Assert.AreEqual(1, deltas.Deltas.Count);

        deltas = tree.Put(11, 105);
        Assert.AreEqual(5, deltas.Deltas.Count);

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }
}

