﻿
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

    [Test]
    public void TestBasicRemove()
    {
        BTree<int> tree = new(0);

        tree.Put(5, 100);
        Assert.IsTrue(tree.Remove(5));

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public void TestRemoveUnknownKey()
    {
        BTree<int> tree = new(0);

        tree.Put(5, 100);
        Assert.IsFalse(tree.Remove(10));

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public void TestMultiInsertRemove()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        Assert.IsTrue(tree.Remove(5));

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertRemoveCheck()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        Assert.IsTrue(tree.Remove(5));

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);
        tree.Put(10, 105);
        tree.Put(11, 106);

        Assert.AreEqual(8, tree.Size());
        Assert.AreEqual(1, tree.Height());

        Assert.IsTrue(tree.Remove(5));

        Assert.AreEqual(7, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck2()
    {
        BTree<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);
        tree.Put(10, 105);
        tree.Put(11, 106);

        Assert.AreEqual(8, tree.Size());
        Assert.AreEqual(1, tree.Height());

        Assert.IsTrue(tree.Remove(11));

        Assert.AreEqual(7, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = tree.Get(11);
        Assert.IsNull(search);

        search = tree.Get(10);
        Assert.IsNotNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck3()
    {
        BTree<int> tree = new(0);

        for (int i = 0; i < 50; i++)        
            tree.Put(i, 100 + i);        

        Assert.AreEqual(50, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 50; i += 5)
            Assert.IsTrue(tree.Remove(i));

        Assert.AreEqual(40, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int? search = tree.Get(5);
        Assert.IsNull(search);

        search = tree.Get(7);
        Assert.IsNotNull(search);

        search = tree.Get(41);
        Assert.IsNotNull(search);

        search = tree.Get(49);
        Assert.IsNotNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck4()
    {
        BTree<int> tree = new(0);

        for (int i = 0; i < 64; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i += 8)
            Assert.IsTrue(tree.Remove(i));

        Assert.AreEqual(56, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            int? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(56, count);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheckEmpty()
    {
        BTree<int> tree = new(0);

        for (int i = 0; i < 64; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i++)
            Assert.IsTrue(tree.Remove(i));

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            int? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(0, count);
    }
}
