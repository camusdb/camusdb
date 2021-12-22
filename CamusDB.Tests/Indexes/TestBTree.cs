
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Util.Trees;
using System.Collections.Generic;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBTree
{
    [Test]
    public void TestEmpty()
    {
        BTree<int, int?> tree = new(0);

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public void TestBasicInsert()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertNoSplit()
    {
        BTree<int, int?> tree = new(0);

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
        BTree<int, int?> tree = new(0);

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
        BTree<int, int?> tree = new(0);

        tree.Put(5, 100);

        int? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public void TestBasicNullGet()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(5, 100);

        int? values = tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public void TestMultiInsertGet()
    {
        BTree<int, int?> tree = new(0);

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
        List<BTreeNode<int, int?>> deltas;

        BTree<int, int?> tree = new(0);

        deltas = tree.Put(4, 100);
        Assert.AreEqual(2, deltas.Count);

        deltas = tree.Put(5, 100);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(6, 101);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(7, 102);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(8, 103);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(9, 104);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(10, 105);
        Assert.AreEqual(1, deltas.Count);

        deltas = tree.Put(11, 105);
        Assert.AreEqual(5, deltas.Count);

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public void TestBasicRemove()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(5, 100);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public void TestRemoveUnknownKey()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(5, 100);

        (bool found, _) = tree.Remove(10);
        Assert.IsFalse(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public void TestMultiInsertRemove()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertRemoveCheck()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public void TestMultiInsertRemoveCheck2()
    {
        BTree<int, int?> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = tree.Get(5);
        Assert.IsNull(search);

        search = tree.Get(4);
        Assert.IsNotNull(search);

        search = tree.Get(6);
        Assert.IsNotNull(search);

        search = tree.Get(7);
        Assert.IsNotNull(search);

        search = tree.Get(8);
        Assert.IsNotNull(search);

        search = tree.Get(9);
        Assert.IsNotNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck()
    {
        BTree<int, int?> tree = new(0);

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

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(7, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheck2()
    {
        BTree<int, int?> tree = new(0);

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

        (bool found, _) = tree.Remove(11);
        Assert.IsTrue(found);

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
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 50; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(50, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 50; i += 5)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

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
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 64; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i += 8)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

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
    public void TestMultiInsertSplitRemoveCheck5()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 49; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(49, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 49; i += 7)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(43, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 49; i++)
        {
            int? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(43, count);
    }

    [Test]
    public void TestMultiInsertSplitRemoveCheckEmpty()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 64; i++)
            tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i++)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

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
