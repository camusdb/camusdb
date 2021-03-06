
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Util.Trees;
using System.Collections.Generic;
using System.Threading.Tasks;

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
    public async Task TestBasicInsert()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertNoSplit()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        Assert.AreEqual(tree.Size(), 6);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertSplit()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(0, 100);
        await tree.Put(1, 100);
        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicGet()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(5, 100);

        int? values = await tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(5, 100);

        int? values = await tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        int? values = await tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = await tree.Get(7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public async Task TestMultiInsertDeltas()
    {
        HashSet<BTreeNode<int, int?>> deltas;

        BTree<int, int?> tree = new(0);

        deltas = await tree.Put(4, 100);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(5, 100);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(6, 101);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(7, 102);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(8, 103);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(9, 104);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(10, 105);
        Assert.AreEqual(1, deltas.Count);        

        deltas = await tree.Put(11, 105);
        Assert.AreEqual(3, deltas.Count);        

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicRemove()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(5, 100);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestRemoveUnknownKey()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(5, 100);

        (bool found, _) = await tree.Remove(10);
        Assert.IsFalse(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemove()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = await tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck2()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);

        int? search = await tree.Get(5);
        Assert.IsNull(search);

        search = await tree.Get(4);
        Assert.IsNotNull(search);

        search = await tree.Get(6);
        Assert.IsNotNull(search);

        search = await tree.Get(7);
        Assert.IsNotNull(search);

        search = await tree.Get(8);
        Assert.IsNotNull(search);

        search = await tree.Get(9);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);
        await tree.Put(10, 105);
        await tree.Put(11, 106);

        Assert.AreEqual(8, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(7, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = await tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck2()
    {
        BTree<int, int?> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);
        await tree.Put(10, 105);
        await tree.Put(11, 106);

        Assert.AreEqual(8, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(11);
        Assert.IsTrue(found);

        Assert.AreEqual(7, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = await tree.Get(11);
        Assert.IsNull(search);

        search = await tree.Get(10);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck3()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 50; i++)
            await tree.Put(i, 100 + i);

        Assert.AreEqual(50, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 50; i += 5)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(40, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int? search = await tree.Get(5);
        Assert.IsNull(search);

        search = await tree.Get(7);
        Assert.IsNotNull(search);

        search = await tree.Get(41);
        Assert.IsNotNull(search);

        search = await tree.Get(49);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck4()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 64; i++)
            await tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i += 8)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(56, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            int? search = await tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(56, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck5()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 49; i++)
            await tree.Put(i, 100 + i);

        Assert.AreEqual(49, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 49; i += 7)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(43, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 49; i++)
        {
            int? search = await tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(43, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheckEmpty()
    {
        BTree<int, int?> tree = new(0);

        for (int i = 0; i < 64; i++)
            await tree.Put(i, 100 + i);

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i++)
        {
            (bool found, _) = await tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            int? search = await tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(0, count);
    }
}
