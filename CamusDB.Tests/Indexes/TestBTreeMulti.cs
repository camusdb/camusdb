
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Text;
using CamusDB.Core;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Tests.Indexes;

public class TestBTreeMulti
{
    [Test]
    public void TestEmpty()
    {
        BTreeMulti<int> tree = new(0);

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public async Task TestBasicInsert()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertNoSplit()
    {
        BTreeMulti<int> tree = new(0);

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
        BTreeMulti<int> tree = new(0);

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
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);

        BTree<int, int?>? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);

        BTree<int, int?>? values = tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        BTree<int, int?>? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = tree.Get(7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public async Task TestBasicSameKeyInsert()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(2, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiSameKeyInsert()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 103);
        await tree.Put(5, 104);
        await tree.Put(5, 105);
        await tree.Put(5, 106);
        await tree.Put(5, 107);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.DenseSize(), 7);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestSameKeyInsertBasicGet()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 103);
        await tree.Put(5, 104);
        await tree.Put(5, 105);
        await tree.Put(5, 106);
        await tree.Put(5, 107);

        BTree<int, int?>? values = tree.Get(5);

        Assert.NotNull(values);
        Assert.AreEqual(values!.Size(), 7);        
    }

    [Test]
    public async Task TestMultiSameKeyInsertSplit()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 103);
        await tree.Put(5, 104);
        await tree.Put(5, 105);
        await tree.Put(5, 106);
        await tree.Put(5, 107);
        await tree.Put(5, 108);
        await tree.Put(5, 109);
        await tree.Put(5, 110);

        Assert.AreEqual(1,tree.Size());
        Assert.AreEqual(10, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiSameKeyInsertTraverse()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 102);
        await tree.Put(5, 103);
        await tree.Put(5, 104);
        await tree.Put(5, 105);
        await tree.Put(5, 106);
        await tree.Put(5, 107);
        await tree.Put(5, 108);
        await tree.Put(5, 109);

        int index = 0;

        await foreach (int value in tree.GetAll(5))
        {
            Assert.AreEqual(100 + index, value);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public async Task TestMultiTwoKeysTraverse()
    {
        BTreeMulti<int> tree = new(0);

        for (int i = 0; i < 10; i++)
            await tree.Put(5, 100 + i);

        for (int i = 0; i < 10; i++)
            await tree.Put(7, 100 + i);        

        int index = 0;

        await foreach (int value in tree.GetAll(5))
        {
            Assert.AreEqual(100 + index, value);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public async Task TestMultiInsertString()
    {
        BTreeMulti<string> tree = new(0);

        for (int i = 0; i < 10; i++)
            await tree.Put("aaa", 100 + i);
        
        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.DenseSize(), 10);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertStringSplit()
    {
        BTreeMulti<string> tree = new(0);

        for (int i = 0; i < 10; i++)
            await tree.Put("aaa" + i, 100);

        Assert.AreEqual(tree.Size(), 10);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicRemove()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestRemoveUnknownKey()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);

        (bool found, _) = tree.Remove(10);
        Assert.IsFalse(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemove()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(4, 100);
        await tree.Put(5, 100);
        await tree.Put(6, 101);
        await tree.Put(7, 102);
        await tree.Put(8, 103);
        await tree.Put(9, 104);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(5, tree.Size());
        Assert.AreEqual(5, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiKeyInsertRemove()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 102);
        await tree.Put(7, 100);
        await tree.Put(7, 101);
        await tree.Put(7, 102);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(3, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 102);
        await tree.Put(7, 100);
        await tree.Put(7, 101);
        await tree.Put(7, 102);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(3, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());

        BTree<int, int?>? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck2()
    {
        BTreeMulti<int> tree = new(0);

        await tree.Put(5, 100);
        await tree.Put(5, 101);
        await tree.Put(5, 102);
        await tree.Put(7, 100);
        await tree.Put(7, 101);
        await tree.Put(7, 102);

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(3, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());

        BTree<int, int?>? search = tree.Get(7);
        Assert.IsNotNull(search);
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
        BTreeMulti<int> tree = new(0);

        for (int i = 0; i < 50; i++)
        {
            for (int j = 0; j < 50; j++)
                await tree.Put(i, j);
        }

        Assert.AreEqual(50, tree.Size());
        Assert.AreEqual(2500, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 50; i += 5)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(40, tree.Size());
        Assert.AreEqual(2000, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        BTree<int, int?>? search = tree.Get(5);
        Assert.IsNull(search);

        search = tree.Get(7);
        Assert.IsNotNull(search);

        search = tree.Get(41);
        Assert.IsNotNull(search);

        search = tree.Get(49);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck4()
    {
        BTreeMulti<int> tree = new(0);

        for (int i = 0; i < 64; i++)
        {
            for (int j = 0; j < 64; j++)
                await tree.Put(i, j);
        }

        Assert.AreEqual(64, tree.Size());
        Assert.AreEqual(4096, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 64; i += 8)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(56, tree.Size());
        Assert.AreEqual(3584, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            BTree<int, int?>? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(56, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck5()
    {
        BTreeMulti<int> tree = new(0);

        for (int i = 0; i < 49; i++)
        {
            for (int j = 0; j < 49; j++)
                await tree.Put(i, j);
        }

        Assert.AreEqual(49, tree.Size());
        Assert.AreEqual(2401, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 49; i += 7)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(43, tree.Size());
        Assert.AreEqual(2107, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 64; i++)
        {
            BTree<int, int?>? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(43, count);
    }
}
