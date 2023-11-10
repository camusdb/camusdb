
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Indexes;

public class TestBTreeMulti
{
    [Test]
    public void TestEmpty()
    {
        BTreeMulti<int> tree = new(new());

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public async Task TestBasicInsert()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertNoSplit()
    {
        BTreeMulti<int> tree = new(ObjectIdGenerator.Generate());

        await tree.Put(4, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(6, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(8, ObjectIdGenerator.Generate());
        await tree.Put(9, ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 6);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertSplit()
    {
        BTreeMulti<int> tree = new(ObjectIdGenerator.Generate());

        for (int i = 0; i < 256; i++)
            await tree.Put(i, ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 256);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicGet()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());

        BTree<ObjectIdValue, ObjectIdValue>? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public async Task TestBasicNullGet()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());

        BTree<ObjectIdValue, ObjectIdValue>? values = tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public async Task TestMultiInsertGet()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(4, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(6, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(8, ObjectIdGenerator.Generate());
        await tree.Put(9, ObjectIdGenerator.Generate());

        BTree<ObjectIdValue, ObjectIdValue>? values = tree.Get(5);

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
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(2, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiSameKeyInsert()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.DenseSize(), 7);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestSameKeyInsertBasicGet()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());

        BTree<ObjectIdValue, ObjectIdValue>? values = tree.Get(5);

        Assert.NotNull(values);
        Assert.AreEqual(values!.Size(), 7);
    }

    [Test]
    public async Task TestMultiSameKeyInsertSplit()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(10, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiSameKeyInsertTraverse()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());

        int index = 0;

        await foreach (ObjectIdValue value in tree.GetAll(5))
        {
            Assert.AreEqual(24, value.ToString().Length);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public async Task TestMultiTwoKeysTraverse()
    {
        BTreeMulti<int> tree = new(new());

        for (int i = 0; i < 10; i++)
            await tree.Put(5, ObjectIdGenerator.Generate());

        for (int i = 0; i < 10; i++)
            await tree.Put(7, ObjectIdGenerator.Generate());

        int index = 0;

        await foreach (ObjectIdValue value in tree.GetAll(5))
        {
            Assert.AreEqual(24, value.ToString().Length);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public async Task TestMultiInsertString()
    {
        BTreeMulti<string> tree = new(new());

        for (int i = 0; i < 10; i++)
            await tree.Put("aaa", ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.DenseSize(), 10);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestMultiInsertStringSplit()
    {
        BTreeMulti<string> tree = new(new());

        for (int i = 0; i < 256; i++)
            await tree.Put("aaa" + i, ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 256);
        Assert.AreEqual(tree.Height(), 1);
    }

    [Test]
    public async Task TestBasicRemove()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(0, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestRemoveUnknownKey()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(10);
        Assert.IsFalse(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemove()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(4, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(6, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(8, ObjectIdGenerator.Generate());
        await tree.Put(9, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(5, tree.Size());
        Assert.AreEqual(5, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiKeyInsertRemove()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(3, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck()
    {
        BTreeMulti<int> tree = new(new());

        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(5, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());
        await tree.Put(7, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(1, tree.Size());
        Assert.AreEqual(3, tree.DenseSize());
        Assert.AreEqual(0, tree.Height());

        BTree<ObjectIdValue, ObjectIdValue>? search = tree.Get(5);
        Assert.IsNull(search);
    }

    [Test]
    public async Task TestMultiInsertRemoveCheck2()
    {
        BTreeMulti<int> tree = new(new());

        for (int i = 0; i < 256; i++)
            await tree.Put(i, ObjectIdGenerator.Generate());

        (bool found, _) = tree.Remove(5);
        Assert.IsTrue(found);

        Assert.AreEqual(255, tree.Size());
        Assert.AreEqual(255, tree.DenseSize());
        Assert.AreEqual(1, tree.Height());

        BTree<ObjectIdValue, ObjectIdValue>? search = tree.Get(7);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck2()
    {
        BTree<int, int?> tree = new(new());

        for (int i = 0; i < 256; i++)
            await tree.Put(i, i + 100);

        Assert.AreEqual(256, tree.Size());
        Assert.AreEqual(1, tree.Height());

        (bool found, _) = await tree.Remove(11);
        Assert.IsTrue(found);

        Assert.AreEqual(255, tree.Size());
        Assert.AreEqual(1, tree.Height());

        int? search = await tree.Get(11);
        Assert.IsNull(search);

        search = await tree.Get(10);
        Assert.IsNotNull(search);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck3()
    {
        BTreeMulti<int> tree = new(new());

        for (int i = 0; i < 8192; i++)
        {
            for (int j = 0; j < 64; j++)
                await tree.Put(i, ObjectIdGenerator.Generate());
        }

        Assert.AreEqual(8192, tree.Size());
        Assert.AreEqual(524288, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 8192; i += 4)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(6144, tree.Size());
        Assert.AreEqual(393216, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        BTree<ObjectIdValue, ObjectIdValue>? search = tree.Get(4);
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
        BTreeMulti<int> tree = new(new());

        for (int i = 0; i < 8192; i++)
        {
            for (int j = 0; j < 64; j++)
                await tree.Put(i, ObjectIdGenerator.Generate());
        }

        Assert.AreEqual(8192, tree.Size());
        Assert.AreEqual(524288, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 0; i < 8192; i += 8)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(7168, tree.Size());
        Assert.AreEqual(458752, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8192; i++)
        {
            BTree<ObjectIdValue, ObjectIdValue>? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(7168, count);
    }

    [Test]
    public async Task TestMultiInsertSplitRemoveCheck5()
    {
        BTreeMulti<int> tree = new(new());

        for (int i = 0; i < 8100; i++)
        {
            for (int j = 0; j < 64; j++)
                await tree.Put(i, ObjectIdGenerator.Generate());
        }

        Assert.AreEqual(8100, tree.Size());
        Assert.AreEqual(518400, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        for (int i = 7; i < 8100; i += 7)
        {
            (bool found, _) = tree.Remove(i);
            Assert.IsTrue(found);
        }

        Assert.AreEqual(6943, tree.Size());
        Assert.AreEqual(444352, tree.DenseSize());
        Assert.AreEqual(2, tree.Height());

        int count = 0;

        for (int i = 0; i < 8192; i++)
        {
            BTree<ObjectIdValue, ObjectIdValue>? search = tree.Get(i);
            if (search is not null)
                count++;
        }

        Assert.AreEqual(6943, count);
    }
}
