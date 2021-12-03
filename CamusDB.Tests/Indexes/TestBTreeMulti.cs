
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
    public void TestBasicInsert()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);

        Assert.AreEqual(tree.Size(), 1);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertNoSplit()
    {
        BTreeMulti<int> tree = new(0);

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
        BTreeMulti<int> tree = new(0);

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
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);

        BTree<int>? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);
    }

    [Test]
    public void TestBasicNullGet()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);

        BTree<int>? values = tree.Get(11);

        Assert.Null(values);
    }

    [Test]
    public void TestMultiInsertGet()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(4, 100);
        tree.Put(5, 100);
        tree.Put(6, 101);
        tree.Put(7, 102);
        tree.Put(8, 103);
        tree.Put(9, 104);

        BTree<int>? values = tree.Get(5);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 100);

        values = tree.Get(7);

        Assert.NotNull(values);
        //Assert.AreEqual(values!.Length, 8);
        //Assert.AreEqual(values[0], 102);
    }

    [Test]
    public void TestBasicSameKeyInsert()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);
        tree.Put(5, 101);

        Assert.AreEqual(tree.Size(), 2);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiSameKeyInsert()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);
        tree.Put(5, 101);
        tree.Put(5, 103);
        tree.Put(5, 104);
        tree.Put(5, 105);
        tree.Put(5, 106);
        tree.Put(5, 107);

        Assert.AreEqual(tree.Size(), 7);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestSameKeyInsertBasicGet()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);
        tree.Put(5, 101);
        tree.Put(5, 103);
        tree.Put(5, 104);
        tree.Put(5, 105);
        tree.Put(5, 106);
        tree.Put(5, 107);

        BTree<int>? values = tree.Get(5);

        Assert.NotNull(values);
        Assert.AreEqual(values!.Size(), 7);        
    }

    [Test]
    public void TestMultiSameKeyInsertSplit()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);
        tree.Put(5, 101);
        tree.Put(5, 103);
        tree.Put(5, 104);
        tree.Put(5, 105);
        tree.Put(5, 106);
        tree.Put(5, 107);
        tree.Put(5, 108);
        tree.Put(5, 109);
        tree.Put(5, 110);

        Assert.AreEqual(tree.Size(), 10);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiSameKeyInsertTraverse()
    {
        BTreeMulti<int> tree = new(0);

        tree.Put(5, 100);
        tree.Put(5, 101);
        tree.Put(5, 102);
        tree.Put(5, 103);
        tree.Put(5, 104);
        tree.Put(5, 105);
        tree.Put(5, 106);
        tree.Put(5, 107);
        tree.Put(5, 108);
        tree.Put(5, 109);

        int index = 0;

        foreach (int value in tree.GetAll(5))
        {
            Assert.AreEqual(100 + index, value);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public void TestMultiTwoKeysTraverse()
    {
        BTreeMulti<int> tree = new(0);

        for (int i = 0; i < 10; i++)
            tree.Put(5, 100 + i);

        for (int i = 0; i < 10; i++)
            tree.Put(7, 100 + i);        

        int index = 0;

        foreach (int value in tree.GetAll(5))
        {
            Assert.AreEqual(100 + index, value);
            index++;
        }

        Assert.AreEqual(10, index);
    }

    [Test]
    public void TestMultiInsertString()
    {
        BTreeMulti<string> tree = new(0);

        for (int i = 0; i < 10; i++)
            tree.Put("aaa", 100 + i);
        
        Assert.AreEqual(tree.Size(), 10);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public void TestMultiInsertStringSplit()
    {
        BTreeMulti<string> tree = new(0);

        for (int i = 0; i < 10; i++)
            tree.Put("aaa" + i, 100);

        Assert.AreEqual(tree.Size(), 10);
        Assert.AreEqual(tree.Height(), 1);
    }
}

