
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
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBTreeObjectIds
{
    private readonly HybridLogicalClock hlc;

    public TestBTreeObjectIds()
    {
        hlc = new();
    }

    [Test]
    public void TestEmpty()
    {
        BTree<ObjectIdValue, ObjectIdValue> tree = new(new(), 8, BTreeDirection.Ascending);

        Assert.AreEqual(tree.Size(), 0);
        Assert.AreEqual(tree.Height(), 0);
        Assert.IsTrue(tree.IsEmpty());
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<ObjectIdValue, ObjectIdValue> tree = new(new(), 8, BTreeDirection.Ascending);

        for (int i = 0; i < 5; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, ObjectIdGenerator.Generate(), ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 5);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestBasicInsert2()
    {
        HLCTimestamp txnid = await hlc.SendOrLocalEvent();

        BTree<ObjectIdValue, ObjectIdValue> tree = new(new(), 8, BTreeDirection.Ascending);

        for (int i = 0; i < 8; i++)
            await tree.Put(txnid, BTreeCommitState.Committed, ObjectIdGenerator.Generate(), ObjectIdGenerator.Generate());

        Assert.AreEqual(tree.Size(), 8);
        Assert.AreEqual(tree.Height(), 1);
    }
}