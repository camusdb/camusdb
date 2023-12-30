
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using NUnit.Framework;

namespace CamusDB.Tests.Indexes;

internal sealed class TestBTreeMvcc
{
    private readonly HybridLogicalClock hlc;

    public TestBTreeMvcc()
    {
        hlc = new();
    }

    [Test]
    public async Task TestBasicInsert()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, 5, 100);
        await tree.Put(txnid2, 7, 100);

        Assert.AreEqual(tree.Size(), 2);
        Assert.AreEqual(tree.Height(), 0);
    }

    [Test]
    public async Task TestBasicGetTxnidSeen()
    {
        HLCTimestamp txnid1 = await hlc.SendOrLocalEvent();
        HLCTimestamp txnid2 = await hlc.SendOrLocalEvent();

        BTree<int, int?> tree = new(new());

        await tree.Put(txnid1, 5, 100);
        await tree.Put(txnid2, 7, 100);        

        int? values1 = await tree.Get(txnid1, 5);
        int? values2 = await tree.Get(txnid2, 5);

        Assert.NotNull(values1);
        Assert.NotNull(values2);
    }
}