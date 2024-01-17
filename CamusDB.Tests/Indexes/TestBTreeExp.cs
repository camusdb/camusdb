
using CamusDB.Core.Util.Experimental;
using NUnit.Framework;

namespace CamusDB.Tests.Indexes;

internal class TestBTreeExp
{
    

    public TestBTreeExp()
    {
    
    }

    [Test]
    public void TestEmpty()
    {
        BPlusTree bpt = new();

        bpt.Put(4, 2);
        bpt.Put(3, 2);
        bpt.Put(2, 2);
        bpt.Put(1, 2);

        bpt.Put(5, 2);
        bpt.Put(6, 2);
        bpt.Put(7, 2);

        bpt.Print();
    }    
}
