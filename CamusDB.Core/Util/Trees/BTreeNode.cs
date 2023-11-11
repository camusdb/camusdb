


using CamusDB.Core.Util.ObjectIds;
/**
* This file is part of CamusDB  
*
* For the full copyright and license information, please view the LICENSE.txt
* file that was distributed with this source code.
*/
namespace CamusDB.Core.Util.Trees;

// helper B-tree node data type
public sealed class BTreeNode<TKey, TValue>
{
    private static int CurrentId = -1;

    public int Id;

    public int KeyCount;         // number of children

    public ObjectIdValue PageOffset;       // on-disk offset    

    public BTreeEntry<TKey, TValue>[] children = new BTreeEntry<TKey, TValue>[BTreeConfig.MaxChildren];   // the array of children

    // create a node with k children
    public BTreeNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;
    }
}
