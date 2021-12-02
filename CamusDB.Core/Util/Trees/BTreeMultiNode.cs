
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

// helper B-tree node data type
public sealed class BTreeMultiNode<T> where T : IComparable<T>
{
    public static int CurrentId = -1;

    public int Id;

    public int KeyCount;         // number of children

    public int PageOffset = -1;       // on-disk offset

    public bool Dirty = true; // whether the node must be persisted

    public BTreeMultiEntry<T>[] children = new BTreeMultiEntry<T>[BTree<int>.MaxChildren];   // the array of children

    // create a node with k children
    public BTreeMultiNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;
    }
}
