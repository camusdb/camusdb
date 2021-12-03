
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
    public const int MaxChildren = 8;

    public const int MaxChildrenHalf = MaxChildren / 2;

    private static int CurrentId = -1;

    public int Id;

    public int KeyCount;         // number of children

    public int PageOffset = -1;       // on-disk offset

    public bool Dirty = true; // whether the node must be persisted

    public BTreeEntry<TKey, TValue>[] children = new BTreeEntry<TKey, TValue>[MaxChildren];   // the array of children

    // create a node with k children
    public BTreeNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;
    }
}
