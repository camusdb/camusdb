
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
    private static int CurrentId = -1;

    public int Id; // unique node id

    public int KeyCount; // number of children

    public int PageOffset = -1; // on-disk offset

    public BTreeMultiEntry<T>[] children = new BTreeMultiEntry<T>[BTreeConfig.MaxChildren];   // the array of children

    // create a node with k children
    public BTreeMultiNode(int keyCount)
    {
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;

        //Console.WriteLine("Allocated new node {0}", Id);
    }
}
