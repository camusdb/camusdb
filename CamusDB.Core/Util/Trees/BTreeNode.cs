
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees;

/// <summary>
/// Helper B-tree node data type
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class BTreeNode<TKey, TValue>
{
    private static int CurrentId = -1;

    public int Id;

    public int KeyCount;         // number of children

    public ObjectIdValue PageOffset;       // on-disk offset    

    public BTreeEntry<TKey, TValue>[] children = new BTreeEntry<TKey, TValue>[BTreeConfig.MaxChildren];   // the array of children

    /// <summary>
    /// Create a node with k children
    /// </summary>
    /// <param name="keyCount"></param>
    public BTreeNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;
    }
}
