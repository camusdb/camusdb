
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeMultiEntry<T> where T : IComparable<T>
{
    public T Key;

    public BTree<ObjectIdValue, ObjectIdValue>? Value;

    public BTreeMultiNode<T>? Next;     // helper field to iterate over array entries

    public bool Lazy;

    public BTreeMultiEntry(T key, BTreeMultiNode<T>? next)
    {
        Key = key;
        Next = next;
    }
}
