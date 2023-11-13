
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
public sealed class BTreeEntry<TKey, TValue>
{
    public TKey Key;

    public TValue? Value;

    public BTreeNode<TKey, TValue>? Next; // helper field to iterate over array entries

    public bool Loaded; // whether the "next" node is loaded from disk

    public ObjectIdValue NextPageOffset; // the address of the next page offset

    public BTreeEntry(TKey key, TValue? value, BTreeNode<TKey, TValue>? next)
    {
        Key = key;
        Value = value;
        Next = next;
    }
}
