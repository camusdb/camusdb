
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeEntry<T>
{
    public T Key;

    public int? Value;

    public BTreeNode<T>? Next;     // helper field to iterate over array entries    

    public BTreeEntry(T key, int? value, BTreeNode<T>? next)
    {
        this.Key = key;
        this.Value = value;
        this.Next = next;
    }
}
