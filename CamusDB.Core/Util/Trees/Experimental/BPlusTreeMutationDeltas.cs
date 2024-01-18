
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees.Experimental;

public sealed class BPlusTreeMutationDeltas<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    public HashSet<BPlusTreeNode<TKey, TValue>> Nodes { get; } = new();

    public HashSet<BPlusTreeEntry<TKey, TValue>> Entries { get; } = new();

    public HashSet<BTreeMvccEntry<TValue>> MvccEntries { get; } = new();
}
