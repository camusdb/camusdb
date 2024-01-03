
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeMutationDeltas<TKey, TValue> where TKey : IComparable<TKey>
{
    public HashSet<BTreeNode<TKey, TValue>> Nodes { get; } = new();

    public HashSet<BTreeEntry<TKey, TValue>> Entries { get; } = new();

    public HashSet<BTreeMvccEntry<TValue>> MvccEntries { get; } = new();
}
