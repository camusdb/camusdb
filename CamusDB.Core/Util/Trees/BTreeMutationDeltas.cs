
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeMutationDeltas<TKey, TValue>
{
    public HashSet<BTreeNode<TKey, TValue>> Nodes { get; } = new();

    public HashSet<BTreeMvccEntry<TValue>> Entries { get; } = new();
}
