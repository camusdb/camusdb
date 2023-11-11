
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeMultiDelta<TKey> where TKey : IComparable<TKey>
{
    public BTreeMultiNode<TKey> Node { get; } // dirty node

    public HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? InnerDeltas { get; set; } // deltas changed in the inner tree

    public BTreeMultiDelta(BTreeMultiNode<TKey> node, HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? innerDeltas)
    {
        Node = node;
        InnerDeltas = innerDeltas;
    }
}
