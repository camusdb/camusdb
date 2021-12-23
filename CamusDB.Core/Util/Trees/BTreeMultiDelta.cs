
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeMultiDelta<TKey> where TKey : IComparable<TKey>
{
    public BTreeMultiNode<TKey> Node { get; } // dirty node

    public HashSet<BTreeNode<int, int?>>? InnerDeltas { get; set; } // deltas changed in the inner tree

    public BTreeMultiDelta(BTreeMultiNode<TKey> node, HashSet<BTreeNode<int, int?>>? innerDeltas)
    {
        Node = node;
        InnerDeltas = innerDeltas;
    }
}
