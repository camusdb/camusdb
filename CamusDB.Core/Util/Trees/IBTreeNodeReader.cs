
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.Util.Trees;

public interface IBTreeNodeReader<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
	Task<BTreeNode<TKey, TValue>?> GetNode(ObjectIdValue offset);
}

public interface IBPlusTreeNodeReader<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    Task<BPlusTreeNode<TKey, TValue>?> GetNode(ObjectIdValue offset);
}
