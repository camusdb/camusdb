
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public interface IBTreeNodeReader<TKey, TValue> where TKey : IComparable<TKey>
{
	Task<BTreeNode<TKey, TValue>?> GetNode(int offset);
}
