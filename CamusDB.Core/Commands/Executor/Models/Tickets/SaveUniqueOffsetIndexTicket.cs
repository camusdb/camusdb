
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveUniqueOffsetIndexTicket
{
	public BufferPoolHandler Tablespace { get; }

	public BTree<ObjectIdValue, ObjectIdValue> Index { get; }

	public HLCTimestamp TxnId { get; }

	public ObjectIdValue Key { get; }

	public ObjectIdValue Value { get; }

    public List<IDisposable> Locks { get; } = new();

    public List<BufferPageOperation> ModifiedPages { get; }

    public HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? Deltas { get; }    

    public SaveUniqueOffsetIndexTicket(
		BufferPoolHandler tablespace,
		BTree<ObjectIdValue, ObjectIdValue> index,
        HLCTimestamp txnId,
        ObjectIdValue key,
        ObjectIdValue value,
        List<IDisposable> locks,
        List<BufferPageOperation> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? deltas = null)
	{
		Tablespace = tablespace;
		Index = index;
		TxnId = txnId;
		Key = key;
		Value = value;
		Locks = locks;
		ModifiedPages = modifiedPages;
		Deltas = deltas;
	}
}

