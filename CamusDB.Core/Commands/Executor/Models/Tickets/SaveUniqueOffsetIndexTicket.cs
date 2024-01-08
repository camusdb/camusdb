
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
    public BufferPoolManager Tablespace { get; }

    public BTree<ObjectIdValue, ObjectIdValue> Index { get; }

    public HLCTimestamp TxnId { get; }

    public ObjectIdValue Key { get; }

    public ObjectIdValue Value { get; }

    public List<BufferPageOperation> ModifiedPages { get; }

    public SaveUniqueOffsetIndexTicket(
        BufferPoolManager tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        HLCTimestamp txnId,
        ObjectIdValue key,
        ObjectIdValue value,
        List<BufferPageOperation> modifiedPages
    )
    {
        Tablespace = tablespace;
        Index = index;
        TxnId = txnId;
        Key = key;
        Value = value;
        ModifiedPages = modifiedPages;
    }
}

