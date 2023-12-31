
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveUniqueIndexTicket
{
    public BufferPoolHandler Tablespace { get; }    

    public BTree<ColumnValue, BTreeTuple?> Index { get; }

    public HLCTimestamp TxnId { get; }

    public BTreeCommitState CommitState { get; }

    public ColumnValue Key { get; }

    public BTreeTuple Value { get; }

    public List<BufferPageOperation> ModifiedPages { get; }

    public SaveUniqueIndexTicket(
        BufferPoolHandler tablespace,        
        BTree<ColumnValue, BTreeTuple?> index,
        HLCTimestamp txnId,
        BTreeCommitState commitState,
        ColumnValue key,
        BTreeTuple value,
        List<BufferPageOperation> modifiedPages
    )
    {
        Tablespace = tablespace;        
        Index = index;
        TxnId = txnId;
        CommitState = commitState;
        Key = key;
        Value = value;
        ModifiedPages = modifiedPages;
    }
}
