
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct RemoveUniqueIndexTicket
{
    public BufferPoolManager Tablespace { get; }

    public uint Sequence { get; }

    public uint SubSequence { get; }

    public BTree<ColumnValue, BTreeTuple?> Index { get; }

    public ColumnValue Key { get; }

    public List<IDisposable> Locks { get; }

    public List<BufferPageOperation> ModifiedPages { get; }

    public RemoveUniqueIndexTicket(
        BufferPoolManager tablespace,
        uint sequence,
        uint subSequence,
        BTree<ColumnValue, BTreeTuple?> index,
        ColumnValue key,
        List<IDisposable> locks,
        List<BufferPageOperation> modifiedPages
    )
    {
        Tablespace = tablespace;
        Sequence = sequence;
        SubSequence = subSequence;
        Index = index;
        Key = key;
        Locks = locks;
        ModifiedPages = modifiedPages;
    }
}

