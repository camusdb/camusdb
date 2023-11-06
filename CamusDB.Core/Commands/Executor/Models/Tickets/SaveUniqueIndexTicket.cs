
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveUniqueIndexTicket
{
    public BufferPoolHandler Tablespace { get; }

    public uint Sequence { get; }

    public uint SubSequence { get; }

    public BTree<ColumnValue, BTreeTuple?> Index { get; }

    public ColumnValue Key { get; }

    public BTreeTuple Value { get; }

    public SaveUniqueIndexTicket(
        BufferPoolHandler tablespace,
        uint sequence,
        uint subSequence,
        BTree<ColumnValue, BTreeTuple?> index,
        ColumnValue key,
        BTreeTuple value
    )
    {
        Tablespace = tablespace;
        Sequence = sequence;
        SubSequence = subSequence;
        Index = index;
        Key = key;
        Value = value;
    }
}
