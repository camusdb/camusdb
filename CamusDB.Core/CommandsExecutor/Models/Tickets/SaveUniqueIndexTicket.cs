
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class SaveUniqueIndexTicket
{
    public BufferPoolHandler Tablespace { get; }

    public JournalWriter Journal { get; }

    public uint Sequence { get; }

    public BTree<ColumnValue, BTreeTuple?> Index { get; }

    public ColumnValue Key { get; }

    public BTreeTuple Value { get;  }

    public bool Insert { get; } = true;

    public SaveUniqueIndexTicket(
        BufferPoolHandler tablespace,
        JournalWriter journal,
        uint sequence,
        BTree<ColumnValue, BTreeTuple?> index,
        ColumnValue key,
        BTreeTuple value,
        bool insert = true
    )
    {
        Tablespace = tablespace;
        Journal = journal;
        Sequence = sequence;
        Index = index;
        Key = key;
        Value = value;
        Insert = insert;
    }
}

