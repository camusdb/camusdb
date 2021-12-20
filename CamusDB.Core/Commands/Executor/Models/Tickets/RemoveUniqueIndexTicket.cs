
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Journal.Controllers;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class RemoveUniqueIndexTicket
{
    public BufferPoolHandler Tablespace { get; }

    public JournalWriter Journal { get; }

    public uint Sequence { get; }

    public uint SubSequence { get; }

    public JournalFailureTypes FailureType { get; }

    public BTree<ColumnValue, BTreeTuple?> Index { get; }

    public ColumnValue Key { get; }

    public RemoveUniqueIndexTicket(
        BufferPoolHandler tablespace,
        JournalWriter journal,
        uint sequence,
        uint subSequence,
        JournalFailureTypes failureType,
        BTree<ColumnValue, BTreeTuple?> index,
        ColumnValue key
    )
    {
        Tablespace = tablespace;
        Journal = journal;
        Sequence = sequence;
        SubSequence = subSequence;
        FailureType = failureType;
        Index = index;
        Key = key;
    }
}

