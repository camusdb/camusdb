﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveIndexTicket
{
    public BPlusTree<CompositeColumnValue, BTreeTuple> Index { get; }

    public HLCTimestamp TxnId { get; }

    public BTreeCommitState CommitState { get; }

    public CompositeColumnValue Key { get; }

    public BTreeTuple Value { get; }

    public SaveIndexTicket(
        BPlusTree<CompositeColumnValue, BTreeTuple> index,
        HLCTimestamp txnId,
        BTreeCommitState commitState,
        CompositeColumnValue key,
        BTreeTuple value
    )
    {
        Index = index;
        TxnId = txnId;
        CommitState = commitState;
        Key = key;
        Value = value;
    }
}
