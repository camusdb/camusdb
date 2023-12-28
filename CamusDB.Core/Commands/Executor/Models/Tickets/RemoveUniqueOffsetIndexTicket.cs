﻿

/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct RemoveUniqueOffsetIndexTicket
{
    public BufferPoolHandler Tablespace { get; }

    public BTree<ObjectIdValue, ObjectIdValue> Index { get; }

    public ObjectIdValue Key { get; }

    public List<IDisposable> Locks { get; } = new();

    public List<BufferPageOperation> ModifiedPages { get; }

    public HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? Deltas { get; }

    public RemoveUniqueOffsetIndexTicket(
        BufferPoolHandler tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        ObjectIdValue key,
        List<IDisposable> locks,
        List<BufferPageOperation> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? deltas = null)
    {
        Tablespace = tablespace;
        Index = index;
        Key = key;
        Locks = locks;
        ModifiedPages = modifiedPages;
        Deltas = deltas;
    }
}

