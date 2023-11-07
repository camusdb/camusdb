﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveUniqueOffsetIndexTicket
{
	public BufferPoolHandler Tablespace { get; }

	public BTree<int, int?> Index { get; }

	public int Key { get; }

	public int Value { get; }	

	public HashSet<BTreeNode<int, int?>>? Deltas { get; }

    public List<IDisposable> Locks { get; } = new();

    public SaveUniqueOffsetIndexTicket(
		BufferPoolHandler tablespace,
		BTree<int, int?> index,
		int key,
		int value,
        List<IDisposable> locks,
		HashSet<BTreeNode<int, int?>>? deltas = null
	)
	{
		Tablespace = tablespace;
		Index = index;
		Key = key;
		Value = value;
		Locks = locks;
		Deltas = deltas;
	}
}

