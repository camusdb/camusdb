
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateUniqueIndexTicket
{
	public DatabaseDescriptor Database { get; }

	public TableDescriptor Table { get; }

	public uint Sequence { get; }

	public BTreeTuple RowTuple { get; }

	public InsertTicket InsertTicket { get; }

	public List<TableIndexSchema> Indexes { get; }

    public List<IDisposable> Locks { get; }

    public UpdateUniqueIndexTicket(
		DatabaseDescriptor database,
		TableDescriptor table,
		uint sequence,
		BTreeTuple rowTuple,
		InsertTicket ticket,
		List<TableIndexSchema> indexes,
		List<IDisposable> locks
	)
	{
		Database = database;
		Table = table;
		Sequence = sequence;
		RowTuple = rowTuple;
		InsertTicket = ticket;
		Indexes = indexes;
		Locks = locks;
	}
}
