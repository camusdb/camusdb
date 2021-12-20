
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public class InsertFluxIndexState
{
	public List<TableIndexSchema> UniqueIndexes { get; } = new();

	public List<TableIndexSchema>? multiIndexes;

	public InsertFluxIndexState()
	{
	}
}
