
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class InsertFluxIndexState
{
	public List<TableIndexSchema> UniqueIndexes { get; } = new();

	public List<TableIndexSchema>? MultiIndexes { get; } = null;

    public BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? MainIndexDeltas { get; set; }

    public List<(BTree<CompositeColumnValue, BTreeTuple>, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)>? UniqueIndexDeltas { get; set; }

    public InsertFluxIndexState()
	{
	}
}
