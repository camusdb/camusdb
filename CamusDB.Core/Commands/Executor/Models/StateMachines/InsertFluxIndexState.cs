
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class InsertFluxIndexState
{
	public List<TableIndexSchema> UniqueIndexes { get; } = new();

	public List<TableIndexSchema> MultiIndexes { get; } = new();

    public BPlusTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? MainIndexDeltas { get; set; }

    public List<(BPlusTree<CompositeColumnValue, BTreeTuple>, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)>? UniqueIndexDeltas { get; set; }

    public List<(BPlusTree<CompositeColumnValue, BTreeTuple>, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)>? MultiIndexDeltas { get; set; }

    public InsertFluxIndexState()
	{
	}
}
