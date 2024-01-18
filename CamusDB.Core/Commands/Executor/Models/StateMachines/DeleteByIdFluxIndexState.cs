
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class DeleteByIdFluxIndexState
{
    public List<TableIndexSchema> UniqueIndexes { get; } = new();

    public List<TableIndexSchema>? MultiIndexes { get; } = null;

    public List<(BPlusTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue)>? UniqueIndexDeltas { get; set; }

    public DeleteByIdFluxIndexState()
    {
    }
}