
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class DeleteByIdFluxIndexState
{
    public List<TableIndexSchema> UniqueIndexes { get; } = new();

    public List<TableIndexSchema>? MultiIndexes { get; } = null;

    public List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue)>? UniqueIndexDeltas { get; set; }

    public DeleteByIdFluxIndexState()
    {
    }
}