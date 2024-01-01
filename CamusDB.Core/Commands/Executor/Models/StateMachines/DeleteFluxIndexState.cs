


using CamusDB.Core.Catalogs.Models;
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public readonly struct DeleteFluxIndexState
{
    public List<TableIndexSchema> UniqueIndexes { get; } = new();

    public List<TableIndexSchema>? MultiIndexes { get; } = null;

    public DeleteFluxIndexState()
    {
    }
}
