
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

public sealed class TableIndexSchema
{
    public string Column { get; }

    public IndexType Type { get; }    

    public BTree<CompositeColumnValue, BTreeTuple> BTree { get; }

    public TableIndexSchema(string column, IndexType type, BTree<CompositeColumnValue, BTreeTuple> index)
    {
        Column = column;
        Type = type;
        BTree = index;
    }
}
