
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

    public BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> BTree { get; }

    public TableIndexSchema(string column, IndexType type, BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> index)
    {
        Column = column;
        Type = type;
        BTree = index;
    }
}
