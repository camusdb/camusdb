
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
    /// <summary>
    /// The list of columns that make up the index
    /// </summary>
    public string[] Columns { get; }

    /// <summary>
    /// The type of index
    /// </summary>
    public IndexType Type { get; }    

    /// <summary>
    /// Reference to the B+Tree that stores the index
    /// </summary>
    public BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> BTree { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="columns"></param>
    /// <param name="type"></param>
    /// <param name="index"></param>
    public TableIndexSchema(string[] columns, IndexType type, BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> index)
    {
        Columns = columns;
        Type = type;
        BTree = index;
    }
}
