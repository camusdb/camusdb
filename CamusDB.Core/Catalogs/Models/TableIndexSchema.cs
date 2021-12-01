
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
    public IndexType Type { get; }

    public BTree? UniqueRows { get; }

    public BTreeMulti? MultiRows { get; }

    public TableIndexSchema(IndexType type, BTree rows)
    {
        Type = type;
        UniqueRows = rows;
    }

    public TableIndexSchema(IndexType type, BTreeMulti rows)
    {
        Type = type;
        MultiRows = rows;
    }
}
