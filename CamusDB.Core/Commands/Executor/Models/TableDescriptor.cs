
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class TableDescriptor
{
    public string Name { get; }

    public TableSchema Schema { get; }

    public BTree<ObjectIdValue, ObjectIdValue> Rows { get; }

    public Dictionary<string, TableIndexSchema> Indexes { get; set; } = new();    

    public TableDescriptor(string name, TableSchema schema, BTree<ObjectIdValue, ObjectIdValue> rows)
    {
        Name = name;
        Schema = schema;
        Rows = rows;
    }
}
