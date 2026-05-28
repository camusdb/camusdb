
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

public sealed class TableIndexSchema
{
    /// <summary>
    /// The name of the index (matches the key in TableDescriptor.Indexes).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The list of columns that make up the index
    /// </summary>
    public string[] Columns { get; }

    /// <summary>
    /// The type of index
    /// </summary>
    public IndexType Type { get; }

    public TableIndexSchema(string name, string[] columns, IndexType type)
    {
        Name = name;
        Columns = columns;
        Type = type;
    }
}
