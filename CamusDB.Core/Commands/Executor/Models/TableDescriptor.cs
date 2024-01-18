
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a descriptor to access a table
/// </summary>
public sealed class TableDescriptor
{
    /// <summary>
    /// Unique identifier of table
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Name of the table
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Pointer to the table schema
    /// </summary>
    public TableSchema Schema { get; }

    /// <summary>
    /// Pointer to the B+Tree that stores the rows
    /// </summary>
    public BPlusTree<ObjectIdValue, ObjectIdValue> Rows { get; }

    /// <summary>
    /// List of indexes on the table
    /// </summary>
    public Dictionary<string, TableIndexSchema> Indexes { get; } = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="schema"></param>
    /// <param name="rows"></param>
    public TableDescriptor(string id, string name, TableSchema schema, BPlusTree<ObjectIdValue, ObjectIdValue> rows)
    {
        Id = id;
        Name = name;
        Schema = schema;
        Rows = rows;
    }
}
