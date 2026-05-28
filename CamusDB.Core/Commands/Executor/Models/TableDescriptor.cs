
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;

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
    /// KV-backed store for row and index data
    /// </summary>
    public KvTableStore Store { get; }

    /// <summary>
    /// List of indexes on the table
    /// </summary>
    public Dictionary<string, TableIndexSchema> Indexes { get; } = new();

    public TableDescriptor(string id, string name, TableSchema schema, KvTableStore store)
    {
        Id = id;
        Name = name;
        Schema = schema;
        Store = store;
    }
}
