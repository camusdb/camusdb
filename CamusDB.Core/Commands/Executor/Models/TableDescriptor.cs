
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
    /// Indexes on the table, keyed by index name. Uses <see cref="StringComparer.OrdinalIgnoreCase"/>
    /// so an index referenced in SQL (e.g. a <c>FORCE INDEX</c> hint or <c>DROP INDEX</c>) matches
    /// regardless of the case written, while the stored name preserves the case it was created with.
    /// </summary>
    public Dictionary<string, TableIndexSchema> Indexes { get; } = new(StringComparer.OrdinalIgnoreCase);

    public TableDescriptor(string id, string name, TableSchema schema, KvTableStore store)
    {
        Id = id;
        Name = name;
        Schema = schema;
        Store = store;
    }
}
