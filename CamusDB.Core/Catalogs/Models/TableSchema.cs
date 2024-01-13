
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Represents the current version of the table schema.
/// </summary>
public sealed class TableSchema
{
    /// <summary>
    /// Unique identifier of the table. It remains immutable throughout the life of the table.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>
    /// The version of the schema. It is incremented every time the schema is modified.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// The name of the table. It can be changed.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The list of columns that make up the table
    /// </summary>
    public List<TableColumnSchema>? Columns { get; set; }

    /// <summary>
    /// A list of all the previous versions of the table schema.
    /// </summary>
    public List<TableSchemaHistory>? SchemaHistory { get; set; }
}
