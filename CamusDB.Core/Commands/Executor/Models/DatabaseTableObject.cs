
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a table object in a database.
/// </summary>
public sealed record DatabaseTableObject
{
    /// <summary>
    /// The type of object: always a table type.
    /// </summary>
    public DatabaseObjectType Type { get; }

    /// <summary>
    /// Unique identifier of the table.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Table name
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Offset of the first page of the table.
    /// </summary>
    public string StartOffset { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="startOffset"></param>
    public DatabaseTableObject(DatabaseObjectType type, string id, string name, string startOffset)
    {
        Type = type;
        Id = id;
        Name = name;
        StartOffset = startOffset;
    }
}
