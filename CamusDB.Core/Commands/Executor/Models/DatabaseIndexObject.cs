
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents an index object in a database.
/// </summary>
public sealed record DatabaseIndexObject
{
    /// <summary>
    /// Unique identifier of the index. It remains immutable throughout the life of the index.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Name of the index
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The table to which the index belongs.
    /// </summary>
    public string TableId { get; set; }

    /// <summary>
    /// IDs of the indexed columns. Instead of using the name, the ID of the column 
    /// is stored to make system objects immune to name changes.
    /// </summary>
    public string[] ColumnIds { get; }    

    /// <summary>
    /// Type of index.
    /// </summary>
    public IndexType Type { get; }

    /// <summary>
    /// Offset of the first page of the index.
    /// </summary>
    public string StartOffset { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="tableId"></param>
    /// <param name="columnIds"></param>
    /// <param name="type"></param>
    /// <param name="startOffset"></param>
    public DatabaseIndexObject(string id, string name, string tableId, string[] columnIds, IndexType type, string startOffset)
    {
        Id = id;
        Name = name;
        TableId = tableId;
        ColumnIds = columnIds;        
        Type = type;
        StartOffset = startOffset;
    }
}
