
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Durable metadata for an index, stored in <c>SystemSchema</c> (the <c>{db}/meta/system</c>
/// blob) rather than in the replicated schema log. Keyed by an immutable <see cref="Id"/> and
/// referencing columns by id (<see cref="ColumnIds"/>) so renames don't touch it. During an
/// online build it carries the backfill checkpoint (<see cref="StartOffset"/>, the last
/// completed rowId) and the element <see cref="State"/>; the index flips to
/// <see cref="SchemaElementState.Public"/> only once the backfill completes. See
/// <c>docs/distributed-schema-architecture.md</c> §7.3 / §8.
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
    /// Last row id checkpoint completed by an online index backfill.
    /// </summary>
    public string StartOffset { get; }

    /// <summary>
    /// Online schema-change state of the index.
    /// </summary>
    public SchemaElementState State { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="tableId"></param>
    /// <param name="columnIds"></param>
    /// <param name="type"></param>
    /// <param name="startOffset"></param>
    /// <param name="state"></param>
    public DatabaseIndexObject(
        string id,
        string name,
        string tableId,
        string[] columnIds,
        IndexType type,
        string startOffset,
        SchemaElementState state = SchemaElementState.Public
    )
    {
        Id = id;
        Name = name;
        TableId = tableId;
        ColumnIds = columnIds;        
        Type = type;
        StartOffset = startOffset;
        State = state;
    }
}
