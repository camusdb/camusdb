/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Definition of an index. Serves two roles:
/// <list type="bullet">
///   <item>
///     <b>Persisted</b> — stored inside <see cref="TableSchema.Indexes"/> (the replicated
///     per-table schema). Carries <see cref="Id"/> (immutable), <see cref="ColumnIds"/>
///     (immutable column IDs, rename-safe), <see cref="StartOffset"/> (backfill checkpoint),
///     and <see cref="State"/>. <see cref="Columns"/> is an empty array in this form; it is
///     resolved to column names at table-open time via <c>TableOpener</c>.
///   </item>
///   <item>
///     <b>In-memory (query/DML)</b> — stored in <c>TableDescriptor.Indexes</c>. Has
///     <see cref="Columns"/> (resolved names); <see cref="ColumnIds"/> is null for entries
///     constructed from the legacy <c>SystemSchema</c> path.
///   </item>
/// </list>
/// </summary>
public sealed class TableIndexSchema
{
    /// <summary>
    /// Immutable unique identifier of the index. Null only for entries built from legacy code
    /// paths that predate B1; all new entries carry an Id.
    /// </summary>
    public string? Id { get; }

    /// <summary>
    /// The name of the index (matches the key in TableDescriptor.Indexes).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Resolved column names, populated at table-open time. Empty when this instance is
    /// persisted inside <see cref="TableSchema.Indexes"/> (column IDs are stored in
    /// <see cref="ColumnIds"/> and resolved by <c>TableOpener</c>).
    /// </summary>
    public string[] Columns { get; }

    /// <summary>
    /// Immutable column IDs. Populated in the persisted form inside
    /// <see cref="TableSchema.Indexes"/>; null in in-memory query-time entries constructed
    /// from the legacy path.
    /// </summary>
    public string[]? ColumnIds { get; }

    /// <summary>
    /// The type of index
    /// </summary>
    public IndexType Type { get; }

    /// <summary>
    /// Online schema-change state of the index.
    /// </summary>
    public SchemaElementState State { get; }

    /// <summary>
    /// Last row-id checkpoint completed by an online index backfill (mirrors
    /// <c>DatabaseIndexObject.StartOffset</c>).
    /// </summary>
    public string? StartOffset { get; }

    /// <summary>
    /// Constructs an in-memory (query/DML) entry with resolved column names.
    /// Used by <c>TableOpener</c> when building <c>TableDescriptor.Indexes</c>.
    /// </summary>
    public TableIndexSchema(string name, string[] columns, IndexType type, SchemaElementState state = SchemaElementState.Public)
    {
        Name = name;
        Columns = columns;
        Type = type;
        State = state;
    }

    /// <summary>
    /// Constructs a fully-populated entry for storage inside <see cref="TableSchema.Indexes"/>.
    /// Column names are absent (<see cref="Columns"/> is empty); they are resolved from
    /// <paramref name="columnIds"/> at open time. This constructor is also the JSON
    /// deserialization target so the persisted form round-trips correctly.
    /// </summary>
    [JsonConstructor]
    public TableIndexSchema(string? id, string name, string[]? columnIds, IndexType type, SchemaElementState state, string? startOffset = null)
    {
        Id = id;
        Name = name;
        Columns = [];
        ColumnIds = columnIds;
        Type = type;
        State = state;
        StartOffset = startOffset;
    }
}
