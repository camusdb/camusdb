
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// One column of a table. The immutable <c>Id</c> (not the mutable <c>Name</c>) is what row
/// bytes and renames key off of — see <c>RowEncoder</c> and the architecture doc §7.4. The
/// online <c>State</c> drives read/write visibility via <see cref="SchemaElementStateRules"/>.
/// Past layouts are retained in <c>TableSchema.SchemaHistory</c> so old rows still decode.
/// </summary>
public sealed class TableColumnSchema
{
    /// <summary>
    /// Unique identifier of the column. It remains immutable throughout the life of the column.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Name of the column. It can be changed.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Data type of the column
    /// </summary>
    public ColumnType Type { get; }

    /// <summary>
    /// If true, the column cannot be null
    /// </summary>
    public bool NotNull { get; }

    /// <summary>
    /// The default value of the column (optional)
    /// </summary>
    public ColumnValue? DefaultValue { get; }

    /// <summary>
    /// Online schema-change state of the column.
    /// </summary>
    public SchemaElementState State { get; }

    public TableColumnSchema(
        string id,
        string name,
        ColumnType type,
        bool notNull,
        ColumnValue? defaultValue,
        SchemaElementState state = SchemaElementState.Public
    )
    {
        Id = id;
        Name = name;
        Type = type;
        NotNull = notNull;
        DefaultValue = defaultValue;
        State = state;
    }
}
