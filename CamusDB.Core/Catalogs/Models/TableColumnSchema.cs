


using CamusDB.Core.CommandsExecutor.Models;
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

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
    /// If true, the column is part of the primary key
    /// </summary>
    public bool Primary { get; }

    /// <summary>
    /// If true, the column cannot be null
    /// </summary>
    public bool NotNull { get; }

    /// <summary>
    /// The type of index on the column
    /// </summary>
    public IndexType Index { get; }

    /// <summary>
    /// The default value of the column (optional)
    /// </summary>
    public ColumnValue? DefaultValue { get; }

    public TableColumnSchema(
        string id,
        string name,
        ColumnType type,
        bool primary,
        bool notNull,
        IndexType index,
        ColumnValue? defaultValue
    )
    {
        Id = id;
        Name = name;
        Type = type;
        Primary = primary;
        NotNull = notNull;
        Index = index;
        DefaultValue = defaultValue;
    }
}

