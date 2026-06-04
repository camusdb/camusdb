
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Discriminator for <see cref="SchemaElementStatePayload"/>: whether the state
/// transition targets a column or an index element.
/// </summary>
public enum SchemaElementKind
{
    Column,
    Index,
}

public sealed class SchemaCreateTablePayload
{
    public string? TableId { get; set; }

    public string TableName { get; set; } = "";

    public SchemaColumnPayload[] Columns { get; set; } = [];
}

public sealed class SchemaAlterColumnPayload
{
    public string TableName { get; set; } = "";

    public SchemaColumnPayload Column { get; set; } = new();
}

public sealed class SchemaColumnPayload
{
    public string? Id { get; set; }

    public string Name { get; set; } = "";

    public ColumnType Type { get; set; }

    public bool NotNull { get; set; }

    public ColumnValue? DefaultValue { get; set; }

    public SchemaElementState State { get; set; } = SchemaElementState.Public;

    public static SchemaColumnPayload FromColumnInfo(ColumnInfo column)
    {
        return new()
        {
            Name = column.Name,
            Type = column.Type,
            NotNull = column.NotNull,
            DefaultValue = column.Default
        };
    }
}

public sealed class SchemaDropTablePayload
{
    public string TableName { get; set; } = "";
}

public sealed class SchemaIndexPayload
{
    public string TableName { get; set; } = "";

    public TableIndexSchema? Index { get; set; }

    public string IndexName { get; set; } = "";
}

public sealed class SchemaElementStatePayload
{
    public string TableName { get; set; } = "";

    public string ElementName { get; set; } = "";

    public SchemaElementState State { get; set; } = SchemaElementState.Public;

    /// <summary>
    /// Identifies whether this transition targets a column (default) or an index.
    /// Absent in legacy log entries — deserialized as <see cref="SchemaElementKind.Column"/>.
    /// </summary>
    public SchemaElementKind ElementKind { get; set; } = SchemaElementKind.Column;
}
