
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

    // Inline constraints (PRIMARY KEY / UNIQUE / INDEX declared in CREATE TABLE) are folded into
    // this single CreateTable delta so creating a table is exactly one schema version. A new table
    // is empty, so its indexes are born at Public with nothing to backfill. Null/empty on tables
    // declared without inline constraints, and on log entries written before this field existed.
    public TableIndexSchema[]? Indexes { get; set; }
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

    /// <summary>
    /// Name of a nullary volatile scalar function evaluated per inserted row for this column's
    /// default (e.g. <c>gen_uuid_v7</c>). Null for constant or absent defaults.
    /// </summary>
    public string? DefaultFunction { get; set; }

    public SchemaElementState State { get; set; } = SchemaElementState.Public;

    /// <summary>
    /// Maximum length in characters (String) or bytes (Bytes). Null = unbounded-but-default-capped.
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// Element type for Array columns. Null for non-Array types.
    /// </summary>
    public ColumnType? ArrayElementType { get; set; }

    public static SchemaColumnPayload FromColumnInfo(ColumnInfo column)
    {
        return new()
        {
            Name = column.Name,
            Type = column.Type,
            NotNull = column.NotNull,
            DefaultValue = column.Default,
            DefaultFunction = column.DefaultFunction,
            MaxLength = column.MaxLength,
            ArrayElementType = column.ArrayElementType,
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

/// <summary>
/// Discriminator for <see cref="SchemaRenamePayload"/>: whether the rename targets a table,
/// a column, or an index.
/// </summary>
public enum SchemaRenameKind
{
    Table,
    Column,
    Index,
}

/// <summary>
/// Payload for <see cref="SchemaOp.RenameTable"/>, <see cref="SchemaOp.RenameColumn"/>,
/// and <see cref="SchemaOp.RenameIndex"/>. Rename ops are single-delta, metadata-only;
/// no row or index bytes move.
/// </summary>
public sealed class SchemaRenamePayload
{
    /// <summary>Current (pre-rename) table name. Always the table being touched.</summary>
    public string TableName { get; set; } = "";

    public SchemaRenameKind Kind { get; set; }

    /// <summary>Old column or index name. Null for <see cref="SchemaRenameKind.Table"/>.</summary>
    public string? ElementName { get; set; }

    public string NewName { get; set; } = "";
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
