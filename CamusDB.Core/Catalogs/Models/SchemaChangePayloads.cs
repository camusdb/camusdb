
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

    /// <summary>
    /// CHECK constraints declared in the CREATE TABLE statement (both column-level, desugared to
    /// named constraints, and explicit table-level forms). Null/empty when no checks were declared;
    /// absent in log entries written before this field existed (backward-compatible: null → no checks).
    /// The <c>ParsedCondition</c> field on each entry is not persisted (<c>[JsonIgnore]</c>); it is
    /// rebuilt from <c>Expression</c> at table-open time.
    /// </summary>
    public CheckConstraintSchema[]? CheckConstraints { get; set; }
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

    /// <summary>
    /// Name of the NOT NULL constraint for this column, when named. Null for unnamed NOT NULL.
    /// </summary>
    public string? NotNullConstraintName { get; set; }

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
            NotNullConstraintName = column.NotNullConstraintName,
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

/// <summary>
/// Payload for <see cref="SchemaOp.AddCheckConstraint"/> and <see cref="SchemaOp.DropCheckConstraint"/>.
/// Carries the table name, constraint name, and (for Add) the condition SQL text and referenced columns.
/// For Drop, only <see cref="TableName"/> and <see cref="ConstraintName"/> are consulted.
/// </summary>
public sealed class SchemaCheckConstraintPayload
{
    /// <summary>Table that owns the constraint.</summary>
    public string TableName { get; set; } = "";

    /// <summary>Unique name of the constraint within the table.</summary>
    public string ConstraintName { get; set; } = "";

    /// <summary>
    /// SQL text of the condition, rendered by <c>PlanRenderer.RenderExpr</c>. Empty string for
    /// Drop operations (the constraint is identified by name alone).
    /// </summary>
    public string Expression { get; set; } = "";

    /// <summary>
    /// Column identifiers referenced by the condition. Empty for Drop operations.
    /// </summary>
    public string[] ReferencedColumns { get; set; } = [];
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetColumnNotNull"/>. Carries the target column name, the new
/// <c>NotNull</c> value (true = SET NOT NULL, false = DROP NOT NULL), and the optional constraint
/// name. Used both by <c>ALTER TABLE … ALTER COLUMN … SET/DROP NOT NULL</c> and by
/// <c>DROP CONSTRAINT name</c> when the name resolves to a column's NOT NULL constraint.
/// </summary>
public sealed class SchemaSetColumnNotNullPayload
{
    public string TableName { get; set; } = "";

    public string ColumnName { get; set; } = "";

    /// <summary>True = SET NOT NULL; false = DROP NOT NULL.</summary>
    public bool NotNull { get; set; }

    /// <summary>
    /// Name assigned to this NOT NULL constraint. Set by SET NOT NULL (auto-named
    /// <c>{table}_{col}_not_null</c>), or preserved from the original column declaration.
    /// Null for DROP NOT NULL (constraint is being removed).
    /// </summary>
    public string? ConstraintName { get; set; }
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
