
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
/// bytes and renames key off of — see <c>RowEncoder</c> and the the architecture documentation. The
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
    /// Name of a nullary volatile scalar function (e.g. <c>gen_uuid_v7</c>) evaluated per inserted
    /// row to produce this column's default. Non-null only for function-call defaults; mutually
    /// exclusive with <see cref="DefaultValue"/>. Persisted in schema JSON as a plain string.
    /// </summary>
    public string? DefaultFunction { get; }

    /// <summary>
    /// Online schema-change state of the column.
    /// </summary>
    public SchemaElementState State { get; }

    /// <summary>
    /// Maximum length in characters (String) or bytes (Bytes). Null means unbounded-but-capped
    /// at the default (see <see cref="CamusDB.Core.CamusDBConfig.DefaultStringMaxLength"/> /
    /// <see cref="CamusDB.Core.CamusDBConfig.DefaultBytesMaxLength"/>). Persisted in schema JSON.
    /// Ignored for other types.
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// Element type for Array columns. Null for all non-Array types. Persisted in schema JSON.
    /// </summary>
    public ColumnType? ArrayElementType { get; }

    /// <summary>
    /// Optional name for the NOT NULL constraint on this column, set when the column was declared
    /// with <c>CONSTRAINT name NOT NULL</c> or when NOT NULL was added via
    /// <c>ALTER TABLE … ALTER COLUMN … SET NOT NULL</c>. When non-null, this name is recognised
    /// by <c>DROP CONSTRAINT name</c> as an alias for dropping the NOT NULL. Null for columns
    /// where the constraint is unnamed (bare <c>NOT NULL</c> in CREATE TABLE).
    /// </summary>
    public string? NotNullConstraintName { get; }

    public TableColumnSchema(
        string id,
        string name,
        ColumnType type,
        bool notNull,
        ColumnValue? defaultValue,
        SchemaElementState state = SchemaElementState.Public,
        int? maxLength = null,
        ColumnType? arrayElementType = null,
        string? defaultFunction = null,
        string? notNullConstraintName = null
    )
    {
        Id = id;
        Name = name;
        Type = type;
        NotNull = notNull;
        DefaultValue = defaultValue;
        State = state;
        MaxLength = maxLength;
        ArrayElementType = arrayElementType;
        DefaultFunction = defaultFunction;
        NotNullConstraintName = notNullConstraintName;
    }
}
