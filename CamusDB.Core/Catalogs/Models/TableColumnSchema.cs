
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
    /// The immutable id of the sequence this column's default draws from, or null when the default
    /// is a constant, a nullary function, or absent.
    ///
    /// <para>Separate from <see cref="DefaultFunction"/> rather than folded into it: a function
    /// default is a nullary call evaluated per row, while a sequence default needs an argument and
    /// an asynchronous reservation the row-shaping path cannot make. Keeping the three default
    /// kinds distinguishable is what stops every existing <c>DefaultFunction</c> reader from having
    /// to learn about sequences.</para>
    ///
    /// <para>The <b>id</b>, never the sequence's name, so <c>ALTER SEQUENCE … RENAME TO</c> does
    /// not break the default. A user-facing rendering resolves the id back to the current
    /// name.</para>
    /// </summary>
    public string? DefaultSequenceId { get; }

    /// <summary>
    /// True when the column was declared <c>GENERATED ALWAYS AS IDENTITY</c>: an INSERT that
    /// supplies a value for it is refused rather than silently overridden. Meaningless unless
    /// <see cref="DefaultSequenceId"/> is set.
    /// </summary>
    public bool IdentityAlways { get; }

    /// <summary>
    /// Online schema-change state of the column.
    /// </summary>
    public SchemaElementState State { get; }

    /// <summary>
    /// Maximum length in characters (String) or bytes (Bytes). Null means unbounded-but-capped
    /// at the default (see <see cref="CamusDB.Core.CamusDBConstants.DefaultStringMaxLength"/> /
    /// <see cref="CamusDB.Core.CamusDBConstants.DefaultBytesMaxLength"/>). Persisted in schema JSON.
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

    /// <summary>
    /// Free-text description attached via <c>COMMENT ON COLUMN</c> or an inline column
    /// <c>COMMENT '…'</c>. Null means no comment; an empty string is a present-but-empty comment,
    /// and the distinction is preserved end to end. Because this type is constructor-set, every
    /// rebuild-copy of a column (rename, SET/DROP NOT NULL, schema-history rewrite) must pass the
    /// existing value through or the comment is silently lost.
    /// </summary>
    public string? Comment { get; }

    /// <summary>
    /// How the writer may compress or move a large value of this column out of the row. Null means
    /// <see cref="ColumnStorageStrategy.Extended"/>, so a schema persisted before the strategy existed
    /// loads unchanged. Meaningful only for <c>String</c>, <c>Bytes</c> and <c>Array</c> columns. A
    /// write-time decision only: a reader follows the per-cell marks in the stored row, never this
    /// value. Like <see cref="Comment"/>, every rebuild-copy of a column must pass it through or the
    /// strategy is silently reset to the default.
    /// </summary>
    public ColumnStorageStrategy? Storage { get; }

    /// <summary>The effective strategy: <see cref="Storage"/>, or <see cref="ColumnStorageStrategy.Extended"/> when unset.</summary>
    public ColumnStorageStrategy EffectiveStorage => Storage ?? ColumnStorageStrategy.Extended;

    /// <summary>True for the column types whose values have a variable-length payload that a storage strategy can act on.</summary>
    public static bool SupportsStorageStrategy(ColumnType type) =>
        type is ColumnType.String or ColumnType.Bytes or ColumnType.Array;

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
        string? notNullConstraintName = null,
        string? comment = null,
        ColumnStorageStrategy? storage = null,
        string? defaultSequenceId = null,
        bool identityAlways = false
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
        Comment = comment;
        Storage = storage;
        DefaultSequenceId = defaultSequenceId;
        IdentityAlways = identityAlways;
    }
}
