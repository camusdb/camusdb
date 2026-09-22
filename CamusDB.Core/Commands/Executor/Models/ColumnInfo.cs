
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ColumnInfo
{
    public string Name { get; }

    public ColumnType Type { get; }

    public bool NotNull { get; }

    public ColumnValue? Default { get; }

    /// <summary>
    /// Name of a nullary volatile scalar function (e.g. <c>gen_uuid_v7</c>) to evaluate per inserted
    /// row for this column's default, when the default is a function call rather than a constant.
    /// Mutually exclusive with <see cref="Default"/>. Null when there is no function default.
    /// </summary>
    public string? DefaultFunction { get; }

    /// <summary>
    /// The immutable id of the sequence this column's default draws from, or null. Set by
    /// <c>SERIAL</c> / <c>GENERATED AS IDENTITY</c> desugaring and by an explicit
    /// <c>DEFAULT nextval('…')</c>, after the sequence has been resolved to an id.
    /// </summary>
    public string? DefaultSequenceId { get; }

    /// <summary>
    /// True for <c>GENERATED ALWAYS AS IDENTITY</c>: a user-supplied value for this column is
    /// refused on INSERT rather than overridden.
    /// </summary>
    public bool IdentityAlways { get; }

    /// <summary>
    /// The name written in <c>DEFAULT nextval('…')</c>, before it has been resolved to a sequence
    /// id. Null when the default is not a sequence. The DDL path resolves it and rewrites the
    /// column with <see cref="DefaultSequenceId"/> set; nothing downstream reads this.
    /// </summary>
    public string? DefaultSequenceName { get; }

    /// <summary>
    /// The identity kind the parser saw on this column, before any sequence exists to point at.
    /// Null when the column was not declared as an identity column. The DDL path reads it, creates
    /// the owned sequence, and rewrites the column with <see cref="DefaultSequenceId"/> set.
    /// </summary>
    public ColumnIdentityKind? Identity { get; }

    /// <summary>
    /// Maximum length in characters (String) or bytes (Bytes). Null means unbounded-but-capped
    /// at the default (see <see cref="CamusDB.Core.CamusDBConstants.DefaultStringMaxLength"/> /
    /// <see cref="CamusDB.Core.CamusDBConstants.DefaultBytesMaxLength"/>). Ignored for other types.
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// Element type for Array columns. Null for all non-Array types.
    /// </summary>
    public ColumnType? ArrayElementType { get; }

    /// <summary>
    /// Name of the NOT NULL constraint when the column was declared with <c>CONSTRAINT name NOT NULL</c>.
    /// Null for bare <c>NOT NULL</c> declarations.
    /// </summary>
    public string? NotNullConstraintName { get; }

    /// <summary>
    /// Free-text description declared with an inline <c>COMMENT '…'</c> on the column in
    /// <c>CREATE TABLE</c> or <c>ALTER TABLE … ADD COLUMN</c>. Null when the column was declared
    /// without one. There is no inline form for removing a comment; that is
    /// <c>COMMENT ON COLUMN … IS NULL</c>.
    /// </summary>
    public string? Comment { get; }

    /// <summary>
    /// Storage strategy declared inline with <c>STORAGE PLAIN | MAIN | EXTERNAL | EXTENDED</c>.
    /// Null when the column was declared without one, which means the default
    /// (<see cref="ColumnStorageStrategy.Extended"/>).
    /// </summary>
    public ColumnStorageStrategy? Storage { get; }

    public ColumnInfo(
        string name,
        ColumnType type,
        bool notNull = false,
        ColumnValue? defaultValue = null,
        int? maxLength = null,
        ColumnType? arrayElementType = null,
        string? defaultFunction = null,
        string? notNullConstraintName = null,
        string? comment = null,
        ColumnStorageStrategy? storage = null,
        string? defaultSequenceId = null,
        bool identityAlways = false,
        ColumnIdentityKind? identity = null,
        string? defaultSequenceName = null
    )
    {
        Name = name;
        Type = type;
        NotNull = notNull;
        Default = defaultValue;
        MaxLength = maxLength;
        ArrayElementType = arrayElementType;
        DefaultFunction = defaultFunction;
        NotNullConstraintName = notNullConstraintName;
        Comment = comment;
        Storage = storage;
        DefaultSequenceId = defaultSequenceId;
        IdentityAlways = identityAlways;
        Identity = identity;
        DefaultSequenceName = defaultSequenceName;
    }
}
