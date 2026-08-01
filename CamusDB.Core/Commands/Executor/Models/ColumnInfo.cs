
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

    public ColumnInfo(
        string name,
        ColumnType type,
        bool notNull = false,
        ColumnValue? defaultValue = null,
        int? maxLength = null,
        ColumnType? arrayElementType = null,
        string? defaultFunction = null,
        string? notNullConstraintName = null,
        string? comment = null
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
    }
}
