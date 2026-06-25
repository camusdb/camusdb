
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
    /// Maximum length in characters (String) or bytes (Bytes). Null means unbounded-but-capped
    /// at the default (see <see cref="CamusDB.Core.CamusDBConfig.DefaultStringMaxLength"/> /
    /// <see cref="CamusDB.Core.CamusDBConfig.DefaultBytesMaxLength"/>). Ignored for other types.
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// Element type for Array columns. Null for all non-Array types.
    /// </summary>
    public ColumnType? ArrayElementType { get; }

    public ColumnInfo(
        string name,
        ColumnType type,
        bool notNull = false,
        ColumnValue? defaultValue = null,
        int? maxLength = null,
        ColumnType? arrayElementType = null
    )
    {
        Name = name;
        Type = type;
        NotNull = notNull;
        Default = defaultValue;
        MaxLength = maxLength;
        ArrayElementType = arrayElementType;
    }
}
