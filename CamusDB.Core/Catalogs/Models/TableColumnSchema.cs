
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public sealed class TableColumnSchema
{
    public string Name { get; }

    public ColumnType Type { get; }

    public bool Primary { get; }

    public bool NotNull { get; }

    public TableColumnSchema(string name, ColumnType type, bool primary, bool notNull)
    {
        Name = name;
        Type = type;
        Primary = primary;
        NotNull = notNull;
    }
}

