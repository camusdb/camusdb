
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

    public bool Primary { get; }

    public bool NotNull { get; }

    public ColumnInfo(string name, ColumnType type, bool primary = false, bool notNull = false)
    {
        Name = name;
        Type = type;
        Primary = primary;
        NotNull = notNull;
    }
}
