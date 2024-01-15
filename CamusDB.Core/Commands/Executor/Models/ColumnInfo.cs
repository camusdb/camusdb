
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

    public ColumnInfo(
        string name,
        ColumnType type,        
        bool notNull = false,        
        ColumnValue? defaultValue = null
    )
    {
        Name = name;
        Type = type;        
        NotNull = notNull;
        Default = defaultValue;
    }
}
