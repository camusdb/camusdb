
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ColumnValue
{
    public ColumnType Type { get; }

    public string Value { get; }

    public ColumnValue(ColumnType type, string value)
    {
        Type = type;
        Value = value;
    }
}
