
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseIndexObject
{
    public string Column { get; }

    public IndexType Type { get; }

    public int StartOffset { get; }

    public DatabaseIndexObject(string column, IndexType type, int startOffset)
    {
        Column = column;
        Type = type;
        StartOffset = startOffset;
    }
}
