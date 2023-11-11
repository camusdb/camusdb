
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed record DatabaseIndexObject
{
    public string Column { get; }

    public IndexType Type { get; }

    public string StartOffset { get; }

    public DatabaseIndexObject(string column, IndexType type, string startOffset)
    {
        Column = column;
        Type = type;
        StartOffset = startOffset;
    }
}
