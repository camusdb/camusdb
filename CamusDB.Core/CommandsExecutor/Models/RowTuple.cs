
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class RowTuple
{
    public int RowId { get; set; }

    public int DataPageOffset { get; set; }

    public RowTuple(int rowId, int dataPageOffset)
    {
        RowId = rowId;
        DataPageOffset = dataPageOffset;
    }
}

