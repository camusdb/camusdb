
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models;

public readonly struct QueryResultRow
{
    public ObjectIdValue RowId { get; }

    public Dictionary<string, ColumnValue> Row { get; }

    public QueryResultRow(ObjectIdValue rowId, Dictionary<string, ColumnValue> row)
	{
        RowId = rowId;
        Row = row;
	}
}

