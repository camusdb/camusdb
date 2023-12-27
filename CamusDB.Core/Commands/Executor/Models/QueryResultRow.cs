
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models;

public readonly struct QueryResultRow
{
    public BTreeTuple Tuple { get; }

    public Dictionary<string, ColumnValue> Row { get; }

    public QueryResultRow(BTreeTuple tuple, Dictionary<string, ColumnValue> row)
	{
        Tuple = tuple;
        Row = row;
	}
}

