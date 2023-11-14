
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class QueryFilter
{
    public string ColumnName { get; }

    public string Op { get; }

    public ColumnValue Value { get; }

    public QueryFilter(string columnName, string op, ColumnValue value)
    {
        ColumnName = columnName;
        Op = op;
        Value = value;
    }
}
