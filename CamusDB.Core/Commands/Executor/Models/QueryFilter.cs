
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

    public string Operator { get; }

    public ColumnValue Value { get; }

    public QueryFilter(string column, string op, ColumnValue value)
    {
        ColumnName = column;
        Operator = op;
        Value = value;
    }
}
