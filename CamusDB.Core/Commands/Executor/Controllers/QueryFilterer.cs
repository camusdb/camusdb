
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryFilterer
{
    internal bool MeetWhere(NodeAst where, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        ColumnValue evaluatedExpr = SqlExecutor.EvalExpr(where, row, parameters);

        switch (evaluatedExpr.Type)
        {
            case ColumnType.Null:
                return false;

            case ColumnType.Bool:
                return evaluatedExpr.BoolValue;

            case ColumnType.Float64:
                return evaluatedExpr.LongValue != 0;

            case ColumnType.Integer64:
                return evaluatedExpr.LongValue != 0;
        }

        return false;
    }

    // @todo : this is a very naive implementation, we should use a proper type conversion and implement all operators
    internal bool MeetFilters(List<QueryFilter> filters, Dictionary<string, ColumnValue> row)
    {
        foreach (QueryFilter filter in filters)
        {
            if (string.IsNullOrEmpty(filter.ColumnName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Found empty or null column name in filters");

            if (string.IsNullOrEmpty(filter.Op))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Found empty or null operator in filters");

            if (!row.TryGetValue(filter.ColumnName, out ColumnValue? value))
                return false;

            switch (filter.Op)
            {
                case "=":
                    if (value.StrValue != filter.Value.StrValue)
                        return false;
                    break;

                case "!=":
                    if (value.StrValue == filter.Value.StrValue)
                        return false;
                    break;

                case ">":
                    return value.CompareTo(filter.Value) == 1;

                case ">=":
                    return value.CompareTo(filter.Value) >= 0;

                case "<":
                    return value.CompareTo(filter.Value) == -1;

                case "<=":
                    return value.CompareTo(filter.Value) <= 0;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown operator :" + filter.Op);
            }
        }

        return true;
    }
}
