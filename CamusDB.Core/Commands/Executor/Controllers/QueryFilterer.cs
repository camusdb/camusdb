
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
    internal bool MeetWhere(NodeAst where, Dictionary<string, ColumnValue> row)
    {
        ColumnValue evaluatedExpr = EvalExpr(where, row);

        switch (evaluatedExpr.Type)
        {
            case ColumnType.Null:
                return false;

            case ColumnType.Bool:
                //Console.WriteLine(evaluatedExpr.Value);
                return evaluatedExpr.Value == "True" || evaluatedExpr.Value == "true";

            case ColumnType.Float:
                if (float.TryParse(evaluatedExpr.Value, out float res))
                {
                    if (res != 0)
                        return true;
                }
                return false;

            case ColumnType.Integer64:
                if (long.TryParse(evaluatedExpr.Value, out long res2))
                {
                    if (res2 != 0)
                        return true;
                }
                return false;
        }

        return false;
    }

    private static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row)
    {
        switch (expr.nodeType)
        {
            case NodeType.Number:
                return new ColumnValue(ColumnType.Integer64, expr.yytext!);

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Bool:
                return new ColumnValue(ColumnType.Bool, expr.yytext!);

            case NodeType.Identifier:

                if (row.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
                    return columnValue;

                throw new Exception("Not found column: " + expr.yytext!);

            case NodeType.ExprEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) == 0).ToString());
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) != 0).ToString());
                }

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) < 0).ToString());
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) > 0).ToString());
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.Value.ToLowerInvariant() == "true" || rightValue.Value.ToLowerInvariant() == "true").ToString());
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.Value.ToLowerInvariant() == "true" && rightValue.Value.ToLowerInvariant() == "true").ToString());
                }

            default:
                Console.WriteLine("ERROR {0}", expr.nodeType);
                break;
        }

        return new ColumnValue(ColumnType.Null, "");
    }

    // @todo : this is a very naive implementation, we should use a proper type conversion and implement all operators
    internal bool MeetFilters(List<QueryFilter> filters, Dictionary<string, ColumnValue> row)
    {
        foreach (QueryFilter filter in filters)
        {
            if (string.IsNullOrEmpty(filter.ColumnName))
            {
                Console.WriteLine("Found empty or null column name in filters");
                return false;
            }

            if (!row.TryGetValue(filter.ColumnName, out ColumnValue? value))
                return false;

            switch (filter.Op)
            {
                case "=":
                    if (value.Value != filter.Value.Value)
                        return false;
                    break;

                case "!=":
                    if (value.Value == filter.Value.Value)
                        return false;
                    break;

                case ">":                    
                    if (long.Parse(value.Value) <= long.Parse(filter.Value.Value))
                        return false;
                    break;

                case ">=":
                    if (long.Parse(value.Value) < long.Parse(filter.Value.Value))
                        return false;
                    break;

                case "<":
                    if (long.Parse(value.Value) >= long.Parse(filter.Value.Value))
                        return false;
                    break;

                case "<=":
                    if (long.Parse(value.Value) > long.Parse(filter.Value.Value))
                        return false;
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown operator :" + filter.Op);
            }
        }

        return true;
    }
}
