
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal abstract class SQLExecutorBaseCreator
{
    protected static List<string> GetIdentifierList(NodeAst orderByAst)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
            return new() { orderByAst.yytext ?? "" };

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {
            List<string> allIdentifiers = new();

            if (orderByAst.leftAst is not null)
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.leftAst));

            if (orderByAst.rightAst is not null)
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.rightAst));

            return allIdentifiers;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }

    public static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row)
    {
        switch (expr.nodeType)
        {
            case NodeType.Number:
                return new ColumnValue(ColumnType.Integer64, expr.yytext!);

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Bool:
                return new ColumnValue(ColumnType.Bool, expr.yytext!);

            case NodeType.Null:
                return new ColumnValue(ColumnType.Null, "");

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

            case NodeType.ExprFuncCall:
                {
                    return new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString());
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, $"ERROR {expr.nodeType}");
        }
    }
}
