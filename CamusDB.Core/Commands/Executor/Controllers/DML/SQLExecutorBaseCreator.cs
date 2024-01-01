﻿
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
                return new ColumnValue(ColumnType.Integer64, long.Parse(expr.yytext!));

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Bool:
                return new ColumnValue(ColumnType.Bool, bool.Parse(expr.yytext!));

            case NodeType.Null:
                return new ColumnValue(ColumnType.Null, 0);

            case NodeType.Identifier:

                if (row.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
                    return columnValue;

                throw new Exception("Not found column: " + expr.yytext!);

            case NodeType.ExprEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) == 0);
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) != 0);
                }

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) < 0);
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) > 0);
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.BoolValue || rightValue.BoolValue);
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, leftValue.BoolValue && rightValue.BoolValue);
                }

            case NodeType.ExprFuncCall:
                {
                    string funcCall = expr.leftAst!.yytext!.ToLowerInvariant();

                    switch (funcCall)
                    {
                        case "gen_id":
                            return new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString());

                        case "str_id":

                            LinkedList<ColumnValue> argumentList = new();

                            GetArgumentList(expr.rightAst!, row, argumentList);

                            return new ColumnValue(ColumnType.Id, argumentList.FirstOrDefault()!.StrValue ?? "");

                        case "now":
                            return new ColumnValue(ColumnType.String, DateTime.UtcNow.ToString());

                        default:
                            throw new CamusDBException(CamusDBErrorCodes.InvalidPageOffset, "Unknown function '" + funcCall + "'");
                    }
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, $"ERROR {expr.nodeType}");
        }
    }

    private static void GetArgumentList(NodeAst argumentAst, Dictionary<string, ColumnValue> row, LinkedList<ColumnValue> argumentList)
    {        
        if (argumentAst.nodeType == NodeType.ExprArgumentList)
        {
            if (argumentAst.leftAst != null)
                GetArgumentList(argumentAst.leftAst, row, argumentList);

            if (argumentAst.rightAst != null)
                GetArgumentList(argumentAst.rightAst, row, argumentList);

            return;
        }

        argumentList.AddLast(EvalExpr(argumentAst, row));
    }
}
