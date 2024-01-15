
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
using System.Text.RegularExpressions;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal abstract class SQLExecutorBaseCreator
{
    protected static void GetIdentifierList(NodeAst orderByAst, List<string> identifierList)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
        {
            identifierList.Add(orderByAst.yytext ?? "");
            return;
        }

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {            
            if (orderByAst.leftAst is not null)
                GetIdentifierList(orderByAst.leftAst, identifierList);

            if (orderByAst.rightAst is not null)
                GetIdentifierList(orderByAst.rightAst, identifierList);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }

    public static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        switch (expr.nodeType)
        {
            case NodeType.Number:
                if (!long.TryParse(expr.yytext!, out long longValue))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Int64: " + expr.yytext!);

                return new ColumnValue(ColumnType.Integer64, longValue);                

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Bool:
                if (!bool.TryParse(expr.yytext!, out bool boolValue))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Bool: " + expr.yytext!);

                return new ColumnValue(ColumnType.Bool, boolValue);

            case NodeType.Null:
                return new ColumnValue(ColumnType.Null, 0);

            case NodeType.Identifier:
                {
                    if (row.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
                        return columnValue;

                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown column: " + expr.yytext!);
                }

            case NodeType.Placeholder:
                {
                    if (parameters is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing placeholders to replace: " + expr.yytext!);

                    if (parameters.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
                        return columnValue;

                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown placeholder: " + expr.yytext!);
                }

            case NodeType.ExprEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) == 0);
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) != 0);
                }

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) < 0);
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) > 0);
                }

            case NodeType.ExprLessEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) <= 0);
                }

            case NodeType.ExprGreaterEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    return new ColumnValue(ColumnType.Bool, leftValue.CompareTo(rightValue) >= 0);
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator OR for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Bool, leftValue.BoolValue || rightValue.BoolValue);
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator AND for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Bool, leftValue.BoolValue && rightValue.BoolValue);
                }

            case NodeType.ExprAdd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator + for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue + rightValue.LongValue);
                }

            case NodeType.ExprSub:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator - for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue - rightValue.LongValue);
                }

            case NodeType.ExprMult:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator * for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue * rightValue.LongValue);
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

                            GetArgumentList(expr.rightAst!, row, parameters, argumentList);

                            return new ColumnValue(ColumnType.Id, argumentList.FirstOrDefault()!.StrValue ?? "");

                        case "now":
                            return new ColumnValue(ColumnType.String, DateTime.UtcNow.ToString());

                        default:
                            throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Function not found '" + funcCall + "'");
                    }
                }

            case NodeType.ExprLike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);
                    
                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator LIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Bool, Like(leftValue.StrValue!, rightValue.StrValue!));
                }

            case NodeType.ExprILike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters);

                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator ILIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Bool, ILike(leftValue.StrValue!, rightValue.StrValue!));
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, $"ERROR {expr.nodeType}");
        }
    }

    private static void GetArgumentList(NodeAst argumentAst, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters, LinkedList<ColumnValue> argumentList)
    {        
        if (argumentAst.nodeType == NodeType.ExprArgumentList)
        {
            if (argumentAst.leftAst != null)
                GetArgumentList(argumentAst.leftAst, row, parameters, argumentList);

            if (argumentAst.rightAst != null)
                GetArgumentList(argumentAst.rightAst, row, parameters, argumentList);

            return;
        }

        argumentList.AddLast(EvalExpr(argumentAst, row, parameters));
    }

    private static bool Like(string text, string pattern)
    {
        // Escape all regex special characters
        string escapedPattern = Regex.Escape(pattern);

        // Replace the escaped '%' with '.*' to simulate SQL LIKE wildcard
        string regexPattern = "^" + escapedPattern.Replace("%", ".*") + "$";

        return Regex.IsMatch(text, regexPattern);
    }

    private static bool ILike(string text, string pattern)
    {
        // Escape all regex special characters
        string escapedPattern = Regex.Escape(pattern);

        // Replace the escaped '%' with '.*' to simulate SQL LIKE wildcard
        string regexPattern = "^" + escapedPattern.Replace("%", ".*") + "$";

        return Regex.IsMatch(text, regexPattern, RegexOptions.IgnoreCase);
    }
}
