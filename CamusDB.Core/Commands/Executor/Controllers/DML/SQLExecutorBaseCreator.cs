
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using System.Text.RegularExpressions;
using System.Globalization;

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

    public static ColumnValue EvalExpr(
        NodeAst expr,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver = null)
    {
        switch (expr.nodeType)
        {
            case NodeType.Integer:
                if (!long.TryParse(expr.yytext!, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Int64: " + expr.yytext!);

                return new ColumnValue(ColumnType.Integer64, longValue);

            case NodeType.Float:
                if (!double.TryParse(expr.yytext!, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Float64: " + expr.yytext!);

                return new ColumnValue(ColumnType.Float64, doubleValue);

            case NodeType.String:
            {
                string raw = expr.yytext!;
                // Strip the outer quoting character (single or double quote).
                string unquoted = (raw.Length >= 2 && raw[0] == raw[^1] && (raw[0] == '"' || raw[0] == '\''))
                    ? raw[1..^1]
                    : raw.Trim('"');
                return new ColumnValue(ColumnType.String, unquoted);
            }

            case NodeType.Bool:
                if (!bool.TryParse(expr.yytext!, out bool boolValue))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Bool: " + expr.yytext!);

                return ColumnValue.FromBool(boolValue);

            case NodeType.Null:
                return ColumnValue.Null;

            case NodeType.ObjectIdLiteral:
                if (string.IsNullOrEmpty(expr.yytext))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid ObjectId literal");

                return new ColumnValue(ColumnType.Id, expr.yytext);

            case NodeType.Identifier:
                {
                    string lookupKey = rowNameResolver?.ResolveRowLookupKey(expr.yytext!) ?? expr.yytext!;

                    if (row.TryGetValue(lookupKey, out ColumnValue? columnValue))
                        return columnValue;

                    throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, "Unknown column: " + expr.yytext!);
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
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) == 0);
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) != 0);
                }

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) < 0);
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) > 0);
                }

            case NodeType.ExprLessEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) <= 0);
                }

            case NodeType.ExprGreaterEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(leftValue.CompareTo(rightValue) >= 0);
                }

            case NodeType.ExprBetween:
                {
                    ColumnValue subject = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue low = EvalExpr(expr.extendedOne!, row, parameters, rowNameResolver);
                    ColumnValue high = EvalExpr(expr.extendedTwo!, row, parameters, rowNameResolver);

                    if (subject.Type == ColumnType.Null || low.Type == ColumnType.Null || high.Type == ColumnType.Null)
                        return ColumnValue.False;

                    return ColumnValue.FromBool(
                        subject.CompareTo(low) >= 0 && subject.CompareTo(high) <= 0);
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator OR for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(leftValue.BoolValue || rightValue.BoolValue);
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator AND for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(leftValue.BoolValue && rightValue.BoolValue);
                }

            case NodeType.ExprNot:
                {
                    ColumnValue value = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);

                    // Three-valued logic: NOT NULL is NULL (unknown), which the predicate filter
                    // treats as non-matching.
                    if (value.Type == ColumnType.Null)
                        return ColumnValue.Null;

                    if (value.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator NOT for argument type: {value.Type}");

                    return ColumnValue.FromBool(!value.BoolValue);
                }

            case NodeType.ExprAdd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator + for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue + rightValue.LongValue);
                }

            case NodeType.ExprSub:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator - for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue - rightValue.LongValue);
                }

            case NodeType.ExprMult:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator * for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue * rightValue.LongValue);
                }

            case NodeType.ExprFuncCall:
                return ScalarFunctionEvaluator.Evaluate(expr, row, parameters, rowNameResolver, EvalExpr);

            case NodeType.ExprCast:
                {
                    ColumnValue input = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    return CastScalarFunctions.CastExpression("cast", input, expr.rightAst!);
                }

            case NodeType.ExprIsNull:
                {
                    ColumnValue columnValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(columnValue.Type == ColumnType.Null);
                }

            case NodeType.ExprIsNotNull:
                {
                    ColumnValue columnValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(columnValue.Type != ColumnType.Null);
                }

            case NodeType.ExprLike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);
                    
                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator LIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(Like(leftValue.StrValue!, rightValue.StrValue!));
                }

            case NodeType.ExprILike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver);

                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator ILIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(ILike(leftValue.StrValue!, rightValue.StrValue!));
                }

            case NodeType.ExprScalarSubquery:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Scalar subquery must be resolved before expression evaluation");

            case NodeType.ExprInSubquery:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "IN subquery must be resolved before expression evaluation");

            case NodeType.ExprNotInSubquery:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "NOT IN subquery must be resolved before expression evaluation");

            case NodeType.ExprInMembership:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);

                    return ColumnValue.FromBool(
                        SubqueryValueListAst.ContainsValue(leftValue, expr.rightAst, parameters));
                }

            case NodeType.ExprNotInMembership:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver);
                    bool? result = SubqueryValueListAst.EvaluateNotInMembership(leftValue, expr, parameters);

                    return ColumnValue.FromBool(result ?? false);
                }

            case NodeType.ExprExistsSubquery:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "EXISTS subquery must be resolved before expression evaluation");

            case NodeType.ExprExistsCorrelated:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Correlated EXISTS subquery must be evaluated by the query filter");

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, $"ERROR {expr.nodeType}");
        }
    }

    protected static void GetColumnConstraintList(NodeAst constraintsList, List<(ColumnConstraintType, ColumnValue?)> constraintTypes)
    {
        if (constraintsList.nodeType == NodeType.ConstraintNotNull)
        {
            constraintTypes.Add((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNull)
        {
            constraintTypes.Add((ColumnConstraintType.Null, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintPrimaryKey)
        {
            constraintTypes.Add((ColumnConstraintType.PrimaryKey, null));
            constraintTypes.Add((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintUnique)
        {
            constraintTypes.Add((ColumnConstraintType.Unique, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintDefault)
        {
            constraintTypes.Add((ColumnConstraintType.Default, EvalExpr(constraintsList.leftAst!, new(), null)));
            return;
        }

        if (constraintsList.nodeType == NodeType.CreateTableFieldConstraintList)
        {
            if (constraintsList.leftAst != null)
                GetColumnConstraintList(constraintsList.leftAst, constraintTypes);

            if (constraintsList.rightAst != null)
                GetColumnConstraintList(constraintsList.rightAst, constraintTypes);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid constraint type found: " + constraintsList.nodeType);
    }

    protected static ColumnValue? GetDefaultFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.Default)
                return value;
        }

        return null;
    }

    private static bool Like(string text, string pattern)
    {
        // Escape all regex special characters
        string escapedPattern = Regex.Escape(pattern);

        // Replace the escaped '%' with '.*' to simulate SQL LIKE wildcard
        string regexPattern = string.Concat("^", escapedPattern.Replace("%", ".*"), "$");

        return Regex.IsMatch(text, regexPattern);
    }

    private static bool ILike(string text, string pattern)
    {
        // Escape all regex special characters
        string escapedPattern = Regex.Escape(pattern);

        // Replace the escaped '%' with '.*' to simulate SQL LIKE wildcard
        string regexPattern = string.Concat("^", escapedPattern.Replace("%", ".*"), "$");

        return Regex.IsMatch(text, regexPattern, RegexOptions.IgnoreCase);
    }
}
