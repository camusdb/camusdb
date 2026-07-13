
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

    /// <summary>
    /// Evaluates an expression AST against a row, resolving column identifiers via the
    /// <paramref name="row"/> dictionary. When <paramref name="queryRow"/> is non-null the
    /// <see cref="NodeType.Identifier"/> branch bypasses the <see cref="IReadOnlyDictionary{TKey,TValue}"/>
    /// adapter and reads <see cref="QueryRow.Values"/> by ordinal through
    /// <see cref="RowLayout.IndexOf"/>, avoiding virtual dispatch on the interface call.
    /// All other expression types are unaffected — only the column-reference lookup differs
    /// between the two paths, keeping a single shared implementation body.
    /// </summary>
    /// <summary>
    /// Orders two values for a filter/predicate comparison, reconciling a bare string literal against
    /// a <see cref="ColumnType.Uuid"/> or <see cref="ColumnType.Id"/> operand by parsing/normalizing
    /// the string to that type. This lets <c>WHERE uuid_col = '…'</c> / <c>WHERE id = '…'</c> (and
    /// range comparisons) work without an explicit CAST, and — for Id — normalizes the literal to the
    /// canonical lowercase 24-hex form so it matches the stored value regardless of input casing.
    /// </summary>
    private static int CompareValues(ColumnValue left, ColumnValue right)
    {
        // Mixed integer/float operands (e.g. `price > 0` where price is Float64 and 0 is an integer
        // literal) compare numerically by widening both to double; ColumnValue.CompareTo rejects the
        // cross-type comparison.
        if (left.Type != right.Type && IsNumeric(left.Type) && IsNumeric(right.Type))
        {
            double l = left.Type == ColumnType.Integer64 ? left.LongValue : left.FloatValue;
            double r = right.Type == ColumnType.Integer64 ? right.LongValue : right.FloatValue;
            return l.CompareTo(r);
        }

        // Coercion is best-effort. If a String operand is not a valid Uuid/Id it can equal no such
        // value, so return a deterministic non-zero ordering rather than throwing — and never call
        // ColumnValue.CompareTo with mismatched types (it throws on Uuid/Id vs String).
        try
        {
            if (left.Type == ColumnType.Uuid && right.Type == ColumnType.String)
                right = ColumnValue.FromUuidString(right.StrValue!);
            else if (right.Type == ColumnType.Uuid && left.Type == ColumnType.String)
                left = ColumnValue.FromUuidString(left.StrValue!);
            else if (left.Type == ColumnType.Id && right.Type == ColumnType.String)
                right = CastScalarFunctions.CoerceToColumnType(right, ColumnType.Id);
            else if (right.Type == ColumnType.Id && left.Type == ColumnType.String)
                left = CastScalarFunctions.CoerceToColumnType(left, ColumnType.Id);
        }
        catch (CamusDBException)
        {
            // Malformed Uuid/Id literal: unequal to any real value. Order the String operand first.
            return left.Type == ColumnType.String ? -1 : 1;
        }

        return left.CompareTo(right);
    }

    private static bool IsNumeric(ColumnType type) =>
        type is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32;

    public static ColumnValue EvalExpr(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver = null,
        QueryRow? queryRow = null)
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

                    // Ordinal fast path: when the caller supplies a QueryRow directly, bypass the
                    // IReadOnlyDictionary adapter and read Values[ordinal] without virtual dispatch.
                    // Falls through to the dictionary path for any key not found in the layout
                    // (e.g. a parameter alias or a column absent from this row's schema version).
                    if (queryRow is not null)
                    {
                        int ordinal = queryRow.Layout.IndexOf(lookupKey);
                        if (ordinal >= 0)
                            return queryRow.Values[ordinal];
                    }

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
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) == 0);
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) != 0);
                }

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) < 0);
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) > 0);
                }

            case NodeType.ExprLessEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) <= 0);
                }

            case NodeType.ExprGreaterEqualsThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(CompareValues(leftValue, rightValue) >= 0);
                }

            case NodeType.ExprBetween:
                {
                    ColumnValue subject = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue low = EvalExpr(expr.extendedOne!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue high = EvalExpr(expr.extendedTwo!, row, parameters, rowNameResolver, queryRow);

                    if (subject.Type == ColumnType.Null || low.Type == ColumnType.Null || high.Type == ColumnType.Null)
                        return ColumnValue.False;

                    return ColumnValue.FromBool(
                        CompareValues(subject, low) >= 0 && CompareValues(subject, high) <= 0);
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator OR for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(leftValue.BoolValue || rightValue.BoolValue);
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator AND for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(leftValue.BoolValue && rightValue.BoolValue);
                }

            case NodeType.ExprNot:
                {
                    ColumnValue value = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

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
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator + for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue + rightValue.LongValue);
                }

            case NodeType.ExprSub:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator - for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue - rightValue.LongValue);
                }

            case NodeType.ExprMult:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.Integer64 || rightValue.Type != ColumnType.Integer64)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator * for argument types: {leftValue.Type}, {rightValue.Type}");

                    return new ColumnValue(ColumnType.Integer64, leftValue.LongValue * rightValue.LongValue);
                }

            case NodeType.ExprDiv:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    // Float64 / Float64
                    if (leftValue.Type == ColumnType.Float64 && rightValue.Type == ColumnType.Float64)
                    {
                        if (rightValue.FloatValue == 0.0)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");
                        return new ColumnValue(ColumnType.Float64, leftValue.FloatValue / rightValue.FloatValue);
                    }

                    // Integer64 / Float64 or Float64 / Integer64 — widen to Float64
                    if ((leftValue.Type == ColumnType.Integer64 && rightValue.Type == ColumnType.Float64) ||
                        (leftValue.Type == ColumnType.Float64 && rightValue.Type == ColumnType.Integer64))
                    {
                        double l = leftValue.Type == ColumnType.Integer64 ? (double)leftValue.LongValue : leftValue.FloatValue;
                        double r = rightValue.Type == ColumnType.Integer64 ? (double)rightValue.LongValue : rightValue.FloatValue;
                        if (r == 0.0)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");
                        return new ColumnValue(ColumnType.Float64, l / r);
                    }

                    // Integer64 / Integer64 — integer (truncating) division
                    if (leftValue.Type == ColumnType.Integer64 && rightValue.Type == ColumnType.Integer64)
                    {
                        if (rightValue.LongValue == 0)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");
                        return new ColumnValue(ColumnType.Integer64, leftValue.LongValue / rightValue.LongValue);
                    }

                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator / for argument types: {leftValue.Type}, {rightValue.Type}");
                }

            case NodeType.ExprFuncCall:
                // The static lambda pins the 4-param (IReadOnlyDictionary) overload because the
                // optional queryRow param makes the method group ambiguous for delegate conversion.
                // Column refs inside function arguments therefore go through the IReadOnlyDictionary
                // adapter rather than the ordinal fast path. This is correctness-neutral (QueryRow
                // implements the interface) but leaves a small per-argument hashing cost on the
                // table. Passing queryRow through EvaluateExpressionDelegate would require changing
                // its signature, which is a broader refactor deferred to RL3.2.
                return ScalarFunctionEvaluator.Evaluate(expr, row, parameters, rowNameResolver,
                    static (e, r, p, rnr) => EvalExpr(e, r, p, rnr));

            case NodeType.ExprCast:
                {
                    ColumnValue input = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    return CastScalarFunctions.CastExpression("cast", input, expr.rightAst!);
                }

            case NodeType.ExprIsNull:
                {
                    ColumnValue columnValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(columnValue.Type == ColumnType.Null);
                }

            case NodeType.ExprIsNotNull:
                {
                    ColumnValue columnValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(columnValue.Type != ColumnType.Null);
                }

            case NodeType.ExprLike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator LIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                    return ColumnValue.FromBool(Like(leftValue.StrValue!, rightValue.StrValue!));
                }

            case NodeType.ExprILike:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

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
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

                    return ColumnValue.FromBool(
                        SubqueryValueListAst.ContainsValue(leftValue, expr.rightAst, parameters));
                }

            case NodeType.ExprNotInMembership:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
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

        if (constraintsList.nodeType == NodeType.ConstraintNotNullNamed)
        {
            // Carry the user-supplied constraint name as the ColumnValue so callers can thread it
            // into ColumnInfo.NotNullConstraintName.
            constraintTypes.Add((ColumnConstraintType.NotNull, new ColumnValue(ColumnType.String, constraintsList.yytext!)));
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
            NodeAst defaultExpr = constraintsList.leftAst!;

            // A volatile default (e.g. gen_uuid_v7()) must be evaluated per inserted row, not once
            // at DDL time — otherwise every defaulted row would share one value. Store the function
            // name so the insert path can call it per row. Only a bare zero-argument volatile call is
            // supported; any richer volatile expression is rejected rather than silently frozen to a
            // constant. Non-volatile defaults (literals, deterministic calls) stay pre-evaluated.
            if (ScalarFunctionEvaluator.ContainsVolatileFunction(defaultExpr))
            {
                if (defaultExpr.nodeType == NodeType.ExprFuncCall
                    && defaultExpr.rightAst is null
                    && defaultExpr.leftAst?.yytext is string functionName
                    && ScalarFunctionEvaluator.TryResolveVolatileNullary(functionName.ToLowerInvariant(), out _))
                {
                    constraintTypes.Add((ColumnConstraintType.DefaultFunction,
                        new ColumnValue(ColumnType.String, functionName.ToLowerInvariant())));
                }
                else
                {
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        "A volatile function default must be a zero-argument function call, e.g. DEFAULT(gen_uuid_v7())");
                }
            }
            else
            {
                constraintTypes.Add((ColumnConstraintType.Default,
                    EvalExpr(defaultExpr, new Dictionary<string, ColumnValue>(), null)));
            }
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintCheck)
        {
            // The column-level check is collected straight from the AST by
            // SQLExecutorCreateTableCreator.CollectCheckConstraints (which desugars it to a named
            // table-level check); this arm only needs to consume the node so it isn't treated as
            // an unknown constraint. Nothing here reads the value back.
            constraintTypes.Add((ColumnConstraintType.Check, null));
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

    /// <summary>
    /// Returns the name of the volatile nullary function to evaluate per inserted row for a
    /// <c>DEFAULT(fn())</c> column default, or null when the default is a constant or absent.
    /// The name is carried in a String <see cref="ColumnValue"/> by <see cref="GetColumnConstraintList"/>.
    /// </summary>
    protected static string? GetDefaultFunctionFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.DefaultFunction)
                return value?.StrValue;
        }

        return null;
    }

    /// <summary>
    /// Returns the user-supplied NOT NULL constraint name when the column was declared with
    /// <c>CONSTRAINT name NOT NULL</c> (carried as a String <see cref="ColumnValue"/> by
    /// <see cref="GetColumnConstraintList"/>), or null for bare <c>NOT NULL</c>.
    /// </summary>
    protected static string? GetNotNullConstraintNameFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.NotNull && value?.StrValue is { Length: > 0 } name)
                return name;
        }

        return null;
    }

    /// <summary>
    /// Validates that a <c>DEFAULT(fn())</c> function is a supported zero-argument volatile function
    /// whose return type matches the column's declared type (e.g. <c>gen_uuid_v7</c> → <c>uuid</c>).
    /// Throws <c>InvalidInput</c> otherwise. Called at DDL time so a bad default fails at CREATE/ALTER,
    /// not at insert.
    /// </summary>
    protected static void ValidateDefaultFunctionType(string functionName, ColumnType columnType, string columnName)
    {
        if (!ScalarFunctionEvaluator.TryResolveVolatileNullary(functionName, out ColumnType returnType))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"DEFAULT function '{functionName}()' on column '{columnName}' is not a supported zero-argument volatile function");

        if (returnType != columnType)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"DEFAULT function '{functionName}()' returns {returnType} but column '{columnName}' is {columnType}");
    }

    private static bool Like(string text, string pattern)
    {
        // Fast paths: only '%' is a wildcard ('_' is literal, matching current semantics).
        // Use ordinal comparison to avoid culture-dependent results for non-ASCII input.
        int percentCount = CountChar(pattern, '%');

        if (percentCount == 0)
            return text.Equals(pattern, StringComparison.Ordinal);

        if (percentCount == 1)
        {
            if (pattern[^1] == '%')
                return text.AsSpan().StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.Ordinal);
            if (pattern[0] == '%')
                return text.AsSpan().EndsWith(pattern.AsSpan(1), StringComparison.Ordinal);
        }

        if (percentCount == 2 && pattern[0] == '%' && pattern[^1] == '%')
        {
            ReadOnlySpan<char> inner = pattern.AsSpan(1, pattern.Length - 2);
            if (!inner.Contains('%'))
                return text.Contains(inner, StringComparison.Ordinal);
        }

        // Fallback: regex for multi-wildcard patterns (e.g. 'a%b%c').
        string escapedPattern = Regex.Escape(pattern);
        string regexPattern = string.Concat("^", escapedPattern.Replace("%", ".*"), "$");
        return Regex.IsMatch(text, regexPattern);
    }

    private static bool ILike(string text, string pattern)
    {
        int percentCount = CountChar(pattern, '%');

        if (percentCount == 0)
            return text.Equals(pattern, StringComparison.OrdinalIgnoreCase);

        if (percentCount == 1)
        {
            if (pattern[^1] == '%')
                return text.AsSpan().StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.OrdinalIgnoreCase);
            if (pattern[0] == '%')
                return text.AsSpan().EndsWith(pattern.AsSpan(1), StringComparison.OrdinalIgnoreCase);
        }

        if (percentCount == 2 && pattern[0] == '%' && pattern[^1] == '%')
        {
            ReadOnlySpan<char> inner = pattern.AsSpan(1, pattern.Length - 2);
            if (!inner.Contains('%'))
                return text.Contains(inner, StringComparison.OrdinalIgnoreCase);
        }

        // Multi-wildcard fallback: sequential ordinal-ignore-case segment scan.
        // Keeps ILIKE fully ordinal — culture-default regex would diverge for non-ASCII.
        return ILikeMultiWildcard(text, pattern);
    }

    private static bool ILikeMultiWildcard(string text, string pattern)
    {
        // Split pattern on '%' and match each segment in sequence with OrdinalIgnoreCase.
        // Leading segment anchors to the start; trailing segment anchors to the end;
        // interior segments advance a left-to-right scan cursor.
        string[] segments = pattern.Split('%');
        ReadOnlySpan<char> span = text.AsSpan();
        int pos = 0;

        string first = segments[0];
        if (first.Length > 0)
        {
            if (!span.StartsWith(first.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;
            pos += first.Length;
        }

        for (int i = 1; i < segments.Length - 1; i++)
        {
            string seg = segments[i];
            if (seg.Length == 0) continue;
            int found = span[pos..].IndexOf(seg.AsSpan(), StringComparison.OrdinalIgnoreCase);
            if (found < 0) return false;
            pos += found + seg.Length;
        }

        string last = segments[^1];
        if (last.Length > 0)
        {
            int lastStart = text.Length - last.Length;
            if (lastStart < pos) return false;
            if (!span[lastStart..].StartsWith(last.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static int CountChar(string s, char c)
    {
        int count = 0;
        foreach (char ch in s)
            if (ch == c) count++;
        return count;
    }
}
