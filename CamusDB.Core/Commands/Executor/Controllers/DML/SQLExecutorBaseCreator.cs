
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

    /// <summary>
    /// Applies <c>+ - * /</c> to two numeric operands using SQL-style numeric promotion: the wider
    /// operand type wins, so <c>Integer64 op Integer64</c> is the only combination that stays integral
    /// (and division there truncates), any <see cref="ColumnType.Float64"/> operand promotes both sides
    /// to double, and a <see cref="ColumnType.Float32"/> operand mixed with Integer64 yields Float32.
    /// Float32 results are rounded through <c>float</c> before being carried in the double-backed
    /// <see cref="ColumnValue.FloatValue"/>, so a Float32 expression never reports precision the type
    /// cannot hold. Non-numeric operands are rejected rather than silently coerced — mixed-type
    /// comparison lives in <see cref="CompareValues"/>, which has different (ordering) semantics.
    /// Division by a zero of any type raises an error instead of yielding IEEE infinity/NaN.
    /// </summary>
    private static ColumnValue EvalArithmetic(NodeType op, ColumnValue left, ColumnValue right)
    {
        if (!IsNumeric(left.Type) || !IsNumeric(right.Type))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {ArithmeticSymbol(op)} for argument types: {left.Type}, {right.Type}"
            );

        if (left.Type == ColumnType.Integer64 && right.Type == ColumnType.Integer64)
        {
            long l = left.LongValue, r = right.LongValue;

            if (op == NodeType.ExprDiv && r == 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

            long integerResult = op switch
            {
                NodeType.ExprAdd => l + r,
                NodeType.ExprSub => l - r,
                NodeType.ExprMult => l * r,
                _ => l / r
            };

            return new ColumnValue(ColumnType.Integer64, integerResult);
        }

        // A Float64 operand forces double precision; otherwise a Float32 operand (possibly against an
        // Integer64) keeps the narrower single-precision result type.
        ColumnType resultType = left.Type == ColumnType.Float64 || right.Type == ColumnType.Float64
            ? ColumnType.Float64
            : ColumnType.Float32;

        double dl = left.Type == ColumnType.Integer64 ? left.LongValue : left.FloatValue;
        double dr = right.Type == ColumnType.Integer64 ? right.LongValue : right.FloatValue;

        if (op == NodeType.ExprDiv && dr == 0.0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

        double result = op switch
        {
            NodeType.ExprAdd => dl + dr,
            NodeType.ExprSub => dl - dr,
            NodeType.ExprMult => dl * dr,
            _ => dl / dr
        };

        return new ColumnValue(resultType, resultType == ColumnType.Float32 ? (float)result : result);
    }

    private static string ArithmeticSymbol(NodeType op) => op switch
    {
        NodeType.ExprAdd => "+",
        NodeType.ExprSub => "-",
        NodeType.ExprMult => "*",
        _ => "/"
    };

    /// <summary>
    /// Decodes a string-literal token (<see cref="NodeType.String"/> <c>yytext</c>) into its value:
    /// strips the outer quote and collapses a doubled quote of the same kind to a single one
    /// (<c>''</c> → <c>'</c> inside a single-quoted string, <c>""</c> → <c>"</c> inside a double-quoted
    /// string — the MySQL/SQL-standard escape the lexer emits as a doubled quote). Other characters,
    /// including backslashes, are preserved verbatim (there is no backslash-escape decoding). A value
    /// that needs the other quote kind carries it literally, since only the delimiter quote is doubled.
    /// </summary>
    internal static string UnquoteStringLiteral(string raw)
    {
        if (raw.Length >= 2 && raw[0] == raw[^1] && (raw[0] == '"' || raw[0] == '\''))
        {
            char quote = raw[0];
            string inner = raw[1..^1];
            return quote == '\'' ? inner.Replace("''", "'") : inner.Replace("\"\"", "\"");
        }

        return raw.Trim('"');
    }

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
                return new ColumnValue(ColumnType.String, UnquoteStringLiteral(expr.yytext!));

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
                    // IReadOnlyDictionary adapter and read the cell by ordinal without virtual dispatch.
                    // Uses GetColumnValue (per-cell), not Values, so evaluating a predicate against a
                    // slot-backed row materializes only the columns the expression references — a row
                    // rejected by a WHERE clause never materializes its projection cells.
                    // Falls through to the dictionary path for any key not found in the layout
                    // (e.g. a parameter alias or a column absent from this row's schema version).
                    if (queryRow is not null)
                    {
                        int ordinal = queryRow.Layout.IndexOf(lookupKey);
                        if (ordinal >= 0)
                            return queryRow.GetColumnValue(ordinal);
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
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprDiv:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    return EvalArithmetic(expr.nodeType, leftValue, rightValue);
                }

            case NodeType.ExprFuncCall:
                // The static lambda pins the 4-param (IReadOnlyDictionary) overload because the
                // optional queryRow param makes the method group ambiguous for delegate conversion.
                // Column refs inside function arguments therefore go through the IReadOnlyDictionary
                // adapter rather than the ordinal fast path. This is correctness-neutral (QueryRow
                // implements the interface) but leaves a small per-argument hashing cost on the
                // table. Reaching the ordinal path here would mean adding queryRow to
                // EvaluateExpressionDelegate, which changes the signature every scalar function
                // implements — a wider refactor than the saving justifies on its own.
                return ScalarFunctionEvaluator.Evaluate(expr, row, parameters, rowNameResolver,
                    static (e, r, p, rnr) => EvalExpr(e, r, p, rnr));

            case NodeType.ExprCast:
                {
                    ColumnValue input = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    return CastScalarFunctions.CastExpression("cast", input, expr.rightAst!);
                }

            case NodeType.ExprCase:
                {
                    // Simple CASE evaluates its operand once; each WHEN then tests operand = value.
                    // Searched CASE has no operand and each WHEN is a boolean condition. Either way the
                    // FIRST matching branch wins and later branches are never evaluated — a later branch
                    // may reference a column only valid under a different WHEN, so evaluating it eagerly
                    // could raise a spurious error.
                    ColumnValue? operand = expr.leftAst is null
                        ? null
                        : EvalExpr(expr.leftAst, row, parameters, rowNameResolver, queryRow);

                    foreach (NodeAst clause in EnumerateWhenClauses(expr.rightAst!))
                    {
                        bool matched;
                        if (operand is null)
                        {
                            ColumnValue cond = EvalExpr(clause.leftAst!, row, parameters, rowNameResolver, queryRow);

                            // Only TRUE matches; FALSE and NULL/UNKNOWN skip, consistent with how the
                            // WHERE evaluator treats a NULL predicate as non-matching.
                            if (cond.Type == ColumnType.Null)
                                matched = false;
                            else if (cond.Type != ColumnType.Bool)
                                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                                    $"No matching signature for CASE WHEN condition; expected Bool, got: {cond.Type}");
                            else
                                matched = cond.BoolValue;
                        }
                        else
                        {
                            ColumnValue value = EvalExpr(clause.leftAst!, row, parameters, rowNameResolver, queryRow);

                            // operand = value with normal equality; a NULL on either side is UNKNOWN → no match.
                            matched = operand.Type != ColumnType.Null
                                   && value.Type != ColumnType.Null
                                   && CompareValues(operand, value) == 0;
                        }

                        if (matched)
                            return EvalExpr(clause.rightAst!, row, parameters, rowNameResolver, queryRow);
                    }

                    // No WHEN matched: the ELSE result, or typed NULL when ELSE is omitted.
                    return expr.extendedOne is null
                        ? ColumnValue.Null
                        : EvalExpr(expr.extendedOne, row, parameters, rowNameResolver, queryRow);
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

            case NodeType.ExprRegexMatch:
            case NodeType.ExprRegexMatchCi:
            case NodeType.ExprRegexNotMatch:
            case NodeType.ExprRegexNotMatchCi:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt,
                            $"No matching signature for operator ~ for argument types: {leftValue.Type}, {rightValue.Type}");

                    bool ci = expr.nodeType is NodeType.ExprRegexMatchCi or NodeType.ExprRegexNotMatchCi;
                    bool negate = expr.nodeType is NodeType.ExprRegexNotMatch or NodeType.ExprRegexNotMatchCi;
                    bool matched = Functions.RegexMatcher.IsMatch(leftValue.StrValue!, rightValue.StrValue!, ci);
                    return ColumnValue.FromBool(negate ? !matched : matched);
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

    /// <summary>
    /// Flattens the left-recursive <see cref="NodeType.ExprCaseWhenList"/> chain that a CASE's WHEN
    /// clauses parse into, yielding each <see cref="NodeType.ExprCaseWhen"/> in source order (top to
    /// bottom). The list is built left-associatively — <c>((c1 c2) c3)</c> — so the leftmost clause is
    /// the deepest node; walking the left spine first restores first-match ordering. A single-clause
    /// CASE has no wrapper node (the grammar collapses it to the bare <see cref="NodeType.ExprCaseWhen"/>).
    /// This is the one place WHEN ordering is defined; every site that visits CASE branches reuses it.
    /// </summary>
    internal static IEnumerable<NodeAst> EnumerateWhenClauses(NodeAst whenList)
    {
        if (whenList.nodeType == NodeType.ExprCaseWhenList)
        {
            foreach (NodeAst clause in EnumerateWhenClauses(whenList.leftAst!))
                yield return clause;

            yield return whenList.rightAst!;
        }
        else
        {
            yield return whenList;
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

        // Fallback: regex for multi-wildcard patterns (e.g. 'a%b%c'). Route through RegexMatcher
        // so this path inherits the bounded compiled-pattern cache and the ReDoS match-timeout.
        // The glob is escaped first and only '%' becomes '.*', so LIKE semantics are unchanged
        // (still ordinal, case-sensitive).
        string regexPattern = string.Concat("^", Regex.Escape(pattern).Replace("%", ".*"), "$");
        return Functions.RegexMatcher.IsMatch(text, regexPattern, ignoreCase: false);
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
