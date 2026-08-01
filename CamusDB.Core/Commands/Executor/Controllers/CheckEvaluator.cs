
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Evaluates a CHECK constraint condition using SQL three-valued logic (TRUE, FALSE, UNKNOWN).
/// This is intentionally separate from the WHERE-path evaluator (<c>SQLExecutorBaseCreator.EvalExpr</c>)
/// which collapses NULL to false — the correct SQL CHECK semantics are: a constraint is violated
/// <em>only when the condition evaluates to FALSE</em>; NULL/UNKNOWN passes.
/// <para>
/// Leaf value evaluation (identifiers, literals, arithmetic, function calls, CAST) is delegated to
/// <c>EvalExpr</c> so coercion behaviour (String→Date, String→Uuid, String→Id, etc.) is identical
/// to the WHERE evaluator. Only the boolean-level combinators and comparison operators apply the
/// three-valued NULL rule.
/// </para>
/// </summary>
internal static class CheckEvaluator
{
    /// <summary>
    /// Evaluates <paramref name="condition"/> against <paramref name="row"/> using three-valued logic.
    /// </summary>
    /// <returns>
    /// <c>false</c> if the check is violated (the row should be rejected);
    /// <c>true</c> if the check is satisfied; <c>null</c> if the result is UNKNOWN (passes — NULL
    /// in a referenced column makes the whole comparison unknown, and UNKNOWN passes per SQL).
    /// </returns>
    public static bool? Evaluate(NodeAst condition, IReadOnlyDictionary<string, ColumnValue> row)
    {
        switch (condition.nodeType)
        {
            // ── comparisons: unknown when either operand is NULL ─────────────────────
            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
            {
                ColumnValue left = EvalLeaf(condition.leftAst!, row);
                ColumnValue right = EvalLeaf(condition.rightAst!, row);

                if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
                    return null; // unknown → passes

                int cmp = CompareValues(left, right);
                return condition.nodeType switch
                {
                    NodeType.ExprEquals => cmp == 0,
                    NodeType.ExprNotEquals => cmp != 0,
                    NodeType.ExprLessThan => cmp < 0,
                    NodeType.ExprGreaterThan => cmp > 0,
                    NodeType.ExprLessEqualsThan => cmp <= 0,
                    NodeType.ExprGreaterEqualsThan => cmp >= 0,
                    _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "unreachable")
                };
            }

            // ── AND: false if either branch is false; unknown if either is unknown; else true ──
            case NodeType.ExprAnd:
            {
                bool? left = Evaluate(condition.leftAst!, row);
                if (left == false) return false;
                bool? right = Evaluate(condition.rightAst!, row);
                if (right == false) return false;
                if (left == null || right == null) return null;
                return true;
            }

            // ── OR: true if either branch is true; unknown if either is unknown; else false ──
            case NodeType.ExprOr:
            {
                bool? left = Evaluate(condition.leftAst!, row);
                if (left == true) return true;
                bool? right = Evaluate(condition.rightAst!, row);
                if (right == true) return true;
                if (left == null || right == null) return null;
                return false;
            }

            // ── NOT: NOT(unknown) = unknown ──────────────────────────────────────────
            case NodeType.ExprNot:
            {
                bool? inner = Evaluate(condition.leftAst!, row);
                return inner.HasValue ? !inner.Value : (bool?)null;
            }

            // ── IS NULL / IS NOT NULL: always definite, never unknown ────────────────
            case NodeType.ExprIsNull:
            {
                ColumnValue val = EvalLeaf(condition.leftAst!, row);
                return val.Type == ColumnType.Null;
            }

            case NodeType.ExprIsNotNull:
            {
                ColumnValue val = EvalLeaf(condition.leftAst!, row);
                return val.Type != ColumnType.Null;
            }

            // ── IS [NOT] TRUE/FALSE: truth tests, also always definite ───────────────
            case NodeType.ExprIsTrue:
            case NodeType.ExprIsNotTrue:
            case NodeType.ExprIsFalse:
            case NodeType.ExprIsNotFalse:
            {
                ColumnValue val = EvalLeaf(condition.leftAst!, row);
                return BooleanTruthTest.Evaluate(condition.nodeType, val);
            }

            // ── BETWEEN: unknown if subject or either bound is NULL ──────────────────
            case NodeType.ExprBetween:
            {
                ColumnValue subject = EvalLeaf(condition.leftAst!, row);
                ColumnValue low = EvalLeaf(condition.extendedOne!, row);
                ColumnValue high = EvalLeaf(condition.extendedTwo!, row);

                if (subject.Type == ColumnType.Null || low.Type == ColumnType.Null || high.Type == ColumnType.Null)
                    return null;

                return CompareValues(subject, low) >= 0 && CompareValues(subject, high) <= 0;
            }

            // ── LIKE / ILIKE: unknown if either operand is NULL ──────────────────────
            case NodeType.ExprLike:
            case NodeType.ExprILike:
            {
                ColumnValue leftVal = EvalLeaf(condition.leftAst!, row);
                ColumnValue rightVal = EvalLeaf(condition.rightAst!, row);

                if (leftVal.Type == ColumnType.Null || rightVal.Type == ColumnType.Null)
                    return null;

                // Delegate the actual pattern match to EvalExpr (reuses the Like/ILike implementation).
                ColumnValue result = SQLExecutorBaseCreator.EvalExpr(condition, row, null);
                return result.Type == ColumnType.Bool ? result.BoolValue : (bool?)null;
            }

            // ── regex match / not-match: unknown if either operand is NULL ───────────
            case NodeType.ExprRegexMatch:
            case NodeType.ExprRegexMatchCi:
            case NodeType.ExprRegexNotMatch:
            case NodeType.ExprRegexNotMatchCi:
            {
                ColumnValue leftVal = EvalLeaf(condition.leftAst!, row);
                ColumnValue rightVal = EvalLeaf(condition.rightAst!, row);

                if (leftVal.Type == ColumnType.Null || rightVal.Type == ColumnType.Null)
                    return null;

                try
                {
                    ColumnValue result = SQLExecutorBaseCreator.EvalExpr(condition, row, null);
                    return result.Type == ColumnType.Bool ? result.BoolValue : (bool?)null;
                }
                catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.InvalidInput)
                {
                    // A malformed pattern or a match timeout inside a CHECK is a client mistake
                    // surfaced at write time. Report it as a check-constraint rejection (HTTP 400,
                    // the row is rejected) rather than letting the regex helper's InvalidInput
                    // (HTTP 500) escape — mirroring how CompareValues translates incompatible types.
                    throw new CamusDBException(CamusDBErrorCodes.CheckConstraintViolation, ex.Message);
                }
            }

            // ── IN membership: unknown if subject is NULL ────────────────────────────
            case NodeType.ExprInMembership:
            {
                ColumnValue subject = EvalLeaf(condition.leftAst!, row);
                if (subject.Type == ColumnType.Null) return null;

                ColumnValue result = SQLExecutorBaseCreator.EvalExpr(condition, row, null);
                return result.Type == ColumnType.Bool ? result.BoolValue : (bool?)null;
            }

            case NodeType.ExprNotInMembership:
            {
                ColumnValue subject = EvalLeaf(condition.leftAst!, row);
                if (subject.Type == ColumnType.Null) return null;

                ColumnValue result = SQLExecutorBaseCreator.EvalExpr(condition, row, null);
                return result.Type == ColumnType.Bool ? result.BoolValue : (bool?)null;
            }

            // ── fallback: delegate to EvalExpr for non-boolean leaves ────────────────
            default:
            {
                ColumnValue val = EvalLeaf(condition, row);
                if (val.Type == ColumnType.Bool) return val.BoolValue;
                return null;
            }
        }
    }

    /// <summary>
    /// Evaluates a non-boolean expression subtree (leaf value: identifier, literal, arithmetic,
    /// function call, CAST) using <c>EvalExpr</c>. For <c>Identifier</c> nodes specifically,
    /// a column not present in <paramref name="row"/> is treated as <c>NULL</c> rather than an
    /// error — a column omitted from an INSERT has no explicit value, which SQL treats as NULL,
    /// and a NULL operand propagates unknown through the CHECK.
    /// </summary>
    private static ColumnValue EvalLeaf(NodeAst expr, IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (expr.nodeType == NodeType.Identifier)
        {
            if (row.TryGetValue(expr.yytext!, out ColumnValue? val))
                return val;
            return ColumnValue.Null;
        }

        return SQLExecutorBaseCreator.EvalExpr(expr, row, null);
    }

    /// <summary>
    /// Compares two column values, first coercing a bare string literal to the other operand's
    /// type for the types where a string is a valid literal form (Uuid, Id, Date, DateTime) —
    /// so <c>CHECK (birth_date &gt; '1900-01-01')</c> and <c>CHECK (id = '…')</c> work without an
    /// explicit CAST, matching the WHERE evaluator's intent. A malformed literal orders
    /// deterministically (it can equal no real value). A genuinely incomparable pair with no
    /// string-literal coercion (e.g. a String column vs a numeric literal) surfaces a domain
    /// <see cref="CamusDBException"/> rather than letting <see cref="ColumnValue.CompareTo"/>
    /// escape as a raw <see cref="ArgumentException"/>.
    /// </summary>
    private static int CompareValues(ColumnValue left, ColumnValue right)
    {
        // Mixed integer/float operands (e.g. a Float64 column vs an integer literal in `price > 0`)
        // compare numerically by widening both to double.
        if (TryCompareNumeric(left, right, out int numericCmp))
            return numericCmp;

        try
        {
            if (left.Type == ColumnType.String && right.Type != ColumnType.String)
                left = CoerceStringOperand(left, right.Type);
            else if (right.Type == ColumnType.String && left.Type != ColumnType.String)
                right = CoerceStringOperand(right, left.Type);
        }
        catch (CamusDBException)
        {
            // Malformed literal for the target type: unequal to any real value.
            return left.Type == ColumnType.String ? -1 : 1;
        }

        try
        {
            return left.CompareTo(right);
        }
        catch (ArgumentException)
        {
            // The condition cannot be evaluated for this row's types (e.g. a string column vs a
            // numeric literal). This is a caller mistake surfaced at write time, so report it as a
            // check-constraint rejection (HTTP 400) rather than letting ColumnValue.CompareTo escape
            // as a raw ArgumentException (which the API would surface as a 500).
            throw new CamusDBException(
                CamusDBErrorCodes.CheckConstraintViolation,
                $"CHECK constraint compares incompatible types ({left.Type} and {right.Type})");
        }
    }

    /// <summary>
    /// Coerces a string literal to <paramref name="target"/> for the types that accept a string
    /// literal form. For any other target the string is returned unchanged, so the subsequent
    /// comparison surfaces the incompatible-type error instead of silently mis-comparing.
    /// </summary>
    private static ColumnValue CoerceStringOperand(ColumnValue str, ColumnType target) => target switch
    {
        ColumnType.Uuid => ColumnValue.FromUuidString(str.StrValue!),
        ColumnType.Id or ColumnType.Date or ColumnType.DateTime => CastScalarFunctions.CoerceToColumnType(str, target),
        _ => str,
    };

    /// <summary>
    /// Compares two operands numerically when they are a mix of integer and floating-point types
    /// (which <see cref="ColumnValue.CompareTo"/> rejects as incompatible), widening both to double —
    /// so an integer literal in a check like <c>price &gt; 0</c> compares against a Float64 column.
    /// Returns false (leaving the caller's other coercion/compare logic in charge) unless both
    /// operands are numeric and at least one is floating point.
    /// </summary>
    private static bool TryCompareNumeric(ColumnValue left, ColumnValue right, out int result)
    {
        result = 0;
        if (!IsNumeric(left.Type) || !IsNumeric(right.Type))
            return false;
        if (left.Type == right.Type)
            return false; // same type — let CompareTo handle it (exact integer/float semantics)

        double l = left.Type == ColumnType.Integer64 ? left.LongValue : left.FloatValue;
        double r = right.Type == ColumnType.Integer64 ? right.LongValue : right.FloatValue;
        result = l.CompareTo(r);
        return true;
    }

    private static bool IsNumeric(ColumnType type) =>
        type is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32;
}
