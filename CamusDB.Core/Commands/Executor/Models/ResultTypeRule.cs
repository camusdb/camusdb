/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The static type of an expression: a column type, plus the element type of an array.
/// <see cref="ElementType"/> is <see cref="ColumnType.Null"/> for a value that is not an array, and
/// for an array whose element type is not known (<c>ARRAY[]</c>, or only NULL elements).
/// </summary>
public readonly record struct ExpressionType(ColumnType Type, ColumnType ElementType = ColumnType.Null)
{
    /// <summary>
    /// True for a quoted string literal written in the statement (<c>'2026-01-01'</c>), and for a
    /// string value whose type only the value shows (a parameter). Such a string combines with a DATE
    /// or DATETIME input and is converted to it; a typed string column does not (see
    /// <see cref="ResultTypeRule"/>).
    /// </summary>
    public bool IsStringLiteral { get; init; }

    /// <summary>The type of a quoted string literal.</summary>
    public static readonly ExpressionType StringLiteral = new(ColumnType.String) { IsStringLiteral = true };

    /// <summary>The type of a value that is NULL: it combines with every type.</summary>
    public static readonly ExpressionType Null = new(ColumnType.Null);

    /// <summary>The type of <paramref name="value"/>, with the element type of an array.</summary>
    public static ExpressionType Of(ColumnValue value) =>
        value.Type == ColumnType.Array ? new(ColumnType.Array, value.ArrayElementType) : new(value.Type);

    /// <summary>The type name the errors use: <c>Integer64</c>, or <c>ARRAY&lt;Integer64&gt;</c>.</summary>
    public override string ToString() =>
        Type == ColumnType.Array ? $"ARRAY<{ElementType}>" : Type.ToString();
}

/// <summary>
/// The one rule for the result type of an expression that returns one of several inputs: a CASE
/// (its THEN and ELSE results), COALESCE, IFNULL and IF (their result arguments), and each output
/// column of UNION, INTERSECT and EXCEPT. It follows GoogleSQL, as <see cref="ComparisonTypeRule"/>
/// does for comparisons: the inputs must have a common supertype, and a pair that has none is an
/// <see cref="CamusDBErrorCodes.InvalidInput"/> error. Before this rule a CASE returned the value of
/// the branch that ran, with that branch's type, so one column could hold an INT64 in one row and a
/// STRING in the next.
///
/// <para>Two types combine, in this order:</para>
/// <list type="number">
/// <item>A NULL input takes the type of the other input.</item>
/// <item>One type gives that type. Two arrays also need element types that agree
///   (<see cref="ComparisonTypeRule.ArrayElementTypesAgree"/>); an unknown element type takes the
///   other one.</item>
/// <item>Two numeric types give <see cref="MixedNumericComparison.ArithmeticResultType"/>, the rule
///   the arithmetic operators use: INT64 with FLOAT64 is FLOAT64, INT64 with NUMERIC is NUMERIC.</item>
/// <item>A date with a datetime gives datetime (<see cref="TemporalComparison.WidenDate"/>).</item>
/// <item>A quoted string literal with a date or a datetime gives that temporal type, and the literal
///   is parsed (<see cref="ExpressionType.IsStringLiteral"/>): <c>CASE WHEN c THEN date_col ELSE
///   '2026-01-01' END</c> is a DATE. GoogleSQL coerces a literal the same way. A typed STRING column
///   does not combine with a date. A string value whose type only its value shows (a parameter) is
///   treated as a literal when the value is returned.</item>
/// <item>Every other pair has no common type. A quoted literal is otherwise a STRING, so
///   <c>CASE WHEN c THEN 1 ELSE '2' END</c> has none; PostgreSQL would read <c>'2'</c> as an
///   integer, GoogleSQL does not.</item>
/// </list>
///
/// <para>The value an expression returns is then converted to the result type with
/// <see cref="Widen"/>, so every row carries one type. See <c>docs/data-types.md</c>, "Result types
/// of CASE, COALESCE, IFNULL and IF".</para>
/// </summary>
public static class ResultTypeRule
{
    /// <summary>
    /// Combines two scalar types by the rules in the class summary, without the array element check.
    /// Returns null when the pair has no common type. A <see cref="ColumnType.Null"/> side gives the
    /// other side.
    /// </summary>
    public static ColumnType? Combine(ColumnType left, ColumnType right)
    {
        if (left == ColumnType.Null)
            return right;

        if (right == ColumnType.Null || left == right)
            return left;

        if (MixedNumericComparison.ArithmeticResultType(left, right) is { } numeric)
            return numeric;

        if (TemporalComparison.IsTemporal(left) && TemporalComparison.IsTemporal(right))
            return ColumnType.DateTime;

        return null;
    }

    /// <summary>
    /// Combines two expression types by the rules in the class summary, including the element type
    /// check for two arrays. Returns false when the pair has no common type.
    /// </summary>
    public static bool TryCombine(ExpressionType left, ExpressionType right, out ExpressionType result)
    {
        if (left.Type == ColumnType.Null)
        {
            result = right;
            return true;
        }

        if (right.Type == ColumnType.Null)
        {
            result = left;
            return true;
        }

        // A string literal takes the type of a date or datetime input; the caller parses it.
        if (left.IsStringLiteral && TemporalComparison.IsTemporal(right.Type))
        {
            result = new ExpressionType(right.Type);
            return true;
        }

        if (right.IsStringLiteral && TemporalComparison.IsTemporal(left.Type))
        {
            result = new ExpressionType(left.Type);
            return true;
        }

        // Two literals stay a literal, so a third input that is a date can still take them.
        if (left.IsStringLiteral && right.IsStringLiteral)
        {
            result = left;
            return true;
        }

        if (left.Type == ColumnType.Array && right.Type == ColumnType.Array)
        {
            if (!ComparisonTypeRule.ArrayElementTypesAgree(left.ElementType, right.ElementType))
            {
                result = default;
                return false;
            }

            result = left.ElementType == ColumnType.Null ? right : left;
            return true;
        }

        if (Combine(left.Type, right.Type) is { } combined)
        {
            result = new ExpressionType(combined);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// The error for two inputs of <paramref name="construct"/> (<c>CASE</c>, <c>COALESCE</c>,
    /// <c>IFNULL</c>, <c>IF</c>) that have no common type.
    /// </summary>
    public static CamusDBException NoCommonType(string construct, ExpressionType left, ExpressionType right) =>
        new(CamusDBErrorCodes.InvalidInput,
            $"No common type for the results of {construct}: {left} and {right}; write a CAST to make the types agree");

    /// <summary>
    /// Returns <paramref name="value"/> converted to <paramref name="resultType"/>: a number widens
    /// (<see cref="NumericWidening.Widen"/>), a date becomes the datetime at its midnight, and a string
    /// becomes the date or datetime it spells (<see cref="ParseTemporal"/>, which throws for text that
    /// does not parse). Any other value, and a value that already has the type, is returned unchanged.
    /// The caller has already combined the value's type into <paramref name="resultType"/>, so no
    /// conversion narrows.
    /// </summary>
    public static ColumnValue Widen(ColumnValue value, ColumnType resultType)
    {
        if (value.Type == resultType || value.Type == ColumnType.Null)
            return value;

        if (value.Type == ColumnType.String && TemporalComparison.IsTemporal(resultType))
            return ParseTemporal(value.StrValue ?? "", resultType);

        if (resultType == ColumnType.DateTime)
            return TemporalComparison.WidenDate(value);

        return NumericWidening.Widen(value, resultType);
    }

    /// <summary>
    /// Parses <paramref name="text"/> as a value of <paramref name="target"/> (a date or a datetime),
    /// with the parse CAST and comparisons use. Text that does not parse is
    /// <see cref="CamusDBErrorCodes.InvalidInput"/>.
    /// </summary>
    public static ColumnValue ParseTemporal(string text, ColumnType target)
    {
        if (!Controllers.Functions.CastScalarFunctions.TryParseTemporalText(text, target, out ColumnValue parsed))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Cannot convert '{text}' to a {(target == ColumnType.Date ? "date" : "datetime")}: " +
                $"expected {Controllers.Functions.CastScalarFunctions.TemporalTextForm(target)}");

        return parsed;
    }
}
