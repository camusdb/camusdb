/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The one answer to "can a value of type A be compared with a value of type B at all". It follows
/// GoogleSQL: a pair with no comparison rule is an error for every comparison, equality included.
/// <c>5 = '5'</c>, <c>bool_col = 1</c> and <c>text_col = int_col</c> raise
/// <see cref="CamusDBErrorCodes.InvalidInput"/> ("No matching signature for operator …"); the user
/// writes a CAST. Before this rule an equality on such a pair was silently FALSE, so a
/// <c>DELETE … WHERE text_col = int_col</c> reported success and deleted nothing.
///
/// <para>A pair has a rule when:</para>
/// <list type="bullet">
/// <item>either side is NULL (the comparison is UNKNOWN, decided by the caller before any type check);</item>
/// <item>both sides have one type;</item>
/// <item>both sides are numeric (<see cref="MixedNumericComparison"/>);</item>
/// <item>the pair is a date with a datetime, or a string with either (<see cref="TemporalComparison"/>,
///   where a string that does not parse is an error);</item>
/// <item>the pair is a string with a Uuid, Id or Jsonb (<see cref="StringOperandCoercion"/>, where a
///   string that does not parse is a non-match, not an error).</item>
/// </list>
///
/// <para>Two arrays have a rule only when their element types are the same, or when one element type
/// is still unknown (<see cref="ColumnType.Null"/>: <c>ARRAY[]</c> or <c>ARRAY[NULL]</c>). The array
/// type alone does not decide it, so a check that knows only <see cref="ColumnType.Array"/> on both
/// sides must use the <see cref="ColumnValue"/> overloads. The element rule is stricter than the
/// scalar rule on purpose: <see cref="ColumnValue.CompareTo"/> orders the elements of two arrays with
/// one same-type comparison, so <c>ARRAY[1] = ARRAY[1.0]</c> has no rule and needs a CAST.</para>
///
/// <para>Every surface that decides equality must apply this rule, or an index seek and a table scan of
/// the same predicate disagree: the WHERE evaluator (<c>=</c>, <c>&lt;&gt;</c> and the ordering
/// operators), the simple CASE, NULLIF, <c>= ANY</c>, <c>IN</c> / <c>NOT IN</c> on the AST path and in
/// the prepared hash set, array membership, CHECK constraints, and the planner, which must leave a
/// constant with no rule to the evaluator instead of building an index key from it. See
/// <c>docs/data-types.md</c>, "Comparing values of different types".</para>
///
/// <para>The check runs when values are evaluated, not when the statement is bound, so a predicate
/// over an empty table, or one whose operand is NULL, raises no error.</para>
/// </summary>
public static class ComparisonTypeRule
{
    /// <summary>True when a value of <paramref name="left"/> can be compared with a value of
    /// <paramref name="right"/>; see the class summary for the list.</summary>
    public static bool HasRule(ColumnType left, ColumnType right)
    {
        if (left == right || left == ColumnType.Null || right == ColumnType.Null)
            return true;

        if (MixedNumericComparison.IsNumeric(left) && MixedNumericComparison.IsNumeric(right))
            return true;

        if (TemporalComparison.IsTemporal(left))
            return TemporalComparison.IsTemporal(right) || right == ColumnType.String;

        if (TemporalComparison.IsTemporal(right))
            return left == ColumnType.String;

        return (left == ColumnType.String && StringOperandCoercion.IsTarget(right))
            || (right == ColumnType.String && StringOperandCoercion.IsTarget(left));
    }

    /// <summary>
    /// True when two values can be compared. It is <see cref="HasRule(ColumnType, ColumnType)"/>,
    /// plus the element type check for two arrays (see the class summary).
    /// </summary>
    public static bool HasRule(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Array && right.Type == ColumnType.Array)
            return ArrayElementTypesAgree(left.ArrayElementType, right.ArrayElementType);

        return HasRule(left.Type, right.Type);
    }

    /// <summary>
    /// True when arrays of these two element types can be compared: the types are the same, or one of
    /// them is still unknown (<see cref="ColumnType.Null"/>), which an array holds only when every
    /// element is NULL.
    /// </summary>
    public static bool ArrayElementTypesAgree(ColumnType left, ColumnType right) =>
        left == right || left == ColumnType.Null || right == ColumnType.Null;

    /// <summary>
    /// Throws the error for a pair with no rule, naming <paramref name="op"/> (<c>=</c>, <c>IN</c>,
    /// <c>CASE</c>, a function name) and both types. Does nothing for a pair that has one.
    /// </summary>
    public static void Require(string op, ColumnType left, ColumnType right)
    {
        if (!HasRule(left, right))
            throw NoRule(op, left, right);
    }

    /// <summary>
    /// <see cref="Require(string, ColumnType, ColumnType)"/> for two values, so that two arrays with
    /// different element types are refused too.
    /// </summary>
    public static void Require(string op, ColumnValue left, ColumnValue right)
    {
        if (!HasRule(left, right))
            throw NoRule(op, left, right);
    }

    /// <summary>The error for a pair with no rule. The text matches the other operator signature errors.</summary>
    public static CamusDBException NoRule(string op, ColumnType left, ColumnType right) =>
        new(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator {op} for argument types: {left}, {right}");

    /// <summary>
    /// <see cref="NoRule(string, ColumnType, ColumnType)"/> for two values. An array names its element
    /// type (<c>ARRAY&lt;Integer64&gt;</c>), because <c>Array, Array</c> does not say what is wrong.
    /// </summary>
    public static CamusDBException NoRule(string op, ColumnValue left, ColumnValue right) =>
        new(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator {op} for argument types: {TypeName(left)}, {TypeName(right)}");

    private static string TypeName(ColumnValue value) =>
        value.Type == ColumnType.Array ? $"ARRAY<{value.ArrayElementType}>" : value.Type.ToString();
}
