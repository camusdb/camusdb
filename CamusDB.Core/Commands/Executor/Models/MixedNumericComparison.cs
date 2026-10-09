/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The one rule for comparing two numeric values of different types (Integer64, Float64,
/// Float32): widen both to <c>double</c> and compare. <see cref="ColumnValue.CompareTo"/> refuses
/// a cross-type pair, so every SQL surface that must accept <c>price &gt; 0</c> on a FLOAT column or
/// <c>a IN (1.0)</c> on an INT column goes through here — the WHERE evaluator, CHECK constraints,
/// <c>IN</c> / <c>NOT IN</c> membership (AST path and prepared hash set), and the planner's
/// numeric bound normalization. Keeping one implementation is what lets an index path and a table
/// scan agree; a second copy of the rule is how they drift apart.
///
/// <para>A Float32 operand is widened at single precision (<c>(double)(float)value</c>), the same
/// precision <see cref="ColumnValue.CompareTo"/> uses for two Float32 values, so a hash computed
/// from the widened value is consistent with equality.</para>
///
/// <para><b>NUMERIC</b> follows Spanner. Against Integer64 the comparison is exact: the integer is
/// scaled into the NUMERIC domain, so <c>9007199254740993</c> is not equal to its neighbour, as it
/// would be after widening to double. Against Float64 or Float32 the NUMERIC value widens to double
/// (<see cref="NumericMath.ToDouble"/>) and the pair compares as doubles. Hashing every numeric
/// value by its widened double stays consistent with both rules: an exact NUMERIC/Integer64 match
/// widens to the same double, so equal values always share a hash.</para>
/// </summary>
public static class MixedNumericComparison
{
    public static bool IsNumeric(ColumnType type) =>
        type is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32 or ColumnType.Numeric;

    /// <summary>
    /// The result type of <c>+ - * /</c> on two operand types, or null when either is not numeric.
    /// The one rule for both evaluation (<c>SQLExecutorBaseCreator.EvalArithmetic</c>) and static
    /// typing (<c>DerivedTableSchemaBuilder</c>), so a CTAS column or client metadata always has the
    /// type the value will have:
    /// <list type="bullet">
    ///   <item>Integer64 with Integer64 stays Integer64.</item>
    ///   <item>Any Float64 operand gives Float64.</item>
    ///   <item>NUMERIC with Integer64 or NUMERIC gives NUMERIC; NUMERIC with Float32 gives Float64
    ///   (Spanner: the supertype of NUMERIC and a float is FLOAT64).</item>
    ///   <item>Float32 with Integer64 or Float32 gives Float32.</item>
    /// </list>
    /// </summary>
    public static ColumnType? ArithmeticResultType(ColumnType left, ColumnType right)
    {
        if (!IsNumeric(left) || !IsNumeric(right))
            return null;

        if (left == ColumnType.Float64 || right == ColumnType.Float64)
            return ColumnType.Float64;

        if (left == ColumnType.Numeric || right == ColumnType.Numeric)
            return left == ColumnType.Float32 || right == ColumnType.Float32 ? ColumnType.Float64 : ColumnType.Numeric;

        if (left == ColumnType.Float32 || right == ColumnType.Float32)
            return ColumnType.Float32;

        return ColumnType.Integer64;
    }

    /// <summary>
    /// The unscaled NUMERIC form of an Integer64 or NUMERIC value. The caller must have checked the
    /// type; used where an operation promotes an integer operand into the NUMERIC domain.
    /// </summary>
    public static Int128 ToNumericUnscaled(ColumnValue value) =>
        value.Type == ColumnType.Integer64 ? NumericMath.FromInt64(value.LongValue) : value.NumericUnscaled;

    /// <summary>Widens a numeric value to double. The caller must check <see cref="IsNumeric"/> first.</summary>
    public static double ToDouble(ColumnValue value) => value.Type switch
    {
        ColumnType.Integer64 => value.LongValue,
        ColumnType.Float32 => (double)(float)value.FloatValue,
        ColumnType.Numeric => NumericMath.ToDouble(value.NumericUnscaled),
        _ => value.FloatValue,
    };

    /// <summary>
    /// Compares a mixed-type numeric pair. Returns false, leaving <paramref name="result"/> at 0,
    /// when the two values share a type or either is not numeric — the caller then applies its own
    /// same-type or non-numeric rule (usually <see cref="ColumnValue.CompareTo"/>).
    /// </summary>
    public static bool TryCompare(ColumnValue left, ColumnValue right, out int result)
    {
        result = 0;

        if (left.Type == right.Type || !IsNumeric(left.Type) || !IsNumeric(right.Type))
            return false;

        // NUMERIC against an integer is exact: every long fits in the NUMERIC range.
        if (left.Type == ColumnType.Numeric && right.Type == ColumnType.Integer64)
        {
            result = left.NumericUnscaled.CompareTo(NumericMath.FromInt64(right.LongValue));
            return true;
        }

        if (left.Type == ColumnType.Integer64 && right.Type == ColumnType.Numeric)
        {
            result = NumericMath.FromInt64(left.LongValue).CompareTo(right.NumericUnscaled);
            return true;
        }

        result = ToDouble(left).CompareTo(ToDouble(right));
        return true;
    }

    /// <summary>
    /// SQL equality for <c>x IN (...)</c> membership: NULL equals nothing, a mixed numeric pair is
    /// equal when the widened values are equal, a String against a Uuid or Id is equal when the
    /// string parses to that value (<see cref="StringOperandCoercion"/>, the rule <c>=</c> uses), a
    /// same-type pair uses <see cref="ColumnValue.CompareTo"/>, and any other cross-type pair
    /// (<c>5 = 'foo'</c>) is a non-match rather than an error.
    ///
    /// <para><c>x IN (a, b)</c> is defined as <c>x = a OR x = b</c>, so this must agree with the WHERE
    /// comparison evaluator for every pair. The index IN-list seek converts its items the same way,
    /// and a table scan that disagrees with it returns different rows for the same predicate.</para>
    /// </summary>
    public static bool EqualsForMembership(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return false;

        if (left.Type != right.Type
            && StringOperandCoercion.TryAlign(ref left, ref right) == StringOperandCoercion.Alignment.Malformed)
            return false;

        return EqualsWithoutStringCoercion(left, right);
    }

    /// <summary>
    /// <see cref="EqualsForMembership"/> without the String-to-Uuid/Id step: a String never equals a
    /// Uuid or Id here. A hash set of list items needs this narrower equality, because a String and
    /// the Uuid it spells hash differently; the caller converts the probe or the items before a
    /// lookup instead (see <c>PreparedInSet</c>).
    /// </summary>
    public static bool EqualsWithoutStringCoercion(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return false;

        if (TryCompare(left, right, out int cmp))
            return cmp == 0;

        if (left.Type != right.Type)
            return false;

        try
        {
            return left.CompareTo(right) == 0;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }
}
