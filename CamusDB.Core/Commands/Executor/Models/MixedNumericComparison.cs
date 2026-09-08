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
/// </summary>
public static class MixedNumericComparison
{
    public static bool IsNumeric(ColumnType type) =>
        type is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32;

    /// <summary>Widens a numeric value to double. The caller must check <see cref="IsNumeric"/> first.</summary>
    public static double ToDouble(ColumnValue value) => value.Type switch
    {
        ColumnType.Integer64 => value.LongValue,
        ColumnType.Float32 => (double)(float)value.FloatValue,
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

        result = ToDouble(left).CompareTo(ToDouble(right));
        return true;
    }

    /// <summary>
    /// SQL equality for <c>x IN (...)</c> membership: NULL equals nothing, a mixed numeric pair is
    /// equal when the widened values are equal, a same-type pair uses <see cref="ColumnValue.CompareTo"/>,
    /// and any other cross-type pair (<c>5 = 'foo'</c>) is a non-match rather than an error.
    /// </summary>
    public static bool EqualsForMembership(ColumnValue left, ColumnValue right)
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
