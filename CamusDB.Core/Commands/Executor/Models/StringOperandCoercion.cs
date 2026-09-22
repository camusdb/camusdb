/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The one rule for comparing a <see cref="ColumnType.String"/> value with a
/// <see cref="ColumnType.Uuid"/> or <see cref="ColumnType.Id"/> value: the string is parsed into the
/// other operand's type, then the two compare as that type. So <c>uuid_col = '0190…'</c> works without
/// a CAST, a parameter bound as a string matches a stored Uuid, and an Id string matches in any casing.
///
/// <para>Every SQL surface that decides equality must use this rule: the WHERE comparison
/// evaluator, <c>IN</c> / <c>NOT IN</c> membership on both the AST path and the prepared hash set,
/// and the planner, which converts the same constants before it builds an index key. If one surface
/// skips it, an index seek and a table scan of the same predicate return different rows. The planner
/// switches between them on estimated selectivity, so that defect shows only past a row-count
/// threshold, and it shows as a silently empty result.</para>
///
/// <para>A string that is not a valid value of the target type can equal no value of that type. It
/// is never an error: callers treat it as a non-match.</para>
/// </summary>
public static class StringOperandCoercion
{
    /// <summary>True for the types that a string operand is parsed into before a comparison.</summary>
    public static bool IsTarget(ColumnType type) => type is ColumnType.Uuid or ColumnType.Id;

    /// <summary>
    /// Parses the string <paramref name="value"/> into <paramref name="target"/> (Uuid or Id).
    /// Returns false when the string is not a valid value of that type. A table scan can call this
    /// once per row, so a string that does not parse is rejected without an exception.
    /// </summary>
    public static bool TryCoerce(ColumnValue value, ColumnType target, out ColumnValue coerced)
    {
        coerced = value;

        if (value.Type != ColumnType.String || value.StrValue is not { } text)
            return false;

        if (target == ColumnType.Uuid)
        {
            if (!Guid.TryParse(text, out Guid parsed))
                return false;

            coerced = ColumnValue.FromUuid(parsed);
            return true;
        }

        if (target != ColumnType.Id || !CastScalarFunctions.IsValidLowerHexObjectId(text))
            return false;

        try
        {
            coerced = CastScalarFunctions.CoerceToColumnType(value, ColumnType.Id);
            return true;
        }
        catch (CamusDBException)
        {
            coerced = value;
            return false;
        }
    }

    /// <summary>The result of <see cref="TryAlign"/>.</summary>
    public enum Alignment
    {
        /// <summary>The pair is not a String against a Uuid or Id. Both values are unchanged.</summary>
        NotApplicable,

        /// <summary>The string operand was parsed into the other operand's type.</summary>
        Aligned,

        /// <summary>The string operand is not a valid value of the other operand's type, so the two
        /// values can never be equal. Both values are unchanged.</summary>
        Malformed,
    }

    /// <summary>
    /// When one operand is a String and the other is a Uuid or Id, parses the string into that type
    /// in place. See <see cref="Alignment"/> for the outcomes.
    /// </summary>
    public static Alignment TryAlign(ref ColumnValue left, ref ColumnValue right)
    {
        if (left.Type == ColumnType.String && IsTarget(right.Type))
        {
            if (!TryCoerce(left, right.Type, out ColumnValue coerced))
                return Alignment.Malformed;

            left = coerced;
            return Alignment.Aligned;
        }

        if (right.Type == ColumnType.String && IsTarget(left.Type))
        {
            if (!TryCoerce(right, left.Type, out ColumnValue coerced))
                return Alignment.Malformed;

            right = coerced;
            return Alignment.Aligned;
        }

        return Alignment.NotApplicable;
    }
}
