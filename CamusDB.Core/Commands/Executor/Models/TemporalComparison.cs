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
/// The one rule for comparing a <see cref="ColumnType.Date"/> or <see cref="ColumnType.DateTime"/>
/// value with a value of the other temporal type or with a <see cref="ColumnType.String"/>:
/// <list type="bullet">
/// <item>A date against a datetime widens to midnight UTC of that day, as in PostgreSQL. Both types
///   store UTC ticks and a date is the ticks of its midnight, so the pair compares by ticks.</item>
/// <item>A string against a date or a datetime is parsed into that type with the CAST parse
///   (<see cref="CastScalarFunctions.TryParseTemporalText"/>), so <c>day = '2026-03-15'</c> and
///   <c>created &gt;= '2026-01-01'</c> work without a CAST.</item>
/// </list>
///
/// <para><b>A string that does not parse is an error</b>
/// (<see cref="CamusDBErrorCodes.InvalidInput"/>), not a non-match. This differs from the Uuid and Id
/// rule (<see cref="StringOperandCoercion"/>) on purpose. A malformed UUID can only fail an equality,
/// but a temporal string is mostly a range bound: with a fixed order in place of an error,
/// <c>created &gt;= '2026-13-01'</c> would return every row and look like a valid answer.</para>
///
/// <para>Every SQL surface that compares values must use this rule: the WHERE comparison evaluator,
/// BETWEEN, the simple CASE, <c>IN</c> / <c>NOT IN</c> membership on the AST path and in the prepared
/// hash set, CHECK constraints, and the planner, which converts the constant into the column's type
/// before it builds an index key. If one surface skips it, an index seek and a table scan of the same
/// predicate return different rows.</para>
/// </summary>
public static class TemporalComparison
{
    /// <summary>True for <see cref="ColumnType.Date"/> and <see cref="ColumnType.DateTime"/>.</summary>
    public static bool IsTemporal(ColumnType type) => type is ColumnType.Date or ColumnType.DateTime;

    /// <summary>
    /// Compares a pair that this rule covers: a date with a datetime, or a string with a date or a
    /// datetime. Returns false, leaving <paramref name="result"/> at 0, for every other pair,
    /// including two values of one type; the caller then applies its own rule. Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> when the string does not parse.
    /// </summary>
    public static bool TryCompare(ColumnValue left, ColumnValue right, out int result)
    {
        result = 0;

        if (left.Type == right.Type)
            return false;

        if (IsTemporal(left.Type) && IsTemporal(right.Type))
        {
            result = left.LongValue.CompareTo(right.LongValue);
            return true;
        }

        if (left.Type == ColumnType.String && IsTemporal(right.Type))
        {
            result = ParseOperand(left, right.Type).LongValue.CompareTo(right.LongValue);
            return true;
        }

        if (right.Type == ColumnType.String && IsTemporal(left.Type))
        {
            result = left.LongValue.CompareTo(ParseOperand(right, left.Type).LongValue);
            return true;
        }

        return false;
    }

    /// <summary>
    /// <see cref="TryCompare"/> for a date and a datetime only, with no string parse. A hash set needs
    /// this narrower rule: a string and the date it spells hash differently, so the caller converts the
    /// strings before a lookup (see <c>PreparedInSet</c>).
    /// </summary>
    public static bool TryCompareDateAndDateTime(ColumnValue left, ColumnValue right, out int result)
    {
        result = 0;

        if (left.Type == right.Type || !IsTemporal(left.Type) || !IsTemporal(right.Type))
            return false;

        result = left.LongValue.CompareTo(right.LongValue);
        return true;
    }

    /// <summary>
    /// Returns a date as the datetime at its midnight, and any other value unchanged. A hash-keyed
    /// caller stores and probes every temporal value in this one type, so a date and the equal
    /// datetime share a hash (<see cref="ColumnValueHash"/> hashes the type too).
    /// </summary>
    public static ColumnValue WidenDate(ColumnValue value) =>
        value.Type == ColumnType.Date ? new ColumnValue(ColumnType.DateTime, value.LongValue) : value;

    /// <summary>
    /// Parses a string operand into <paramref name="target"/> (Date or DateTime). Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> when the text is not a valid value of that type.
    /// </summary>
    public static ColumnValue ParseOperand(ColumnValue value, ColumnType target)
    {
        string text = value.StrValue ?? "";

        if (!CastScalarFunctions.TryParseTemporalText(text, target, out ColumnValue parsed))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Cannot compare a {(target == ColumnType.Date ? "date" : "datetime")} with '{text}': " +
                $"expected {CastScalarFunctions.TemporalTextForm(target)}");

        return parsed;
    }
}
