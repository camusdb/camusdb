/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The arithmetic operators on dates, datetimes and intervals, with the PostgreSQL rules, and the
/// static result type of each one:
/// <list type="bullet">
///   <item><c>date + int64</c>, <c>int64 + date</c> and <c>date - int64</c> give a <c>date</c>. The
///   integer is a number of days.</item>
///   <item><c>date - date</c> gives an <c>int64</c>: the number of days from the right date to the
///   left date.</item>
///   <item><c>datetime ± interval</c>, <c>date ± interval</c>, <c>interval + datetime</c> and
///   <c>interval + date</c> give a <c>datetime</c>. A date counts as its midnight UTC.</item>
///   <item><c>datetime - datetime</c>, <c>date - datetime</c> and <c>datetime - date</c> give an
///   <c>interval</c> of days and time (<see cref="IntervalMath.BetweenTicks"/>).</item>
///   <item><c>interval ± interval</c> gives an <c>interval</c>.</item>
///   <item><c>interval * number</c>, <c>number * interval</c> and <c>interval / number</c> give an
///   <c>interval</c>. The number is an int64, a float or a NUMERIC, read as a double.</item>
/// </list>
///
/// <para>Every other pair with a date, a datetime or an interval operand has no rule here. The
/// operators fall back to <see cref="MixedNumericComparison.ArithmeticResultType"/>, which refuses it.
/// Thus <c>*</c> and <c>/</c> with a date, <c>date + 1.5</c>, <c>datetime + int64</c>,
/// <c>interval + int64</c> and <c>number / interval</c> stay errors, as in PostgreSQL.</para>
///
/// <para>A quoted string is not read as a date or an interval here. <c>'2024-01-01' + 1</c> is an error.
/// PostgreSQL gives an error too, because it reads the literal as an integer.</para>
///
/// <para>These rules are not part of <see cref="MixedNumericComparison.ArithmeticResultType"/>,
/// because that method is also the numeric supertype of CASE, COALESCE and UNION. A date and an
/// integer have no common type there.</para>
///
/// <para>A date result before year 1 or after year 9999 is an <see cref="CamusDBErrorCodes.InvalidInput"/>
/// error, never a wrapped value. <c>date_add</c> applies the same limit. The interval rules raise the
/// errors of <see cref="IntervalMath"/>.</para>
/// </summary>
internal static class TemporalArithmetic
{
    /// <summary>The day number of <see cref="DateTime.MaxValue"/>, 9999-12-31. Day 0 is 0001-01-01.</summary>
    private const long MaxDayNumber = 3_652_058;

    internal const string DateOutOfRangeMessage = "date out of range";

    /// <summary>
    /// The result type of <c>+ - * /</c> on two operand types: the rule in the class summary when it
    /// applies, otherwise <see cref="MixedNumericComparison.ArithmeticResultType"/>. Null when no rule
    /// applies. The evaluator and all static typing sites call this one method, so a derived table
    /// column or client metadata always has the type of the value.
    /// </summary>
    public static ColumnType? ArithmeticResultType(NodeType op, ColumnType left, ColumnType right)
        => TemporalResultType(op, left, right) ?? MixedNumericComparison.ArithmeticResultType(left, right);

    /// <summary>The result type of the temporal rules only, or null when the pair has no such rule.</summary>
    public static ColumnType? TemporalResultType(NodeType op, ColumnType left, ColumnType right)
    {
        bool add = op == NodeType.ExprAdd;
        bool sub = op == NodeType.ExprSub;

        // date ± int64, int64 + date, date - date.
        if (left == ColumnType.Date && right == ColumnType.Integer64 && (add || sub))
            return ColumnType.Date;

        if (add && left == ColumnType.Integer64 && right == ColumnType.Date)
            return ColumnType.Date;

        if (sub && left == ColumnType.Date && right == ColumnType.Date)
            return ColumnType.Integer64;

        // A point in time with an interval.
        if (IsPointInTime(left) && right == ColumnType.Interval && (add || sub))
            return ColumnType.DateTime;

        if (add && left == ColumnType.Interval && IsPointInTime(right))
            return ColumnType.DateTime;

        // The distance between two points in time, at least one of them a datetime.
        if (sub && IsPointInTime(left) && IsPointInTime(right) && (left == ColumnType.DateTime || right == ColumnType.DateTime))
            return ColumnType.Interval;

        if (left == ColumnType.Interval && right == ColumnType.Interval && (add || sub))
            return ColumnType.Interval;

        // Scaling.
        if (op == NodeType.ExprMult
            && ((left == ColumnType.Interval && MixedNumericComparison.IsNumeric(right))
                || (right == ColumnType.Interval && MixedNumericComparison.IsNumeric(left))))
            return ColumnType.Interval;

        if (op == NodeType.ExprDiv && left == ColumnType.Interval && MixedNumericComparison.IsNumeric(right))
            return ColumnType.Interval;

        return null;
    }

    /// <summary>
    /// Applies the temporal rules. The caller must first check that <see cref="TemporalResultType"/> is
    /// not null for the operand types, and that no operand is NULL.
    /// </summary>
    public static ColumnValue Evaluate(NodeType op, ColumnValue left, ColumnValue right)
    {
        ColumnType resultType = TemporalResultType(op, left.Type, right.Type)!.Value;

        switch (resultType)
        {
            case ColumnType.Date:
                return AddDays(op, left, right);

            case ColumnType.Integer64:
                return new ColumnValue(ColumnType.Integer64, DayNumber(left) - DayNumber(right));

            case ColumnType.DateTime:
            {
                bool intervalOnLeft = left.Type == ColumnType.Interval;
                ColumnValue point = intervalOnLeft ? right : left;
                IntervalParts interval = (intervalOnLeft ? left : right).IntervalParts;

                if (op == NodeType.ExprSub)
                    interval = IntervalMath.Negate(interval);

                return new ColumnValue(ColumnType.DateTime, IntervalMath.AddToTicks(point.LongValue, interval));
            }

            default:
                return ColumnValue.FromInterval(EvaluateInterval(op, left, right));
        }
    }

    /// <summary>Applies unary minus to an interval: each part changes sign.</summary>
    public static ColumnValue Negate(ColumnValue interval) =>
        ColumnValue.FromInterval(IntervalMath.Negate(interval.IntervalParts));

    private static IntervalParts EvaluateInterval(NodeType op, ColumnValue left, ColumnValue right)
    {
        // A date or a datetime on both sides: the distance. A date holds the ticks of its midnight.
        if (left.Type != ColumnType.Interval && right.Type != ColumnType.Interval)
            return IntervalMath.BetweenTicks(left.LongValue, right.LongValue);

        if (left.Type == ColumnType.Interval && right.Type == ColumnType.Interval)
            return op == NodeType.ExprAdd
                ? IntervalMath.Add(left.IntervalParts, right.IntervalParts)
                : IntervalMath.Subtract(left.IntervalParts, right.IntervalParts);

        if (op == NodeType.ExprDiv)
            return IntervalMath.Divide(left.IntervalParts, MixedNumericComparison.ToDouble(right));

        return left.Type == ColumnType.Interval
            ? IntervalMath.Multiply(left.IntervalParts, MixedNumericComparison.ToDouble(right))
            : IntervalMath.Multiply(right.IntervalParts, MixedNumericComparison.ToDouble(left));
    }

    private static bool IsPointInTime(ColumnType type) => type is ColumnType.Date or ColumnType.DateTime;

    private static ColumnValue AddDays(NodeType op, ColumnValue left, ColumnValue right)
    {
        ColumnValue date = left.Type == ColumnType.Date ? left : right;
        long days = left.Type == ColumnType.Date ? right.LongValue : left.LongValue;

        // Check the size before the sum, so that the sum cannot overflow and the negation of
        // long.MinValue does not occur.
        if (days is > MaxDayNumber or < -MaxDayNumber)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, DateOutOfRangeMessage);

        long result = op == NodeType.ExprSub ? DayNumber(date) - days : DayNumber(date) + days;

        if (result is < 0 or > MaxDayNumber)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, DateOutOfRangeMessage);

        return new ColumnValue(ColumnType.Date, result * TimeSpan.TicksPerDay);
    }

    /// <summary>
    /// The day number of a date value. A date holds the ticks of its UTC midnight, so the division is
    /// exact. Ticks are never negative, so the division also rounds the right way for a value that
    /// holds a time of day.
    /// </summary>
    private static long DayNumber(ColumnValue date) => date.LongValue / TimeSpan.TicksPerDay;
}
