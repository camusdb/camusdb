
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The functions that build a date or a datetime from its parts: PostgreSQL <c>make_date(y, m, d)</c>
/// and <c>make_timestamp(y, mo, d, h, mi, s)</c>, MySQL <c>makedate(year, dayofyear)</c>, and MySQL
/// <c>last_day(x)</c>, the last day of the month of <c>x</c>.
/// </summary>
/// <remarks>
/// <para><b>Deterministic.</b> Each result depends on the arguments only, so none of these functions
/// is volatile: a <c>CHECK</c> can call one, a <c>DEFAULT</c> that calls one is evaluated once at DDL
/// time, and a query that calls one can use the result cache.</para>
/// <para><b>An invalid part is an error, not a NULL and not a carry.</b> <c>make_date(2026, 2, 30)</c>
/// raises <see cref="CamusDBErrorCodes.InvalidInput"/> with a message that names the part, as
/// PostgreSQL does. A year outside 1-9999, the range of <see cref="DateTime"/>, is rejected the same
/// way. PostgreSQL reads a negative year as a year BC; CamusDB has no BC dates, so it rejects one.</para>
/// <para><b><c>makedate</c> keeps the MySQL carry for a large day of the year.</b> MySQL defines
/// <c>MAKEDATE(2011, 366)</c> as 2012-01-01, so a day count past the end of the year moves into the
/// next year. MySQL answers NULL for a day of the year below 1; CamusDB raises an error instead, which
/// is the rule of the other three functions. MySQL also maps a two-digit year to 1970-2069; CamusDB
/// reads every year literally.</para>
/// <para><b>Fractional seconds.</b> <c>make_timestamp</c> accepts an integer, float or NUMERIC second
/// in [0, 60) and rounds the fraction to the nearest microsecond, the precision of a PostgreSQL
/// timestamp. A float second such as <c>0.123</c> has no exact binary form, so rounding to the 100 ns
/// tick of a <see cref="DateTime"/> instead would store <c>0.1229999</c>. A second that rounds up to
/// 60 carries into the next minute, as in PostgreSQL. Every value is UTC.</para>
/// </remarks>
internal static class DateConstructorScalarFunctions
{
    private const int MinYear = 1;

    private const int MaxYear = 9999;

    private const long TicksPerMicrosecond = TimeSpan.TicksPerMicrosecond;

    public static void Register(ScalarFunctionRegistry registry)
    {
        Register(registry, "make_date", 3, EvaluateMakeDate, ColumnType.Date);
        Register(registry, "make_timestamp", 6, EvaluateMakeTimestamp, ColumnType.DateTime);
        Register(registry, "makedate", 2, EvaluateMakeDateFromDayOfYear, ColumnType.Date);
        Register(registry, "last_day", 1, EvaluateLastDay, ColumnType.Date);
    }

    private static void Register(
        ScalarFunctionRegistry registry,
        string name,
        int arity,
        ScalarFunctionEvaluatorDelegate evaluator,
        ColumnType returnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = arity,
            MaxArity = arity,
            Evaluator = evaluator,
            InferReturnType = _ => returnType,
        });
    }

    private static ColumnValue EvaluateMakeDate(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTime date = BuildDate(calledName, arguments, 0);
        return new ColumnValue(ColumnType.Date, date.Ticks);
    }

    private static ColumnValue EvaluateMakeTimestamp(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTime date = BuildDate(calledName, arguments, 0);
        int hour = ReadPart(calledName, arguments, 3, "hour", 0, 23);
        int minute = ReadPart(calledName, arguments, 4, "minute", 0, 59);
        long secondTicks = ReadSecondTicks(calledName, arguments[5]);

        long ticks = date.Ticks + hour * TimeSpan.TicksPerHour + minute * TimeSpan.TicksPerMinute + secondTicks;

        // Only 9999-12-31 23:59:59.9999995 and later get here: the rounded second carries past the
        // last tick a DateTime can hold.
        if (ticks > DateTime.MaxValue.Ticks)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' result is after year {MaxYear.ToString(CultureInfo.InvariantCulture)}");

        return new ColumnValue(ColumnType.DateTime, ticks);
    }

    private static ColumnValue EvaluateMakeDateFromDayOfYear(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        int year = ReadPart(calledName, arguments, 0, "year", MinYear, MaxYear);

        ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], ColumnType.Integer64);
        long dayOfYear = arguments[1].LongValue;

        DateTime firstDay = new(year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        long maxDayOfYear = (DateTime.MaxValue.Date - firstDay).Days + 1L;

        if (dayOfYear < 1 || dayOfYear > maxDayOfYear)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' day of year {dayOfYear.ToString(CultureInfo.InvariantCulture)} is out of range 1-{maxDayOfYear.ToString(CultureInfo.InvariantCulture)} for year {year.ToString(CultureInfo.InvariantCulture)}");

        return new ColumnValue(ColumnType.Date, firstDay.AddDays(dayOfYear - 1).Ticks);
    }

    private static ColumnValue EvaluateLastDay(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTime utc = DateTimeScalarFunctions.ReadTemporal(calledName, 0, arguments[0]).UtcDateTime;
        DateTime lastDay = new(utc.Year, utc.Month, DateTime.DaysInMonth(utc.Year, utc.Month), 0, 0, 0, DateTimeKind.Utc);

        return new ColumnValue(ColumnType.Date, lastDay.Ticks);
    }

    /// <summary>
    /// Midnight UTC of the year, month and day at <paramref name="firstIndex"/> and the two arguments
    /// after it. The day is checked against the length of that month, so 2026-02-29 is rejected and
    /// 2024-02-29 is accepted.
    /// </summary>
    private static DateTime BuildDate(string calledName, IReadOnlyList<ColumnValue> arguments, int firstIndex)
    {
        int year = ReadPart(calledName, arguments, firstIndex, "year", MinYear, MaxYear);
        int month = ReadPart(calledName, arguments, firstIndex + 1, "month", 1, 12);

        ScalarFunctionArguments.RequireType(calledName, firstIndex + 2, arguments[firstIndex + 2], ColumnType.Integer64);
        long day = arguments[firstIndex + 2].LongValue;
        int daysInMonth = DateTime.DaysInMonth(year, month);

        if (day < 1 || day > daysInMonth)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' day {day.ToString(CultureInfo.InvariantCulture)} is out of range 1-{daysInMonth.ToString(CultureInfo.InvariantCulture)} for {year.ToString("D4", CultureInfo.InvariantCulture)}-{month.ToString("D2", CultureInfo.InvariantCulture)}");

        return new DateTime(year, month, (int)day, 0, 0, 0, DateTimeKind.Utc);
    }

    private static int ReadPart(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        int index,
        string partName,
        int min,
        int max)
    {
        ScalarFunctionArguments.RequireType(calledName, index, arguments[index], ColumnType.Integer64);
        long value = arguments[index].LongValue;

        if (value < min || value > max)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' {partName} {value.ToString(CultureInfo.InvariantCulture)} is out of range {min.ToString(CultureInfo.InvariantCulture)}-{max.ToString(CultureInfo.InvariantCulture)}");

        return (int)value;
    }

    /// <summary>
    /// The second argument of <c>make_timestamp</c> as ticks, with the fraction rounded to the nearest
    /// microsecond. The range check runs on the value as given, before rounding, so 59.9999999 is
    /// accepted and carries into the next minute while 60 is rejected.
    /// </summary>
    private static long ReadSecondTicks(string calledName, ColumnValue second)
    {
        ScalarFunctionArguments.RequireNumeric(calledName, 5, second);

        switch (second.Type)
        {
            case ColumnType.Integer64:
                if (second.LongValue is < 0 or >= 60)
                    throw InvalidSecond(calledName, second.LongValue.ToString(CultureInfo.InvariantCulture));

                return second.LongValue * TimeSpan.TicksPerSecond;

            case ColumnType.Numeric:
                Int128 unscaled = second.NumericUnscaled;

                if (unscaled < 0 || unscaled >= 60 * (Int128)NumericMath.ScaleFactor)
                    throw InvalidSecond(calledName, NumericMath.Format(unscaled));

                // NUMERIC holds nanoseconds (scale 9); half a microsecond is 500 of them.
                return (long)((unscaled + 500) / 1000) * TicksPerMicrosecond;

            default:
                double seconds = second.FloatValue;

                // A NaN fails both comparisons, so it is rejected here too.
                if (!(seconds >= 0 && seconds < 60))
                    throw InvalidSecond(calledName, seconds.ToString("R", CultureInfo.InvariantCulture));

                return (long)Math.Round(seconds * 1_000_000, MidpointRounding.AwayFromZero) * TicksPerMicrosecond;
        }
    }

    private static CamusDBException InvalidSecond(string calledName, string value)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' second {value} is out of range: it must be at least 0 and less than 60");
}
