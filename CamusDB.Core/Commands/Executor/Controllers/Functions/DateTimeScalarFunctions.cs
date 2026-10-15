
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text.RegularExpressions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The date and time scalar functions: <c>date_part</c> (and <c>EXTRACT</c>, which the parser turns
/// into a call to it), <c>date_trunc</c>, <c>date_add</c>, <c>date_diff</c>, the MySQL one-argument
/// shortcuts (<c>year(x)</c>, <c>dayofweek(x)</c>, ...), and the unix-time functions. The functions
/// that report the current time are in <see cref="ClockScalarFunctions"/>, and the functions that
/// build a date from its parts are in <see cref="DateConstructorScalarFunctions"/>.
/// </summary>
/// <remarks>
/// <para>Every unit is computed on the UTC value. A string argument must carry an explicit offset,
/// so a part never depends on the server time zone.</para>
/// <para>A part is always <c>Integer64</c>, because return-type inference sees the argument types and
/// not the unit literal. PostgreSQL returns <c>numeric</c> instead, so two of its units differ here:
/// <c>epoch</c> is whole seconds (rounded down, also before 1970), and <c>millisecond</c> /
/// <c>microsecond</c> are the fraction of the current second only (0-999 and 0-999999, as MySQL's
/// <c>MICROSECOND()</c>), where PostgreSQL also counts the whole seconds.</para>
/// <para>Each unit is valid only in the functions where it has a meaning. <c>dow</c>, <c>isodow</c>,
/// <c>doy</c>, <c>isoyear</c> and <c>epoch</c> name a position, not a length, so <c>date_trunc</c>,
/// <c>date_add</c> and <c>date_diff</c> reject them with the same error as an unknown unit.</para>
/// </remarks>
internal static class DateTimeScalarFunctions
{
    private const string DateOnlyFormat = "yyyy-MM-dd";

    private static readonly string[] IsoUtcDateTimeFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF",
    ];

    private static readonly string[] IsoDateTimeOffsetFormats =
    [
        "yyyy-MM-dd'T'HH:mm:sszzz",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFzzz",
    ];

    private static readonly Regex LocalDateTimeWithoutOffsetPattern = new(
        @"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static void Register(ScalarFunctionRegistry registry)
    {
        // date_add always infers DateTime: the Date-vs-DateTime promotion rule (Date+subday→DateTime,
        // Date+day→Date) requires knowing the unit value at plan time, which InferReturnType cannot do
        // (it only sees argument types). Widening to DateTime keeps Evaluator and InferReturnType in agreement.
        RegisterTernary(registry, "date_add", EvaluateDateAdd, _ => ColumnType.DateTime);
        RegisterTernary(registry, "date_diff", EvaluateDateDiff, _ => ColumnType.Integer64);
        // EXTRACT(field FROM x) parses to a call named "extract" whose arguments are (field, x), the
        // same order as date_part, so one evaluator serves both spellings.
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "date_part",
            Aliases = ["extract"],
            MinArity = 2,
            MaxArity = 2,
            Evaluator = EvaluateDatePart,
            InferReturnType = _ => ColumnType.Integer64,
        });

        RegisterBinary(registry, "date_trunc", EvaluateDateTrunc, _ => ColumnType.DateTime);

        // With no argument it reports the start of the transaction, as now() does, so it reads the
        // statement's times; with one it converts its argument.
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "unix_timestamp",
            MinArity = 0,
            MaxArity = 1,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = EvaluateUnixTimestamp,
            Evaluator = static (calledName, arguments) => EvaluateUnixTimestamp(calledName, arguments, null),
            InferReturnType = _ => ColumnType.Integer64,
        });

        // The millisecond epoch that JavaScript clients use (Date.now()). Neither PostgreSQL nor MySQL
        // has one, so the name extends unix_timestamp with its unit: same arity, same clock rule, and a
        // reader who knows one can guess the other. DuckDB's epoch_ms is not used because DuckDB
        // overloads it both ways (an integer argument returns a timestamp), which this function does
        // not do; from_unixtime(ms / 1000.0) is the reverse direction.
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "unix_timestamp_ms",
            MinArity = 0,
            MaxArity = 1,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = EvaluateUnixTimestampMs,
            Evaluator = static (calledName, arguments) => EvaluateUnixTimestampMs(calledName, arguments, null),
            InferReturnType = _ => ColumnType.Integer64,
        });

        RegisterUnary(registry, "from_unixtime", EvaluateFromUnixtime, _ => ColumnType.DateTime);

        RegisterMySqlShortcuts(registry);
    }

    /// <summary>
    /// The MySQL one-argument date-part functions. Each one reads one part of the UTC value, with the
    /// MySQL numbering where it differs from <c>date_part</c>: <c>dayofweek</c> counts 1 = Sunday and
    /// <c>weekday</c> counts 0 = Monday, while <c>date_part('dow', x)</c> counts 0 = Sunday.
    /// <c>DATE(x)</c> is not here, because <c>date</c> is a reserved type keyword; <c>CAST(x AS date)</c>
    /// does the same thing.
    /// </summary>
    private static void RegisterMySqlShortcuts(ScalarFunctionRegistry registry)
    {
        RegisterPartShortcut(registry, "year", [], DateTimeUnit.Year);
        RegisterPartShortcut(registry, "month", [], DateTimeUnit.Month);
        RegisterPartShortcut(registry, "day", ["dayofmonth"], DateTimeUnit.Day);
        RegisterPartShortcut(registry, "hour", [], DateTimeUnit.Hour);
        RegisterPartShortcut(registry, "minute", [], DateTimeUnit.Minute);
        RegisterPartShortcut(registry, "second", [], DateTimeUnit.Second);
        RegisterPartShortcut(registry, "quarter", [], DateTimeUnit.Quarter);
        RegisterPartShortcut(registry, "dayofyear", [], DateTimeUnit.DayOfYear);

        RegisterUnary(
            registry,
            "dayofweek",
            (calledName, arguments) => EvaluateShortcut(calledName, arguments, static utc => (long)utc.DayOfWeek + 1),
            _ => ColumnType.Integer64);

        RegisterUnary(
            registry,
            "weekday",
            (calledName, arguments) => EvaluateShortcut(calledName, arguments, static utc => ((long)utc.DayOfWeek + 6) % 7),
            _ => ColumnType.Integer64);

        RegisterUnary(registry, "dayname", EvaluateDayName, _ => ColumnType.String);
        RegisterUnary(registry, "monthname", EvaluateMonthName, _ => ColumnType.String);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "week",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = EvaluateMySqlWeek,
            InferReturnType = _ => ColumnType.Integer64,
        });
    }

    private static void RegisterPartShortcut(
        ScalarFunctionRegistry registry,
        string name,
        IReadOnlyList<string> aliases,
        DateTimeUnit unit)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            Aliases = aliases,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = (calledName, arguments) => EvaluateShortcut(calledName, arguments, utc => ExtractPart(utc, unit)),
            InferReturnType = _ => ColumnType.Integer64,
        });
    }

    private static void RegisterUnary(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = evaluator,
            InferReturnType = inferReturnType,
        });
    }

    private static void RegisterBinary(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 2,
            MaxArity = 2,
            Evaluator = evaluator,
            InferReturnType = inferReturnType,
        });
    }

    private static void RegisterTernary(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 3,
            MaxArity = 3,
            Evaluator = evaluator,
            InferReturnType = inferReturnType,
        });
    }

    private static ColumnValue EvaluateDateAdd(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireInteger(calledName, 1, arguments[1]);
        RequireString(calledName, 2, arguments[2]);

        DateTimeOffset value = ReadTemporal(calledName, 0, arguments[0]);
        long amount = arguments[1].LongValue;
        DateTimeUnit unit = ParseUnit(calledName, arguments[2].StrValue!);

        DateTimeOffset result = ApplyDateAdd(calledName, arguments[2].StrValue!, value, amount, unit);
        return MakeDateTime(result);
    }

    private static ColumnValue EvaluateDateDiff(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 2, arguments[2]);

        DateTimeOffset start = ReadTemporal(calledName, 0, arguments[0]);
        DateTimeOffset end = ReadTemporal(calledName, 1, arguments[1]);
        DateTimeUnit unit = ParseUnit(calledName, arguments[2].StrValue!);

        long difference = ComputeDateDiff(calledName, arguments[2].StrValue!, start, end, unit);
        return new ColumnValue(ColumnType.Integer64, difference);
    }

    private static ColumnValue EvaluateDatePart(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        DateTimeUnit unit = ParseUnit(calledName, arguments[0].StrValue!);
        DateTimeOffset value = ReadTemporal(calledName, 1, arguments[1]);

        return new ColumnValue(ColumnType.Integer64, ExtractPart(value.ToUniversalTime(), unit));
    }

    /// <summary>
    /// One part of a UTC value. Every unit has a part, so this never rejects a unit. The ISO week and
    /// ISO year use the ISO-8601 rule: week 1 is the week that holds the first Thursday of the year,
    /// so 2021-01-01 is in week 53 of ISO year 2020. Decade, century and millennium follow PostgreSQL:
    /// the year 2000 is in century 20 and the year 2001 starts century 21.
    /// </summary>
    private static long ExtractPart(DateTimeOffset utc, DateTimeUnit unit)
    {
        return unit switch
        {
            DateTimeUnit.Year => utc.Year,
            DateTimeUnit.Month => utc.Month,
            DateTimeUnit.Day => utc.Day,
            DateTimeUnit.Hour => utc.Hour,
            DateTimeUnit.Minute => utc.Minute,
            DateTimeUnit.Second => utc.Second,
            DateTimeUnit.Millisecond => utc.Millisecond,
            DateTimeUnit.Microsecond => utc.Ticks % TimeSpan.TicksPerSecond / TimeSpan.TicksPerMicrosecond,
            DateTimeUnit.Week => ISOWeek.GetWeekOfYear(utc.UtcDateTime),
            DateTimeUnit.Quarter => (utc.Month - 1) / 3 + 1,
            DateTimeUnit.DayOfWeek => (long)utc.DayOfWeek,
            DateTimeUnit.IsoDayOfWeek => utc.DayOfWeek == DayOfWeek.Sunday ? 7 : (long)utc.DayOfWeek,
            DateTimeUnit.DayOfYear => utc.DayOfYear,
            DateTimeUnit.IsoYear => ISOWeek.GetYear(utc.UtcDateTime),
            DateTimeUnit.Epoch => utc.ToUnixTimeSeconds(),
            DateTimeUnit.Decade => utc.Year / 10,
            DateTimeUnit.Century => (utc.Year + 99) / 100,
            DateTimeUnit.Millennium => (utc.Year + 999) / 1000,
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, null),
        };
    }

    private static ColumnValue EvaluateShortcut(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        Func<DateTimeOffset, long> part)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTimeOffset value = ReadTemporal(calledName, 0, arguments[0]);
        return new ColumnValue(ColumnType.Integer64, part(value.ToUniversalTime()));
    }

    private static ColumnValue EvaluateDayName(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTimeOffset utc = ReadTemporal(calledName, 0, arguments[0]).ToUniversalTime();
        return new ColumnValue(ColumnType.String, CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(utc.DayOfWeek));
    }

    private static ColumnValue EvaluateMonthName(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTimeOffset utc = ReadTemporal(calledName, 0, arguments[0]).ToUniversalTime();
        return new ColumnValue(ColumnType.String, CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(utc.Month));
    }

    /// <summary>
    /// MySQL <c>WEEK(x [, mode])</c> for the two modes that have a clear rule. Mode 0, the MySQL
    /// default, starts a week on Sunday and counts 0-53: the days before the first Sunday of the
    /// year are week 0. Mode 3 is the ISO-8601 week, 1-53, the same as <c>date_part('week', x)</c>.
    /// Any other mode is rejected rather than answered with a rule the caller did not ask for.
    /// </summary>
    private static ColumnValue EvaluateMySqlWeek(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        long mode = 0;
        if (arguments.Count == 2)
        {
            RequireInteger(calledName, 1, arguments[1]);
            mode = arguments[1].LongValue;
        }

        DateTimeOffset utc = ReadTemporal(calledName, 0, arguments[0]).ToUniversalTime();

        long week = mode switch
        {
            0 => SundayWeekOfYear(utc),
            3 => ISOWeek.GetWeekOfYear(utc.UtcDateTime),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' supports mode 0 (Sunday start, 0-53) and mode 3 (ISO-8601) only, but received mode {mode.ToString(CultureInfo.InvariantCulture)}"),
        };

        return new ColumnValue(ColumnType.Integer64, week);
    }

    private static long SundayWeekOfYear(DateTimeOffset utc)
    {
        int firstDayOfYearDow = (int)new DateTime(utc.Year, 1, 1).DayOfWeek;
        int firstSundayDayOfYear = 1 + (7 - firstDayOfYearDow) % 7;

        return utc.DayOfYear < firstSundayDayOfYear ? 0 : (utc.DayOfYear - firstSundayDayOfYear) / 7 + 1;
    }

    private static ColumnValue EvaluateDateTrunc(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        DateTimeUnit unit = ParseUnit(calledName, arguments[0].StrValue!);
        DateTimeOffset value = ReadTemporal(calledName, 1, arguments[1]);
        DateTimeOffset truncated = TruncateUtc(calledName, arguments[0].StrValue!, value, unit);

        return MakeDateTime(truncated);
    }

    private static ColumnValue EvaluateUnixTimestamp(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        if (arguments.Count == 0)
        {
            DateTime transactionStart = ClockScalarFunctions.ReadTransactionTime(calledName, parameters);
            return new ColumnValue(ColumnType.Integer64, new DateTimeOffset(transactionStart).ToUnixTimeSeconds());
        }

        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTimeOffset value = ReadTemporal(calledName, 0, arguments[0]);
        return new ColumnValue(ColumnType.Integer64, value.ToUnixTimeSeconds());
    }

    /// <summary>
    /// Whole milliseconds since 1970-01-01 UTC, rounded down, so the result is the epoch of
    /// <c>date_trunc('millisecond', x)</c> also before 1970. With no argument it reports the start of
    /// the transaction, as <c>now()</c> and <c>unix_timestamp()</c> do.
    /// </summary>
    private static ColumnValue EvaluateUnixTimestampMs(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        if (arguments.Count == 0)
        {
            DateTime transactionStart = ClockScalarFunctions.ReadTransactionTime(calledName, parameters);
            return new ColumnValue(ColumnType.Integer64, new DateTimeOffset(transactionStart).ToUnixTimeMilliseconds());
        }

        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        DateTimeOffset value = ReadTemporal(calledName, 0, arguments[0]);
        return new ColumnValue(ColumnType.Integer64, value.ToUnixTimeMilliseconds());
    }

    /// <summary>
    /// The UTC datetime <paramref name="arguments"/>[0] seconds after 1970-01-01. An integer is exact.
    /// A fraction rounds to the nearest microsecond, the precision of PostgreSQL <c>to_timestamp</c>
    /// and MySQL <c>FROM_UNIXTIME</c>: most decimal fractions have no exact float form, so a conversion
    /// to the 100 ns tick would turn <c>1.001</c> into <c>1.0009999</c> and
    /// <c>from_unixtime(unix_timestamp_ms(t) / 1000.0)</c> would miss <c>t</c>'s millisecond. A
    /// NUMERIC argument is rounded from its exact value. A float holds a microsecond only up to about
    /// 2^53 microseconds (the year 2255); later values keep the float's own precision.
    /// </summary>
    internal static ColumnValue EvaluateFromUnixtime(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        ColumnValue argument = arguments[0];
        long minOffsetTicks = -DateTimeOffset.UnixEpoch.Ticks;
        long maxOffsetTicks = DateTime.MaxValue.Ticks - DateTimeOffset.UnixEpoch.Ticks;

        long offsetTicks;

        switch (argument.Type)
        {
            case ColumnType.Integer64:
                if (argument.LongValue < minOffsetTicks / TimeSpan.TicksPerSecond
                    || argument.LongValue > maxOffsetTicks / TimeSpan.TicksPerSecond)
                    throw InvalidUnixTimestamp(calledName, argument.LongValue.ToString(CultureInfo.InvariantCulture));

                offsetTicks = argument.LongValue * TimeSpan.TicksPerSecond;
                break;

            case ColumnType.Numeric:
                // NUMERIC holds nanoseconds (scale 9). Round half away from zero to the microsecond.
                Int128 unscaled = argument.NumericUnscaled;
                Int128 micros = (unscaled >= 0 ? unscaled + 500 : unscaled - 500) / 1000;

                if (micros < minOffsetTicks / TimeSpan.TicksPerMicrosecond
                    || micros > maxOffsetTicks / TimeSpan.TicksPerMicrosecond)
                    throw InvalidUnixTimestamp(calledName, NumericMath.Format(unscaled));

                offsetTicks = (long)micros * TimeSpan.TicksPerMicrosecond;
                break;

            default:
                double seconds = ScalarFunctionArguments.ToDouble(argument);
                double roundedMicros = Math.Round(seconds * 1_000_000, MidpointRounding.AwayFromZero);

                // A NaN fails both comparisons, so it is rejected here too.
                if (!(roundedMicros >= minOffsetTicks / TimeSpan.TicksPerMicrosecond
                      && roundedMicros <= maxOffsetTicks / TimeSpan.TicksPerMicrosecond))
                    throw InvalidUnixTimestamp(calledName, seconds.ToString(CultureInfo.InvariantCulture));

                offsetTicks = (long)roundedMicros * TimeSpan.TicksPerMicrosecond;
                break;
        }

        return new ColumnValue(ColumnType.DateTime, DateTimeOffset.UnixEpoch.Ticks + offsetTicks);
    }

    internal static DateTimeOffset ReadTemporal(string calledName, int argIndex, ColumnValue arg)
    {
        return arg.Type switch
        {
            ColumnType.Date or ColumnType.DateTime => new DateTimeOffset(new DateTime(arg.LongValue, DateTimeKind.Utc)),
            ColumnType.String => ParseDateTimeValue(calledName, arg.StrValue!),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' argument {argIndex} expects a date/datetime value but received {arg.Type}"),
        };
    }

    private static ColumnValue MakeDateTime(DateTimeOffset value)
        => new(ColumnType.DateTime, value.ToUniversalTime().Ticks);

    private static DateTimeOffset ApplyDateAdd(
        string calledName,
        string unitText,
        DateTimeOffset value,
        long amount,
        DateTimeUnit unit)
    {
        try
        {
            return unit switch
            {
                DateTimeUnit.Year => value.AddYears(checked((int)amount)),
                DateTimeUnit.Month => value.AddMonths(checked((int)amount)),
                DateTimeUnit.Day => value.AddDays(amount),
                DateTimeUnit.Hour => value.AddHours(amount),
                DateTimeUnit.Minute => value.AddMinutes(amount),
                DateTimeUnit.Second => value.AddSeconds(amount),
                DateTimeUnit.Millisecond => value.AddMilliseconds(amount),
                DateTimeUnit.Microsecond => value.AddTicks(checked(amount * TimeSpan.TicksPerMicrosecond)),
                DateTimeUnit.Week => value.AddDays(checked(amount * 7)),
                DateTimeUnit.Quarter => value.AddMonths(checked((int)(amount * 3))),
                DateTimeUnit.Decade => value.AddYears(checked((int)(amount * 10))),
                DateTimeUnit.Century => value.AddYears(checked((int)(amount * 100))),
                DateTimeUnit.Millennium => value.AddYears(checked((int)(amount * 1000))),
                _ => throw InvalidUnit(calledName, unitText),
            };
        }
        catch (OverflowException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' date/time overflow for amount {amount.ToString(CultureInfo.InvariantCulture)}");
        }
        catch (ArgumentOutOfRangeException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' date/time overflow for amount {amount.ToString(CultureInfo.InvariantCulture)}");
        }
    }

    /// <summary>
    /// The number of whole units from <paramref name="start"/> to <paramref name="end"/>, truncated
    /// toward zero. Day and week count calendar days and ignore the time of day; month, quarter and
    /// the year-based units count complete anniversaries; the sub-day units count elapsed time.
    /// </summary>
    private static long ComputeDateDiff(
        string calledName,
        string unitText,
        DateTimeOffset start,
        DateTimeOffset end,
        DateTimeUnit unit)
    {
        DateTimeOffset startUtc = start.ToUniversalTime();
        DateTimeOffset endUtc = end.ToUniversalTime();

        return unit switch
        {
            DateTimeUnit.Millennium => DiffYears(startUtc, endUtc) / 1000,
            DateTimeUnit.Century => DiffYears(startUtc, endUtc) / 100,
            DateTimeUnit.Decade => DiffYears(startUtc, endUtc) / 10,
            DateTimeUnit.Quarter => DiffMonths(startUtc, endUtc) / 3,
            DateTimeUnit.Week => (endUtc.UtcDateTime.Date - startUtc.UtcDateTime.Date).Days / 7,
            DateTimeUnit.Microsecond => (endUtc.UtcTicks - startUtc.UtcTicks) / TimeSpan.TicksPerMicrosecond,
            DateTimeUnit.Year => DiffYears(startUtc, endUtc),
            DateTimeUnit.Month => DiffMonths(startUtc, endUtc),
            DateTimeUnit.Day => (endUtc.UtcDateTime.Date - startUtc.UtcDateTime.Date).Days,
            DateTimeUnit.Hour => (long)(endUtc - startUtc).TotalHours,
            DateTimeUnit.Minute => (long)(endUtc - startUtc).TotalMinutes,
            DateTimeUnit.Second => (long)(endUtc - startUtc).TotalSeconds,
            DateTimeUnit.Millisecond => (endUtc.ToUnixTimeMilliseconds() - startUtc.ToUnixTimeMilliseconds()),
            _ => throw InvalidUnit(calledName, unitText),
        };
    }

    private static long DiffYears(DateTimeOffset start, DateTimeOffset end)
    {
        long years = end.Year - start.Year;

        if (years > 0 && IsBeforeAnniversary(end, start))
            years--;
        else if (years < 0 && IsAfterAnniversary(end, start))
            years++;

        return years;
    }

    private static long DiffMonths(DateTimeOffset start, DateTimeOffset end)
    {
        long months = (end.Year - start.Year) * 12L + end.Month - start.Month;

        if (months > 0 && end.Day < start.Day)
            months--;
        else if (months < 0 && end.Day > start.Day)
            months++;

        return months;
    }

    private static bool IsBeforeAnniversary(DateTimeOffset value, DateTimeOffset anniversary)
        => value.Month < anniversary.Month
           || (value.Month == anniversary.Month && value.Day < anniversary.Day);

    private static bool IsAfterAnniversary(DateTimeOffset value, DateTimeOffset anniversary)
        => value.Month > anniversary.Month
           || (value.Month == anniversary.Month && value.Day > anniversary.Day);

    /// <summary>
    /// The start of the unit that holds <paramref name="value"/>, in UTC. A week starts on Monday
    /// (ISO-8601 and PostgreSQL). Century and millennium start at a year that ends in 01, as in
    /// PostgreSQL, so 2024 truncates to 2001-01-01; a decade starts at a year that ends in 0.
    /// </summary>
    private static DateTimeOffset TruncateUtc(string calledName, string unitText, DateTimeOffset value, DateTimeUnit unit)
    {
        DateTimeOffset utc = value.ToUniversalTime();

        return unit switch
        {
            DateTimeUnit.Millennium => StartOfYear(calledName, (utc.Year - 1) / 1000 * 1000 + 1),
            DateTimeUnit.Century => StartOfYear(calledName, (utc.Year - 1) / 100 * 100 + 1),
            DateTimeUnit.Decade => StartOfYear(calledName, utc.Year / 10 * 10),
            DateTimeUnit.Quarter => new DateTimeOffset(utc.Year, (utc.Month - 1) / 3 * 3 + 1, 1, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Week => new DateTimeOffset(utc.Year, utc.Month, utc.Day, 0, 0, 0, TimeSpan.Zero)
                .AddDays(-(((int)utc.DayOfWeek + 6) % 7)),
            DateTimeUnit.Year => new DateTimeOffset(utc.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Month => new DateTimeOffset(utc.Year, utc.Month, 1, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Day => new DateTimeOffset(utc.Year, utc.Month, utc.Day, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Hour => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Minute => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, TimeSpan.Zero),
            DateTimeUnit.Second => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, TimeSpan.Zero),
            DateTimeUnit.Millisecond => new DateTimeOffset(
                utc.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond,
                TimeSpan.Zero),
            DateTimeUnit.Microsecond => new DateTimeOffset(
                utc.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond,
                TimeSpan.Zero),
            _ => throw InvalidUnit(calledName, unitText),
        };
    }

    /// <summary>
    /// January 1 of <paramref name="year"/>. The first decade starts at year 0, which a
    /// <see cref="DateTimeOffset"/> cannot hold, so a value in the years 1-9 has no decade start.
    /// </summary>
    private static DateTimeOffset StartOfYear(string calledName, int year)
    {
        if (year < 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' result is before year 1");

        return new DateTimeOffset(year, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }

    private static DateTimeOffset ParseDateTimeValue(string calledName, string value)
    {
        if (DateTime.TryParseExact(
                value,
                DateOnlyFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime dateOnly))
        {
            return new DateTimeOffset(DateTime.SpecifyKind(dateOnly, DateTimeKind.Utc));
        }

        if (LocalDateTimeWithoutOffsetPattern.IsMatch(value))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects a UTC date/time string with an explicit offset but received '{value}'");
        }

        if (DateTimeOffset.TryParseExact(
                value,
                IsoDateTimeOffsetFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out DateTimeOffset parsedWithOffset))
        {
            return parsedWithOffset.ToUniversalTime();
        }

        if (value.EndsWith("Z", StringComparison.OrdinalIgnoreCase)
            && DateTimeOffset.TryParseExact(
                value[..^1],
                IsoUtcDateTimeFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out DateTimeOffset parsedUtc))
        {
            return parsedUtc.ToUniversalTime();
        }

        throw InvalidDate(calledName, value);
    }

    private static DateTimeUnit ParseUnit(string calledName, string unit)
    {
        return unit.Trim().ToLowerInvariant() switch
        {
            "year" or "years" => DateTimeUnit.Year,
            "month" or "months" => DateTimeUnit.Month,
            "day" or "days" => DateTimeUnit.Day,
            "hour" or "hours" => DateTimeUnit.Hour,
            "minute" or "minutes" => DateTimeUnit.Minute,
            "second" or "seconds" => DateTimeUnit.Second,
            "millisecond" or "milliseconds" => DateTimeUnit.Millisecond,
            "microsecond" or "microseconds" or "us" => DateTimeUnit.Microsecond,
            "week" or "weeks" => DateTimeUnit.Week,
            "quarter" or "quarters" => DateTimeUnit.Quarter,
            "dow" => DateTimeUnit.DayOfWeek,
            "isodow" => DateTimeUnit.IsoDayOfWeek,
            "doy" => DateTimeUnit.DayOfYear,
            "isoyear" => DateTimeUnit.IsoYear,
            "epoch" => DateTimeUnit.Epoch,
            "decade" or "decades" => DateTimeUnit.Decade,
            "century" or "centuries" => DateTimeUnit.Century,
            "millennium" or "millennia" or "millenniums" => DateTimeUnit.Millennium,
            _ => throw InvalidUnit(calledName, unit),
        };
    }

    private static CamusDBException InvalidDate(string calledName, string value)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' could not parse date/time value '{value}'");

    private static CamusDBException InvalidUnit(string calledName, string unit)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' expects a supported date/time unit but received '{unit}'");

    private static CamusDBException InvalidUnixTimestamp(string calledName, string seconds)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' unix timestamp out of range: {seconds}");

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static void RequireInteger(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Integer64);

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);

    /// <summary>
    /// The units the date functions accept. <c>DayOfWeek</c>, <c>IsoDayOfWeek</c>, <c>DayOfYear</c>,
    /// <c>IsoYear</c> and <c>Epoch</c> are parts only:
    /// <c>date_trunc</c>, <c>date_add</c> and <c>date_diff</c> reject them.
    /// </summary>
    private enum DateTimeUnit
    {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second,
        Millisecond,

        /// <summary>As a part, the fraction of the current second in microseconds, 0-999999.</summary>
        Microsecond,

        /// <summary>The ISO-8601 week: a part is 1-53, and a week starts on Monday.</summary>
        Week,

        Quarter,

        /// <summary>0 = Sunday to 6 = Saturday (PostgreSQL <c>dow</c>).</summary>
        DayOfWeek,

        /// <summary>1 = Monday to 7 = Sunday (PostgreSQL <c>isodow</c>).</summary>
        IsoDayOfWeek,

        DayOfYear,

        /// <summary>The ISO-8601 year that owns the ISO week, which can differ from the calendar year near January 1.</summary>
        IsoYear,

        /// <summary>Whole seconds since 1970-01-01 UTC, rounded down.</summary>
        Epoch,

        Decade,
        Century,
        Millennium,
    }
}
