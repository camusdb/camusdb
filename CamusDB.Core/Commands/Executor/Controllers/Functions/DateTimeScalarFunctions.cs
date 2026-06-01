
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
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "current_timestamp",
            Aliases = ["now"],
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = EvaluateCurrentTimestamp,
            InferReturnType = _ => ColumnType.String,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "current_date",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = EvaluateCurrentDate,
            InferReturnType = _ => ColumnType.String,
        });

        RegisterTernary(registry, "date_add", EvaluateDateAdd, _ => ColumnType.String);
        RegisterTernary(registry, "date_diff", EvaluateDateDiff, _ => ColumnType.Integer64);
        RegisterBinary(registry, "date_part", EvaluateDatePart, _ => ColumnType.Integer64);
        RegisterBinary(registry, "date_trunc", EvaluateDateTrunc, _ => ColumnType.String);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "unix_timestamp",
            MinArity = 0,
            MaxArity = 1,
            IsVolatile = true,
            Evaluator = EvaluateUnixTimestamp,
            InferReturnType = _ => ColumnType.Integer64,
        });

        RegisterUnary(registry, "from_unixtime", EvaluateFromUnixtime, _ => ColumnType.String);
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

    private static ColumnValue EvaluateCurrentTimestamp(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        string timestamp = FormatUtc(DateTimeOffset.UtcNow);
        return new ColumnValue(ColumnType.String, timestamp);
    }

    private static ColumnValue EvaluateCurrentDate(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        string date = DateTimeOffset.UtcNow.ToString(DateOnlyFormat, CultureInfo.InvariantCulture);
        return new ColumnValue(ColumnType.String, date);
    }

    private static ColumnValue EvaluateDateAdd(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireInteger(calledName, 1, arguments[1]);
        RequireString(calledName, 2, arguments[2]);

        DateTimeOffset value = ParseDateTimeValue(calledName, arguments[0].StrValue!);
        long amount = arguments[1].LongValue;
        DateTimeUnit unit = ParseUnit(calledName, arguments[2].StrValue!);

        DateTimeOffset result = ApplyDateAdd(calledName, value, amount, unit);
        return new ColumnValue(ColumnType.String, FormatUtc(result));
    }

    private static ColumnValue EvaluateDateDiff(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);
        RequireString(calledName, 2, arguments[2]);

        DateTimeOffset start = ParseDateTimeValue(calledName, arguments[0].StrValue!);
        DateTimeOffset end = ParseDateTimeValue(calledName, arguments[1].StrValue!);
        DateTimeUnit unit = ParseUnit(calledName, arguments[2].StrValue!);

        long difference = ComputeDateDiff(start, end, unit);
        return new ColumnValue(ColumnType.Integer64, difference);
    }

    private static ColumnValue EvaluateDatePart(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        DateTimeUnit unit = ParseUnit(calledName, arguments[0].StrValue!);
        DateTimeOffset value = ParseDateTimeValue(calledName, arguments[1].StrValue!);
        DateTimeOffset utc = value.ToUniversalTime();

        long part = unit switch
        {
            DateTimeUnit.Year => utc.Year,
            DateTimeUnit.Month => utc.Month,
            DateTimeUnit.Day => utc.Day,
            DateTimeUnit.Hour => utc.Hour,
            DateTimeUnit.Minute => utc.Minute,
            DateTimeUnit.Second => utc.Second,
            DateTimeUnit.Millisecond => utc.Millisecond,
            _ => throw InvalidUnit(calledName, arguments[0].StrValue!),
        };

        return new ColumnValue(ColumnType.Integer64, part);
    }

    private static ColumnValue EvaluateDateTrunc(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        DateTimeUnit unit = ParseUnit(calledName, arguments[0].StrValue!);
        DateTimeOffset value = ParseDateTimeValue(calledName, arguments[1].StrValue!);
        DateTimeOffset truncated = TruncateUtc(value, unit);

        return new ColumnValue(ColumnType.String, FormatUtc(truncated));
    }

    private static ColumnValue EvaluateUnixTimestamp(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments.Count == 0)
            return new ColumnValue(ColumnType.Integer64, DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        DateTimeOffset value = ParseDateTimeValue(calledName, arguments[0].StrValue!);
        return new ColumnValue(ColumnType.Integer64, value.ToUnixTimeSeconds());
    }

    private static ColumnValue EvaluateFromUnixtime(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        double seconds = ScalarFunctionArguments.ToDouble(arguments[0]);

        try
        {
            DateTimeOffset value = DateTimeOffset.UnixEpoch.AddSeconds(seconds);
            return new ColumnValue(ColumnType.String, FormatUtc(value));
        }
        catch (ArgumentOutOfRangeException)
        {
            throw InvalidUnixTimestamp(calledName, seconds);
        }
    }

    private static DateTimeOffset ApplyDateAdd(string calledName, DateTimeOffset value, long amount, DateTimeUnit unit)
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
                _ => throw InvalidUnit(calledName, unit.ToString()),
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

    private static long ComputeDateDiff(DateTimeOffset start, DateTimeOffset end, DateTimeUnit unit)
    {
        DateTimeOffset startUtc = start.ToUniversalTime();
        DateTimeOffset endUtc = end.ToUniversalTime();

        return unit switch
        {
            DateTimeUnit.Year => DiffYears(startUtc, endUtc),
            DateTimeUnit.Month => DiffMonths(startUtc, endUtc),
            DateTimeUnit.Day => (endUtc.UtcDateTime.Date - startUtc.UtcDateTime.Date).Days,
            DateTimeUnit.Hour => (long)(endUtc - startUtc).TotalHours,
            DateTimeUnit.Minute => (long)(endUtc - startUtc).TotalMinutes,
            DateTimeUnit.Second => (long)(endUtc - startUtc).TotalSeconds,
            DateTimeUnit.Millisecond => (endUtc.ToUnixTimeMilliseconds() - startUtc.ToUnixTimeMilliseconds()),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unsupported date/time unit '{unit}'"),
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

    private static DateTimeOffset TruncateUtc(DateTimeOffset value, DateTimeUnit unit)
    {
        DateTimeOffset utc = value.ToUniversalTime();

        return unit switch
        {
            DateTimeUnit.Year => new DateTimeOffset(utc.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Month => new DateTimeOffset(utc.Year, utc.Month, 1, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Day => new DateTimeOffset(utc.Year, utc.Month, utc.Day, 0, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Hour => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, TimeSpan.Zero),
            DateTimeUnit.Minute => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, TimeSpan.Zero),
            DateTimeUnit.Second => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, TimeSpan.Zero),
            DateTimeUnit.Millisecond => new DateTimeOffset(
                utc.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond,
                TimeSpan.Zero),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unsupported date/time unit '{unit}'"),
        };
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
            _ => throw InvalidUnit(calledName, unit),
        };
    }

    private static string FormatUtc(DateTimeOffset value)
        => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    private static CamusDBException InvalidDate(string calledName, string value)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' could not parse date/time value '{value}'");

    private static CamusDBException InvalidUnit(string calledName, string unit)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' expects a supported date/time unit but received '{unit}'");

    private static CamusDBException InvalidUnixTimestamp(string calledName, double seconds)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' unix timestamp out of range: {seconds.ToString(CultureInfo.InvariantCulture)}");

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static void RequireInteger(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Integer64);

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);

    private enum DateTimeUnit
    {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second,
        Millisecond,
    }
}
