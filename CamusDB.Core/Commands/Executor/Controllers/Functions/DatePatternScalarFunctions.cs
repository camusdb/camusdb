
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The functions that format and parse a date with a pattern: PostgreSQL <c>to_char</c>,
/// <c>to_timestamp</c> and the two-argument <c>to_date</c>, and MySQL <c>date_format</c> and
/// <c>str_to_date</c>. All of them use <see cref="DatePattern"/>, which compiles each distinct pattern
/// once.
/// </summary>
/// <remarks>
/// <para>None of these functions is volatile: the result depends on the arguments only, so a
/// <c>CHECK</c>, a <c>DEFAULT</c> and a view body can use them.</para>
/// <para><c>str_to_date</c> always returns <c>datetime</c>. MySQL returns a date or a datetime from
/// the pattern, but return-type inference sees the argument types only, not the pattern text.</para>
/// <para>The one-argument <c>to_date(x)</c> is the cast to date and is registered with the casts in
/// <see cref="CastScalarFunctions"/>; its two-argument form calls <see cref="EvaluateParseToDate"/>.</para>
/// </remarks>
internal static class DatePatternScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "to_char",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => EvaluateFormat(calledName, arguments, DatePatternDialect.PostgreSql),
            InferReturnType = _ => ColumnType.String,
        });

        // to_timestamp(number) is the PostgreSQL spelling of from_unixtime(number).
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "to_timestamp",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => arguments.Count == 1
                ? DateTimeScalarFunctions.EvaluateFromUnixtime(calledName, arguments)
                : EvaluateParse(calledName, arguments, DatePatternDialect.PostgreSql, ColumnType.DateTime),
            InferReturnType = _ => ColumnType.DateTime,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "date_format",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => EvaluateFormat(calledName, arguments, DatePatternDialect.MySql),
            InferReturnType = _ => ColumnType.String,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "str_to_date",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => EvaluateParse(calledName, arguments, DatePatternDialect.MySql, ColumnType.DateTime),
            InferReturnType = _ => ColumnType.DateTime,
        });
    }

    /// <summary>
    /// <c>to_date(text, pattern)</c>: reads the text with a PostgreSQL template and keeps the date
    /// only. Time fields in the pattern are read and checked, then dropped, as PostgreSQL does.
    /// </summary>
    internal static ColumnValue EvaluateParseToDate(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateParse(calledName, arguments, DatePatternDialect.PostgreSql, ColumnType.Date);

    private static ColumnValue EvaluateFormat(string calledName, IReadOnlyList<ColumnValue> arguments, DatePatternDialect dialect)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireString(calledName, 1, arguments[1]);

        DateTime utc = DateTimeScalarFunctions.ReadTemporal(calledName, 0, arguments[0]).UtcDateTime;
        DatePattern pattern = DatePattern.Get(calledName, arguments[1].StrValue!, dialect);

        return new ColumnValue(ColumnType.String, pattern.Format(utc));
    }

    private static ColumnValue EvaluateParse(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        DatePatternDialect dialect,
        ColumnType resultType)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireString(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireString(calledName, 1, arguments[1]);

        DatePattern pattern = DatePattern.Get(calledName, arguments[1].StrValue!, dialect);
        DateTime utc = pattern.Parse(calledName, arguments[0].StrValue!);

        return resultType == ColumnType.Date
            ? new ColumnValue(ColumnType.Date, utc.Date.Ticks)
            : new ColumnValue(ColumnType.DateTime, utc.Ticks);
    }
}
