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
/// The PostgreSQL functions that make or change an interval: <c>age</c>, <c>make_interval</c>,
/// <c>justify_days</c>, <c>justify_hours</c>, <c>justify_interval</c> and <c>date_bin</c>. The
/// arithmetic is in <see cref="IntervalMath"/>; <c>EXTRACT</c> from an interval is in
/// <see cref="DateTimeScalarFunctions"/>, next to <c>EXTRACT</c> from a datetime.
/// </summary>
/// <remarks>
/// <para><b>Arguments.</b> A date, a datetime, or a string with an explicit offset is a point in time,
/// as in the other date functions. A date is midnight UTC. An interval argument must be an interval
/// value: write <c>INTERVAL '1 day'</c> or <c>CAST('1 day' AS interval)</c>. A NULL argument gives
/// NULL.</para>
/// <para><b><c>age</c> is volatile.</b> With one argument it reads the current date of the statement,
/// so the function reads the statement clock (<see cref="ClockScalarFunctions"/>). Volatility is a
/// property of the function name, so the two-argument form is volatile too: a statement that calls
/// it does not use the result cache, and a CHECK constraint refuses it. PostgreSQL marks the
/// two-argument form immutable.</para>
/// <para><b>Named arguments.</b> PostgreSQL accepts <c>make_interval(days => 10)</c>. CamusDB has no
/// named arguments, so the parts are by position only, and a missing trailing part is zero.</para>
/// </remarks>
internal static class IntervalScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "age",
            MinArity = 1,
            MaxArity = 2,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = static (calledName, arguments, parameters) => EvaluateAge(calledName, arguments, parameters),
            Evaluator = static (calledName, arguments) => EvaluateAge(calledName, arguments, null),
            InferReturnType = static _ => ColumnType.Interval,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "make_interval",
            MinArity = 0,
            MaxArity = 7,
            Evaluator = EvaluateMakeInterval,
            InferReturnType = static _ => ColumnType.Interval,
        });

        RegisterJustify(registry, "justify_days", IntervalMath.JustifyDays);
        RegisterJustify(registry, "justify_hours", IntervalMath.JustifyHours);
        RegisterJustify(registry, "justify_interval", IntervalMath.JustifyInterval);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "date_bin",
            MinArity = 3,
            MaxArity = 3,
            Evaluator = EvaluateDateBin,
            InferReturnType = static _ => ColumnType.DateTime,
        });
    }

    private static void RegisterJustify(ScalarFunctionRegistry registry, string name, Func<IntervalParts, IntervalParts> justify)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = (calledName, arguments) =>
            {
                if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
                    return nullResult;

                return ColumnValue.FromInterval(justify(ReadInterval(calledName, 0, arguments[0])));
            },
            InferReturnType = static _ => ColumnType.Interval,
        });
    }

    /// <summary>
    /// <c>age(a, b)</c> is the symbolic difference <c>a - b</c> (<see cref="IntervalMath.Age"/>);
    /// <c>age(a)</c> is <c>age(current_date, a)</c>, where the current date is midnight UTC of the
    /// transaction start, as <c>current_date</c> reports it.
    /// </summary>
    private static ColumnValue EvaluateAge(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        IReadOnlyDictionary<string, ColumnValue>? parameters)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        long left;
        long right;

        if (arguments.Count == 1)
        {
            left = ClockScalarFunctions.ReadTransactionTime(calledName, parameters).Date.Ticks;
            right = DateTimeScalarFunctions.ReadTemporal(calledName, 0, arguments[0]).UtcTicks;
        }
        else
        {
            left = DateTimeScalarFunctions.ReadTemporal(calledName, 0, arguments[0]).UtcTicks;
            right = DateTimeScalarFunctions.ReadTemporal(calledName, 1, arguments[1]).UtcTicks;
        }

        return ColumnValue.FromInterval(IntervalMath.Age(left, right));
    }

    /// <summary>
    /// <c>make_interval(years, months, weeks, days, hours, mins, secs)</c>. Each part except the
    /// seconds must be an integer; the seconds can be any number.
    /// </summary>
    private static ColumnValue EvaluateMakeInterval(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        Span<long> parts = stackalloc long[6];
        for (int i = 0; i < arguments.Count && i < 6; i++)
        {
            ScalarFunctionArguments.RequireType(calledName, i, arguments[i], ColumnType.Integer64);
            parts[i] = arguments[i].LongValue;
        }

        double seconds = 0;
        if (arguments.Count == 7)
        {
            ScalarFunctionArguments.RequireNumeric(calledName, 6, arguments[6]);
            seconds = ScalarFunctionArguments.ToDouble(arguments[6]);
        }

        return ColumnValue.FromInterval(IntervalMath.Make(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], seconds));
    }

    /// <summary><c>date_bin(stride, source, origin)</c> (<see cref="IntervalMath.BinTicks"/>).</summary>
    private static ColumnValue EvaluateDateBin(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        IntervalParts stride = ReadInterval(calledName, 0, arguments[0]);
        long source = DateTimeScalarFunctions.ReadTemporal(calledName, 1, arguments[1]).UtcTicks;
        long origin = DateTimeScalarFunctions.ReadTemporal(calledName, 2, arguments[2]).UtcTicks;

        return new ColumnValue(ColumnType.DateTime, IntervalMath.BinTicks(stride, source, origin));
    }

    private static IntervalParts ReadInterval(string calledName, int argumentIndex, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Interval);
        return argument.IntervalParts;
    }
}
