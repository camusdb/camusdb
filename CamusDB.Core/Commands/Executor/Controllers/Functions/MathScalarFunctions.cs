
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class MathScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnary(registry, "abs", EvaluateAbs, InferAbsReturnType);
        RegisterUnary(registry, "ceil", EvaluateCeil, InferCeilFloorReturnType, aliases: ["ceiling"]);
        RegisterUnary(registry, "floor", EvaluateFloor, InferCeilFloorReturnType);
        RegisterRound(registry, "round", EvaluateRound);
        RegisterRound(registry, "trunc", EvaluateTrunc);
        RegisterUnary(registry, "sqrt", EvaluateSqrt, _ => ColumnType.Float64);
        RegisterBinary(registry, "pow", EvaluatePow, _ => ColumnType.Float64, aliases: ["power"]);
        RegisterBinary(registry, "mod", EvaluateMod, InferModReturnType);
        RegisterUnary(registry, "sign", EvaluateSign, InferSignReturnType);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "random",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = EvaluateRandom,
            InferReturnType = _ => ColumnType.Float64,
        });
    }

    private static void RegisterUnary(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType,
        IReadOnlyList<string>? aliases = null)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            Aliases = aliases ?? [],
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
        ScalarReturnTypeInferenceDelegate inferReturnType,
        IReadOnlyList<string>? aliases = null)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            Aliases = aliases ?? [],
            MinArity = 2,
            MaxArity = 2,
            Evaluator = evaluator,
            InferReturnType = inferReturnType,
        });
    }

    private static void RegisterRound(ScalarFunctionRegistry registry, string name, ScalarFunctionEvaluatorDelegate evaluator)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 2,
            Evaluator = evaluator,
            InferReturnType = InferRoundReturnType,
        });
    }

    private static ColumnValue EvaluateAbs(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Integer64)
        {
            long value = arguments[0].LongValue;

            if (value == long.MinValue)
            {
                throw Int64Math.OutOfRange($"Function '{calledName}' integer overflow for argument value {value}");
            }

            return new ColumnValue(ColumnType.Integer64, Math.Abs(value));
        }

        // The NUMERIC range is symmetric, so the absolute value always fits.
        if (arguments[0].Type == ColumnType.Numeric)
            return ColumnValue.FromNumeric(Int128.Abs(arguments[0].NumericUnscaled));

        return new ColumnValue(ColumnType.Float64, Math.Abs(arguments[0].FloatValue));
    }

    private static ColumnValue EvaluateCeil(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateCeilFloor(calledName, arguments, Math.Ceiling, NumericRounding.Ceiling);

    private static ColumnValue EvaluateFloor(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateCeilFloor(calledName, arguments, Math.Floor, NumericRounding.Floor);

    private static ColumnValue EvaluateCeilFloor(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        Func<double, double> transform,
        NumericRounding numericMode)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Integer64)
            return arguments[0];

        // NUMERIC stays NUMERIC (Spanner); ceil of the maximum is past the range and throws.
        if (arguments[0].Type == ColumnType.Numeric)
            return ColumnValue.FromNumeric(NumericMath.Round(arguments[0].NumericUnscaled, 0, numericMode, calledName));

        return new ColumnValue(ColumnType.Float64, transform(arguments[0].FloatValue));
    }

    private static ColumnValue EvaluateRound(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Numeric)
            return RoundNumeric(calledName, arguments, NumericRounding.HalfAwayFromZero);

        if (arguments.Count == 1)
        {
            if (arguments[0].Type == ColumnType.Integer64)
                return arguments[0];

            return new ColumnValue(
                ColumnType.Float64,
                Math.Round(arguments[0].FloatValue, MidpointRounding.AwayFromZero));
        }

        ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], ColumnType.Integer64);

        int scale = RequireInt32Scale(calledName, arguments[1].LongValue);
        return RoundWithScale(arguments[0], scale);
    }

    /// <summary>
    /// <c>trunc(x[, n])</c>: drops the digits past <c>n</c> places after the decimal point (default 0;
    /// a negative <c>n</c> drops digits left of it). Integer64 is already whole; Float64 truncates in
    /// double; NUMERIC truncates exactly and stays NUMERIC, as Spanner's TRUNC.
    /// </summary>
    private static ColumnValue EvaluateTrunc(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Numeric)
            return RoundNumeric(calledName, arguments, NumericRounding.TowardZero);

        long digits = 0;
        if (arguments.Count == 2)
        {
            ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], ColumnType.Integer64);
            digits = RequireInt32Scale(calledName, arguments[1].LongValue);
        }

        if (arguments[0].Type == ColumnType.Integer64)
        {
            if (digits >= 0)
                return arguments[0];

            // Integer arithmetic: a double division can land just below the multiple and lose one.
            // 10^19 is past the long range, so dropping 19 or more digits leaves zero.
            if (digits < -18)
                return new ColumnValue(ColumnType.Integer64, 0);

            long unit = (long)Math.Pow(10, -digits);
            return new ColumnValue(ColumnType.Integer64, arguments[0].LongValue / unit * unit);
        }

        return new ColumnValue(ColumnType.Float64, RoundAtScale(calledName, arguments[0].FloatValue, (int)digits, Math.Truncate));
    }

    /// <summary>
    /// The NUMERIC arm of <c>round</c> and <c>trunc</c>: exact, at <c>n</c> digits (default 0), and the
    /// result stays NUMERIC. Rounding up past the maximum throws
    /// <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/>.
    /// </summary>
    private static ColumnValue RoundNumeric(string calledName, IReadOnlyList<ColumnValue> arguments, NumericRounding mode)
    {
        long digits = 0;
        if (arguments.Count == 2)
        {
            ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], ColumnType.Integer64);
            digits = arguments[1].LongValue;
        }

        return ColumnValue.FromNumeric(NumericMath.Round(arguments[0].NumericUnscaled, digits, mode, calledName));
    }

    private static int RequireInt32Scale(string calledName, long scaleValue)
    {
        if (scaleValue > int.MaxValue || scaleValue < int.MinValue)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' scale argument out of range: {scaleValue}");
        }

        return (int)scaleValue;
    }

    private static ColumnValue EvaluateSqrt(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        double value = ScalarFunctionArguments.ToDouble(arguments[0]);

        if (value < 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: negative square root");
        }

        return new ColumnValue(ColumnType.Float64, Math.Sqrt(value));
    }

    /// <summary>
    /// <c>pow(x, y)</c> with the PostgreSQL rules. <c>pow(0, y)</c> for a negative <c>y</c> and a
    /// negative <c>x</c> to a finite non-integer power have no real result, so they are
    /// <see cref="CamusDBErrorCodes.InvalidInput"/>; IEEE would give infinity and NaN. A result past
    /// the range from finite arguments (<c>pow(10, 400)</c>) is an overflow, and a zero result from a
    /// nonzero base (<c>pow(10, -400)</c>) is an underflow: both are
    /// <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>. NaN and infinite arguments give the IEEE
    /// result: <c>pow('NaN', 0)</c> is 1, <c>pow(2, 'Infinity')</c> is infinity.
    /// </summary>
    private static ColumnValue EvaluatePow(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireNumeric(calledName, 1, arguments[1]);

        double left = ScalarFunctionArguments.ToDouble(arguments[0]);
        double right = ScalarFunctionArguments.ToDouble(arguments[1]);

        if (left == 0 && right < 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: zero raised to a negative power is undefined");
        }

        if (left < 0 && double.IsFinite(right) && Math.Floor(right) != right)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: a negative number raised to a non-integer power yields a complex result");
        }

        double result = Math.Pow(left, right);

        return new ColumnValue(ColumnType.Float64, FloatMath.CheckFunctionResult(
            calledName, result, finiteInputs: double.IsFinite(left) && double.IsFinite(right), nonzeroInput: left != 0));
    }

    private static ColumnValue EvaluateMod(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireNumeric(calledName, 1, arguments[1]);

        if (arguments[0].Type == ColumnType.Integer64 && arguments[1].Type == ColumnType.Integer64)
        {
            long dividend = arguments[0].LongValue;
            long divisor = arguments[1].LongValue;

            if (divisor == 0)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' division by zero");
            }

            if (dividend == long.MinValue && divisor == -1)
            {
                return new ColumnValue(ColumnType.Integer64, 0);
            }

            return new ColumnValue(ColumnType.Integer64, dividend % divisor);
        }

        // NUMERIC with NUMERIC or Integer64 stays exact; the result has the sign of the dividend.
        if (MixedNumericComparison.ArithmeticResultType(arguments[0].Type, arguments[1].Type) == ColumnType.Numeric)
        {
            Int128 divisorUnscaled = MixedNumericComparison.ToNumericUnscaled(arguments[1]);

            if (divisorUnscaled == 0)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' division by zero");
            }

            return ColumnValue.FromNumeric(NumericMath.Remainder(
                MixedNumericComparison.ToNumericUnscaled(arguments[0]), divisorUnscaled));
        }

        double floatDivisor = ScalarFunctionArguments.ToDouble(arguments[1]);

        if (floatDivisor == 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' division by zero");
        }

        return new ColumnValue(
            ColumnType.Float64,
            ScalarFunctionArguments.ToDouble(arguments[0]) % floatDivisor);
    }

    private static ColumnValue EvaluateSign(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Integer64)
        {
            long intValue = arguments[0].LongValue;
            long sign = intValue == 0 ? 0 : intValue > 0 ? 1 : -1;
            return new ColumnValue(ColumnType.Integer64, sign);
        }

        // Spanner: SIGN of a NUMERIC is a NUMERIC, so SIGN(p) / 2 divides exactly instead of as integers.
        if (arguments[0].Type == ColumnType.Numeric)
            return ColumnValue.FromNumeric(NumericMath.FromInt64(Int128.Sign(arguments[0].NumericUnscaled)));

        double floatValue = arguments[0].FloatValue;

        // The result is an Integer64, which has no NaN, and NaN has no sign; the comparisons below are
        // false for it, so without this check it would report -1.
        if (double.IsNaN(floatValue))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: NaN has no sign");
        }

        long floatSign = floatValue == 0 ? 0 : floatValue > 0 ? 1 : -1;
        return new ColumnValue(ColumnType.Integer64, floatSign);
    }

    private static ColumnValue EvaluateRandom(string calledName, IReadOnlyList<ColumnValue> arguments)
        => new(ColumnType.Float64, Random.Shared.NextDouble());

    private static ColumnValue RoundWithScale(ColumnValue value, int scale)
    {
        if (scale == 0 && value.Type == ColumnType.Integer64)
            return value;

        double number = ScalarFunctionArguments.ToDouble(value);

        return new ColumnValue(
            ColumnType.Float64,
            RoundAtScale("round", number, scale, static x => Math.Round(x, MidpointRounding.AwayFromZero)));
    }

    /// <summary>
    /// Rounds <paramref name="number"/> to <paramref name="scale"/> decimal places with
    /// <paramref name="roundUnit"/> (a negative scale rounds left of the point): the value is scaled
    /// by 10^scale, rounded to a whole number and scaled back. A plain <c>number * 10^scale</c> gives
    /// infinity or NaN for a large scale (10^400 is infinity, and 0 × infinity is NaN), so a finite
    /// argument would give a non-finite result. The edge cases are handled instead:
    /// <list type="bullet">
    ///   <item>±infinity and NaN are returned as they are.</item>
    ///   <item>When the scaled value is past the range, the value has no digit at that place, so it
    ///   is returned unchanged. A double that large relative to 10^-scale has a spacing far wider
    ///   than the rounding step, so this is exact.</item>
    ///   <item>Past 323 places every double is its own nearest value at that step, and past -308
    ///   places every double rounds to zero.</item>
    ///   <item>A result past the range (<c>round(1.7e308, -308)</c> is 2e308) is
    ///   <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>.</item>
    /// </list>
    /// </summary>
    private static double RoundAtScale(string calledName, double number, int scale, Func<double, double> roundUnit)
    {
        if (!double.IsFinite(number))
            return number;

        if (scale >= 0)
        {
            if (scale > 323)
                return number;

            // 10^scale in two factors, so each is finite up to scale 323.
            double factor = Math.Pow(10, Math.Min(scale, 308));
            double extra = scale > 308 ? Math.Pow(10, scale - 308) : 1;
            double scaled = number * factor * extra;

            if (!double.IsFinite(scaled))
                return number;

            return roundUnit(scaled) / extra / factor;
        }

        if (scale < -308)
            return 0.0;

        double divisor = Math.Pow(10, -scale);
        double rounded = roundUnit(number / divisor) * divisor;

        if (double.IsInfinity(rounded))
            throw FloatMath.OutOfRange($"Function '{calledName}' float64 overflow: the result is outside the float64 range");

        return rounded;
    }

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);

    private static ColumnType InferAbsReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] is ColumnType.Integer64 or ColumnType.Numeric
            ? argumentTypes[0]
            : ColumnType.Float64;

    /// <summary>NUMERIC for a NUMERIC argument, as in Spanner; Integer64 for every other argument.</summary>
    private static ColumnType InferSignReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] == ColumnType.Numeric ? ColumnType.Numeric : ColumnType.Integer64;

    private static ColumnType InferCeilFloorReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] is ColumnType.Integer64 or ColumnType.Numeric
            ? argumentTypes[0]
            : ColumnType.Float64;

    private static ColumnType InferModReturnType(IReadOnlyList<ColumnType> argumentTypes)
    {
        if (argumentTypes.Count < 2)
            return ColumnType.Float64;

        if (argumentTypes[0] == ColumnType.Integer64 && argumentTypes[1] == ColumnType.Integer64)
            return ColumnType.Integer64;

        return MixedNumericComparison.ArithmeticResultType(argumentTypes[0], argumentTypes[1]) == ColumnType.Numeric
            ? ColumnType.Numeric
            : ColumnType.Float64;
    }

    private static ColumnType InferRoundReturnType(IReadOnlyList<ColumnType> argumentTypes)
    {
        if (argumentTypes.Count > 0 && argumentTypes[0] == ColumnType.Numeric)
            return ColumnType.Numeric;

        if (argumentTypes.Count == 1 && argumentTypes[0] == ColumnType.Integer64)
            return ColumnType.Integer64;

        if (argumentTypes.Count == 2
            && argumentTypes[0] == ColumnType.Integer64
            && argumentTypes[1] == ColumnType.Integer64)
        {
            return ColumnType.Integer64;
        }

        return ColumnType.Float64;
    }
}
