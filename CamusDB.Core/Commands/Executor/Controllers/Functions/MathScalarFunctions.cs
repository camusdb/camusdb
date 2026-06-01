
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
        RegisterRound(registry);
        RegisterUnary(registry, "sqrt", EvaluateSqrt, _ => ColumnType.Float64);
        RegisterBinary(registry, "pow", EvaluatePow, _ => ColumnType.Float64, aliases: ["power"]);
        RegisterBinary(registry, "mod", EvaluateMod, InferModReturnType);
        RegisterUnary(registry, "sign", EvaluateSign, _ => ColumnType.Integer64);

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

    private static void RegisterRound(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "round",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = EvaluateRound,
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
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' integer overflow for argument value {value}");
            }

            return new ColumnValue(ColumnType.Integer64, Math.Abs(value));
        }

        return new ColumnValue(ColumnType.Float64, Math.Abs(arguments[0].FloatValue));
    }

    private static ColumnValue EvaluateCeil(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateCeilFloor(calledName, arguments, Math.Ceiling);

    private static ColumnValue EvaluateFloor(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateCeilFloor(calledName, arguments, Math.Floor);

    private static ColumnValue EvaluateCeilFloor(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        Func<double, double> transform)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

        if (arguments[0].Type == ColumnType.Integer64)
            return arguments[0];

        return new ColumnValue(ColumnType.Float64, transform(arguments[0].FloatValue));
    }

    private static ColumnValue EvaluateRound(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

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

    private static ColumnValue EvaluatePow(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireNumeric(calledName, 1, arguments[1]);

        double left = ScalarFunctionArguments.ToDouble(arguments[0]);
        double right = ScalarFunctionArguments.ToDouble(arguments[1]);

        return new ColumnValue(ColumnType.Float64, Math.Pow(left, right));
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

        double floatValue = arguments[0].FloatValue;
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

        if (scale >= 0)
        {
            double factor = Math.Pow(10, scale);
            double rounded = Math.Round(number * factor, MidpointRounding.AwayFromZero) / factor;

            if (scale == 0 && value.Type == ColumnType.Integer64)
                return new ColumnValue(ColumnType.Integer64, (long)rounded);

            return new ColumnValue(ColumnType.Float64, rounded);
        }

        double divisor = Math.Pow(10, -scale);
        double roundedLeft = Math.Round(number / divisor, MidpointRounding.AwayFromZero) * divisor;
        return new ColumnValue(ColumnType.Float64, roundedLeft);
    }

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);

    private static ColumnType InferAbsReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] == ColumnType.Integer64
            ? ColumnType.Integer64
            : ColumnType.Float64;

    private static ColumnType InferCeilFloorReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] == ColumnType.Integer64
            ? ColumnType.Integer64
            : ColumnType.Float64;

    private static ColumnType InferModReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count >= 2
           && argumentTypes[0] == ColumnType.Integer64
           && argumentTypes[1] == ColumnType.Integer64
            ? ColumnType.Integer64
            : ColumnType.Float64;

    private static ColumnType InferRoundReturnType(IReadOnlyList<ColumnType> argumentTypes)
    {
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
