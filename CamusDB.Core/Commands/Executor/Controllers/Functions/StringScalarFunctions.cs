
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

internal static class StringScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnaryString(registry, "length", EvaluateLength, _ => ColumnType.Integer64);
        RegisterUnaryString(registry, "lower", EvaluateLower, _ => ColumnType.String);
        RegisterUnaryString(registry, "upper", EvaluateUpper, _ => ColumnType.String);
        RegisterUnaryString(registry, "trim", EvaluateTrim, _ => ColumnType.String);
        RegisterUnaryString(registry, "ltrim", EvaluateLTrim, _ => ColumnType.String);
        RegisterUnaryString(registry, "rtrim", EvaluateRTrim, _ => ColumnType.String);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "substring",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = EvaluateSubstring,
            InferReturnType = _ => ColumnType.String,
        });

        RegisterTernaryString(registry, "replace", EvaluateReplace, _ => ColumnType.String);
        RegisterBinaryStringPredicate(registry, "contains", EvaluateContains);
        RegisterBinaryStringPredicate(registry, "starts_with", EvaluateStartsWith);
        RegisterBinaryStringPredicate(registry, "ends_with", EvaluateEndsWith);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "concat",
            MinArity = 1,
            MaxArity = int.MaxValue,
            Evaluator = EvaluateConcat,
            InferReturnType = _ => ColumnType.String,
        });
    }

    private static void RegisterUnaryString(
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

    private static void RegisterTernaryString(
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

    private static void RegisterBinaryStringPredicate(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 2,
            MaxArity = 2,
            Evaluator = evaluator,
            InferReturnType = _ => ColumnType.Bool,
        });
    }

    private static ColumnValue EvaluateLength(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        return new ColumnValue(ColumnType.Integer64, arguments[0].StrValue!.Length);
    }

    private static ColumnValue EvaluateLower(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.ToLowerInvariant());

    private static ColumnValue EvaluateUpper(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.ToUpperInvariant());

    private static ColumnValue EvaluateTrim(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.Trim());

    private static ColumnValue EvaluateLTrim(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.TrimStart());

    private static ColumnValue EvaluateRTrim(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.TrimEnd());

    private static ColumnValue EvaluateSubstring(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireInteger(calledName, 1, arguments[1]);

        long start = arguments[1].LongValue;

        if (start < 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects argument 2 to be a 1-based position greater than or equal to 1 but received {start}");
        }

        string input = arguments[0].StrValue!;
        long? length = null;

        if (arguments.Count == 3)
        {
            RequireInteger(calledName, 2, arguments[2]);
            length = arguments[2].LongValue;

            if (length < 0)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' expects argument 3 to be a non-negative length but received {length}");
            }
        }

        string result = ExtractSubstring(input, start, length);
        return new ColumnValue(ColumnType.String, result);
    }

    private static ColumnValue EvaluateReplace(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);
        RequireString(calledName, 2, arguments[2]);

        string result = arguments[0].StrValue!.Replace(
            arguments[1].StrValue!,
            arguments[2].StrValue!,
            StringComparison.Ordinal);

        return new ColumnValue(ColumnType.String, result);
    }

    private static ColumnValue EvaluateContains(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateStringPredicate(calledName, arguments, (left, right) => left.Contains(right, StringComparison.Ordinal));

    private static ColumnValue EvaluateStartsWith(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateStringPredicate(calledName, arguments, (left, right) => left.StartsWith(right, StringComparison.Ordinal));

    private static ColumnValue EvaluateEndsWith(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateStringPredicate(calledName, arguments, (left, right) => left.EndsWith(right, StringComparison.Ordinal));

    private static ColumnValue EvaluateConcat(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments.All(argument => argument.Type == ColumnType.Null))
            return ColumnValue.Null;

        System.Text.StringBuilder builder = new();

        foreach (ColumnValue argument in arguments)
        {
            if (argument.Type == ColumnType.Null)
                continue;

            builder.Append(ToSqlString(calledName, argument));
        }

        return new ColumnValue(ColumnType.String, builder.ToString());
    }

    private static ColumnValue TransformString(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        Func<string, string> transform)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        return new ColumnValue(ColumnType.String, transform(arguments[0].StrValue!));
    }

    private static ColumnValue EvaluateStringPredicate(
        string calledName,
        IReadOnlyList<ColumnValue> arguments,
        Func<string, string, bool> predicate)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        bool result = predicate(arguments[0].StrValue!, arguments[1].StrValue!);
        return ColumnValue.FromBool(result);
    }

    private static string ExtractSubstring(string input, long start, long? length)
    {
        if (start > input.Length)
            return "";

        int zeroBasedStart = (int)(start - 1);

        if (length is null)
            return input[zeroBasedStart..];

        if (length == 0)
            return "";

        int maxLength = input.Length - zeroBasedStart;
        int takeLength = (int)Math.Min(length.Value, maxLength);
        return input.Substring(zeroBasedStart, takeLength);
    }

    private static string ToSqlString(string calledName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.String or ColumnType.Id => value.StrValue ?? "",
            ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Bool => value.BoolValue ? "true" : "false",
            ColumnType.Null => "",
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' cannot convert argument of type {value.Type} to string"),
        };
    }

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static void RequireInteger(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Integer64);

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);
}
