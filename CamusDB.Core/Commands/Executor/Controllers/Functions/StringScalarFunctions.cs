
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
/// The text functions. <c>length</c>, <c>substring</c>, <c>trim</c> / <c>ltrim</c> / <c>rtrim</c>,
/// <c>strpos</c>, <c>contains</c>, <c>starts_with</c> and <c>ends_with</c> also take <c>bytes</c>, as in
/// PostgreSQL. A <c>bytes</c> call counts and slices bytes, and each function that returns a part of its
/// first argument (<c>substring</c>, the trims) returns the type of that argument, so the inferred type
/// reads the argument type.
///
/// <para>A call that mixes the two types (<c>contains(bytes, string)</c>) is an error: there is no
/// implicit conversion between text and bytes. Use <c>convert_to</c> / <c>convert_from</c>.</para>
///
/// <para>Text positions and lengths count UTF-16 code units, the same as <c>length</c>. A character
/// outside the Basic Multilingual Plane therefore counts as two.</para>
/// </summary>
internal static class StringScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnaryString(registry, "length", EvaluateLength, _ => ColumnType.Integer64);
        RegisterUnaryString(registry, "lower", EvaluateLower, _ => ColumnType.String);
        RegisterUnaryString(registry, "upper", EvaluateUpper, _ => ColumnType.String);
        RegisterTrim(registry, "trim", TrimSide.Both);
        RegisterTrim(registry, "ltrim", TrimSide.Start);
        RegisterTrim(registry, "rtrim", TrimSide.End);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "substring",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = EvaluateSubstring,
            InferReturnType = SameTypeAsFirstArgument,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "strpos",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = EvaluateStrpos,
            InferReturnType = _ => ColumnType.Integer64,
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

    private enum TrimSide
    {
        Both,
        Start,
        End,
    }

    /// <summary>
    /// The trims take an optional second argument, the set of characters (or bytes) to remove. Without
    /// it a string loses its white space and a <c>bytes</c> value loses its <c>\x00</c> bytes.
    /// </summary>
    private static void RegisterTrim(ScalarFunctionRegistry registry, string name, TrimSide side)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => EvaluateTrim(calledName, arguments, side),
            InferReturnType = SameTypeAsFirstArgument,
        });
    }

    /// <summary>
    /// The return type of a function that returns a part of its first argument: <c>bytes</c> for a
    /// <c>bytes</c> argument and <c>string</c> for every other argument, including an unknown type.
    /// </summary>
    private static ColumnType SameTypeAsFirstArgument(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 && argumentTypes[0] == ColumnType.Bytes ? ColumnType.Bytes : ColumnType.String;

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

        ColumnValue input = arguments[0];

        if (input.Type == ColumnType.Bytes)
            return new ColumnValue(ColumnType.Integer64, input.BytesValue?.Length ?? 0);

        RequireString(calledName, 0, input);
        return new ColumnValue(ColumnType.Integer64, input.StrValue!.Length);
    }

    private static ColumnValue EvaluateLower(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.ToLowerInvariant());

    private static ColumnValue EvaluateUpper(string calledName, IReadOnlyList<ColumnValue> arguments)
        => TransformString(calledName, arguments, value => value.ToUpperInvariant());

    private static ColumnValue EvaluateTrim(string calledName, IReadOnlyList<ColumnValue> arguments, TrimSide side)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ColumnValue input = arguments[0];
        RequireStringOrBytes(calledName, 0, input);

        if (arguments.Count == 2)
            ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], input.Type);

        if (input.Type == ColumnType.Bytes)
        {
            ReadOnlySpan<byte> value = input.BytesValue;
            ReadOnlySpan<byte> set = arguments.Count == 2 ? arguments[1].BytesValue : [0];

            ReadOnlySpan<byte> trimmed = side switch
            {
                TrimSide.Start => value.TrimStart(set),
                TrimSide.End => value.TrimEnd(set),
                _ => value.Trim(set),
            };

            // A value with nothing to remove is returned as it is; bytes values are never changed in
            // place, so the result can share the input array.
            return trimmed.Length == value.Length ? input : new ColumnValue(trimmed.ToArray());
        }

        string text = input.StrValue!;

        if (arguments.Count == 1)
        {
            return new ColumnValue(ColumnType.String, side switch
            {
                TrimSide.Start => text.TrimStart(),
                TrimSide.End => text.TrimEnd(),
                _ => text.Trim(),
            });
        }

        // An empty set removes nothing, as in PostgreSQL. It must return early: string.Trim and the
        // char span overloads both remove white space when they get an empty set.
        ReadOnlySpan<char> chars = arguments[1].StrValue;

        if (chars.IsEmpty)
            return input;

        ReadOnlySpan<char> trimmedText = side switch
        {
            TrimSide.Start => text.AsSpan().TrimStart(chars),
            TrimSide.End => text.AsSpan().TrimEnd(chars),
            _ => text.AsSpan().Trim(chars),
        };

        return trimmedText.Length == text.Length ? input : new ColumnValue(ColumnType.String, trimmedText.ToString());
    }

    private static ColumnValue EvaluateSubstring(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireStringOrBytes(calledName, 0, arguments[0]);
        RequireInteger(calledName, 1, arguments[1]);

        long start = arguments[1].LongValue;

        if (start < 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects argument 2 to be a 1-based position greater than or equal to 1 but received {start}");
        }

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

        if (arguments[0].Type == ColumnType.Bytes)
        {
            byte[] bytes = arguments[0].BytesValue ?? [];
            (int offset, int count) = SliceBounds(bytes.Length, start, length);
            return new ColumnValue(bytes.AsSpan(offset, count).ToArray());
        }

        string input = arguments[0].StrValue!;
        (int textOffset, int textCount) = SliceBounds(input.Length, start, length);
        return new ColumnValue(ColumnType.String, input.Substring(textOffset, textCount));
    }

    /// <summary>
    /// <c>strpos(haystack, needle)</c>: the 1-based position of the first <c>needle</c> in
    /// <c>haystack</c>, or 0 when there is none. An empty needle is found at position 1, as in
    /// PostgreSQL. <c>POSITION(needle IN haystack)</c> parses to this call.
    /// </summary>
    private static ColumnValue EvaluateStrpos(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireStringOrBytes(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], arguments[0].Type);

        int index = arguments[0].Type == ColumnType.Bytes
            ? arguments[0].BytesValue.AsSpan().IndexOf(arguments[1].BytesValue.AsSpan())
            : arguments[0].StrValue!.IndexOf(arguments[1].StrValue!, StringComparison.Ordinal);

        return new ColumnValue(ColumnType.Integer64, index + 1);
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

    private enum MatchKind
    {
        Contains,
        StartsWith,
        EndsWith,
    }

    private static ColumnValue EvaluateContains(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateMatch(calledName, arguments, MatchKind.Contains);

    private static ColumnValue EvaluateStartsWith(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateMatch(calledName, arguments, MatchKind.StartsWith);

    private static ColumnValue EvaluateEndsWith(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateMatch(calledName, arguments, MatchKind.EndsWith);

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

    private static ColumnValue EvaluateMatch(string calledName, IReadOnlyList<ColumnValue> arguments, MatchKind kind)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireStringOrBytes(calledName, 0, arguments[0]);
        ScalarFunctionArguments.RequireType(calledName, 1, arguments[1], arguments[0].Type);

        bool result;

        if (arguments[0].Type == ColumnType.Bytes)
        {
            ReadOnlySpan<byte> value = arguments[0].BytesValue;
            ReadOnlySpan<byte> part = arguments[1].BytesValue;

            result = kind switch
            {
                MatchKind.StartsWith => value.StartsWith(part),
                MatchKind.EndsWith => value.EndsWith(part),
                _ => value.IndexOf(part) >= 0,
            };
        }
        else
        {
            string value = arguments[0].StrValue!;
            string part = arguments[1].StrValue!;

            result = kind switch
            {
                MatchKind.StartsWith => value.StartsWith(part, StringComparison.Ordinal),
                MatchKind.EndsWith => value.EndsWith(part, StringComparison.Ordinal),
                _ => value.Contains(part, StringComparison.Ordinal),
            };
        }

        return ColumnValue.FromBool(result);
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

    /// <summary>
    /// The 0-based offset and the count of a 1-based <c>substring</c> slice of a value of
    /// <paramref name="valueLength"/> units. A start past the end, or a zero length, gives an empty
    /// slice; a length past the end stops at the end.
    /// </summary>
    private static (int offset, int count) SliceBounds(int valueLength, long start, long? length)
    {
        if (start > valueLength)
            return (valueLength, 0);

        int offset = (int)(start - 1);
        int available = valueLength - offset;

        if (length is null)
            return (offset, available);

        return (offset, (int)Math.Min(length.Value, available));
    }

    private static string ToSqlString(string calledName, ColumnValue value)
    {
        if (!ScalarTextForm.TryFormat(value, out string text))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' cannot convert argument of type {value.Type} to string");

        return text;
    }

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static void RequireStringOrBytes(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.String, ColumnType.Bytes);

    private static void RequireInteger(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Integer64);

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);
}
