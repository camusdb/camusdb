
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text.Json;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class JsonScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnary(registry, "json_valid", EvaluateJsonValid, _ => ColumnType.Bool);
        RegisterUnary(registry, "json_type", EvaluateJsonType, _ => ColumnType.String);
        RegisterBinary(registry, "json_extract", EvaluateJsonExtract, _ => ColumnType.String);
        RegisterBinary(registry, "json_value", EvaluateJsonValue, _ => ColumnType.String);
        RegisterBinary(registry, "json_contains", EvaluateJsonContains, _ => ColumnType.Bool);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "json_array_length",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = EvaluateJsonArrayLength,
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

    private static ColumnValue EvaluateJsonValid(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null)
            return new ColumnValue(ColumnType.Bool, false);

        RequireString(calledName, 0, arguments[0]);

        return new ColumnValue(ColumnType.Bool, TryParseJson(arguments[0].StrValue!, out _, out _));
    }

    private static ColumnValue EvaluateJsonType(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        if (!TryParseJson(arguments[0].StrValue!, out JsonDocument? document, out JsonElement root))
            return new ColumnValue(ColumnType.Null, 0);

        using (document)
        {
            string? typeName = root.ValueKind switch
            {
                JsonValueKind.Object => "object",
                JsonValueKind.Array => "array",
                JsonValueKind.String => "string",
                JsonValueKind.Number => "number",
                JsonValueKind.True or JsonValueKind.False => "boolean",
                JsonValueKind.Null => "null",
                _ => null,
            };

            if (typeName is null)
                return new ColumnValue(ColumnType.Null, 0);

            return new ColumnValue(ColumnType.String, typeName);
        }
    }

    private static ColumnValue EvaluateJsonExtract(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, out JsonDocument? document, out JsonElement root))
            return new ColumnValue(ColumnType.Null, 0);

        using (document)
        {
            if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out JsonElement element, out bool found))
                return new ColumnValue(ColumnType.Null, 0);

            if (!found)
                return new ColumnValue(ColumnType.Null, 0);

            return new ColumnValue(ColumnType.String, element.GetRawText());
        }
    }

    private static ColumnValue EvaluateJsonValue(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, out JsonDocument? document, out JsonElement root))
            return new ColumnValue(ColumnType.Null, 0);

        using (document)
        {
            if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out JsonElement element, out bool found))
                return new ColumnValue(ColumnType.Null, 0);

            if (!found)
                return new ColumnValue(ColumnType.Null, 0);

            return JsonElementToScalarColumnValue(element);
        }
    }

    private static ColumnValue EvaluateJsonArrayLength(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        if (arguments.Count == 2)
            RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, out JsonDocument? document, out JsonElement root))
            return new ColumnValue(ColumnType.Null, 0);

        using (document)
        {
            JsonElement target = root;

            if (arguments.Count == 2)
            {
                if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out target, out bool found))
                    return new ColumnValue(ColumnType.Null, 0);

                if (!found)
                    return new ColumnValue(ColumnType.Null, 0);
            }

            if (target.ValueKind != JsonValueKind.Array)
                return new ColumnValue(ColumnType.Null, 0);

            return new ColumnValue(ColumnType.Integer64, target.GetArrayLength());
        }
    }

    private static ColumnValue EvaluateJsonContains(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, out JsonDocument? valueDocument, out JsonElement valueRoot))
            return new ColumnValue(ColumnType.Null, 0);

        if (!TryParseJson(arguments[1].StrValue!, out JsonDocument? candidateDocument, out JsonElement candidateRoot))
            return new ColumnValue(ColumnType.Null, 0);

        using (valueDocument)
        using (candidateDocument)
        {
            bool contains = JsonContains(valueRoot, candidateRoot);
            return new ColumnValue(ColumnType.Bool, contains);
        }
    }

    private static bool TryParseJson(string json, out JsonDocument? document, out JsonElement root)
    {
        try
        {
            document = JsonDocument.Parse(json);
            root = document.RootElement;
            return true;
        }
        catch (JsonException)
        {
            document = null;
            root = default;
            return false;
        }
    }

    private static bool TryResolvePath(
        string calledName,
        string path,
        JsonElement root,
        out JsonElement element,
        out bool found)
    {
        element = default;
        found = false;

        if (path == "$")
        {
            element = root;
            found = true;
            return true;
        }

        if (!path.StartsWith("$", StringComparison.Ordinal))
            throw UnsupportedPath(calledName, path);

        JsonElement current = root;
        int index = 1;

        while (index < path.Length)
        {
            char token = path[index];

            if (token == '.')
            {
                index++;
                if (!TryReadPropertyName(path, ref index, out string propertyName))
                    throw UnsupportedPath(calledName, path);

                if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(propertyName, out current))
                    return true;

                continue;
            }

            if (token == '[')
            {
                index++;
                if (!TryReadArrayIndex(path, ref index, out int arrayIndex))
                    throw UnsupportedPath(calledName, path);

                if (current.ValueKind != JsonValueKind.Array || arrayIndex >= current.GetArrayLength())
                    return true;

                current = current[arrayIndex];
                continue;
            }

            throw UnsupportedPath(calledName, path);
        }

        element = current;
        found = true;
        return true;
    }

    private static bool TryReadPropertyName(string path, ref int index, out string propertyName)
    {
        propertyName = "";

        int start = index;
        while (index < path.Length)
        {
            char current = path[index];
            if (current == '.' || current == '[')
                break;

            if (!char.IsLetterOrDigit(current) && current != '_')
                return false;

            index++;
        }

        if (start == index)
            return false;

        propertyName = path[start..index];
        return true;
    }

    private static bool TryReadArrayIndex(string path, ref int index, out int arrayIndex)
    {
        arrayIndex = 0;

        int start = index;
        while (index < path.Length && char.IsDigit(path[index]))
            index++;

        if (start == index || index >= path.Length || path[index] != ']')
            return false;

        index++;

        if (!int.TryParse(path[start..(index - 1)], NumberStyles.None, CultureInfo.InvariantCulture, out arrayIndex))
            return false;

        return true;
    }

    private static ColumnValue JsonElementToScalarColumnValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => new ColumnValue(ColumnType.String, element.GetString()!),
            JsonValueKind.True => new ColumnValue(ColumnType.Bool, true),
            JsonValueKind.False => new ColumnValue(ColumnType.Bool, false),
            JsonValueKind.Null => new ColumnValue(ColumnType.Null, 0),
            JsonValueKind.Number => ToNumericColumnValue(element),
            _ => new ColumnValue(ColumnType.Null, 0),
        };
    }

    private static ColumnValue ToNumericColumnValue(JsonElement element)
    {
        if (element.TryGetInt64(out long longValue))
            return new ColumnValue(ColumnType.Integer64, longValue);

        if (!element.TryGetDouble(out double doubleValue) || !IsFinite(doubleValue))
            return new ColumnValue(ColumnType.Null, 0);

        return new ColumnValue(ColumnType.Float64, doubleValue);
    }

    private static bool IsFinite(double value)
        => !double.IsNaN(value) && !double.IsInfinity(value);

    private static bool JsonContains(JsonElement value, JsonElement candidate)
    {
        if (candidate.ValueKind != value.ValueKind)
            return false;

        return candidate.ValueKind switch
        {
            JsonValueKind.Object => ObjectContains(value, candidate),
            JsonValueKind.Array => ArrayContains(value, candidate),
            _ => JsonElementsEqual(value, candidate),
        };
    }

    private static bool JsonElementsEqual(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
            return false;

        return left.ValueKind switch
        {
            JsonValueKind.String => left.GetString() == right.GetString(),
            JsonValueKind.Number => NumbersEqual(left, right),
            JsonValueKind.True or JsonValueKind.False => left.GetBoolean() == right.GetBoolean(),
            JsonValueKind.Null => true,
            _ => false,
        };
    }

    private static bool NumbersEqual(JsonElement left, JsonElement right)
    {
        if (left.TryGetInt64(out long leftLong) && right.TryGetInt64(out long rightLong))
            return leftLong == rightLong;

        if (left.TryGetDouble(out double leftDouble) && right.TryGetDouble(out double rightDouble))
            return leftDouble == rightDouble;

        return false;
    }

    private static bool ObjectContains(JsonElement value, JsonElement candidate)
    {
        foreach (JsonProperty candidateProperty in candidate.EnumerateObject())
        {
            if (!value.TryGetProperty(candidateProperty.Name, out JsonElement valueProperty))
                return false;

            if (!JsonContains(valueProperty, candidateProperty.Value))
                return false;
        }

        return true;
    }

    private static bool ArrayContains(JsonElement value, JsonElement candidate)
    {
        foreach (JsonElement candidateItem in candidate.EnumerateArray())
        {
            bool matched = false;

            foreach (JsonElement valueItem in value.EnumerateArray())
            {
                if (JsonContains(valueItem, candidateItem))
                {
                    matched = true;
                    break;
                }
            }

            if (!matched)
                return false;
        }

        return true;
    }

    private static CamusDBException UnsupportedPath(string calledName, string path)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' unsupported JSON path '{path}'");

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
        => ScalarFunctionArguments.PropagateNull(arguments);
}
