
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
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The MySQL/SQLite-style JSON functions over text: <c>json_valid</c>, <c>json_type</c>,
/// <c>json_extract</c>, <c>json_value</c>, <c>json_contains</c> and <c>json_array_length</c>, with
/// <c>$.a.b[0]</c> paths. A <c>jsonb</c> argument is read as its text, so each function keeps one
/// behavior whatever the argument type; the PostgreSQL functions are in
/// <see cref="JsonbScalarFunctions"/>.
/// </summary>
internal static class JsonScalarFunctions
{
    /// <summary>
    /// The evaluator shape of these functions. <paramref name="documentMask"/> has bit <c>i</c> set
    /// when argument <c>i</c> was a <c>jsonb</c> value that arrives here as its text.
    /// </summary>
    private delegate ColumnValue JsonEvaluatorDelegate(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask);

    /// <summary>
    /// Wraps an evaluator so that a jsonb argument arrives as its PostgreSQL text. The evaluator gets
    /// a mask of the arguments that were documents: their text is parsed with the jsonb nesting limit
    /// (<see cref="JsonbLimits.MaxNestingDepth"/>), because a stored document can nest deeper than the
    /// default text limit of 64 levels. A STRING argument keeps that default limit, so its behavior
    /// does not change.
    /// </summary>
    private static ScalarFunctionEvaluatorDelegate AcceptDocuments(JsonEvaluatorDelegate evaluator) =>
        (calledName, arguments) =>
        {
            int documentMask = 0;
            for (int i = 0; i < arguments.Count; i++)
                if (arguments[i].Type == ColumnType.Jsonb)
                    documentMask |= 1 << i;

            if (documentMask == 0)
                return evaluator(calledName, arguments, 0);

            ColumnValue[] text = new ColumnValue[arguments.Count];
            for (int i = 0; i < arguments.Count; i++)
                text[i] = arguments[i].Type == ColumnType.Jsonb
                    ? new ColumnValue(ColumnType.String, arguments[i].JsonValue!)
                    : arguments[i];
            return evaluator(calledName, text, documentMask);
        };

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnary(registry, "json_valid", EvaluateJsonValid, _ => ColumnType.Bool);
        RegisterUnary(registry, "json_type", EvaluateJsonType, _ => ColumnType.String);
        RegisterBinary(registry, "json_extract", EvaluateJsonExtract, _ => ColumnType.String);
        // The result takes the type of the JSON scalar: INT64, FLOAT64, BOOL or STRING.
        RegisterBinary(registry, "json_value", EvaluateJsonValue, _ => ColumnType.String, resultTypeFromValue: true);
        RegisterBinary(registry, "json_contains", EvaluateJsonContains, _ => ColumnType.Bool);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "json_array_length",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = AcceptDocuments(EvaluateJsonArrayLength),
            InferReturnType = _ => ColumnType.Integer64,
        });
    }

    private static void RegisterUnary(
        ScalarFunctionRegistry registry,
        string name,
        JsonEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = AcceptDocuments(evaluator),
            InferReturnType = inferReturnType,
        });
    }

    private static void RegisterBinary(
        ScalarFunctionRegistry registry,
        string name,
        JsonEvaluatorDelegate evaluator,
        ScalarReturnTypeInferenceDelegate inferReturnType,
        bool resultTypeFromValue = false)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 2,
            MaxArity = 2,
            Evaluator = AcceptDocuments(evaluator),
            InferReturnType = inferReturnType,
            ResultTypeFromValue = resultTypeFromValue,
        });
    }

    private static ColumnValue EvaluateJsonValid(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (arguments[0].Type == ColumnType.Null)
            return ColumnValue.False;

        RequireString(calledName, 0, arguments[0]);

        return ColumnValue.FromBool(TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out _, out _));
    }

    private static ColumnValue EvaluateJsonType(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        if (!TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out JsonDocument? document, out JsonElement root))
            return ColumnValue.Null;

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
                return ColumnValue.Null;

            return new ColumnValue(ColumnType.String, typeName);
        }
    }

    private static ColumnValue EvaluateJsonExtract(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out JsonDocument? document, out JsonElement root))
            return ColumnValue.Null;

        using (document)
        {
            if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out JsonElement element, out bool found))
                return ColumnValue.Null;

            if (!found)
                return ColumnValue.Null;

            return new ColumnValue(ColumnType.String, element.GetRawText());
        }
    }

    private static ColumnValue EvaluateJsonValue(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out JsonDocument? document, out JsonElement root))
            return ColumnValue.Null;

        using (document)
        {
            if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out JsonElement element, out bool found))
                return ColumnValue.Null;

            if (!found)
                return ColumnValue.Null;

            return JsonElementToScalarColumnValue(element);
        }
    }

    private static ColumnValue EvaluateJsonArrayLength(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);

        if (arguments.Count == 2)
            RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out JsonDocument? document, out JsonElement root))
            return ColumnValue.Null;

        using (document)
        {
            JsonElement target = root;

            if (arguments.Count == 2)
            {
                if (!TryResolvePath(calledName, arguments[1].StrValue!, root, out target, out bool found))
                    return ColumnValue.Null;

                if (!found)
                    return ColumnValue.Null;
            }

            if (target.ValueKind != JsonValueKind.Array)
                return ColumnValue.Null;

            return new ColumnValue(ColumnType.Integer64, target.GetArrayLength());
        }
    }

    private static ColumnValue EvaluateJsonContains(string calledName, IReadOnlyList<ColumnValue> arguments, int documentMask)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        RequireString(calledName, 0, arguments[0]);
        RequireString(calledName, 1, arguments[1]);

        if (!TryParseJson(arguments[0].StrValue!, IsDocument(documentMask, 0), out JsonDocument? valueDocument, out JsonElement valueRoot))
            return ColumnValue.Null;

        if (!TryParseJson(arguments[1].StrValue!, IsDocument(documentMask, 1), out JsonDocument? candidateDocument, out JsonElement candidateRoot))
            return ColumnValue.Null;

        using (valueDocument)
        using (candidateDocument)
        {
            bool contains = JsonContains(valueRoot, candidateRoot);
            return ColumnValue.FromBool(contains);
        }
    }

    private static readonly JsonDocumentOptions DocumentParseOptions = new() { MaxDepth = JsonbLimits.MaxNestingDepth };

    private static bool IsDocument(int documentMask, int index) => (documentMask & (1 << index)) != 0;

    /// <summary>
    /// Parses one argument. The text of a jsonb argument uses the jsonb nesting limit: the document
    /// was already checked against it, so it is always valid here. Other text uses the default limit.
    /// </summary>
    private static bool TryParseJson(string json, bool isDocument, out JsonDocument? document, out JsonElement root)
    {
        try
        {
            document = isDocument ? JsonDocument.Parse(json, DocumentParseOptions) : JsonDocument.Parse(json);
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
            JsonValueKind.String => new ColumnValue(ColumnType.String, ReadJsonText(element)),
            JsonValueKind.True => ColumnValue.True,
            JsonValueKind.False => ColumnValue.False,
            JsonValueKind.Null => ColumnValue.Null,
            JsonValueKind.Number => ToNumericColumnValue(element),
            _ => ColumnValue.Null,
        };
    }

    /// <summary>
    /// The text of a JSON string. Text that a <c>json</c> function reads is not validated when it is stored,
    /// and the parser accepts a <c>\ud83d</c> escape with no pair, so the read itself fails with
    /// <see cref="InvalidOperationException"/>. That must reach the client as a SQL error.
    /// </summary>
    private static string ReadJsonText(JsonElement element)
    {
        try
        {
            return element.GetString()!;
        }
        catch (InvalidOperationException)
        {
            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText, "invalid input syntax for type json: invalid Unicode surrogate");
        }
    }

    private static ColumnValue ToNumericColumnValue(JsonElement element)
    {
        if (element.TryGetInt64(out long longValue))
            return new ColumnValue(ColumnType.Integer64, longValue);

        if (!element.TryGetDouble(out double doubleValue) || !IsFinite(doubleValue))
            return ColumnValue.Null;

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
            JsonValueKind.String => ReadJsonText(left) == ReadJsonText(right),
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
