/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The PostgreSQL <c>jsonb</c> functions, and the functions behind the jsonb operators: the parser
/// turns <c>-&gt;</c>, <c>-&gt;&gt;</c>, <c>#&gt;</c>, <c>#&gt;&gt;</c>, <c>@&gt;</c>, <c>&lt;@</c>,
/// <c>?</c>, <c>?|</c>, <c>?&amp;</c> and <c>#-</c> into calls of the functions registered here, as
/// PostgreSQL defines them. <c>||</c> and <c>-</c> stay operators and reach
/// <see cref="JsonbBuilder"/> from their evaluators.
///
/// <para>Every function is strict, as in PostgreSQL: a SQL NULL argument gives a SQL NULL result,
/// except where a function documents another rule (<c>jsonb_build_*</c> turn a NULL into JSON null;
/// <c>jsonb_set_lax</c> has a treatment for a NULL value).</para>
///
/// <para>A text argument where a document belongs is parsed as a document. PostgreSQL resolves an
/// untyped literal (<c>doc @&gt; '{"a": 1}'</c>) that way. CamusDB cannot tell an untyped literal from
/// a text value at run time, so a text column in that position is parsed too, where PostgreSQL would
/// refuse the operator.</para>
///
/// <para>A path (<c>#&gt;</c>, <c>#-</c>, <c>jsonb_set</c>, ...) is an array of strings or the
/// PostgreSQL text form of one (<c>'{a,b,0}'</c>). Each <c>json_*</c> spelling that PostgreSQL has is
/// an alias.</para>
/// </summary>
internal static class JsonbScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        // The operator functions.
        Register(registry, "jsonb_object_field", 2, 2, ObjectField, ColumnType.Jsonb);
        Register(registry, "jsonb_object_field_text", 2, 2, ObjectFieldText, ColumnType.String);
        Register(registry, "jsonb_path_get", 2, 2, PathGet, ColumnType.Jsonb);
        Register(registry, "jsonb_path_get_text", 2, 2, PathGetText, ColumnType.String);
        Register(registry, "jsonb_contains", 2, 2, Contains, ColumnType.Bool);
        Register(registry, "jsonb_contained", 2, 2, Contained, ColumnType.Bool);
        Register(registry, "jsonb_exists", 2, 2, Exists, ColumnType.Bool);
        Register(registry, "jsonb_exists_any", 2, 2, ExistsAny, ColumnType.Bool);
        Register(registry, "jsonb_exists_all", 2, 2, ExistsAll, ColumnType.Bool);
        Register(registry, "jsonb_delete_path", 2, 2, DeletePath, ColumnType.Jsonb);

        // Constructors.
        Register(registry, "to_jsonb", 1, 1, ToJsonb, ColumnType.Jsonb, "to_json");
        Register(registry, "jsonb_build_object", 0, int.MaxValue, BuildObject, ColumnType.Jsonb, "json_build_object");
        Register(registry, "jsonb_build_array", 0, int.MaxValue, BuildArray, ColumnType.Jsonb, "json_build_array");
        Register(registry, "jsonb_object", 1, 2, ObjectFromArrays, ColumnType.Jsonb, "json_object");

        // Inspection.
        Register(registry, "jsonb_typeof", 1, 1, TypeOf, ColumnType.String, "json_typeof");
        Register(registry, "jsonb_array_length", 1, 1, ArrayLength, ColumnType.Integer64);
        Register(registry, "jsonb_extract_path", 2, int.MaxValue, ExtractPath, ColumnType.Jsonb, "json_extract_path");
        Register(registry, "jsonb_extract_path_text", 2, int.MaxValue, ExtractPathText, ColumnType.String, "json_extract_path_text");

        // Changes.
        Register(registry, "jsonb_set", 3, 4, Set, ColumnType.Jsonb);
        Register(registry, "jsonb_set_lax", 3, 5, SetLax, ColumnType.Jsonb);
        Register(registry, "jsonb_insert", 3, 4, Insert, ColumnType.Jsonb);
        Register(registry, "jsonb_strip_nulls", 1, 1, StripNulls, ColumnType.Jsonb, "json_strip_nulls");
        Register(registry, "jsonb_pretty", 1, 1, Pretty, ColumnType.String);
    }

    private static void Register(
        ScalarFunctionRegistry registry, string name, int minArity, int maxArity,
        ScalarFunctionEvaluatorDelegate evaluator, ColumnType returnType, params string[] aliases)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            Aliases = aliases,
            MinArity = minArity,
            MaxArity = maxArity,
            Evaluator = evaluator,
            InferReturnType = _ => returnType,
        });
    }

    // ---------------------------------------------------------------------------------------------
    // Argument helpers
    // ---------------------------------------------------------------------------------------------

    private static bool AnyNull(IReadOnlyList<ColumnValue> arguments)
    {
        for (int i = 0; i < arguments.Count; i++)
            if (arguments[i].Type == ColumnType.Null)
                return true;
        return false;
    }

    /// <summary>The cell of a document argument; text parses as a document.</summary>
    private static byte[] Document(string function, ColumnValue value, int index) => value.Type switch
    {
        ColumnType.Jsonb => value.BytesValue!,
        ColumnType.String => JsonbEncoder.FromText(value.StrValue!),
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
            $"Function '{function}' argument {index + 1} must be jsonb, got {value.Type}"),
    };

    private static ReadOnlySpan<byte> Item(byte[] cell) => JsonbDocument.Item(cell);

    private static ColumnValue Slice(ReadOnlySpan<byte> item, Range range) =>
        ColumnValue.FromJsonbCell(JsonbEncoder.ToCell(item[range]));

    private static string RequireText(string function, ColumnValue value, int index) =>
        value.Type == ColumnType.String
            ? value.StrValue!
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Function '{function}' argument {index + 1} must be a string, got {value.Type}");

    /// <summary>
    /// A path or key-list argument: an array of strings, or the PostgreSQL text form of one
    /// (<c>'{a,b}'</c>). A NULL element is kept as null; each caller decides what it means.
    /// </summary>
    internal static List<string?> TextArray(string function, ColumnValue value, int index)
    {
        if (value.Type == ColumnType.String)
            return PostgreSqlTextArray.Parse(value.StrValue!);

        if (value.Type == ColumnType.Array)
        {
            List<string?> items = new(value.ArrayValues?.Count ?? 0);
            foreach (ColumnValue element in value.ArrayValues ?? [])
            {
                if (element.Type == ColumnType.Null)
                    items.Add(null);
                else if (ScalarTextForm.TryFormat(element, out string text))
                    items.Add(text);
                else
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Function '{function}' argument {index + 1} must be an array of strings");
            }
            return items;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
            $"Function '{function}' argument {index + 1} must be a text array, got {value.Type}");
    }

    /// <summary>A path for a change: a NULL element is an error, as in PostgreSQL.</summary>
    private static List<string> ChangePath(string function, ColumnValue value, int index)
    {
        List<string?> raw = TextArray(function, value, index);
        List<string> path = new(raw.Count);
        for (int i = 0; i < raw.Count; i++)
            path.Add(raw[i] ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"path element at position {i + 1} is null"));
        return path;
    }

    private static bool OptionalBool(string function, IReadOnlyList<ColumnValue> arguments, int index, bool fallback)
    {
        if (arguments.Count <= index)
            return fallback;

        ColumnValue value = arguments[index];
        return value.Type == ColumnType.Bool
            ? value.BoolValue
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Function '{function}' argument {index + 1} must be a boolean, got {value.Type}");
    }

    private static ColumnValue Result(byte[] cell) => ColumnValue.FromJsonbCell(cell);

    // ---------------------------------------------------------------------------------------------
    // Operator functions
    // ---------------------------------------------------------------------------------------------

    /// <summary><c>-&gt;</c>: a key of an object (text) or an element of an array (integer).</summary>
    private static ColumnValue ObjectField(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return TryStep(function, Item(cell), arguments[1], out Range range)
            ? Slice(Item(cell), range)
            : ColumnValue.Null;
    }

    /// <summary><c>-&gt;&gt;</c>: as <c>-&gt;</c>, then the text of the value (a JSON null is SQL NULL).</summary>
    private static ColumnValue ObjectFieldText(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return TryStep(function, Item(cell), arguments[1], out Range range)
            ? SqlText(Item(cell)[range])
            : ColumnValue.Null;
    }

    private static bool TryStep(string function, ReadOnlySpan<byte> item, ColumnValue step, out Range range)
    {
        switch (step.Type)
        {
            case ColumnType.String:
                return JsonbDocument.TryGetField(item, step.StrValue!, out range);
            case ColumnType.Integer64:
                return JsonbDocument.TryGetElement(item, step.LongValue, out range);
            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{function}' takes a text key or an integer index, got {step.Type}");
        }
    }

    private static ColumnValue SqlText(ReadOnlySpan<byte> item)
    {
        string? text = JsonbDocument.ToSqlText(item);
        return text is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, text);
    }

    /// <summary><c>#&gt;</c>: the value at a path; a step that does not match gives NULL.</summary>
    private static ColumnValue PathGet(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return TryPath(TextArray(function, arguments[1], 1), Item(cell), out Range range)
            ? Slice(Item(cell), range)
            : ColumnValue.Null;
    }

    /// <summary><c>#&gt;&gt;</c>: as <c>#&gt;</c>, then the text of the value.</summary>
    private static ColumnValue PathGetText(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return TryPath(TextArray(function, arguments[1], 1), Item(cell), out Range range)
            ? SqlText(Item(cell)[range])
            : ColumnValue.Null;
    }

    private static bool TryPath(List<string?> path, ReadOnlySpan<byte> item, out Range range)
    {
        range = default;
        List<string> steps = new(path.Count);
        foreach (string? step in path)
        {
            if (step is null)
                return false;
            steps.Add(step);
        }

        return JsonbDocument.TryGetPath(item, steps, out range);
    }

    private static ColumnValue Contains(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        return ColumnValue.FromBool(JsonbDocument.Contains(
            Item(Document(function, arguments[0], 0)), Item(Document(function, arguments[1], 1))));
    }

    private static ColumnValue Contained(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        return ColumnValue.FromBool(JsonbDocument.Contains(
            Item(Document(function, arguments[1], 1)), Item(Document(function, arguments[0], 0))));
    }

    private static ColumnValue Exists(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return ColumnValue.FromBool(JsonbDocument.Exists(Item(cell), Encoding.UTF8.GetBytes(RequireText(function, arguments[1], 1))));
    }

    private static ColumnValue ExistsAny(string function, IReadOnlyList<ColumnValue> arguments) =>
        ExistsMany(function, arguments, any: true);

    private static ColumnValue ExistsAll(string function, IReadOnlyList<ColumnValue> arguments) =>
        ExistsMany(function, arguments, any: false);

    private static ColumnValue ExistsMany(string function, IReadOnlyList<ColumnValue> arguments, bool any)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        foreach (string? key in TextArray(function, arguments[1], 1))
        {
            // PostgreSQL skips a NULL key in both forms.
            if (key is null)
                continue;

            bool found = JsonbDocument.Exists(Item(cell), Encoding.UTF8.GetBytes(key));
            if (any && found)
                return ColumnValue.True;
            if (!any && !found)
                return ColumnValue.False;
        }

        return ColumnValue.FromBool(!any);
    }

    private static ColumnValue DeletePath(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return Result(JsonbBuilder.DeletePath(Item(cell), ChangePath(function, arguments[1], 1)));
    }

    // ---------------------------------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------------------------------

    private static ColumnValue ToJsonb(string function, IReadOnlyList<ColumnValue> arguments) =>
        arguments[0].Type == ColumnType.Null ? ColumnValue.Null : Result(JsonbEncoder.ToCell(ToItem(arguments[0])));

    /// <summary>
    /// The JSON item of a SQL value, as PostgreSQL <c>to_jsonb</c> builds it: numbers stay exact, a
    /// float takes its shortest round-trip text (NaN and the infinities become strings, as in
    /// PostgreSQL), a date is <c>"yyyy-MM-dd"</c>, a datetime the ISO form with <c>+00:00</c> (the
    /// PostgreSQL output of a <c>timestamptz</c> under UTC), bytes the <c>"\x…"</c> hex text, an array a
    /// JSON array, a document itself, and SQL NULL a JSON null.
    /// </summary>
    internal static byte[] ToItem(ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.Null:
                return JsonbBuilder.NullItem();

            case ColumnType.Jsonb:
                return Item(value.BytesValue!).ToArray();

            case ColumnType.Bool:
                return JsonbBuilder.BooleanItem(value.BoolValue);

            case ColumnType.Integer64:
                return JsonbBuilder.NumberItem(new JsonbNumber(value.LongValue, 0));

            case ColumnType.Float64:
            case ColumnType.Float32:
            {
                double number = value.Type == ColumnType.Float32 ? (float)value.FloatValue : value.FloatValue;
                if (double.IsNaN(number))
                    return JsonbBuilder.StringItem("NaN");
                if (double.IsInfinity(number))
                    return JsonbBuilder.StringItem(number > 0 ? "Infinity" : "-Infinity");

                string text = value.Type == ColumnType.Float32
                    ? ((float)number).ToString("R", CultureInfo.InvariantCulture)
                    : number.ToString("R", CultureInfo.InvariantCulture);
                return JsonbBuilder.NumberItem(JsonbNumber.ParseJsonText(Encoding.ASCII.GetBytes(text)));
            }

            case ColumnType.Numeric:
                return JsonbBuilder.NumberItem(JsonbNumber.ParseJsonText(Encoding.ASCII.GetBytes(value.NumericValue!)));

            case ColumnType.DateTime:
                return JsonbBuilder.StringItem(FormatDateTime(value.LongValue));

            case ColumnType.Array:
            {
                List<byte[]> items = new(value.ArrayValues?.Count ?? 0);
                foreach (ColumnValue element in value.ArrayValues ?? [])
                    items.Add(ToItem(element));
                return Item(JsonbBuilder.Array(items)).ToArray();
            }

            default:
                if (ScalarTextForm.TryFormat(value, out string scalarText))
                    return JsonbBuilder.StringItem(scalarText);

                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"cannot convert {value.Type} to jsonb");
        }
    }

    /// <summary>
    /// PostgreSQL's JSON text of a <c>timestamptz</c> under UTC: <c>2026-01-02T03:04:05+00:00</c>, with
    /// the fraction of a second only when there is one, at most six digits, trailing zeros removed.
    /// </summary>
    private static string FormatDateTime(long ticks)
    {
        DateTime moment = new(ticks, DateTimeKind.Utc);
        string text = moment.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture);
        long microseconds = (ticks % TimeSpan.TicksPerSecond) / 10;
        if (microseconds != 0)
            text += "." + microseconds.ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0');
        return text + "+00:00";
    }

    private static ColumnValue BuildObject(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments.Count % 2 != 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Function '{function}': argument list must have even number of elements");

        List<KeyValuePair<string, byte[]>> pairs = new(arguments.Count / 2);
        for (int i = 0; i < arguments.Count; i += 2)
        {
            ColumnValue key = arguments[i];
            if (key.Type == ColumnType.Null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{function}': argument {i + 1}: key must not be null");
            if (!ScalarTextForm.TryFormat(key, out string keyText) || key.Type == ColumnType.Jsonb)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{function}': key value must be scalar, not array, composite, or json");

            pairs.Add(new(keyText, ToItem(arguments[i + 1])));
        }

        return Result(JsonbBuilder.Object(pairs));
    }

    private static ColumnValue BuildArray(string function, IReadOnlyList<ColumnValue> arguments)
    {
        List<byte[]> items = new(arguments.Count);
        foreach (ColumnValue argument in arguments)
            items.Add(ToItem(argument));
        return Result(JsonbBuilder.Array(items));
    }

    /// <summary>
    /// PostgreSQL <c>jsonb_object(text[])</c> (alternating keys and values) and
    /// <c>jsonb_object(keys text[], values text[])</c>. Values become JSON strings; a NULL value is
    /// JSON null; a NULL key is an error.
    /// </summary>
    private static ColumnValue ObjectFromArrays(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        List<string?> keys;
        List<string?> values;
        if (arguments.Count == 1)
        {
            List<string?> flat = TextArray(function, arguments[0], 0);
            if (flat.Count % 2 != 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "array must have even number of elements");
            keys = [];
            values = [];
            for (int i = 0; i < flat.Count; i += 2)
            {
                keys.Add(flat[i]);
                values.Add(flat[i + 1]);
            }
        }
        else
        {
            keys = TextArray(function, arguments[0], 0);
            values = TextArray(function, arguments[1], 1);
            if (keys.Count != values.Count)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "mismatched array dimensions");
        }

        List<KeyValuePair<string, byte[]>> pairs = new(keys.Count);
        for (int i = 0; i < keys.Count; i++)
        {
            string key = keys[i] ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                "null value not allowed for object key");
            pairs.Add(new(key, values[i] is { } text ? JsonbBuilder.StringItem(text) : JsonbBuilder.NullItem()));
        }

        return Result(JsonbBuilder.Object(pairs));
    }

    // ---------------------------------------------------------------------------------------------
    // Inspection
    // ---------------------------------------------------------------------------------------------

    private static ColumnValue TypeOf(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        return new ColumnValue(ColumnType.String, JsonbDocument.TypeName(JsonbDocument.Kind(Item(cell))));
    }

    private static ColumnValue ArrayLength(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        return ArrayLengthOf(Document(function, arguments[0], 0));
    }

    /// <summary>The PostgreSQL <c>jsonb_array_length</c> of a cell, with its two errors.</summary>
    internal static ColumnValue ArrayLengthOf(byte[] cell)
    {
        JsonbKind kind = JsonbDocument.Kind(Item(cell));
        if (kind == JsonbKind.Object)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "cannot get array length of a non-array");
        if (kind != JsonbKind.Array)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "cannot get array length of a scalar");

        return new ColumnValue(ColumnType.Integer64, JsonbDocument.ArrayLength(Item(cell)));
    }

    private static ColumnValue ExtractPath(string function, IReadOnlyList<ColumnValue> arguments) =>
        ExtractPathCore(function, arguments, text: false);

    private static ColumnValue ExtractPathText(string function, IReadOnlyList<ColumnValue> arguments) =>
        ExtractPathCore(function, arguments, text: true);

    /// <summary>
    /// <c>jsonb_extract_path(doc, VARIADIC text)</c>: each argument after the document is one path
    /// step, so <c>'{a,b}'</c> here is one key with that name, not a path.
    /// </summary>
    private static ColumnValue ExtractPathCore(string function, IReadOnlyList<ColumnValue> arguments, bool text)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        List<string> steps = new(arguments.Count - 1);
        for (int i = 1; i < arguments.Count; i++)
        {
            if (!ScalarTextForm.TryFormat(arguments[i], out string step))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{function}' argument {i + 1} must be a string");
            steps.Add(step);
        }

        if (!JsonbDocument.TryGetPath(Item(cell), steps, out Range range))
            return ColumnValue.Null;

        return text ? SqlText(Item(cell)[range]) : Slice(Item(cell), range);
    }

    // ---------------------------------------------------------------------------------------------
    // Changes
    // ---------------------------------------------------------------------------------------------

    private static ColumnValue Set(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        List<string> path = ChangePath(function, arguments[1], 1);
        byte[] value = Document(function, arguments[2], 2);
        bool createMissing = OptionalBool(function, arguments, 3, fallback: true);

        return Result(JsonbBuilder.Set(Item(cell), path, Item(value), createMissing));
    }

    /// <summary>
    /// PostgreSQL <c>jsonb_set_lax</c>: as <c>jsonb_set</c>, but a SQL NULL value follows
    /// <c>null_value_treatment</c>: <c>use_json_null</c> (the default) sets a JSON null,
    /// <c>delete_key</c> removes the path, <c>return_target</c> returns the document unchanged and
    /// <c>raise_exception</c> fails.
    /// </summary>
    private static ColumnValue SetLax(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null || arguments[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        for (int i = 3; i < arguments.Count; i++)
            if (arguments[i].Type == ColumnType.Null)
                return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        List<string> path = ChangePath(function, arguments[1], 1);
        bool createMissing = OptionalBool(function, arguments, 3, fallback: true);

        if (arguments[2].Type != ColumnType.Null)
            return Result(JsonbBuilder.Set(Item(cell), path, Item(Document(function, arguments[2], 2)), createMissing));

        string treatment = arguments.Count > 4 ? RequireText(function, arguments[4], 4) : "use_json_null";
        return treatment switch
        {
            "use_json_null" => Result(JsonbBuilder.Set(Item(cell), path, JsonbBuilder.NullItem(), createMissing)),
            "delete_key" => Result(JsonbBuilder.DeletePath(Item(cell), path)),
            "return_target" => arguments[0].Type == ColumnType.Jsonb ? arguments[0] : Result(cell),
            "raise_exception" => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "JSON value must not be null"),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\""),
        };
    }

    private static ColumnValue Insert(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        byte[] cell = Document(function, arguments[0], 0);
        List<string> path = ChangePath(function, arguments[1], 1);
        byte[] value = Document(function, arguments[2], 2);
        bool insertAfter = OptionalBool(function, arguments, 3, fallback: false);

        return Result(JsonbBuilder.Insert(Item(cell), path, Item(value), insertAfter));
    }

    private static ColumnValue StripNulls(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        return Result(JsonbBuilder.StripNulls(Item(Document(function, arguments[0], 0))));
    }

    private static ColumnValue Pretty(string function, IReadOnlyList<ColumnValue> arguments)
    {
        if (AnyNull(arguments))
            return ColumnValue.Null;

        return new ColumnValue(ColumnType.String, JsonbText.ToText(Item(Document(function, arguments[0], 0)), pretty: true));
    }

    // ---------------------------------------------------------------------------------------------
    // The || and - operators
    // ---------------------------------------------------------------------------------------------

    /// <summary>True when <c>||</c> or <c>-</c> applies its jsonb meaning to this pair.</summary>
    internal static bool IsJsonbOperation(ColumnValue left, ColumnValue right) =>
        left.Type == ColumnType.Jsonb || right.Type == ColumnType.Jsonb;

    /// <summary><c>jsonb || jsonb</c>; a text operand parses as a document.</summary>
    internal static ColumnValue Concat(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        return Result(JsonbBuilder.Concat(Item(Document("||", left, 0)), Item(Document("||", right, 1))));
    }

    /// <summary>
    /// <c>jsonb - text</c> (a key), <c>jsonb - integer</c> (an array index) and <c>jsonb - text[]</c>
    /// (several keys).
    /// </summary>
    internal static ColumnValue Delete(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (left.Type != ColumnType.Jsonb)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Operator - has no signature for {left.Type} and {right.Type}");

        ReadOnlySpan<byte> item = Item(left.BytesValue!);
        return right.Type switch
        {
            ColumnType.String => Result(JsonbBuilder.DeleteKeys(item, [right.StrValue!])),
            ColumnType.Integer64 => Result(JsonbBuilder.DeleteIndex(item, right.LongValue)),
            ColumnType.Array => Result(JsonbBuilder.DeleteKeys(item, TextArray("-", right, 1).Where(k => k is not null).Select(k => k!).ToList())),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Operator - has no signature for jsonb and {right.Type}"),
        };
    }
}

/// <summary>
/// Reads the PostgreSQL text form of a one-dimensional text array, <c>{a,"b c",NULL}</c>: the form a
/// path takes in <c>doc #&gt; '{a,b}'</c> and <c>jsonb_set(doc, '{a,b}', …)</c>. An element may be
/// double-quoted (with backslash escapes inside); an unquoted element is trimmed, and the unquoted
/// word NULL is a null element. A nested array or any other shape is
/// <see cref="CamusDBErrorCodes.InvalidInput"/> ("malformed array literal").
/// </summary>
internal static class PostgreSqlTextArray
{
    public static List<string?> Parse(string text)
    {
        string trimmed = text.Trim();
        if (trimmed.Length < 2 || trimmed[0] != '{' || trimmed[^1] != '}')
            throw Malformed(text, "Array value must start with \"{\" or dimension information.");

        List<string?> items = [];
        string body = trimmed[1..^1];
        if (body.Trim().Length == 0)
            return items;

        int i = 0;
        while (true)
        {
            while (i < body.Length && char.IsWhiteSpace(body[i]))
                i++;

            if (i < body.Length && body[i] == '"')
            {
                StringBuilder value = new();
                i++;
                while (true)
                {
                    if (i >= body.Length)
                        throw Malformed(text, "Unexpected end of input.");
                    char c = body[i++];
                    if (c == '"')
                        break;
                    if (c == '\\')
                    {
                        if (i >= body.Length)
                            throw Malformed(text, "Unexpected end of input.");
                        c = body[i++];
                    }
                    value.Append(c);
                }

                items.Add(value.ToString());
                while (i < body.Length && char.IsWhiteSpace(body[i]))
                    i++;
            }
            else
            {
                StringBuilder value = new();
                bool escaped = false;
                while (i < body.Length && body[i] != ',')
                {
                    char c = body[i++];
                    if (c is '{' or '}' or '"')
                        throw Malformed(text, "Unexpected \"" + c + "\" character.");
                    if (c == '\\')
                    {
                        if (i >= body.Length)
                            throw Malformed(text, "Unexpected end of input.");
                        c = body[i++];
                        escaped = true;
                    }
                    value.Append(c);
                }

                string element = value.ToString().Trim();
                if (element.Length == 0)
                    throw Malformed(text, "Unexpected \",\" character.");

                items.Add(!escaped && string.Equals(element, "NULL", StringComparison.OrdinalIgnoreCase) ? null : element);
            }

            if (i >= body.Length)
                return items;

            if (body[i] != ',')
                throw Malformed(text, "Unexpected array element.");
            i++;
        }
    }

    private static CamusDBException Malformed(string text, string detail) =>
        new(CamusDBErrorCodes.InvalidInput, $"malformed array literal: \"{text}\" ({detail})");
}
