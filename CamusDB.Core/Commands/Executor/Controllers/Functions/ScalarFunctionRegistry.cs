
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal sealed class ScalarFunctionRegistry
{
    private readonly Dictionary<string, ScalarFunctionDescriptor> functions = new(StringComparer.OrdinalIgnoreCase);

    public void Register(ScalarFunctionDescriptor descriptor)
    {
        foreach (string name in descriptor.AllNames)
        {
            if (!functions.TryAdd(name, descriptor))
                throw new InvalidOperationException($"Scalar function '{name}' is already registered");
        }
    }

    public bool TryGet(string functionName, out ScalarFunctionDescriptor descriptor)
    {
        return functions.TryGetValue(functionName, out descriptor!);
    }

    public bool IsRegisteredScalarFunction(string functionName)
    {
        return functions.ContainsKey(functionName);
    }

    public ColumnType InferReturnType(string functionName, IReadOnlyList<ColumnType> argumentTypes)
    {
        return !TryGet(functionName, out ScalarFunctionDescriptor? descriptor) ? ColumnType.String : descriptor.InferReturnType(argumentTypes);
    }

    /// <summary>Every descriptor in the registry, once each, whatever the number of its names.</summary>
    internal IEnumerable<ScalarFunctionDescriptor> Descriptors => functions.Values.Distinct();

    /// <summary>
    /// The built-in functions whose result depends on their arguments only: the same arguments give the
    /// same value in every row, statement, session, node and build. An expression index stores the
    /// value of its expression in the key of each entry, so it may call only these
    /// (<c>IndexExpressionBinder</c>). Keyed by <see cref="ScalarFunctionDescriptor.Name"/>; an alias
    /// counts through its descriptor.
    ///
    /// <para>The list is opt-in. A function that is not on it is refused in an index expression, and
    /// that includes every extension and inline function. A new built-in must go on this list or on
    /// <see cref="NotImmutableFunctionNames"/>: a test fails while it is on neither, so nobody adds a
    /// function that reads the clock, a session or a random source to an index by mistake. A function
    /// that reads the clock only for one arity (<c>age</c>, <c>unix_timestamp</c>) is not on the list.</para>
    /// </summary>
    internal static readonly IReadOnlySet<string> ImmutableFunctionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        // Arrays
        "cardinality", "array_length", "array_contains",
        // Bytes
        "encode", "decode", "hex", "unhex", "to_base64", "from_base64", "convert_to", "convert_from",
        "get_byte", "set_byte", "get_bit", "set_bit", "bit_length", "bit_count",
        // Casts
        "to_string", "to_int64", "to_float64", "to_bool", "to_float32", "to_date", "to_datetime", "to_bytes", "to_id",
        // Dates and times (no session time zone exists: every value is UTC)
        "make_date", "make_timestamp", "makedate", "last_day",
        "to_char", "to_timestamp", "date_format", "str_to_date",
        "date_add", "date_sub", "date_diff", "date_part", "date_trunc", "from_unixtime",
        "year", "month", "day", "hour", "minute", "second", "quarter", "dayofyear", "dayofweek", "weekday",
        "dayname", "monthname", "week",
        "make_interval", "justify_days", "justify_hours", "justify_interval", "date_bin",
        // Comparison and NULL handling
        "greatest", "least", "coalesce", "ifnull", "nullif", "any", "some", "all",
        // Hashes
        "md5", "sha1", "sha256", "sha512", "digest", "hmac", "crc32",
        // JSON and jsonb
        "jsonb_object_field", "jsonb_object_field_text", "jsonb_path_get", "jsonb_path_get_text",
        "jsonb_contains", "jsonb_contained", "jsonb_exists", "jsonb_exists_any", "jsonb_exists_all",
        "jsonb_delete_path", "to_jsonb", "jsonb_build_object", "jsonb_build_array", "jsonb_object",
        "jsonb_typeof", "jsonb_array_length", "jsonb_extract_path", "jsonb_extract_path_text",
        "jsonb_set", "jsonb_set_lax", "jsonb_insert", "jsonb_strip_nulls", "jsonb_pretty",
        "json_valid", "json_type", "json_extract", "json_value", "json_contains", "json_array_length",
        // Math and trigonometry
        "abs", "ceil", "floor", "round", "trunc", "sqrt", "pow", "ln", "log10", "exp", "log", "mod", "sign",
        "sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2", "sind", "cosd", "tand", "cotd",
        "asind", "acosd", "atand", "atan2d", "radians", "degrees", "pi",
        // Regular expressions
        "regexp_like", "regexp_match", "regexp_replace", "regexp_count", "regexp_instr", "regexp_substr",
        "regexp_split_to_array", "regexp_matches",
        // Text (lower and upper use the invariant culture)
        "length", "lower", "upper", "trim", "ltrim", "rtrim", "substring", "substr", "strpos", "replace",
        "like_escape", "contains", "starts_with", "ends_with", "concat",
        // Vectors
        "octet_length", "vector_dims", "l2_distance", "inner_product", "cosine_distance",
    };

    /// <summary>
    /// The built-in functions that an index expression must not call: each reads the clock, the
    /// session, a sequence or a random source, or returns rows. Kept so that the classification test
    /// can tell a function that was judged from one that nobody judged.
    /// </summary>
    internal static readonly IReadOnlySet<string> NotImmutableFunctionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "current_timestamp", "current_date", "statement_timestamp", "clock_timestamp",
        "unix_timestamp", "unix_timestamp_ms", "age",
        "random", "gen_random_bytes", "gen_id", "gen_uuid_v4", "gen_uuid_v7",
        "nextval", "currval", "lastval", "setval",
        "current_database", "current_user", "current_role", "is_superuser",
        "regexp_split_to_table",
    };

    /// <summary>
    /// True when <paramref name="descriptor"/> is a built-in on <see cref="ImmutableFunctionNames"/>.
    /// The flags are checked as well, so a descriptor that is volatile or reads the clock or the
    /// session is refused even when its name is on the list by mistake.
    /// </summary>
    internal static bool IsImmutable(ScalarFunctionDescriptor descriptor) =>
        ImmutableFunctionNames.Contains(descriptor.Name)
        && !descriptor.IsVolatile
        && !descriptor.ReadsStatementClock
        && !descriptor.IsSessionScoped
        && descriptor.SessionEvaluator is null
        && descriptor.StatementEvaluator is null;

    public static ScalarFunctionRegistry CreateDefault()
    {
        ScalarFunctionRegistry registry = new();
        IdScalarFunctions.Register(registry);
        UuidScalarFunctions.Register(registry);
        DateTimeScalarFunctions.Register(registry);
        DateConstructorScalarFunctions.Register(registry);
        ClockScalarFunctions.Register(registry);
        IntervalScalarFunctions.Register(registry);
        DatePatternScalarFunctions.Register(registry);
        MathScalarFunctions.Register(registry);
        TrigonometricScalarFunctions.Register(registry);
        StringScalarFunctions.Register(registry);
        HashScalarFunctions.Register(registry);
        BytesScalarFunctions.Register(registry);
        VectorScalarFunctions.Register(registry);
        JsonScalarFunctions.Register(registry);
        JsonbScalarFunctions.Register(registry);
        CastScalarFunctions.Register(registry);
        NullScalarFunctions.Register(registry);
        ExtremumScalarFunctions.Register(registry);
        RegexScalarFunctions.Register(registry);
        SessionScalarFunctions.Register(registry);
        SequenceScalarFunctions.Register(registry);
        ArrayScalarFunctions.Register(registry);
        QuantifierScalarFunctions.Register(registry);
        return registry;
    }
}
