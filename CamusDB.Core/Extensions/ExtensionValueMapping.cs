/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The one mapping between engine values (<see cref="ColumnValue"/>) and the plain CLR values that
/// extension code sees. Function adapters, trigger row accessors, procedure arguments and session
/// parameters all convert through here, so an extension author meets one set of rules everywhere.
///
/// <para>The mapping (both directions unless noted): Integer64 ↔ <c>long</c>; Float64 ↔
/// <c>double</c>; Float32 ↔ <c>float</c>; Bool ↔ <c>bool</c>; String ↔ <c>string</c>; Id →
/// <c>string</c>; Uuid ↔ <c>Guid</c>; Bytes ↔ <c>byte[]</c>; Date and DateTime → <c>DateTime</c>
/// (UTC), and <c>DateTime</c> → DateTime; Numeric ↔ <c>decimal</c>.</para>
///
/// <para><b>Arrays.</b> <c>array(E)</c> ↔ <c>IReadOnlyList&lt;T&gt;</c>, where <c>T</c> is the scalar
/// mapping of <c>E</c> (<c>IReadOnlyList&lt;long&gt;</c>, <c>IReadOnlyList&lt;string&gt;</c>). A NULL
/// element reaches a reference element as <c>null</c>, and a value element only when it is declared
/// <c>Nullable&lt;T&gt;</c> (<c>IReadOnlyList&lt;long?&gt;</c>); a NULL element in an
/// <c>IReadOnlyList&lt;long&gt;</c> parameter is an error that names the function and the argument.
/// Nested arrays and arrays of <c>byte[]</c> are not mapped: a nested mutable value would need a deep
/// copy at the boundary, and no use is known. The boxed form (<see cref="ToObject"/>, for
/// <c>IRow.GetValue</c> and procedure arguments) always uses nullable elements, because only the
/// value tells whether an element is NULL.</para>
///
/// <para><b>Numeric is never truncated.</b> NUMERIC is an <see cref="Int128"/> scaled by 10⁹
/// (precision 38) and <c>decimal</c> holds 28-29 significant digits, so a NUMERIC value whose digits
/// do not fit a <c>decimal</c> throws <see cref="CamusDBErrorCodes.ExtensionValueOutOfRange"/>
/// instead of silently losing digits. A <c>decimal</c> with more than nine fractional digits rounds
/// half away from zero, the same as text cast to NUMERIC.</para>
///
/// <para><b>Byte arrays are copied at the boundary, both ways.</b> A <c>byte[]</c> is the one mutable
/// value in the mapping. Handing extension code the engine's own array would let it change a stored
/// value, an OLD image, or the value an index comparison reads, without calling any setter; keeping
/// an array the extension returned would let it change the value after the call. So every
/// <c>byte[]</c> that leaves the engine is a copy, and every one that enters is copied. A list follows
/// the same rule: the engine passes a new read-only list and never its own element storage, and it
/// copies a returned list into an engine array value before the call returns.</para>
///
/// <para>The <c>To*</c> methods are called from compiled function adapters with the function name
/// and argument index, so a wrong argument type reports which argument of which function was
/// wrong.</para>
/// </summary>
internal static class ExtensionValueMapping
{
    private static readonly Int128 MaxDecimalMantissa = (Int128)ulong.MaxValue << 32 | uint.MaxValue;

    /// <summary>The CLR types a function parameter, a return value or a row accessor can use: a scalar
    /// type of the mapping, or a mapped array (<see cref="ArrayRefusal"/> is null).</summary>
    public static bool IsMappedType(Type type) =>
        IsMappedScalarType(type) || (ArrayElementOf(type) is not null && ArrayRefusal(type) is null);

    /// <summary>The scalar CLR types of the mapping, each also as <c>Nullable&lt;T&gt;</c>.</summary>
    public static bool IsMappedScalarType(Type type)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;

        return underlying == typeof(long) || underlying == typeof(double) || underlying == typeof(float) ||
               underlying == typeof(bool) || underlying == typeof(string) || underlying == typeof(Guid) ||
               underlying == typeof(byte[]) || underlying == typeof(DateTime) || underlying == typeof(decimal);
    }

    /// <summary>The element type <c>T</c> of an <c>IReadOnlyList&lt;T&gt;</c>, or null for any other type.</summary>
    public static Type? ArrayElementOf(Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>)
            ? type.GetGenericArguments()[0]
            : null;

    /// <summary>
    /// Why an <c>IReadOnlyList&lt;T&gt;</c> is not a mapped array, or null when it is one. The text
    /// completes "the type ... is refused: ", so the boot error names the member and the reason.
    /// </summary>
    public static string? ArrayRefusal(Type type)
    {
        Type? element = ArrayElementOf(type);

        if (element is null)
            return "it is not IReadOnlyList<T>";

        if (ArrayElementOf(element) is not null)
            return "nested arrays are not mapped";

        Type underlying = Nullable.GetUnderlyingType(element) ?? element;

        if (underlying == typeof(byte[]))
            return "arrays of byte[] are not mapped";

        if (!IsMappedScalarType(element))
            return $"the element type {element} is not in the extension value mapping";

        return null;
    }

    /// <summary>
    /// A readable reason for an unmapped type, for boot errors: the array rule when the type is an
    /// <c>IReadOnlyList&lt;T&gt;</c>, a hint for a CLR array of anything but bytes, and the plain
    /// "not in the mapping" otherwise.
    /// </summary>
    public static string DescribeUnmapped(Type type)
    {
        if (ArrayElementOf(type) is not null)
            return $"has type {type}, which is refused: {ArrayRefusal(type)}";

        if (type.IsArray && type != typeof(byte[]))
            return $"has type {type}, which is not in the extension value mapping; declare an array as IReadOnlyList<T>";

        return $"has type {type}, which is not in the extension value mapping";
    }

    /// <summary>The column type a CLR return type produces, for return-type inference.</summary>
    public static ColumnType ColumnTypeOf(Type type)
    {
        if (ArrayElementOf(type) is not null)
            return ColumnType.Array;

        Type underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying == typeof(long)) return ColumnType.Integer64;
        if (underlying == typeof(double)) return ColumnType.Float64;
        if (underlying == typeof(float)) return ColumnType.Float32;
        if (underlying == typeof(bool)) return ColumnType.Bool;
        if (underlying == typeof(string)) return ColumnType.String;
        if (underlying == typeof(Guid)) return ColumnType.Uuid;
        if (underlying == typeof(byte[])) return ColumnType.Bytes;
        if (underlying == typeof(DateTime)) return ColumnType.DateTime;
        if (underlying == typeof(decimal)) return ColumnType.Numeric;

        throw new ArgumentException($"Type {type} is not in the extension value mapping", nameof(type));
    }

    /// <summary>The element column type of a mapped array type, or null for a scalar type.</summary>
    public static ColumnType? ArrayElementColumnTypeOf(Type type) =>
        ArrayElementOf(type) is { } element ? ColumnTypeOf(element) : null;

    /// <summary>
    /// The SQL spelling of a mapped CLR type, for <c>SHOW EXTENSIONS</c> signatures:
    /// <c>INT64</c>, <c>STRING</c>, <c>ARRAY(INT64)</c>. A nullable type has no separate spelling,
    /// because every SQL value can be NULL.
    /// </summary>
    public static string SqlTypeName(Type type)
    {
        if (ArrayElementOf(type) is { } element)
            return "ARRAY(" + SqlTypeName(element) + ")";

        return ColumnTypeOf(type) switch
        {
            ColumnType.Integer64 => "INT64",
            ColumnType.Float64 => "FLOAT64",
            ColumnType.Float32 => "FLOAT32",
            ColumnType.Bool => "BOOL",
            ColumnType.String => "STRING",
            ColumnType.Uuid => "UUID",
            ColumnType.Bytes => "BYTES",
            ColumnType.DateTime => "DATETIME",
            ColumnType.Numeric => "NUMERIC",
            ColumnType other => other.ToString().ToUpperInvariant(),
        };
    }

    // ─── Engine value → CLR value (function arguments) ─────────────────────────

    public static long ToInt64(ColumnValue value, string function, int index)
    {
        Require(value, function, index, "Integer64", value.Type == ColumnType.Integer64);
        return value.LongValue;
    }

    public static double ToDouble(ColumnValue value, string function, int index) => value.Type switch
    {
        ColumnType.Float64 or ColumnType.Float32 => value.FloatValue,
        ColumnType.Integer64 => value.LongValue,
        ColumnType.Numeric => NumericMath.ToDouble(value.NumericUnscaled),
        _ => throw WrongType(value, function, index, "Float64, Float32, Integer64 or Numeric"),
    };

    public static float ToSingle(ColumnValue value, string function, int index) => value.Type switch
    {
        ColumnType.Float64 or ColumnType.Float32 => (float)value.FloatValue,
        ColumnType.Integer64 => value.LongValue,
        _ => throw WrongType(value, function, index, "Float32, Float64 or Integer64"),
    };

    public static bool ToBoolean(ColumnValue value, string function, int index)
    {
        Require(value, function, index, "Bool", value.Type == ColumnType.Bool);
        return value.BoolValue;
    }

    public static string ToStringValue(ColumnValue value, string function, int index)
    {
        Require(value, function, index, "String or Id", value.Type is ColumnType.String or ColumnType.Id);
        return value.StrValue!;
    }

    /// <summary>Accepts a Uuid, or a String in a UUID form, because SQL text often spells a UUID as a
    /// string literal.</summary>
    public static Guid ToGuid(ColumnValue value, string function, int index) => value.Type switch
    {
        ColumnType.Uuid => value.ToGuid(),
        ColumnType.String when Guid.TryParse(value.StrValue, out Guid parsed) => parsed,
        _ => throw WrongType(value, function, index, "Uuid"),
    };

    public static byte[] ToBytes(ColumnValue value, string function, int index)
    {
        Require(value, function, index, "Bytes", value.Type == ColumnType.Bytes);
        return CopyBytes(value.BytesValue!);
    }

    public static DateTime ToDateTime(ColumnValue value, string function, int index)
    {
        Require(value, function, index, "Date or DateTime", value.Type is ColumnType.Date or ColumnType.DateTime);
        return new DateTime(value.LongValue, DateTimeKind.Utc);
    }

    /// <summary>Accepts Numeric and Integer64, which convert exactly. A Float64 does not, so it is
    /// refused rather than converted with a silent rounding.</summary>
    public static decimal ToDecimal(ColumnValue value, string function, int index) => value.Type switch
    {
        ColumnType.Numeric => UnscaledToDecimal(value.NumericUnscaled),
        ColumnType.Integer64 => value.LongValue,
        _ => throw WrongType(value, function, index, "Numeric or Integer64"),
    };

    public static long? ToNullableInt64(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToInt64(value, function, index);

    public static double? ToNullableDouble(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToDouble(value, function, index);

    public static float? ToNullableSingle(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToSingle(value, function, index);

    public static bool? ToNullableBoolean(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToBoolean(value, function, index);

    public static string? ToNullableString(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToStringValue(value, function, index);

    public static Guid? ToNullableGuid(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToGuid(value, function, index);

    public static byte[]? ToNullableBytes(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToBytes(value, function, index);

    public static DateTime? ToNullableDateTime(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToDateTime(value, function, index);

    public static decimal? ToNullableDecimal(ColumnValue value, string function, int index) =>
        value.Type == ColumnType.Null ? null : ToDecimal(value, function, index);

    /// <summary>
    /// Converts an array argument into a new read-only list. Each element goes through the scalar
    /// converter <paramref name="element"/>, so the element rules are the scalar rules (an Integer64
    /// element reaches an <c>IReadOnlyList&lt;double&gt;</c> as a double). A NULL element is refused
    /// unless <paramref name="nullElements"/> says the element type accepts it.
    /// </summary>
    public static IReadOnlyList<T> ToList<T>(
        ColumnValue value, string function, int index, Func<ColumnValue, string, int, T> element, bool nullElements)
    {
        Require(value, function, index, "Array", value.Type == ColumnType.Array);

        IReadOnlyList<ColumnValue> items = value.ArrayValues ?? [];

        if (items.Count == 0)
            return Array.Empty<T>();

        T[] copy = new T[items.Count];

        for (int i = 0; i < copy.Length; i++)
        {
            ColumnValue item = items[i];

            if (item.Type == ColumnType.Null && !nullElements)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{function}' argument {index + 1} has a NULL element at position {i + 1}; " +
                    "declare the parameter with a nullable element type (IReadOnlyList<T?>) to accept NULL elements");

            copy[i] = element(item, function, index);
        }

        return new System.Collections.ObjectModel.ReadOnlyCollection<T>(copy);
    }

    /// <summary><see cref="ToList{T}"/> for a parameter that accepts a NULL array as <c>null</c>.</summary>
    public static IReadOnlyList<T>? ToNullableList<T>(
        ColumnValue value, string function, int index, Func<ColumnValue, string, int, T> element, bool nullElements) =>
        value.Type == ColumnType.Null ? null : ToList(value, function, index, element, nullElements);

    // ─── CLR value → engine value (return values, NEW rows, parameters) ────────

    public static ColumnValue FromInt64(long value) => new(ColumnType.Integer64, value);

    public static ColumnValue FromDouble(double value) => new(ColumnType.Float64, value);

    public static ColumnValue FromSingle(float value) => new(ColumnType.Float32, value);

    public static ColumnValue FromBoolean(bool value) => ColumnValue.FromBool(value);

    public static ColumnValue FromString(string? value) =>
        value is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, value);

    public static ColumnValue FromGuid(Guid value) => ColumnValue.FromUuid(value);

    public static ColumnValue FromBytes(byte[]? value) => value is null ? ColumnValue.Null : new ColumnValue(CopyBytes(value));

    /// <summary>A copy of an array that crosses the extension boundary; see the class summary.</summary>
    public static byte[] CopyBytes(byte[] value) => value.Length == 0 ? [] : (byte[])value.Clone();

    public static ColumnValue FromDateTime(DateTime value) =>
        new(ColumnType.DateTime, (value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime()).Ticks);

    public static ColumnValue FromDecimal(decimal value) => ColumnValue.FromNumeric(DecimalToUnscaled(value));

    public static ColumnValue FromNullableInt64(long? value) => value is { } v ? FromInt64(v) : ColumnValue.Null;

    public static ColumnValue FromNullableDouble(double? value) => value is { } v ? FromDouble(v) : ColumnValue.Null;

    public static ColumnValue FromNullableSingle(float? value) => value is { } v ? FromSingle(v) : ColumnValue.Null;

    public static ColumnValue FromNullableBoolean(bool? value) => value is { } v ? FromBoolean(v) : ColumnValue.Null;

    public static ColumnValue FromNullableGuid(Guid? value) => value is { } v ? FromGuid(v) : ColumnValue.Null;

    public static ColumnValue FromNullableDateTime(DateTime? value) => value is { } v ? FromDateTime(v) : ColumnValue.Null;

    public static ColumnValue FromNullableDecimal(decimal? value) => value is { } v ? FromDecimal(v) : ColumnValue.Null;

    /// <summary>
    /// Copies a returned list into an engine array value with element type
    /// <paramref name="elementType"/>; <c>null</c> is SQL NULL. The copy is made before the call
    /// returns, so a later change to the list by the extension has no effect.
    /// </summary>
    public static ColumnValue FromList<T>(IReadOnlyList<T>? list, ColumnType elementType, Func<T, ColumnValue> element)
    {
        if (list is null)
            return ColumnValue.Null;

        int count = list.Count;
        ColumnValue[] values = count == 0 ? [] : new ColumnValue[count];

        for (int i = 0; i < count; i++)
            values[i] = element(list[i]);

        return ColumnValue.FromArray(elementType, values);
    }

    /// <summary>
    /// Converts a boxed CLR value of the mapping (a procedure argument, a session parameter, a value a
    /// trigger set) into an engine value. <c>null</c> is SQL NULL. Small integer types widen to
    /// <c>long</c> because a caller that writes <c>["n"] = 5</c> passes an <c>int</c>. A list of a mapped
    /// element type (<c>long[]</c>, <c>List&lt;string&gt;</c>, <c>IReadOnlyList&lt;long?&gt;</c>) becomes an
    /// array value; the element type comes from the list's type, not from its values.
    /// </summary>
    public static ColumnValue FromObject(object? value) => value switch
    {
        null => ColumnValue.Null,
        long v => FromInt64(v),
        int v => FromInt64(v),
        short v => FromInt64(v),
        byte v => FromInt64(v),
        double v => FromDouble(v),
        float v => FromSingle(v),
        bool v => FromBoolean(v),
        string v => FromString(v),
        Guid v => FromGuid(v),
        byte[] v => FromBytes(v),
        DateTime v => FromDateTime(v),
        decimal v => FromDecimal(v),
        IReadOnlyList<long> v => FromList(v, ColumnType.Integer64, FromInt64),
        IReadOnlyList<long?> v => FromList(v, ColumnType.Integer64, FromNullableInt64),
        IReadOnlyList<double> v => FromList(v, ColumnType.Float64, FromDouble),
        IReadOnlyList<double?> v => FromList(v, ColumnType.Float64, FromNullableDouble),
        IReadOnlyList<float> v => FromList(v, ColumnType.Float32, FromSingle),
        IReadOnlyList<float?> v => FromList(v, ColumnType.Float32, FromNullableSingle),
        IReadOnlyList<bool> v => FromList(v, ColumnType.Bool, FromBoolean),
        IReadOnlyList<bool?> v => FromList(v, ColumnType.Bool, FromNullableBoolean),
        IReadOnlyList<string?> v => FromList(v, ColumnType.String, FromString),
        IReadOnlyList<Guid> v => FromList(v, ColumnType.Uuid, FromGuid),
        IReadOnlyList<Guid?> v => FromList(v, ColumnType.Uuid, FromNullableGuid),
        IReadOnlyList<DateTime> v => FromList(v, ColumnType.DateTime, FromDateTime),
        IReadOnlyList<DateTime?> v => FromList(v, ColumnType.DateTime, FromNullableDateTime),
        IReadOnlyList<decimal> v => FromList(v, ColumnType.Numeric, FromDecimal),
        IReadOnlyList<decimal?> v => FromList(v, ColumnType.Numeric, FromNullableDecimal),
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Type {value.GetType().FullName} is not a supported extension value"),
    };

    /// <summary>
    /// Converts an engine value into the boxed CLR value of the mapping, for a procedure argument or
    /// <see cref="CamusDB.Extensions.IRow.GetValue"/>. An array becomes a new read-only list with
    /// nullable elements (<c>IReadOnlyList&lt;long?&gt;</c>, <c>IReadOnlyList&lt;string?&gt;</c>); an empty
    /// array whose element type is unknown becomes an empty <c>IReadOnlyList&lt;object?&gt;</c>.
    /// </summary>
    public static object? ToObject(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => null,
        ColumnType.Integer64 => value.LongValue,
        ColumnType.Float64 => value.FloatValue,
        ColumnType.Float32 => (float)value.FloatValue,
        ColumnType.Bool => value.BoolValue,
        ColumnType.String or ColumnType.Id => value.StrValue,
        ColumnType.Uuid => value.ToGuid(),
        ColumnType.Bytes => value.BytesValue is { } bytes ? CopyBytes(bytes) : null,
        ColumnType.Date or ColumnType.DateTime => new DateTime(value.LongValue, DateTimeKind.Utc),
        ColumnType.Numeric => UnscaledToDecimal(value.NumericUnscaled),
        ColumnType.Array => ToObjectList(value),
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"A {value.Type} value has no extension value mapping"),
    };

    private static object ToObjectList(ColumnValue value)
    {
        IReadOnlyList<ColumnValue> items = value.ArrayValues ?? [];

        return value.ArrayElementType switch
        {
            ColumnType.Integer64 => Elements(items, static v => (long?)v.LongValue),
            ColumnType.Float64 => Elements(items, static v => (double?)v.FloatValue),
            ColumnType.Float32 => Elements(items, static v => (float?)(float)v.FloatValue),
            ColumnType.Bool => Elements(items, static v => (bool?)v.BoolValue),
            ColumnType.String or ColumnType.Id => Elements(items, static v => v.StrValue),
            ColumnType.Uuid => Elements(items, static v => (Guid?)v.ToGuid()),
            ColumnType.Date or ColumnType.DateTime => Elements(items, static v => (DateTime?)new DateTime(v.LongValue, DateTimeKind.Utc)),
            ColumnType.Numeric => Elements(items, static v => (decimal?)UnscaledToDecimal(v.NumericUnscaled)),
            ColumnType.Null when items.Count == 0 => Array.Empty<object?>(),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"An array of {value.ArrayElementType} has no extension value mapping"),
        };

        static IReadOnlyList<T?> Elements<T>(IReadOnlyList<ColumnValue> items, Func<ColumnValue, T?> read)
        {
            T?[] copy = new T?[items.Count];

            for (int i = 0; i < copy.Length; i++)
                copy[i] = items[i].Type == ColumnType.Null ? default : read(items[i]);

            return new System.Collections.ObjectModel.ReadOnlyCollection<T?>(copy);
        }
    }

    // ─── Numeric ↔ decimal ─────────────────────────────────────────────────────

    /// <summary>
    /// The NUMERIC unscaled form (value × 10⁹) of a decimal. Every decimal is inside NUMERIC's range
    /// (|decimal| &lt; 7.93·10²⁸ &lt; 10²⁹), and its mantissa times 10⁹ fits an <see cref="Int128"/>, so
    /// only rounding of digits past the ninth fractional place can change the value.
    /// </summary>
    public static Int128 DecimalToUnscaled(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);

        Int128 mantissa = ((Int128)(uint)bits[2] << 64) | ((Int128)(uint)bits[1] << 32) | (uint)bits[0];
        int scale = (bits[3] >> 16) & 0xFF;
        bool negative = bits[3] < 0;

        Int128 unscaled;

        if (scale <= 9)
            unscaled = mantissa * Pow10(9 - scale);
        else
        {
            Int128 divisor = Pow10(scale - 9);
            (Int128 quotient, Int128 remainder) = Int128.DivRem(mantissa, divisor);

            // Half away from zero: the magnitude rounds up at exactly half.
            if (remainder * 2 >= divisor)
                quotient++;

            unscaled = quotient;
        }

        return negative ? -unscaled : unscaled;
    }

    /// <summary>
    /// The decimal of a NUMERIC unscaled value. Trailing zero digits are dropped to fit the 96-bit
    /// mantissa; a value whose remaining digits still do not fit throws
    /// <see cref="CamusDBErrorCodes.ExtensionValueOutOfRange"/>, because dropping a non-zero digit
    /// would change the value.
    /// </summary>
    public static decimal UnscaledToDecimal(Int128 unscaled)
    {
        bool negative = unscaled < 0;
        Int128 magnitude = negative ? -unscaled : unscaled;
        int scale = 9;

        while (magnitude > MaxDecimalMantissa)
        {
            if (scale == 0 || magnitude % 10 != 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionValueOutOfRange,
                    $"NUMERIC value {NumericMath.Format(unscaled)} does not fit a .NET decimal without losing digits");

            magnitude /= 10;
            scale--;
        }

        int lo = (int)(uint)(magnitude & uint.MaxValue);
        int mid = (int)(uint)((magnitude >> 32) & uint.MaxValue);
        int hi = (int)(uint)((magnitude >> 64) & uint.MaxValue);

        return new decimal(lo, mid, hi, negative, (byte)scale);
    }

    private static Int128 Pow10(int exponent)
    {
        Int128 result = 1;

        for (int i = 0; i < exponent; i++)
            result *= 10;

        return result;
    }

    private static void Require(ColumnValue value, string function, int index, string expected, bool ok)
    {
        if (!ok)
            throw WrongType(value, function, index, expected);
    }

    private static CamusDBException WrongType(ColumnValue value, string function, int index, string expected) =>
        new(CamusDBErrorCodes.InvalidInput,
            $"Function '{function}' expects argument {index + 1} of type {expected} but received {value.Type}");
}
