
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using System.Buffers.Text;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class CastScalarFunctions
{
    // Date formats accepted by CamusDB for date literals (yyyy-MM-dd).
    private const string DateFormat = "yyyy-MM-dd";

    // DateTime formats accepted by CamusDB for datetime literals (UTC only). The seven-digit form is
    // the text form a datetime casts to (ScalarTextForm), so a datetime round-trips through text. A
    // bare date is midnight UTC of that day, as in PostgreSQL, so `created >= '2026-01-01'` compares a
    // datetime column with the start of the day. It is last because it is the least common form.
    private static readonly string[] DateTimeFormats =
    [
        "yyyy-MM-ddTHH:mm:ssZ",
        "yyyy-MM-ddTHH:mm:ss.fffffffZ",
        "yyyy-MM-ddTHH:mm:ss.ffffffZ",
        "yyyy-MM-ddTHH:mm:ss.fffffZ",
        "yyyy-MM-ddTHH:mm:ss.ffffZ",
        "yyyy-MM-ddTHH:mm:ss.fffZ",
        "yyyy-MM-ddTHH:mm:ss.ffZ",
        "yyyy-MM-ddTHH:mm:ss.fZ",
        "yyyy-MM-ddTHH:mm:sszzz",
        "yyyy-MM-ddTHH:mm:ss",
        "yyyy-MM-dd",
    ];

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnaryCast(registry, "to_string",   ColumnType.String);
        RegisterUnaryCast(registry, "to_int64",    ColumnType.Integer64);
        RegisterUnaryCast(registry, "to_float64",  ColumnType.Float64);
        RegisterUnaryCast(registry, "to_bool",     ColumnType.Bool);
        RegisterUnaryCast(registry, "to_float32",  ColumnType.Float32);
        // to_date(x) is the cast to date. to_date(text, pattern) reads the text with a PostgreSQL
        // template pattern instead (DatePatternScalarFunctions).
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "to_date",
            MinArity = 1,
            MaxArity = 2,
            Evaluator = (calledName, arguments) => arguments.Count == 1
                ? CastValue(calledName, arguments[0], ColumnType.Date)
                : DatePatternScalarFunctions.EvaluateParseToDate(calledName, arguments),
            InferReturnType = _ => ColumnType.Date,
        });

        RegisterUnaryCast(registry, "to_datetime", ColumnType.DateTime);
        RegisterUnaryCast(registry, "to_bytes",    ColumnType.Bytes);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "to_id",
            Aliases = ["str_id"],
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateToId,
            InferReturnType = _ => ColumnType.Id,
        });
    }

    public static ColumnValue CastExpression(string castName, ColumnValue input, NodeAst targetTypeAst)
    {
        ColumnType targetType = ResolveTargetType(castName, targetTypeAst);
        ColumnValue result = CastValue(castName, input, targetType);

        return targetTypeAst.nodeType switch
        {
            NodeType.TypeInteger16 => IntegerWidth.EnforceOnCast(IntegerWidth.SmallIntBits, result),
            NodeType.TypeInteger32 => IntegerWidth.EnforceOnCast(IntegerWidth.IntBits, result),
            _ => result,
        };
    }

    public static ColumnType InferCastReturnType(NodeAst targetTypeAst)
        => ResolveTargetType("cast", targetTypeAst);

    private static void RegisterUnaryCast(
        ScalarFunctionRegistry registry,
        string name,
        ColumnType targetType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = (calledName, arguments) => CastValue(calledName, arguments[0], targetType),
            InferReturnType = _ => targetType,
        });
    }

    private static ColumnValue EvaluateToId(string calledName, IReadOnlyList<ColumnValue> arguments)
        => CastValue(calledName, arguments[0], ColumnType.Id);

    internal static ColumnValue CastValue(string castName, ColumnValue value, ColumnType targetType)
    {
        if (value.Type == ColumnType.Null)
            return ColumnValue.Null;

        return targetType switch
        {
            ColumnType.String    => CastToString(castName, value),
            ColumnType.Integer64 => CastToInt64(castName, value),
            ColumnType.Float64   => CastToFloat64(castName, value),
            ColumnType.Float32   => CastToFloat32(castName, value),
            ColumnType.Bool      => CastToBool(castName, value),
            ColumnType.Id        => CastToId(castName, value),
            ColumnType.Date      => CastToDate(castName, value),
            ColumnType.DateTime  => CastToDateTime(castName, value),
            ColumnType.Bytes     => CastToBytes(castName, value),
            ColumnType.Uuid      => CastToUuid(castName, value),
            ColumnType.Numeric   => CastToNumeric(castName, value),
            ColumnType.Jsonb     => CastToJsonb(castName, value),
            _ => throw UnknownTargetType(castName, targetType.ToString()),
        };
    }

    /// <summary>
    /// Coerces a value produced by EvalExpr to the declared column type.
    /// Only fires for the narrow set of implicit conversions the engine supports;
    /// all other combinations are passed through unchanged (type mismatches are
    /// caught by the row encoder or constraint layer).
    /// </summary>
    internal static ColumnValue CoerceToColumnType(ColumnValue value, TableColumnSchema column)
        => column.Type == ColumnType.Array && column.ArrayElementType is { } elementType
            ? CoerceArrayToElementType(value, elementType)
            : CoerceToColumnType(value, column.Type);

    /// <summary>
    /// Adapts an array value to a column's declared element type.
    ///
    /// <para>This overload exists because the type-only overload cannot do it: an array's target is
    /// <c>array(T)</c>, and <c>T</c> lives on the column schema, not in <see cref="ColumnType"/>. An
    /// <c>ARRAY[…]</c> literal infers its element type from its first non-NULL element, so
    /// <c>ARRAY[1, 2]</c> written into an <c>array(float64)</c> column arrives as Integer64 elements
    /// and has to widen — the same implicit conversion a scalar column would apply. An empty
    /// <c>ARRAY[]</c> arrives with element type <see cref="ColumnType.Null"/> ("unknown") and simply
    /// adopts the declared type here.</para>
    /// </summary>
    private static ColumnValue CoerceArrayToElementType(ColumnValue value, ColumnType elementType)
    {
        if (value.Type != ColumnType.Array)
            return value;

        IReadOnlyList<ColumnValue> elements = value.ArrayValues ?? [];

        if (elements.Count == 0)
            return ColumnValue.FromArray(elementType, []);

        if (value.ArrayElementType == elementType)
            return value;

        List<ColumnValue> coerced = new(elements.Count);

        foreach (ColumnValue element in elements)
            coerced.Add(CoerceToColumnType(element, elementType));

        return ColumnValue.FromArray(elementType, coerced);
    }

    /// <summary>
    /// Coerces a value to a target column type using the same narrow set of implicit conversions as
    /// the <see cref="TableColumnSchema"/> overload. Used where only the type is known — e.g. coercing
    /// a <c>DEFAULT('…')</c> string literal to the declared column type at DDL time, so a typed default
    /// (date/datetime/bytes/uuid) is stored in the column's type rather than as a raw String.
    /// </summary>
    internal static ColumnValue CoerceToColumnType(ColumnValue value, ColumnType columnType)
    {
        if (value.Type == ColumnType.Null || value.Type == columnType)
            return value;

        return (value.Type, columnType) switch
        {
            (ColumnType.String,    ColumnType.Id)       => CastToId("coerce", value),
            // Integer literals widen to the floating-point column type, so `price FLOAT64` accepts
            // `VALUES (100)` and `price FLOAT32` accepts `VALUES (100)` without an explicit CAST.
            (ColumnType.Integer64, ColumnType.Float64)  => CastToFloat64("coerce", value),
            (ColumnType.Float64,   ColumnType.Float32)  => CastToFloat32("coerce", value),
            (ColumnType.Integer64, ColumnType.Float32)  => CastToFloat32("coerce", value),
            // A float column accepts the float text form, as a PostgreSQL string literal is: this is
            // how 'Infinity', '-Infinity' and 'NaN' are written in VALUES, a DEFAULT or a text HTTP
            // parameter. A number past the range is FloatValueOutOfRange, as in CAST.
            (ColumnType.String,    ColumnType.Float64)  => CastToFloat64("coerce", value),
            (ColumnType.String,    ColumnType.Float32)  => CastToFloat32("coerce", value),
            (ColumnType.String,    ColumnType.Date)     => CastToDate("coerce", value),
            (ColumnType.String,    ColumnType.DateTime) => CastToDateTime("coerce", value),
            // A date widens to midnight UTC of that day, as in PostgreSQL and MySQL, so a datetime
            // column accepts make_date(...) or a date column. The reverse would drop the time of day,
            // so a datetime written into a date column is still refused.
            (ColumnType.Date,      ColumnType.DateTime) => CastToDateTime("coerce", value),
            (ColumnType.String,    ColumnType.Bytes)    => CastToBytes("coerce", value),
            (ColumnType.String,    ColumnType.Uuid)     => CastToUuid("coerce", value),
            // A NUMERIC column accepts an integer, a float (a literal converts from its source text)
            // and a string such as an HTTP parameter, as CAST does; NUMERIC widens to a float column.
            (ColumnType.Integer64, ColumnType.Numeric)  => CastToNumeric("coerce", value),
            (ColumnType.Float64,   ColumnType.Numeric)  => CastToNumeric("coerce", value),
            (ColumnType.Float32,   ColumnType.Numeric)  => CastToNumeric("coerce", value),
            (ColumnType.String,    ColumnType.Numeric)  => CastToNumeric("coerce", value),
            (ColumnType.Numeric,   ColumnType.Float64)  => CastToFloat64("coerce", value),
            (ColumnType.Numeric,   ColumnType.Float32)  => CastToFloat32("coerce", value),
            // A jsonb column accepts JSON text, as PostgreSQL accepts a string literal or a text
            // parameter there; text that is not a document is InvalidJsonText.
            (ColumnType.String,    ColumnType.Jsonb)    => CastToJsonb("coerce", value),
            _ => value,
        };
    }

    /// <summary>
    /// True when <see cref="CoerceToColumnType(ColumnValue, ColumnType)"/> converts a value of type
    /// <paramref name="from"/> to <paramref name="to"/>, and does not pass it through. The two lists must
    /// stay equal: the statement checks read this one to refuse a write before any row is read, and a pair
    /// missing here would refuse a correct statement. <c>TestStaticStatementChecks</c> compares them.
    /// </summary>
    internal static bool HasImplicitConversion(ColumnType from, ColumnType to) => (from, to) is
        (ColumnType.String, ColumnType.Id) or
        (ColumnType.Integer64, ColumnType.Float64) or
        (ColumnType.Float64, ColumnType.Float32) or
        (ColumnType.Integer64, ColumnType.Float32) or
        (ColumnType.String, ColumnType.Float64) or
        (ColumnType.String, ColumnType.Float32) or
        (ColumnType.String, ColumnType.Date) or
        (ColumnType.String, ColumnType.DateTime) or
        (ColumnType.Date, ColumnType.DateTime) or
        (ColumnType.String, ColumnType.Bytes) or
        (ColumnType.String, ColumnType.Uuid) or
        (ColumnType.Integer64, ColumnType.Numeric) or
        (ColumnType.Float64, ColumnType.Numeric) or
        (ColumnType.Float32, ColumnType.Numeric) or
        (ColumnType.String, ColumnType.Numeric) or
        (ColumnType.Numeric, ColumnType.Float64) or
        (ColumnType.Numeric, ColumnType.Float32) or
        (ColumnType.String, ColumnType.Jsonb);

    internal static ColumnType ResolveTargetType(string castName, NodeAst targetTypeAst)
    {
        return targetTypeAst.nodeType switch
        {
            NodeType.TypeString    => ColumnType.String,
            NodeType.TypeInteger64 => ColumnType.Integer64,
            NodeType.TypeInteger16 => ColumnType.Integer64,
            NodeType.TypeInteger32 => ColumnType.Integer64,
            NodeType.TypeFloat64   => ColumnType.Float64,
            NodeType.TypeFloat32   => ColumnType.Float32,
            NodeType.TypeBool      => ColumnType.Bool,
            NodeType.TypeObjectId  => ColumnType.Id,
            NodeType.TypeDate      => ColumnType.Date,
            NodeType.TypeDateTime  => ColumnType.DateTime,
            NodeType.TypeBytes     => ColumnType.Bytes,
            NodeType.TypeUuid      => ColumnType.Uuid,
            NodeType.TypeNumeric   => ColumnType.Numeric,
            NodeType.TypeJsonb     => ColumnType.Jsonb,
            NodeType.TypeStringSized => ColumnType.String,
            NodeType.TypeBytesSized  => ColumnType.Bytes,
            NodeType.Identifier    => ResolveIdentifierTargetType(castName, targetTypeAst.yytext!),
            _ => throw UnknownTargetType(castName, targetTypeAst.nodeType.ToString()),
        };
    }

    private static ColumnType ResolveIdentifierTargetType(string castName, string identifier)
    {
        return identifier.ToLowerInvariant() switch
        {
            "id" or "object_id" => ColumnType.Id,
            "bigint" => ColumnType.Integer64,
            "char" or "varchar" or "text" => ColumnType.String,
            "double" => ColumnType.Float64,
            "uuid" or "guid" => ColumnType.Uuid,
            "jsonb" or "json" => ColumnType.Jsonb,
            _ => throw UnknownTargetType(castName, identifier),
        };
    }

    private static ColumnValue CastToString(string castName, ColumnValue value)
    {
        if (value.Type == ColumnType.String)
            return value;

        if (!ScalarTextForm.TryFormat(value, out string text))
            throw InvalidConversion(castName, value.Type, ColumnType.String);

        return new ColumnValue(ColumnType.String, text);
    }

    /// <summary>
    /// The cast to <c>int64</c>. Every number with a fraction rounds half away from zero, the GoogleSQL
    /// rule, so <c>2.5</c> gives 3 and <c>-2.5</c> gives -3 for a float, a NUMERIC and a jsonb number
    /// alike, and <c>round</c> agrees with them. A number with no integer in the int64 range is
    /// <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>. Text does not round: a string must spell
    /// an integer, so <c>'2.5'</c> is refused. A place that takes an integer without naming a cast
    /// (an INT64 column, a function argument, LIMIT) refuses a float, so this is the only conversion.
    /// </summary>
    private static ColumnValue CastToInt64(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Integer64 => value,
            ColumnType.Float64 => FromDoubleToInt64(castName, value.FloatValue),
            ColumnType.Float32 => FromDoubleToInt64(castName, (float)value.FloatValue),
            ColumnType.Bool => new ColumnValue(ColumnType.Integer64, value.BoolValue ? 1 : 0),
            ColumnType.String => FromStringToInt64(castName, value.StrValue!),
            ColumnType.Numeric => FromNumericToInt64(castName, value),
            ColumnType.Jsonb => FromJsonbToInt64(castName, value),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Integer64),
        };
    }

    /// <summary>
    /// The cast to <c>float64</c>. A Float64 value, ±infinity and NaN included, casts to itself, and a
    /// Float32 value widens exactly. A string uses the PostgreSQL <c>float8</c> text form, so
    /// <c>'Infinity'</c>, <c>'-Infinity'</c> and <c>'NaN'</c> are values, and a number past the range
    /// is <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/> (see <see cref="FloatMath"/>). A jsonb
    /// number keeps its decimal text, so <c>'1e400'::jsonb</c> is out of range too, as in PostgreSQL.
    /// </summary>
    private static ColumnValue CastToFloat64(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Float64 => value,
            ColumnType.Float32 => new ColumnValue(ColumnType.Float64, (double)(float)value.FloatValue),
            ColumnType.Integer64 => new ColumnValue(ColumnType.Float64, (double)value.LongValue),
            ColumnType.Bool => new ColumnValue(ColumnType.Float64, value.BoolValue ? 1.0 : 0.0),
            ColumnType.String => FromStringToFloat64(castName, value.StrValue!),
            ColumnType.Numeric => new ColumnValue(ColumnType.Float64, NumericMath.ToDouble(value.NumericUnscaled)),
            ColumnType.Jsonb => FromStringToFloat64(castName, JsonbScalarNumber(castName, value, "double precision").ToString()),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Float64),
        };
    }

    private static ColumnValue CastToBool(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Bool => value,
            ColumnType.String => FromStringToBool(castName, value.StrValue!),
            ColumnType.Jsonb => JsonbScalarToBool(castName, value),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Bool),
        };
    }

    private static ColumnValue CastToId(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Id => value,
            ColumnType.String => FromStringToId(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Id),
        };
    }

    private static ColumnValue CastToUuid(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Uuid => value,
            ColumnType.String => FromStringToUuid(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Uuid),
        };
    }

    /// <summary>
    /// Converts to NUMERIC with the Spanner rules: an integer exactly; a string by parsing it exactly;
    /// a float literal from its source text (<see cref="FloatLiteralText"/>); any other float from its
    /// exact binary value. More than nine fractional digits round half away from zero. NaN and the
    /// infinities are refused, and a value outside the range is a
    /// <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/> error.
    /// </summary>
    private static ColumnValue CastToNumeric(string castName, ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.Numeric:
                return value;

            case ColumnType.Jsonb:
                return JsonbScalarToNumeric(castName, value, "numeric");

            case ColumnType.Integer64:
                return ColumnValue.FromNumeric(NumericMath.FromInt64(value.LongValue));

            case ColumnType.String:
                return ToNumeric(castName, value.StrValue!,
                    NumericMath.TryParse(value.StrValue, out Int128 parsed), parsed);

            case ColumnType.Float64:
            case ColumnType.Float32:
            {
                if (FloatLiteralText.TryGet(value, out string? text))
                    return ToNumeric(castName, text!, NumericMath.TryParse(text, out Int128 fromText), fromText);

                double number = value.Type == ColumnType.Float32 ? (float)value.FloatValue : value.FloatValue;
                return ToNumeric(castName, number.ToString("R", CultureInfo.InvariantCulture),
                    NumericMath.TryFromDouble(number, out Int128 fromDouble), fromDouble);
            }

            default:
                throw InvalidConversion(castName, value.Type, ColumnType.Numeric);
        }
    }

    private static ColumnValue ToNumeric(string castName, string source, NumericConversionStatus status, Int128 unscaled) => status switch
    {
        NumericConversionStatus.Ok => ColumnValue.FromNumeric(unscaled),
        NumericConversionStatus.Overflow => throw new CamusDBException(
            CamusDBErrorCodes.NumericValueOutOfRange,
            $"Function '{castName}' NUMERIC overflow for value '{Truncate(source)}': the range is ±99999999999999999999999999999.999999999"),
        _ => throw InvalidStringConversion(castName, source, ColumnType.Numeric, "a decimal number (e.g. 123.45)"),
    };

    /// <summary>Rounds half away from zero to an integer, the Spanner rule for CAST of NUMERIC to INT64.</summary>
    private static ColumnValue FromNumericToInt64(string castName, ColumnValue value)
    {
        if (!NumericMath.TryToInt64(value.NumericUnscaled, out long result))
            throw Int64Math.OutOfRange($"Function '{castName}' integer overflow for value {value.NumericValue}");

        return new ColumnValue(ColumnType.Integer64, result);
    }

    private static ColumnValue FromStringToUuid(string castName, string value)
    {
        if (!Guid.TryParse(value, out Guid parsed))
            throw InvalidStringConversion(castName, value, ColumnType.Uuid,
                "a valid UUID string (e.g. 550e8400-e29b-41d4-a716-446655440000)");

        return ColumnValue.FromUuid(parsed);
    }

    /// <summary>
    /// The cast to <c>float32</c>, also the write of a wider value into a <c>float32</c> column. A
    /// finite Float64 past the single-precision range, or a nonzero one that rounds to zero, is
    /// <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>, never ±infinity or zero (see
    /// <see cref="FloatMath.NarrowToSingle"/>). ±infinity and NaN narrow to themselves.
    /// </summary>
    private static ColumnValue CastToFloat32(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Float32   => value,
            ColumnType.Float64   => new ColumnValue(ColumnType.Float32, (double)FloatMath.NarrowToSingle(value.FloatValue)),
            ColumnType.Integer64 => new ColumnValue(ColumnType.Float32, (double)(float)value.LongValue),
            ColumnType.String    => FromStringToFloat32(castName, value.StrValue!),
            ColumnType.Numeric   => new ColumnValue(ColumnType.Float32, (double)NumericMath.ToSingle(value.NumericUnscaled)),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Float32),
        };
    }

    private static ColumnValue CastToDate(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Date     => value,
            ColumnType.DateTime => new ColumnValue(ColumnType.Date, TruncateToMidnight(value.LongValue)),
            ColumnType.String   => FromStringToDate(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Date),
        };
    }

    private static ColumnValue CastToDateTime(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.DateTime => value,
            // A date is stored as the ticks of midnight UTC, so only the type changes. Returning the
            // input as-is would keep the Date type, and the cast result would still read as a date.
            ColumnType.Date     => new ColumnValue(ColumnType.DateTime, value.LongValue),
            ColumnType.String   => FromStringToDateTime(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.DateTime),
        };
    }

    private static ColumnValue CastToBytes(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Bytes  => value,
            ColumnType.String => FromStringToBytes(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Bytes),
        };
    }

    /// <summary>
    /// Parses the PostgreSQL <c>float4</c> text form (see <see cref="FloatMath.TryParseFloat32"/>):
    /// a number past the float32 range is <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>, and
    /// text that is not a number is <see cref="CamusDBErrorCodes.InvalidInput"/>.
    /// </summary>
    private static ColumnValue FromStringToFloat32(string castName, string text)
    {
        switch (FloatMath.TryParseFloat32(text, out float parsed))
        {
            case FloatMath.ParseStatus.Ok:
                return new ColumnValue(ColumnType.Float32, (double)parsed);

            case FloatMath.ParseStatus.Overflow:
                throw FloatMath.OutOfRange($"Function '{castName}' float32 overflow: '{Truncate(text)}' is outside the float32 range");

            case FloatMath.ParseStatus.Underflow:
                throw FloatMath.OutOfRange($"Function '{castName}' float32 underflow: '{Truncate(text)}' is a nonzero value too small for float32");

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{castName}' cannot convert string '{text}' to float32");
        }
    }

    private static ColumnValue FromStringToDate(string castName, string text)
    {
        if (!TryParseTemporalText(text, ColumnType.Date, out ColumnValue value))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot parse '{text}' as date — expected format: {TemporalTextForm(ColumnType.Date)}");

        return value;
    }

    private static ColumnValue FromStringToDateTime(string castName, string text)
    {
        if (!TryParseTemporalText(text, ColumnType.DateTime, out ColumnValue value))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot parse '{text}' as datetime — expected {TemporalTextForm(ColumnType.DateTime)}");

        return value;
    }

    /// <summary>
    /// Parses the text form of a <see cref="ColumnType.Date"/> or <see cref="ColumnType.DateTime"/>
    /// value, the one parse that CAST, assignment and comparison share (see
    /// <see cref="TemporalComparison"/>). Returns false, without an exception, when the text is not a
    /// valid value of <paramref name="target"/>. A date keeps the UTC day of the text.
    /// </summary>
    internal static bool TryParseTemporalText(string text, ColumnType target, out ColumnValue value)
    {
        value = ColumnValue.Null;

        if (target == ColumnType.Date)
        {
            if (!DateTimeOffset.TryParseExact(text, DateFormat, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal, out DateTimeOffset date))
                return false;

            value = new ColumnValue(ColumnType.Date, TruncateToMidnight(date.UtcDateTime.Ticks));
            return true;
        }

        if (!DateTimeOffset.TryParseExact(text, DateTimeFormats, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out DateTimeOffset instant))
            return false;

        value = new ColumnValue(ColumnType.DateTime, instant.UtcTicks);
        return true;
    }

    /// <summary>The text form <see cref="TryParseTemporalText"/> expects, for an error message.</summary>
    internal static string TemporalTextForm(ColumnType target) => target == ColumnType.Date
        ? "yyyy-MM-dd"
        : "ISO-8601 UTC format, e.g. 2026-01-01T12:00:00Z or 2026-01-01";

    // Bytes text is hex behind a 0x prefix ('0xABCD01') or the PostgreSQL \x prefix ('\xabcd01').
    // The \x form is what a bytes value casts to (ScalarTextForm), so bytes round-trip through text.
    // Base-64 is NOT accepted.
    private static ColumnValue FromStringToBytes(string castName, string text)
    {
        ReadOnlySpan<char> span = text.AsSpan();

        if (span.Length < 2 || (span[0] != '0' && span[0] != '\\') || (span[1] != 'x' && span[1] != 'X'))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' expects a 0x- or \\x-prefixed hex literal for bytes, got '{Truncate(text)}'");

        span = span[2..];

        if (span.Length % 2 != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' hex literal must have an even number of hex digits");

        byte[] bytes = new byte[span.Length / 2];

        for (int i = 0; i < bytes.Length; i++)
        {
            if (!TryParseHexByte(span[i * 2], span[i * 2 + 1], out byte b))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{castName}' invalid hex character in bytes literal '{Truncate(text)}'");
            bytes[i] = b;
        }

        return new ColumnValue(bytes);
    }

    private static bool TryParseHexByte(char hi, char lo, out byte result)
    {
        if (!TryHexChar(hi, out int h) || !TryHexChar(lo, out int l))
        {
            result = 0;
            return false;
        }
        result = (byte)((h << 4) | l);
        return true;
    }

    private static bool TryHexChar(char c, out int value)
    {
        if (c >= '0' && c <= '9') { value = c - '0'; return true; }
        if (c >= 'a' && c <= 'f') { value = c - 'a' + 10; return true; }
        if (c >= 'A' && c <= 'F') { value = c - 'A' + 10; return true; }
        value = 0;
        return false;
    }

    // Truncate ticks to UTC midnight (date-only).
    private static long TruncateToMidnight(long ticks)
    {
        const long ticksPerDay = TimeSpan.TicksPerDay;
        return ticks - (ticks % ticksPerDay);
    }

    /// <summary>
    /// Parses a decimal integer string. A well-formed integer past the int64 range is
    /// <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>, as PostgreSQL reports <c>value out of
    /// range for type bigint</c>; any other text is a conversion error.
    /// </summary>
    private static ColumnValue FromStringToInt64(string castName, string text)
    {
        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            if (Int64Math.IsDecimalIntegerText(text))
                throw Int64Math.OutOfRange($"Function '{castName}' integer overflow for value '{Truncate(text)}'");

            throw InvalidStringConversion(castName, text, ColumnType.Integer64, "a 64-bit integer");
        }

        return new ColumnValue(ColumnType.Integer64, parsed);
    }

    /// <summary>
    /// Parses the PostgreSQL <c>float8</c> text form (see <see cref="FloatMath.TryParseFloat64"/>):
    /// <c>'Infinity'</c> and <c>'NaN'</c> are values, a number past the float64 range is
    /// <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>, and text that is not a number is
    /// <see cref="CamusDBErrorCodes.InvalidInput"/>.
    /// </summary>
    private static ColumnValue FromStringToFloat64(string castName, string text)
    {
        switch (FloatMath.TryParseFloat64(text, out double parsed))
        {
            case FloatMath.ParseStatus.Ok:
                return new ColumnValue(ColumnType.Float64, parsed);

            case FloatMath.ParseStatus.Overflow:
                throw FloatMath.OutOfRange($"Function '{castName}' float64 overflow: '{Truncate(text)}' is outside the float64 range");

            case FloatMath.ParseStatus.Underflow:
                throw FloatMath.OutOfRange($"Function '{castName}' float64 underflow: '{Truncate(text)}' is a nonzero value too small for float64");

            default:
                throw InvalidStringConversion(castName, text, ColumnType.Float64, "a number");
        }
    }

    private static ColumnValue FromStringToBool(string castName, string text)
    {
        if (string.Equals(text, "true", StringComparison.OrdinalIgnoreCase))
            return ColumnValue.True;

        if (string.Equals(text, "false", StringComparison.OrdinalIgnoreCase))
            return ColumnValue.False;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{castName}' cannot convert string value '{text}' to bool");
    }

    private static ColumnValue FromStringToId(string castName, string text)
    {
        if (!IsValidLowerHexObjectId(text))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' expects a 24-character lowercase hex object id but received '{text}'");
        }

        try
        {
            ObjectIdValue parsed = ObjectId.ToValue(text);
            return new ColumnValue(ColumnType.Id, parsed.ToString());
        }
        catch (FormatException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' expects a 24-character lowercase hex object id but received '{text}'");
        }
    }

    private const double MaxInt64ExclusiveUpperBound = 9223372036854775808.0;

    private const double MinInt64InclusiveLowerBound = -9223372036854775808.0;

    /// <summary>
    /// A float to Integer64, rounded half away from zero (see <see cref="CastToInt64"/>).
    /// <see cref="Math.Round(double, MidpointRounding)"/> reads the exact binary value, so the largest
    /// double below one half gives 0; adding 0.5 and taking the floor would give 1. Rounding cannot
    /// cross a bound, because a double near ±2^63 has no fraction. ±infinity and NaN have no integer
    /// value, so they are out of range like a value past the int64 bounds, as PostgreSQL reports
    /// <c>bigint out of range</c> for <c>'NaN'::float8::bigint</c>. The comparisons are false for NaN,
    /// so it is tested first.
    /// </summary>
    private static ColumnValue FromDoubleToInt64(string castName, double value)
    {
        double rounded = Math.Round(value, MidpointRounding.AwayFromZero);

        if (!double.IsFinite(rounded) || rounded >= MaxInt64ExclusiveUpperBound || rounded < MinInt64InclusiveLowerBound)
        {
            throw Int64Math.OutOfRange(
                $"Function '{castName}' integer overflow for value {value.ToString(CultureInfo.InvariantCulture)}");
        }

        return new ColumnValue(ColumnType.Integer64, (long)rounded);
    }

    internal static bool IsValidLowerHexObjectId(string value)
    {
        if (value.Length != 24)
            return false;

        foreach (char character in value)
        {
            if (character is >= '0' and <= '9' or >= 'a' and <= 'f')
                continue;

            return false;
        }

        return true;
    }

    /// <summary>
    /// The cast to <c>jsonb</c>: only text parses into a document, as in PostgreSQL, where a number or
    /// a boolean becomes a document through <c>to_jsonb</c>, not through a cast.
    /// </summary>
    private static ColumnValue CastToJsonb(string castName, ColumnValue value) => value.Type switch
    {
        ColumnType.Jsonb => value,
        ColumnType.String => ColumnValue.FromJsonText(value.StrValue!),
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{castName}' cannot convert {value.Type} to jsonb; use to_jsonb(...) to build a document from a value"),
    };

    /// <summary>
    /// The number of a scalar jsonb document, for the PostgreSQL casts <c>jsonb::numeric</c>,
    /// <c>jsonb::bigint</c> and <c>jsonb::float8</c>. Any other kind is refused with the PostgreSQL
    /// wording ("cannot cast jsonb string to type numeric").
    /// </summary>
    private static JsonbNumber JsonbScalarNumber(string castName, ColumnValue value, string targetName)
    {
        ReadOnlySpan<byte> item = JsonbDocument.Item(value.BytesValue);
        JsonbKind kind = JsonbDocument.Kind(item);
        if (kind != JsonbKind.Number)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot cast jsonb {JsonbDocument.TypeName(kind)} to type {targetName}");

        int pos = 0;
        return JsonbCbor.ReadNumber(item, ref pos);
    }

    private static ColumnValue JsonbScalarToNumeric(string castName, ColumnValue value, string targetName)
    {
        string text = JsonbScalarNumber(castName, value, targetName).ToString();
        return ToNumeric(castName, text, NumericMath.TryParse(text, out Int128 parsed), parsed);
    }

    /// <summary>
    /// A <c>jsonb</c> number to int64, rounded half away from zero as a NUMERIC is. The rounding is
    /// done one time, on the exact JSON number (<see cref="JsonbNumber.RoundToInteger"/>), as
    /// PostgreSQL does. A detour through NUMERIC would first round the fraction to nine digits, and
    /// that first rounding can cross the half-way point (<c>0.4999999999</c> would give 1). A number
    /// past the int64 range is <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>, as PostgreSQL
    /// reports <c>bigint out of range</c>.
    /// </summary>
    private static ColumnValue FromJsonbToInt64(string castName, ColumnValue value)
    {
        JsonbNumber number = JsonbScalarNumber(castName, value, "bigint");
        System.Numerics.BigInteger rounded = number.RoundToInteger();

        if (rounded < long.MinValue || rounded > long.MaxValue)
            throw Int64Math.OutOfRange($"Function '{castName}' integer overflow for value {Truncate(number.ToString())}");

        return new ColumnValue(ColumnType.Integer64, (long)rounded);
    }

    private static ColumnValue JsonbScalarToBool(string castName, ColumnValue value)
    {
        ReadOnlySpan<byte> item = JsonbDocument.Item(value.BytesValue);
        JsonbKind kind = JsonbDocument.Kind(item);
        if (kind != JsonbKind.Boolean)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot cast jsonb {JsonbDocument.TypeName(kind)} to type boolean");

        int pos = 0;
        return ColumnValue.FromBool(JsonbCbor.ReadBoolean(item, ref pos));
    }

    private static CamusDBException UnknownTargetType(string castName, string targetType)
    {
        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{castName}' unknown target type '{targetType}'");
    }

    private static CamusDBException InvalidConversion(string castName, ColumnType sourceType, ColumnType targetType)
    {
        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{castName}' cannot convert {sourceType} to {targetType}");
    }

    /// <summary>
    /// Builds a conversion error that quotes the offending source value and states the expected
    /// format. Used for string-parse failures where the generic <see cref="InvalidConversion"/>
    /// (which names only the types) would leave the caller guessing which value was rejected.
    /// The value is truncated so a pathologically long input can't bloat the message.
    /// </summary>
    private static CamusDBException InvalidStringConversion(string castName, string value, ColumnType targetType, string expected)
    {
        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{castName}' cannot convert the value '{Truncate(value)}' to {targetType}: expected {expected}");
    }

    private static string Truncate(string value) => value.Length <= 64 ? value : value[..64] + "…";
}
