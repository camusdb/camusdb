
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using System.Buffers.Text;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class CastScalarFunctions
{
    // Date formats accepted by CamusDB for date literals (yyyy-MM-dd).
    private const string DateFormat = "yyyy-MM-dd";

    // DateTime formats accepted by CamusDB for datetime literals (UTC only).
    private static readonly string[] DateTimeFormats =
    [
        "yyyy-MM-ddTHH:mm:ssZ",
        "yyyy-MM-ddTHH:mm:ss.fffZ",
        "yyyy-MM-ddTHH:mm:ss.ffZ",
        "yyyy-MM-ddTHH:mm:ss.fZ",
        "yyyy-MM-ddTHH:mm:sszzz",
        "yyyy-MM-ddTHH:mm:ss",
    ];

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnaryCast(registry, "to_string",   ColumnType.String);
        RegisterUnaryCast(registry, "to_int64",    ColumnType.Integer64);
        RegisterUnaryCast(registry, "to_float64",  ColumnType.Float64);
        RegisterUnaryCast(registry, "to_bool",     ColumnType.Bool);
        RegisterUnaryCast(registry, "to_float32",  ColumnType.Float32);
        RegisterUnaryCast(registry, "to_date",     ColumnType.Date);
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
        return CastValue(castName, input, targetType);
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
        => CoerceToColumnType(value, column.Type);

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
            (ColumnType.String,    ColumnType.Date)     => CastToDate("coerce", value),
            (ColumnType.String,    ColumnType.DateTime) => CastToDateTime("coerce", value),
            (ColumnType.String,    ColumnType.Bytes)    => CastToBytes("coerce", value),
            (ColumnType.String,    ColumnType.Uuid)     => CastToUuid("coerce", value),
            _ => value,
        };
    }

    internal static ColumnType ResolveTargetType(string castName, NodeAst targetTypeAst)
    {
        return targetTypeAst.nodeType switch
        {
            NodeType.TypeString    => ColumnType.String,
            NodeType.TypeInteger64 => ColumnType.Integer64,
            NodeType.TypeFloat64   => ColumnType.Float64,
            NodeType.TypeFloat32   => ColumnType.Float32,
            NodeType.TypeBool      => ColumnType.Bool,
            NodeType.TypeObjectId  => ColumnType.Id,
            NodeType.TypeDate      => ColumnType.Date,
            NodeType.TypeDateTime  => ColumnType.DateTime,
            NodeType.TypeBytes     => ColumnType.Bytes,
            NodeType.TypeUuid      => ColumnType.Uuid,
            NodeType.TypeStringSized => ColumnType.String,
            NodeType.Identifier    => ResolveIdentifierTargetType(castName, targetTypeAst.yytext!),
            _ => throw UnknownTargetType(castName, targetTypeAst.nodeType.ToString()),
        };
    }

    private static ColumnType ResolveIdentifierTargetType(string castName, string identifier)
    {
        return identifier.ToLowerInvariant() switch
        {
            "id" or "object_id" => ColumnType.Id,
            "integer" => ColumnType.Integer64,
            "char" or "varchar" or "text" => ColumnType.String,
            "double" => ColumnType.Float64,
            "uuid" or "guid" => ColumnType.Uuid,
            _ => throw UnknownTargetType(castName, identifier),
        };
    }

    private static ColumnValue CastToString(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.String => value,
            ColumnType.Id => new ColumnValue(ColumnType.String, value.StrValue!),
            ColumnType.Integer64 => new ColumnValue(ColumnType.String, value.LongValue.ToString(CultureInfo.InvariantCulture)),
            ColumnType.Float64 => new ColumnValue(ColumnType.String, FormatFloat(value.FloatValue)),
            ColumnType.Bool => new ColumnValue(ColumnType.String, value.BoolValue ? "true" : "false"),
            ColumnType.Uuid => new ColumnValue(ColumnType.String, value.ToGuid().ToString("D")),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.String),
        };
    }

    private static ColumnValue CastToInt64(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Integer64 => value,
            ColumnType.Float64 => FromDoubleToInt64(castName, value.FloatValue),
            ColumnType.Bool => new ColumnValue(ColumnType.Integer64, value.BoolValue ? 1 : 0),
            ColumnType.String => FromStringToInt64(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Integer64),
        };
    }

    private static ColumnValue CastToFloat64(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Float64 => RejectNonFinite(castName, value.FloatValue),
            ColumnType.Integer64 => new ColumnValue(ColumnType.Float64, (double)value.LongValue),
            ColumnType.Bool => new ColumnValue(ColumnType.Float64, value.BoolValue ? 1.0 : 0.0),
            ColumnType.String => FromStringToFloat64(castName, value.StrValue!),
            _ => throw InvalidConversion(castName, value.Type, ColumnType.Float64),
        };
    }

    private static ColumnValue CastToBool(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Bool => value,
            ColumnType.String => FromStringToBool(castName, value.StrValue!),
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

    private static ColumnValue FromStringToUuid(string castName, string value)
    {
        if (!Guid.TryParse(value, out Guid parsed))
            throw InvalidConversion(castName, ColumnType.String, ColumnType.Uuid);

        return ColumnValue.FromUuid(parsed);
    }

    private static ColumnValue CastToFloat32(string castName, ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Float32   => value,
            ColumnType.Float64   => new ColumnValue(ColumnType.Float32, (double)(float)value.FloatValue),
            ColumnType.Integer64 => new ColumnValue(ColumnType.Float32, (double)(float)value.LongValue),
            ColumnType.String    => FromStringToFloat32(castName, value.StrValue!),
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
            ColumnType.Date     => value, // Date is already stored as ticks at midnight UTC
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

    private static ColumnValue FromStringToFloat32(string castName, string text)
    {
        if (!float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed)
            || float.IsNaN(parsed) || float.IsInfinity(parsed))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot convert string '{text}' to float32");

        return new ColumnValue(ColumnType.Float32, (double)parsed);
    }

    private static ColumnValue FromStringToDate(string castName, string text)
    {
        if (!DateTimeOffset.TryParseExact(text, DateFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out DateTimeOffset dto))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot parse '{text}' as date — expected format: yyyy-MM-dd");

        long ticks = TruncateToMidnight(dto.UtcDateTime.Ticks);
        return new ColumnValue(ColumnType.Date, ticks);
    }

    private static ColumnValue FromStringToDateTime(string castName, string text)
    {
        if (!DateTimeOffset.TryParseExact(text, DateTimeFormats, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out DateTimeOffset dto))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot parse '{text}' as datetime — expected ISO-8601 UTC format, e.g. 2026-01-01T12:00:00Z");

        return new ColumnValue(ColumnType.DateTime, dto.UtcTicks);
    }

    // Bytes literals use 0x-prefixed hex: '0xABCD01'. Base-64 is NOT accepted.
    private static ColumnValue FromStringToBytes(string castName, string text)
    {
        ReadOnlySpan<char> span = text.AsSpan();

        if (span.Length < 2 || span[0] != '0' || (span[1] != 'x' && span[1] != 'X'))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' expects a 0x-prefixed hex literal for bytes, got '{text}'");

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
                    $"Function '{castName}' invalid hex character in bytes literal '{text}'");
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

    private static ColumnValue FromStringToInt64(string castName, string text)
    {
        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
            throw InvalidConversion(castName, ColumnType.String, ColumnType.Integer64);

        return new ColumnValue(ColumnType.Integer64, parsed);
    }

    private static ColumnValue FromStringToFloat64(string castName, string text)
    {
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed))
            throw InvalidConversion(castName, ColumnType.String, ColumnType.Float64);

        return new ColumnValue(ColumnType.Float64, RejectNonFinite(castName, parsed).FloatValue);
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

    private static ColumnValue FromDoubleToInt64(string castName, double value)
    {
        ColumnValue finite = RejectNonFinite(castName, value);

        if (finite.FloatValue >= MaxInt64ExclusiveUpperBound || finite.FloatValue < MinInt64InclusiveLowerBound)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' integer overflow for value {value.ToString(CultureInfo.InvariantCulture)}");
        }

        return new ColumnValue(ColumnType.Integer64, (long)finite.FloatValue);
    }

    private static ColumnValue RejectNonFinite(string castName, double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{castName}' cannot convert non-finite float value");
        }

        return new ColumnValue(ColumnType.Float64, value);
    }

    private static bool IsValidLowerHexObjectId(string value)
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

    private static string FormatFloat(double value)
        => value.ToString(CultureInfo.InvariantCulture);

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
}
