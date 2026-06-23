
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class CastScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnaryCast(registry, "to_string", ColumnType.String);
        RegisterUnaryCast(registry, "to_int64", ColumnType.Integer64);
        RegisterUnaryCast(registry, "to_float64", ColumnType.Float64);
        RegisterUnaryCast(registry, "to_bool", ColumnType.Bool);

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
            ColumnType.String => CastToString(castName, value),
            ColumnType.Integer64 => CastToInt64(castName, value),
            ColumnType.Float64 => CastToFloat64(castName, value),
            ColumnType.Bool => CastToBool(castName, value),
            ColumnType.Id => CastToId(castName, value),
            _ => throw UnknownTargetType(castName, targetType.ToString()),
        };
    }

    internal static ColumnType ResolveTargetType(string castName, NodeAst targetTypeAst)
    {
        return targetTypeAst.nodeType switch
        {
            NodeType.TypeString => ColumnType.String,
            NodeType.TypeInteger64 => ColumnType.Integer64,
            NodeType.TypeFloat64 => ColumnType.Float64,
            NodeType.TypeBool => ColumnType.Bool,
            NodeType.TypeObjectId => ColumnType.Id,
            NodeType.Identifier => ResolveIdentifierTargetType(castName, targetTypeAst.yytext!),
            _ => throw UnknownTargetType(castName, targetTypeAst.nodeType.ToString()),
        };
    }

    private static ColumnType ResolveIdentifierTargetType(string castName, string identifier)
    {
        return identifier.ToLowerInvariant() switch
        {
            "id" or "object_id" => ColumnType.Id,
            "integer" => ColumnType.Integer64,
            "double" => ColumnType.Float64,
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
