
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Registers the null-coalescing scalar function family: COALESCE (variadic), IFNULL (2-arg),
/// and NVL (alias of IFNULL). Each returns the first argument whose type is not ColumnType.Null;
/// if every argument is null the function returns ColumnValue.Null.
///
/// Return type is inferred from the non-null argument types using these rules in order:
///   1. Numeric widening: Float64 beats Float32 beats Integer64 — always yields the broader type.
///   2. Incompatible mixes (e.g. numeric + String, Id + String) are rejected with InvalidInput.
///   3. Otherwise the first non-null type is kept as-is (e.g. Bool + Bool → Bool).
/// When all argument types are Null the inferred type is ColumnType.Null.
///
/// The evaluator coerces the returned value to the inferred type for numeric pairs so that the
/// runtime ColumnValue.Type always matches the schema-declared type. For example,
/// COALESCE(int_col, 3.5) declares Float64 and the returned Integer64 value is widened to Float64.
/// This keeps the schema-declared type consistent with the actual value for derived-table column typing.
/// </summary>
internal static class NullScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "coalesce",
            MinArity = 1,
            MaxArity = int.MaxValue,
            IsVolatile = false,
            Evaluator = EvaluateCoalesce,
            InferReturnType = InferCoalesceReturnType,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "ifnull",
            Aliases = ["nvl"],
            MinArity = 2,
            MaxArity = 2,
            IsVolatile = false,
            Evaluator = EvaluateCoalesce,
            InferReturnType = InferCoalesceReturnType,
        });
    }

    private static ColumnValue EvaluateCoalesce(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        // Compute the declared return type from all argument types so we can coerce the winning
        // value to match — otherwise COALESCE(int_col, 3.5) would declare Float64 but hand back
        // an Integer64, making the runtime type disagree with the schema-declared type.
        ColumnType targetType = InferCoalesceReturnType(arguments.Select(a => a.Type).ToArray());

        foreach (ColumnValue arg in arguments)
        {
            if (arg.Type != ColumnType.Null)
                return CoerceNumeric(arg, targetType);
        }

        return ColumnValue.Null;
    }

    /// <summary>
    /// Widens a numeric value to <paramref name="targetType"/> when both sides are numeric.
    /// Returns <paramref name="value"/> unchanged for non-numeric or already-matching types.
    /// </summary>
    private static ColumnValue CoerceNumeric(ColumnValue value, ColumnType targetType)
    {
        if (value.Type == targetType)
            return value;

        return (value.Type, targetType) switch
        {
            (ColumnType.Integer64, ColumnType.Float64) => new ColumnValue(ColumnType.Float64, (double)value.LongValue),
            (ColumnType.Integer64, ColumnType.Float32) => new ColumnValue(ColumnType.Float32, (double)(float)value.LongValue),
            (ColumnType.Float32,   ColumnType.Float64) => new ColumnValue(ColumnType.Float64, value.FloatValue),
            _ => value,
        };
    }

    private static ColumnType InferCoalesceReturnType(IReadOnlyList<ColumnType> argumentTypes)
    {
        ColumnType result = ColumnType.Null;

        foreach (ColumnType t in argumentTypes)
        {
            if (t == ColumnType.Null)
                continue;

            if (result == ColumnType.Null)
            {
                result = t;
                continue;
            }

            result = WiderType(result, t);
        }

        return result;
    }

    /// <summary>
    /// Returns the wider of two non-null column types: numeric widening first, then String
    /// as a universal fallback, then the first type for any other incompatible pair.
    /// </summary>
    private static ColumnType WiderType(ColumnType a, ColumnType b)
    {
        if (a == b)
            return a;

        // Numeric promotion: Float64 > Float32 > Integer64
        if (IsNumeric(a) && IsNumeric(b))
        {
            if (a == ColumnType.Float64 || b == ColumnType.Float64) return ColumnType.Float64;
            if (a == ColumnType.Float32 || b == ColumnType.Float32) return ColumnType.Float32;
            return ColumnType.Integer64;
        }

        // String cannot be combined with any other type — reject explicitly rather than silently
        // inferring String and then returning a mismatched runtime value.
        if (a == ColumnType.String || b == ColumnType.String)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"COALESCE arguments have incompatible types: cannot combine '{a}' and '{b}'");

        // Mixed incompatible types — keep the first encountered type.
        return a;
    }

    private static bool IsNumeric(ColumnType t)
        => t is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32;
}
