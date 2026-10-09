
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
///   1. Numeric widening, the arithmetic rule (MixedNumericComparison.ArithmeticResultType): Float64
///      beats Float32 beats Integer64, NUMERIC beats Integer64, and NUMERIC beside a float gives Float64.
///   2. Incompatible mixes (e.g. numeric + String, Id + String) are rejected with InvalidInput.
///   3. Otherwise the first non-null type is kept as-is (e.g. Bool + Bool → Bool).
/// When all argument types are Null the inferred type is ColumnType.Null.
///
/// The evaluator widens the returned value to the type inferred from the argument values
/// (<see cref="NumericWidening.Widen"/>): COALESCE(int_col, 3.5) returns a Float64 even for a row
/// where int_col is not NULL.
///
/// <para>That is not always the declared type of the call. A NULL argument value has the type Null,
/// so the evaluator cannot see the declared type of a NULL column: COALESCE(p, 0) on a NUMERIC p is
/// declared NUMERIC but returns an INT64 zero when p is NULL. A derived table widens its cells to the
/// declared column types when it is filled (see <see cref="NumericWidening"/>), so a join over the
/// derived column sees one type. A top-level result is not widened, so its metadata can name the
/// declared type while a cell carries the narrower one.</para>
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
        // Widen the winning value to the supertype of the argument values, so that
        // COALESCE(int_col, 3.5) returns a Float64 for every row, not only for a NULL int_col.
        ColumnType targetType = InferCoalesceReturnType(arguments.Select(a => a.Type).ToArray());

        foreach (ColumnValue arg in arguments)
        {
            if (arg.Type != ColumnType.Null)
                return NumericWidening.Widen(arg, targetType);
        }

        return ColumnValue.Null;
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

        // Numeric promotion, the arithmetic rule: Float64 > Float32 > Integer64, NUMERIC above
        // Integer64, and NUMERIC beside a float gives Float64.
        if (MixedNumericComparison.ArithmeticResultType(a, b) is { } widened)
            return widened;

        // String cannot be combined with any other type — reject explicitly rather than silently
        // inferring String and then returning a mismatched runtime value.
        if (a == ColumnType.String || b == ColumnType.String)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"COALESCE arguments have incompatible types: cannot combine '{a}' and '{b}'");

        // Mixed incompatible types — keep the first encountered type.
        return a;
    }

}
