
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Registers the null scalar function family: COALESCE (variadic), IFNULL (2-arg), NVL (alias of
/// IFNULL), and NULLIF (2-arg). The first three return the first argument whose type is not
/// ColumnType.Null, or ColumnValue.Null when every argument is null.
///
/// For those three, the result type is the common type of the arguments, by
/// <see cref="ResultTypeRule"/> (the rule CASE, IF and the set operations use): numbers widen by the
/// arithmetic rule, a date with a datetime gives datetime, two arrays need one element type, and any
/// other pair (a number with a String, a Bool with an INT64) is InvalidInput. Every argument is
/// evaluated, so the evaluator checks the types of all argument values in every row, and widens the
/// value it returns to their common type: COALESCE(int_col, 3.5) returns a Float64 even for a row
/// where int_col is not NULL.
///
/// <para>A NULL argument value has the type Null, so the values alone cannot show the declared type
/// of a NULL column. The statement's binding therefore checks the declared types before any row is
/// read and records the result type (<see cref="Queries.ConditionalResultTypes"/>); the expression
/// evaluator widens the returned value to it, so COALESCE(int_col, float_col) is a Float64 also in a
/// row where float_col is NULL. A call that no binding checked (a CHECK constraint, a default) keeps
/// the value rule alone.</para>
///
/// <para>NULLIF(a, b) is the inverse rule: it returns NULL when <c>a = b</c> is TRUE and returns
/// <c>a</c> otherwise. It compares through <see cref="SQLExecutorBaseCreator.EvalComparison"/>, the
/// evaluator of the <c>=</c> operator, so mixed numeric pairs, a string literal beside a Uuid or Id,
/// and a pair with no comparison rule (a type error, see <see cref="ComparisonTypeRule"/>) behave
/// exactly as <c>a = b</c> does. A NULL
/// <c>b</c> makes the comparison UNKNOWN, so the result is <c>a</c>. The declared type is the type
/// of <c>a</c>, and <c>a</c> is returned without widening.</para>
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

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "nullif",
            MinArity = 2,
            MaxArity = 2,
            IsVolatile = false,
            Evaluator = EvaluateNullIf,
            InferReturnType = InferNullIfReturnType,
        });
    }

    private static ColumnValue EvaluateNullIf(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue value = arguments[0];

        // Only a TRUE comparison yields NULL: FALSE and UNKNOWN (a NULL operand) both keep the value.
        ColumnValue equal = SQLExecutorBaseCreator.EvalComparison(NodeType.ExprEquals, value, arguments[1]);

        return equal.Type == ColumnType.Bool && equal.BoolValue ? ColumnValue.Null : value;
    }

    private static ColumnType InferNullIfReturnType(IReadOnlyList<ColumnType> argumentTypes)
        => argumentTypes.Count > 0 ? argumentTypes[0] : ColumnType.Null;

    private static ColumnValue EvaluateCoalesce(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        // Every argument is evaluated, so the result type comes from all of their values, and the
        // winning value widens to it: COALESCE(int_col, 3.5) returns a Float64 for every row, not only
        // for a NULL int_col. A pair with no common type is an error in every row, also when an earlier
        // argument wins.
        ExpressionType resultType = ExpressionType.Null;
        ColumnValue? winner = null;

        foreach (ColumnValue arg in arguments)
        {
            if (arg.Type == ColumnType.Null)
                continue;

            // A string value may be a literal, so it can take a date or datetime result type; a typed
            // string column with a date column was already refused when the statement was bound.
            ExpressionType argType = ExpressionType.Of(arg) with { IsStringLiteral = arg.Type == ColumnType.String };
            if (!ResultTypeRule.TryCombine(resultType, argType, out resultType))
                throw ResultTypeRule.NoCommonType(calledName.ToUpperInvariant(), PreviousType(arguments, arg), argType);

            winner ??= arg;
        }

        return winner is null ? ColumnValue.Null : ResultTypeRule.Widen(winner, resultType.Type);
    }

    // The combined type of the non-NULL arguments before `failing`, for the error message.
    private static ExpressionType PreviousType(IReadOnlyList<ColumnValue> arguments, ColumnValue failing)
    {
        ExpressionType combined = ExpressionType.Null;

        foreach (ColumnValue arg in arguments)
        {
            if (ReferenceEquals(arg, failing))
                break;

            if (arg.Type != ColumnType.Null
                && ResultTypeRule.TryCombine(combined, ExpressionType.Of(arg) with { IsStringLiteral = arg.Type == ColumnType.String }, out ExpressionType next))
                combined = next;
        }

        return combined;
    }

    /// <summary>
    /// The declared type of a COALESCE, IFNULL or NVL call from its argument types, by
    /// <see cref="ResultTypeRule.Combine"/>. Throws for a pair with no common type; the metadata
    /// builder then falls back to the argument-free inference (the binding's check reports the real
    /// error, see <see cref="Queries.ConditionalResultTypes"/>).
    /// </summary>
    private static ColumnType InferCoalesceReturnType(IReadOnlyList<ColumnType> argumentTypes)
    {
        ColumnType result = ColumnType.Null;

        foreach (ColumnType t in argumentTypes)
        {
            if (ResultTypeRule.Combine(result, t) is not { } combined)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"No common type for the results of COALESCE: {result} and {t}; write a CAST to make the types agree");

            result = combined;
        }

        return result;
    }
}
