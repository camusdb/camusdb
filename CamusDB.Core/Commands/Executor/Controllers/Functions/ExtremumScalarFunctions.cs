/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Registers <c>GREATEST(a, b, …)</c> and <c>LEAST(a, b, …)</c>, with the PostgreSQL rules: any number of
/// arguments, at least one; a NULL argument is left out, and the result is NULL only when every argument is
/// NULL. MySQL returns NULL when any argument is NULL; this follows PostgreSQL.
///
/// <para><b>The result type</b> is the common type of the arguments, by <see cref="ResultTypeRule"/> (the rule
/// of CASE and COALESCE): numbers widen by the arithmetic rule, a date with a datetime gives datetime, a
/// string literal takes a date or datetime type, and any other pair (a number with a String) is
/// <see cref="CamusDBErrorCodes.InvalidInput"/>. The type does not depend on which argument wins, so
/// <c>GREATEST(3, 2.5)</c> is the FLOAT64 3.0. The statement checks see the call as a conditional
/// (<see cref="Queries.ConditionalResultTypes"/>): a pair of known types with no common type fails when the
/// statement is bound, and the evaluator widens the value to the recorded type, so a NULL column still gives
/// its declared type to the result.</para>
///
/// <para><b>The comparison</b> is the one of the <c>&lt;</c> and <c>&gt;</c> operators
/// (<see cref="SQLExecutorBaseCreator.EvalComparison"/>), on values already widened to the result type:
/// float NaN is above +Infinity, strings compare in the string order, and a pair with no comparison rule is
/// an error. Of two equal values the first one wins. Every argument runs, as in PostgreSQL, because each one
/// can be the result.</para>
/// </summary>
internal static class ExtremumScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "greatest",
            MinArity = 1,
            MaxArity = int.MaxValue,
            IsVolatile = false,
            Evaluator = static (calledName, arguments) => Evaluate(calledName, arguments, NodeType.ExprGreaterThan),
            InferReturnType = static types => InferReturnType("GREATEST", types),
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "least",
            MinArity = 1,
            MaxArity = int.MaxValue,
            IsVolatile = false,
            Evaluator = static (calledName, arguments) => Evaluate(calledName, arguments, NodeType.ExprLessThan),
            InferReturnType = static types => InferReturnType("LEAST", types),
        });
    }

    /// <summary>
    /// The extreme value of <paramref name="arguments"/>: the one that beats every other one by
    /// <paramref name="beats"/> (<c>&gt;</c> for GREATEST, <c>&lt;</c> for LEAST), widened to the common type of the
    /// values that are not NULL. NULL when every argument is NULL.
    /// </summary>
    private static ColumnValue Evaluate(string calledName, IReadOnlyList<ColumnValue> arguments, NodeType beats)
    {
        ExpressionType? resultType = null;

        foreach (ColumnValue argument in arguments)
        {
            if (argument.Type == ColumnType.Null)
                continue;

            // A string value may be a literal or a parameter, so it can take a date or datetime type.
            ExpressionType valueType = ExpressionType.Of(argument) with { IsStringLiteral = argument.Type == ColumnType.String };

            if (resultType is not { } current)
            {
                resultType = valueType;
                continue;
            }

            if (!ResultTypeRule.TryCombine(current, valueType, out ExpressionType combined))
                throw ResultTypeRule.NoCommonType(calledName.ToUpperInvariant(), current, valueType);

            resultType = combined;
        }

        if (resultType is not { } result)
            return ColumnValue.Null;

        ColumnValue? best = null;

        foreach (ColumnValue argument in arguments)
        {
            if (argument.Type == ColumnType.Null)
                continue;

            ColumnValue value = ResultTypeRule.Widen(argument, result.Type);

            if (best is not { } leader)
            {
                best = value;
                continue;
            }

            ColumnValue wins = SQLExecutorBaseCreator.EvalComparison(beats, value, leader);

            if (wins.Type == ColumnType.Bool && wins.BoolValue)
                best = value;
        }

        return best!;
    }

    /// <summary>
    /// The declared type of a call from its argument types, by <see cref="ResultTypeRule.Combine"/>. Throws for a
    /// pair with no common type; the metadata builder then falls back to the argument-free inference, and the
    /// binding's check reports the error (see <see cref="Queries.ConditionalResultTypes"/>).
    /// </summary>
    private static ColumnType InferReturnType(string construct, IReadOnlyList<ColumnType> argumentTypes)
    {
        ColumnType result = ColumnType.Null;

        foreach (ColumnType type in argumentTypes)
        {
            if (ResultTypeRule.Combine(result, type) is not { } combined)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"No common type for the results of {construct}: {result} and {type}; write a CAST to make the types agree");

            result = combined;
        }

        return result;
    }
}
