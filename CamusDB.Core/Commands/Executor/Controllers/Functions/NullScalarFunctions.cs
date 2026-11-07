
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Registers the null scalar function family: COALESCE (variadic), IFNULL (2-arg), NVL (alias of
/// IFNULL), and NULLIF (2-arg). The first three return the first argument whose type is not
/// ColumnType.Null, or ColumnValue.Null when every argument is null.
///
/// For those three, the arguments run in order, and the first that is not NULL ends the call (a
/// <see cref="ScalarFunctionDescriptor.LazyEvaluator"/>). An argument after it does not run, so it raises no
/// error, and a sequence call in it draws no value. This makes COALESCE usable as a guard:
/// COALESCE(n, 1 / v) divides only in a row where n is NULL. A subquery, an aggregate and a window
/// function are computed before the row is evaluated, so stopping here does not stop them.
///
/// <para>The result type is the common type of the arguments, by <see cref="ResultTypeRule"/> (the rule
/// CASE, IF and the set operations use): numbers widen by the arithmetic rule, a date with a datetime
/// gives datetime, two arrays need one element type, and any other pair (a number with a String, a Bool
/// with an INT64) is InvalidInput. So the type must not depend on which argument won: COALESCE(int_col,
/// 3.5) returns a Float64 even for a row where int_col is not NULL. For an argument that does not run,
/// the type comes from what shows it without running it: the literal, the cast or the arithmetic over
/// those, a function call by its declared type when its argument types show, and the value of a
/// parameter or a column. So COALESCE(1, sqrt(4.0)) is a FLOAT64, as in PostgreSQL, also in a FROM-less
/// SELECT, which has no binding to record the type.</para>
///
/// <para>A NULL argument value has the type Null, so the values alone cannot show the declared type
/// of a NULL column. The statement's binding therefore checks the declared types before any row is
/// read and records the result type (<see cref="Queries.ConditionalResultTypes"/>); the expression
/// evaluator widens the returned value to it, so COALESCE(int_col, float_col) is a Float64 also in a
/// row where float_col is NULL. A call that no binding checked (a CHECK constraint, a default) keeps
/// the rule above alone.</para>
///
/// <para>NULLIF(a, b) is the inverse rule: it returns NULL when <c>a = b</c> is TRUE and returns
/// <c>a</c> otherwise. It compares through <see cref="SQLExecutorBaseCreator.EvalComparison"/>, the
/// evaluator of the <c>=</c> operator, so mixed numeric pairs, a string literal beside a Uuid or Id,
/// and a pair with no comparison rule (a type error, see <see cref="ComparisonTypeRule"/>) behave
/// exactly as <c>a = b</c> does. A NULL
/// <c>b</c> makes the comparison UNKNOWN, so the result is <c>a</c>. The declared type is the type
/// of <c>a</c>, and <c>a</c> is returned without widening. Both arguments always run, as in PostgreSQL,
/// because the comparison needs both.</para>
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
            LazyEvaluator = EvaluateUntilNotNull,
            Evaluator = static (calledName, _) => throw NotLazy(calledName),
            InferReturnType = InferCoalesceReturnType,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "ifnull",
            Aliases = ["nvl"],
            MinArity = 2,
            MaxArity = 2,
            IsVolatile = false,
            LazyEvaluator = EvaluateUntilNotNull,
            Evaluator = static (calledName, _) => throw NotLazy(calledName),
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

    /// <summary>
    /// Runs the arguments in order and stops at the first that is not NULL. The arguments after it do not
    /// run, so only the type rule reads them (<see cref="TypeWithoutRunning"/>): a pair with no common type
    /// that the row can show is still an error, also when an earlier argument wins.
    /// </summary>
    private static ColumnValue EvaluateUntilNotNull(
        string calledName,
        IReadOnlyList<NodeAst> arguments,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        ScalarFunctionArguments.EvaluateExpressionDelegate evaluateExpression)
    {
        ColumnValue? winner = null;
        int next = 0;

        while (winner is null && next < arguments.Count)
        {
            ColumnValue value = evaluateExpression(arguments[next++], row, parameters, rowNameResolver);

            if (value.Type != ColumnType.Null)
                winner = value;
        }

        if (winner is null)
            return ColumnValue.Null;

        ExpressionType resultType = ValueType(winner);

        for (; next < arguments.Count; next++)
        {
            if (TypeWithoutRunning(arguments[next], row, parameters, rowNameResolver, evaluateExpression) is not { } later)
                continue;

            if (!ResultTypeRule.TryCombine(resultType, later, out ExpressionType combined))
                throw ResultTypeRule.NoCommonType(calledName.ToUpperInvariant(), resultType, later);

            resultType = combined;
        }

        return ResultTypeRule.Widen(winner, resultType.Type);
    }

    /// <summary>
    /// The type of an argument that must not run, or null when only running it shows the type. A parameter
    /// or a column is read, because a read has no effect and cannot fail for a reason the statement did not
    /// already have. Everything else comes from the syntax, so a division or a sequence call in it stays
    /// unevaluated. A function call has its declared type; an extension call is read too, because this runs
    /// in the extension scope of the statement and nothing caches the type.
    /// </summary>
    private static ExpressionType? TypeWithoutRunning(
        NodeAst argument,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        ScalarFunctionArguments.EvaluateExpressionDelegate evaluateExpression)
    {
        if (argument.nodeType is not (NodeType.Placeholder or NodeType.Identifier))
            return Queries.ConditionalResultTypes.TypeOf(argument, lookup: null, Queries.ConditionalResultTypes.CallTyping.All);

        ColumnValue value = evaluateExpression(argument, row, parameters, rowNameResolver);

        return value.Type == ColumnType.Null ? null : ValueType(value);
    }

    // A string value may be a literal, so it can take a date or datetime result type; a typed string
    // column with a date column was already refused when the statement was bound.
    private static ExpressionType ValueType(ColumnValue value) =>
        ExpressionType.Of(value) with { IsStringLiteral = value.Type == ColumnType.String };

    // The evaluator always takes the lazy path for these functions, so this is not reachable from SQL. It
    // exists because a descriptor requires a plain evaluator, and a plain evaluator has no way to stop.
    private static CamusDBException NotLazy(string calledName) =>
        new(CamusDBErrorCodes.InvalidInternalOperation, $"Function '{calledName}' must run through its lazy evaluator");

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
