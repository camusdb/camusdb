
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class ScalarFunctionArguments
{
    public delegate ColumnValue EvaluateExpressionDelegate(
        NodeAst expression,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver);

    /// <summary>
    /// Evaluates the arguments of the argument list <paramref name="argumentAst"/> in source order. It
    /// flattens the list on each call, so it suits a statement that runs one time, such as <c>CALL</c>.
    /// The row evaluator uses <see cref="EvaluateArguments"/> with the cached argument nodes of
    /// <see cref="NodeShapes.CallOf"/>.
    /// </summary>
    public static IReadOnlyList<ColumnValue> EvaluateArgumentList(
        NodeAst? argumentAst,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        EvaluateExpressionDelegate evaluateExpression) =>
        EvaluateArguments(NodeShapes.BuildArguments(argumentAst), row, parameters, rowNameResolver, evaluateExpression);

    /// <summary>
    /// Evaluates <paramref name="argumentNodes"/> in source order into a new array. The array is the
    /// one allocation for each call: a function can keep its argument list, so the list is not reused.
    ///
    /// <para>Not recursive. A nested call such as <c>abs(abs(abs(x)))</c> re-enters the expression
    /// evaluator one time per level through this method, so each frame here is paid one time per level
    /// of nesting.</para>
    /// </summary>
    public static IReadOnlyList<ColumnValue> EvaluateArguments(
        NodeAst[] argumentNodes,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        EvaluateExpressionDelegate evaluateExpression)
    {
        if (argumentNodes.Length == 0)
            return [];

        ColumnValue[] values = new ColumnValue[argumentNodes.Length];

        for (int i = 0; i < argumentNodes.Length; i++)
            values[i] = evaluateExpression(argumentNodes[i], row, parameters, rowNameResolver);

        return values;
    }

    public static void ValidateExactArity(string functionName, int expectedArity, int actualArity)
    {
        ValidateArity(functionName, expectedArity, expectedArity, actualArity);
    }

    public static void ValidateArity(string functionName, int minArity, int maxArity, int actualArity)
    {
        if (actualArity >= minArity && actualArity <= maxArity)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{functionName}' expects {FormatExpectedArity(minArity, maxArity)} argument(s) but received {actualArity}");
    }

    public static ColumnValue? PropagateNull(IReadOnlyList<ColumnValue> arguments)
    {
        // An indexed loop: a foreach over the interface allocates an enumerator for each row.
        for (int i = 0; i < arguments.Count; i++)
        {
            if (arguments[i].Type == ColumnType.Null)
                return ColumnValue.Null;
        }

        return null;
    }

    public static void RequireType(string functionName, int argumentIndex, ColumnValue argument, params ColumnType[] allowedTypes)
    {
        if (allowedTypes.Contains(argument.Type))
            return;

        throw WrongType(functionName, argumentIndex, argument.Type, allowedTypes);
    }

    /// <summary>The error of <see cref="RequireType"/>, so a bind-time rule can raise the same code and text.</summary>
    public static CamusDBException WrongType(string functionName, int argumentIndex, ColumnType received, params ColumnType[] allowedTypes) =>
        new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{functionName}' expects argument {argumentIndex + 1} of type {FormatTypes(allowedTypes)} but received {received}");

    /// <summary>
    /// Accepts Integer64, Float64 and NUMERIC. A caller that branches on the type must handle NUMERIC
    /// before any arm that reads <see cref="ColumnValue.FloatValue"/>, which is zero for a NUMERIC
    /// value; a caller that only needs a double uses <see cref="ToDouble"/>, which converts it.
    /// </summary>
    public static void RequireNumeric(string functionName, int argumentIndex, ColumnValue argument)
    {
        if (argument.Type is ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Numeric)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{functionName}' expects argument {argumentIndex + 1} of type Integer64, Float64 or Numeric but received {argument.Type}");
    }

    public static void RequireString(string functionName, int argumentIndex, ColumnValue argument)
    {
        RequireType(functionName, argumentIndex, argument, ColumnType.String);
    }

    public static void RequireBool(string functionName, int argumentIndex, ColumnValue argument)
    {
        RequireType(functionName, argumentIndex, argument, ColumnType.Bool);
    }

    public static void RequireId(string functionName, int argumentIndex, ColumnValue argument)
    {
        RequireType(functionName, argumentIndex, argument, ColumnType.Id);
    }

    public static double ToDouble(ColumnValue argument)
    {
        return argument.Type switch
        {
            ColumnType.Integer64 => argument.LongValue,
            ColumnType.Float64 => argument.FloatValue,
            ColumnType.Numeric => NumericMath.ToDouble(argument.NumericUnscaled),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Expected numeric argument but received {argument.Type}"),
        };
    }

    private static string FormatExpectedArity(int minArity, int maxArity)
    {
        if (minArity == maxArity)
            return minArity.ToString();

        if (maxArity == int.MaxValue)
            return $"at least {minArity}";

        return $"{minArity} to {maxArity}";
    }

    private static string FormatTypes(IReadOnlyList<ColumnType> types)
    {
        return string.Join(" or ", types);
    }
}
