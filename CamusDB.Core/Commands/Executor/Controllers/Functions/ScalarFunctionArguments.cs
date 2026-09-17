
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
    /// Evaluates a call's arguments in source order.
    ///
    /// <para>Deliberately not recursive. A nested call such as <c>abs(abs(abs(x)))</c> re-enters the
    /// expression evaluator once per level through this method, so every frame here is paid once per
    /// level of nesting; a separate recursive walk over the argument list added one more. The common
    /// single-argument call skips the list walk entirely.</para>
    /// </summary>
    public static IReadOnlyList<ColumnValue> EvaluateArgumentList(
        NodeAst? argumentAst,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        EvaluateExpressionDelegate evaluateExpression)
    {
        if (argumentAst is null)
            return [];

        if (argumentAst.nodeType != NodeType.ExprArgumentList)
            return new List<ColumnValue>(1) { evaluateExpression(argumentAst, row, parameters, rowNameResolver) };

        List<NodeAst> argumentNodes = [];
        ExpressionChains.Flatten(argumentAst, NodeType.ExprArgumentList, argumentNodes);

        List<ColumnValue> argumentList = new(argumentNodes.Count);

        for (int i = 0; i < argumentNodes.Count; i++)
            argumentList.Add(evaluateExpression(argumentNodes[i], row, parameters, rowNameResolver));

        return argumentList;
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
        foreach (ColumnValue argument in arguments)
        {
            if (argument.Type == ColumnType.Null)
                return ColumnValue.Null;
        }

        return null;
    }

    public static void RequireType(string functionName, int argumentIndex, ColumnValue argument, params ColumnType[] allowedTypes)
    {
        if (allowedTypes.Contains(argument.Type))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{functionName}' expects argument {argumentIndex + 1} of type {FormatTypes(allowedTypes)} but received {argument.Type}");
    }

    public static void RequireNumeric(string functionName, int argumentIndex, ColumnValue argument)
    {
        if (argument.Type is ColumnType.Integer64 or ColumnType.Float64)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{functionName}' expects argument {argumentIndex + 1} of type Integer64 or Float64 but received {argument.Type}");
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
