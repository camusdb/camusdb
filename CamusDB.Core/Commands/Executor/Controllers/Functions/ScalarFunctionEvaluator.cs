
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

internal static class ScalarFunctionEvaluator
{
    private static readonly ScalarFunctionRegistry Registry = ScalarFunctionRegistry.CreateDefault();

    public static ColumnValue Evaluate(
        NodeAst funcCallExpr,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        ScalarFunctionArguments.EvaluateExpressionDelegate evaluateExpression)
    {
        string functionName = funcCallExpr.leftAst!.yytext!.ToLowerInvariant();

        if (!Registry.TryGet(functionName, out ScalarFunctionDescriptor? descriptor))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidAstStmt,
                "Function not found '" + functionName + "'");
        }

        IReadOnlyList<ColumnValue> arguments = ScalarFunctionArguments.EvaluateArgumentList(
            funcCallExpr.rightAst,
            row,
            parameters,
            rowNameResolver,
            evaluateExpression);

        ScalarFunctionArguments.ValidateArity(
            functionName,
            descriptor.MinArity,
            descriptor.MaxArity,
            arguments.Count);

        return descriptor.Evaluator(functionName, arguments);
    }

    public static bool IsRegisteredScalarFunction(string functionName)
    {
        return Registry.IsRegisteredScalarFunction(functionName);
    }

    public static ColumnType InferReturnType(string functionName, IReadOnlyList<ColumnType> argumentTypes)
    {
        return Registry.InferReturnType(functionName, argumentTypes);
    }
}
