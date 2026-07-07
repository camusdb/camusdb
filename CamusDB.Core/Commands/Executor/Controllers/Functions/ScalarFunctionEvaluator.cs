
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
        IReadOnlyDictionary<string, ColumnValue> row,
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

    /// <summary>
    /// Returns <c>true</c> if <paramref name="ast"/> or any descendant contains a call to a
    /// volatile scalar function (one whose result changes across evaluations: <c>random</c>,
    /// <c>now</c>/<c>current_timestamp</c>, <c>current_date</c>, <c>unix_timestamp</c>,
    /// <c>gen_id</c>). Used to gate cache eligibility: a query whose projection, WHERE,
    /// HAVING, or GROUP BY contains a volatile call must bypass the result cache.
    ///
    /// <para><b>Registration invariant:</b> correctness depends on every volatile function
    /// having <see cref="ScalarFunctionDescriptor.IsVolatile"/> set to <c>true</c> in its
    /// <see cref="ScalarFunctionRegistry"/> registration. Unregistered names throw at
    /// execution, so the only silent gap is a newly-added volatile function whose descriptor
    /// omits the flag — it would be cached incorrectly. Set <c>IsVolatile = true</c> whenever
    /// a new non-deterministic function is registered.</para>
    /// </summary>
    internal static bool ContainsVolatileFunction(NodeAst? ast)
    {
        if (ast is null)
            return false;

        if (ast.nodeType == NodeType.ExprFuncCall)
        {
            string name = ast.leftAst?.yytext?.ToLowerInvariant() ?? string.Empty;
            if (Registry.TryGet(name, out ScalarFunctionDescriptor? descriptor) && descriptor.IsVolatile)
                return true;
        }

        return ContainsVolatileFunction(ast.leftAst)
            || ContainsVolatileFunction(ast.rightAst)
            || ContainsVolatileFunction(ast.extendedOne)
            || ContainsVolatileFunction(ast.extendedTwo)
            || ContainsVolatileFunction(ast.extendedThree)
            || ContainsVolatileFunction(ast.extendedFour)
            || ContainsVolatileFunction(ast.extendedFive);
    }
}
