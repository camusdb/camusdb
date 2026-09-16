
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

        if (descriptor.SessionEvaluator is not null)
            return descriptor.SessionEvaluator(functionName, parameters);

        return descriptor.Evaluator(functionName, arguments);
    }

    public static bool IsRegisteredScalarFunction(string functionName)
    {
        return Registry.IsRegisteredScalarFunction(functionName);
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="ast"/> or any descendant calls a function that reports
    /// the session (<c>current_database</c>, <c>current_user</c>, <c>current_role</c>). The SQL entry
    /// points use it to decide whether a statement needs the session snapshot attached to its
    /// parameters, so it must be answered against the AST actually executed — notably the
    /// <b>view-expanded</b> one, since a session call can arrive from a view body the outer statement
    /// never mentions.
    /// </summary>
    internal static bool ContainsSessionScopedFunction(NodeAst? ast)
        => NodeAstWalk.Any(ast, IsSessionScopedCall);

    /// <summary>
    /// True when <paramref name="functionName"/> reports the session it runs in
    /// (<c>current_database</c>, <c>current_user</c>, <c>current_role</c>). Used by DDL to explain why
    /// such a call cannot be frozen into a schema element such as a column default.
    /// </summary>
    internal static bool IsSessionScopedFunction(string functionName)
    {
        return Registry.TryGet(functionName, out ScalarFunctionDescriptor? descriptor) && descriptor.IsSessionScoped;
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
        => NodeAstWalk.Any(ast, IsVolatileCall);

    private static bool IsSessionScopedCall(NodeAst node)
        => TryGetCalledDescriptor(node, out ScalarFunctionDescriptor? d) && d!.IsSessionScoped;

    private static bool IsVolatileCall(NodeAst node)
        => TryGetCalledDescriptor(node, out ScalarFunctionDescriptor? d) && d!.IsVolatile;

    /// <summary>
    /// Resolves the descriptor of the scalar function <paramref name="node"/> calls, or answers
    /// false when the node is not a call at all. An unregistered name also answers false here; it
    /// throws later, at execution, which is where an unknown function belongs.
    /// </summary>
    private static bool TryGetCalledDescriptor(NodeAst node, out ScalarFunctionDescriptor? descriptor)
    {
        if (node.nodeType != NodeType.ExprFuncCall)
        {
            descriptor = null;
            return false;
        }

        string name = node.leftAst?.yytext?.ToLowerInvariant() ?? string.Empty;

        return Registry.TryGet(name, out descriptor);
    }

    /// <summary>
    /// Resolves a zero-argument volatile scalar function usable as a per-row column default (e.g.
    /// <c>gen_uuid_v7</c>). Returns true only when <paramref name="functionName"/> is registered,
    /// <see cref="ScalarFunctionDescriptor.IsVolatile"/>, and takes no arguments; <paramref name="returnType"/>
    /// receives its inferred return type so the DDL layer can validate it against the column type.
    /// </summary>
    internal static bool TryResolveVolatileNullary(string functionName, out ColumnType returnType)
    {
        returnType = ColumnType.Null;

        if (!Registry.TryGet(functionName, out ScalarFunctionDescriptor? descriptor))
            return false;

        if (!descriptor.IsVolatile || descriptor.MinArity != 0 || descriptor.MaxArity != 0)
            return false;

        // A session function is volatile only so queries naming it bypass the result cache. It cannot
        // back a default: the insert path replays the default with no session to report.
        if (descriptor.IsSessionScoped)
            return false;

        returnType = descriptor.InferReturnType([]);
        return true;
    }

    /// <summary>
    /// Evaluates a zero-argument scalar function by name, producing a fresh value. Used to apply a
    /// function-call column default per inserted row. Assumes the name was validated via
    /// <see cref="TryResolveVolatileNullary"/> at DDL time.
    /// </summary>
    internal static ColumnValue EvaluateNullary(string functionName)
    {
        if (!Registry.TryGet(functionName, out ScalarFunctionDescriptor? descriptor))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Default function not found: " + functionName);

        return descriptor.Evaluator(functionName, []);
    }
}
