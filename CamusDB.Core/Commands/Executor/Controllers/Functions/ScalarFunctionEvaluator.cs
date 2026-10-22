
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

        if (!TryResolve(functionName, out ScalarFunctionDescriptor? descriptor))
        {
            // SELECT generate_series(1, 3): a set-returning function in the select list.
            if (TableFunctionRegistry.IsTableFunction(functionName))
                throw new CamusDBException(
                    CamusDBErrorCodes.FeatureNotSupported,
                    $"The set-returning function '{functionName}' is supported only in FROM, as in SELECT ... FROM {functionName}(...)");

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

        // Checked after the session evaluator and before the plain one: a statement-scoped function
        // needs both its arguments and a dictionary it may write, which neither of the others gives.
        if (descriptor.StatementEvaluator is not null)
            return descriptor.StatementEvaluator(functionName, arguments, parameters);

        return descriptor.Evaluator(functionName, arguments);
    }

    public static bool IsRegisteredScalarFunction(string functionName)
    {
        return Registry.IsRegisteredScalarFunction(functionName) ||
               (Extensions.ExtensionFunctionScope.CurrentInstalled?.HasFunction(functionName) ?? false);
    }

    /// <summary>
    /// True when <paramref name="functionName"/> is a built-in, whatever extension scope the flow
    /// carries. The extension loader uses it to refuse an extension name that would shadow a
    /// built-in, and fragment shippability uses it so the decision does not depend on the flow.
    /// </summary>
    internal static bool IsBuiltInScalarFunction(string functionName)
    {
        return Registry.IsRegisteredScalarFunction(functionName);
    }

    /// <summary>
    /// Resolves a function name: the built-in table first, then the functions of the extensions
    /// installed in the database whose statement this flow runs, as this engine loaded them
    /// (<see cref="Extensions.ExtensionFunctionScope"/>). A name may be qualified with its extension
    /// (<c>geo.distance</c>). The loader refuses an extension name that collides with a built-in, so the
    /// order never changes which function a name means; it keeps a built-in lookup exactly as cheap as
    /// before. A flow with no scope resolves built-ins only, so a missing scope fails closed ("Function
    /// not found"). A member of an installed extension this node does not load throws
    /// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>.
    /// </summary>
    private static bool TryResolve(string functionName, out ScalarFunctionDescriptor descriptor)
    {
        if (Registry.TryGet(functionName, out descriptor))
            return true;

        Extensions.InstalledExtensionSet? installed = Extensions.ExtensionFunctionScope.CurrentInstalled;
        return installed is not null && installed.TryGetFunction(functionName, out descriptor);
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
    /// Returns <c>true</c> if <paramref name="ast"/> or any descendant calls a function that reports
    /// the start of the transaction or of the statement (<c>now()</c>, <c>current_date()</c>,
    /// <c>statement_timestamp()</c> and the others in <see cref="ClockScalarFunctions"/>). The SQL
    /// entry points use it to decide whether a statement needs those times attached to its
    /// parameters, so, as for <see cref="ContainsSessionScopedFunction"/>, it must be answered against
    /// the view-expanded AST.
    /// </summary>
    internal static bool ContainsStatementClockFunction(NodeAst? ast)
        => NodeAstWalk.Any(ast, IsStatementClockCall);

    /// <summary>
    /// Returns <c>true</c> if <paramref name="ast"/> or any descendant calls a function that reads a
    /// value of the statement rather than of the rows: a session function or a clock function. One
    /// walk that lets <see cref="StatementScopedValues.Attach"/> skip both per-kind walks for the
    /// statements that call neither.
    /// </summary>
    internal static bool ContainsStatementScopedFunction(NodeAst? ast)
        => NodeAstWalk.Any(ast, IsStatementScopedCall);

    /// <summary>
    /// True when <paramref name="functionName"/> reports the start of the transaction or of the
    /// statement. The insert paths use it to decide whether a function default needs those times.
    /// </summary>
    internal static bool IsStatementClockFunction(string functionName)
    {
        return TryResolve(functionName, out ScalarFunctionDescriptor? descriptor) && descriptor.ReadsStatementClock;
    }

    /// <summary>
    /// True when <paramref name="functionName"/> reports the session it runs in
    /// (<c>current_database</c>, <c>current_user</c>, <c>current_role</c>). Used by DDL to explain why
    /// such a call cannot be frozen into a schema element such as a column default.
    /// </summary>
    internal static bool IsSessionScopedFunction(string functionName)
    {
        return TryResolve(functionName, out ScalarFunctionDescriptor? descriptor) && descriptor.IsSessionScoped;
    }

    public static ColumnType InferReturnType(string functionName, IReadOnlyList<ColumnType> argumentTypes)
    {
        return TryResolve(functionName, out ScalarFunctionDescriptor descriptor)
            ? descriptor.InferReturnType(argumentTypes)
            : Registry.InferReturnType(functionName, argumentTypes);
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="ast"/> or any descendant contains a call to a
    /// volatile scalar function (one whose result changes across evaluations: <c>random</c>,
    /// <c>now</c>/<c>current_timestamp</c>, <c>statement_timestamp</c>, <c>clock_timestamp</c>,
    /// <c>current_date</c>, <c>unix_timestamp</c>, <c>unix_timestamp_ms</c>, <c>gen_id</c>). The clock
    /// functions are fixed within a transaction or a statement, but not between two, so they are
    /// volatile here too.
    /// Used to gate cache eligibility: a query whose projection, WHERE,
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

    private static bool IsStatementClockCall(NodeAst node)
        => TryGetCalledDescriptor(node, out ScalarFunctionDescriptor? d) && d!.ReadsStatementClock;

    private static bool IsStatementScopedCall(NodeAst node)
        => TryGetCalledDescriptor(node, out ScalarFunctionDescriptor? d) && (d!.IsSessionScoped || d.ReadsStatementClock);

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

        return TryResolve(name, out descriptor);
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

        if (!TryResolve(functionName, out ScalarFunctionDescriptor? descriptor))
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
    /// Evaluates a zero-argument scalar function by name. Used to apply a function-call column
    /// default per inserted row. Assumes the name was validated via
    /// <see cref="TryResolveVolatileNullary"/> at DDL time.
    ///
    /// <para>A per-row generator (<c>gen_uuid_v7()</c>) gives each call a fresh value. A clock
    /// function (<c>now()</c>) reads the times in <paramref name="parameters"/> instead, so every
    /// row of one statement takes the same default, and the default agrees with an explicit
    /// <c>now()</c> in the same statement. The caller attaches the times when
    /// <see cref="IsStatementClockFunction"/> says the default needs them.</para>
    /// </summary>
    internal static ColumnValue EvaluateNullary(string functionName, Dictionary<string, ColumnValue>? parameters)
    {
        if (!TryResolve(functionName, out ScalarFunctionDescriptor? descriptor))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Default function not found: " + functionName);

        // Only a clock function reads the statement here. A sequence function that passed the DDL
        // check keeps its old path, which refuses: a default draws from a sequence through
        // DefaultSequenceId, not through a call.
        if (descriptor.ReadsStatementClock)
            return descriptor.StatementEvaluator!(functionName, [], parameters);

        return descriptor.Evaluator(functionName, []);
    }
}
