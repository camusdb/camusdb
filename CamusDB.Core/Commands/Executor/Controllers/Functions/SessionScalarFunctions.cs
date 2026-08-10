/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Functions that report the session the statement runs in rather than anything about the rows:
/// <c>current_database()</c>, <c>current_user()</c>, <c>current_role()</c> and
/// <c>is_superuser()</c>.
///
/// <para><b>How the session reaches the evaluator.</b> Expression evaluation is a static, row-oriented
/// path with no request context, and rows are produced lazily — a projection is evaluated while the
/// transport enumerates the cursor, long after the entry method returned, so the ambient
/// <see cref="AuthorizationContext"/> (an <see cref="AsyncLocal{T}"/> that does not flow back out of
/// the entry method) is not readable there. Instead the entry points snapshot the session into the
/// statement's parameter dictionary under the reserved keys below via
/// <see cref="AttachSessionValues"/>, and the snapshot travels with the ticket wherever the
/// expression goes — streaming, spill, subqueries. The keys start with a doubled <c>@</c>, which the
/// lexer cannot produce for a placeholder (<c>@name</c>), so they can never collide with a user
/// parameter named in SQL.</para>
///
/// <para><b>Why they are marked volatile.</b> Their value is fixed for a session but differs between
/// sessions, and the query result cache is per-node and shared across callers; caching
/// <c>SELECT current_user()</c> would serve one caller's identity to the next. Marking them volatile
/// makes any query referencing them bypass the cache. <see cref="ScalarFunctionDescriptor.IsSessionScoped"/>
/// then keeps that volatility from being mistaken for the per-row generator kind that may back a
/// column <c>DEFAULT</c>.</para>
///
/// <para><c>current_role()</c> returns the same value as <c>current_user()</c>: privileges are granted
/// directly to users here, so a session has no role identity distinct from its user. It exists so
/// SQL written against engines that do separate the two still runs.</para>
///
/// <para><c>is_superuser()</c> answers for the caller's own session and no one else's, which is why it
/// needs no privilege of its own: a caller can already learn the same bit by running a superuser-only
/// statement and reading whether it is refused.</para>
/// </summary>
internal static class SessionScalarFunctions
{
    /// <summary>Reserved parameter key carrying the statement's database name.</summary>
    internal const string DatabaseParameterKey = "@@current_database";

    /// <summary>Reserved parameter key carrying the authenticated user name (absent value = SQL NULL).</summary>
    internal const string UserParameterKey = "@@current_user";

    /// <summary>Reserved parameter key carrying the authenticated role name (absent value = SQL NULL).</summary>
    internal const string RoleParameterKey = "@@current_role";

    /// <summary>Reserved parameter key carrying whether the caller is a superuser (absent value = SQL NULL).</summary>
    internal const string SuperuserParameterKey = "@@is_superuser";

    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(Describe("current_database", DatabaseParameterKey, ColumnType.String));
        registry.Register(Describe("current_user", UserParameterKey, ColumnType.String));
        registry.Register(Describe("current_role", RoleParameterKey, ColumnType.String));
        registry.Register(Describe("is_superuser", SuperuserParameterKey, ColumnType.Bool));
    }

    /// <summary>
    /// Returns <paramref name="ticket"/> with the session snapshot added to its parameters, or the
    /// ticket unchanged when <paramref name="ast"/> calls none of these functions. Called by the SQL
    /// entry points before any sub-ticket is built, so every downstream expression evaluation sees the
    /// same snapshot — and again wherever an AST grows a call the entry point could not see, which is
    /// any point a view body is expanded into the statement.
    ///
    /// <para>Attaching only when the statement actually needs it is what keeps the reserved keys out
    /// of the result-cache fingerprint (which hashes every parameter) for the queries that don't:
    /// unconditional attachment would key every cached entry by user and database and so stop two
    /// callers from ever sharing one.</para>
    ///
    /// <para>The caller's parameter dictionary is never mutated — it belongs to the transport and may
    /// be reused across statements — so a statement that needs the snapshot gets a copy that preserves
    /// the original's key comparer.</para>
    /// </summary>
    public static ExecuteSQLTicket AttachSessionValues(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!ScalarFunctionEvaluator.ContainsSessionScopedFunction(ast))
            return ticket;

        Dictionary<string, ColumnValue> parameters = ticket.Parameters is null
            ? new Dictionary<string, ColumnValue>(3)
            : new Dictionary<string, ColumnValue>(ticket.Parameters, ticket.Parameters.Comparer);

        // An empty database name means a server-level statement (CREATE DATABASE, CREATE USER, …):
        // there is no current database to report, which is SQL NULL rather than an empty name.
        parameters[DatabaseParameterKey] = string.IsNullOrEmpty(ticket.DatabaseName)
            ? ColumnValue.Null
            : new ColumnValue(ColumnType.String, ticket.DatabaseName);

        // With authentication disabled there is no authenticated identity to report. NULL says that,
        // where any placeholder name would assert an identity the server never verified.
        ColumnValue user = ticket.Principal is null
            ? ColumnValue.Null
            : new ColumnValue(ColumnType.String, ticket.Principal.UserName);

        parameters[UserParameterKey] = user;
        parameters[RoleParameterKey] = user;

        // Reported for the caller's own session only, so it discloses nothing the caller could not
        // already establish by running a superuser-only statement and reading the error.
        parameters[SuperuserParameterKey] = ticket.Principal is null
            ? ColumnValue.Null
            : ColumnValue.FromBool(ticket.Principal.IsSuperuser);

        return new ExecuteSQLTicket(ticket.TxnState, ticket.DatabaseName, ticket.Sql, parameters, ticket.Principal);
    }

    private static ScalarFunctionDescriptor Describe(string name, string parameterKey, ColumnType returnType)
    {
        return new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            IsSessionScoped = true,
            SessionEvaluator = (calledName, parameters) => Resolve(calledName, parameterKey, parameters),
            Evaluator = static (calledName, _) => throw Unavailable(calledName),
            InferReturnType = _ => returnType,
        };
    }

    private static ColumnValue Resolve(string calledName, string parameterKey, IReadOnlyDictionary<string, ColumnValue>? parameters)
    {
        if (parameters is null || !parameters.TryGetValue(parameterKey, out ColumnValue? value))
            throw Unavailable(calledName);

        return value;
    }

    /// <summary>
    /// Raised where an expression is evaluated outside a session-bearing statement — a stored CHECK
    /// condition or a column default replayed by the insert path, for instance. Those uses are
    /// rejected when the schema is written, so reaching this means a new evaluation path was added
    /// without carrying the snapshot.
    /// </summary>
    private static CamusDBException Unavailable(string calledName)
    {
        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' is only available in a statement executed within a session");
    }
}
