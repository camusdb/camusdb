/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Extensions;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Core.SQLParser;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Runs <c>CREATE [OR REPLACE] FUNCTION ... LANGUAGE csharp</c>, <c>DROP FUNCTION [IF EXISTS]</c> and
/// <c>ALTER FUNCTION ... OWNER TO</c>: builds the function record from the statement, compiles the
/// body to check it, and proposes the replicated delta through <see cref="CatalogsManager"/>.
///
/// <para><b>The proposer compiles; the log carries source.</b> <c>CREATE FUNCTION</c> compiles the
/// body on this node, all the way to bound code, before it takes any lock. A body that the compiler
/// or the allowed list refuses therefore never reaches the schema log, and the author gets the
/// diagnostics in the response. The delta carries the source text and its hash. Every node, this one
/// included, compiles the function again at its first call.</para>
///
/// <para><b>One name space.</b> A function cannot take the name of a built-in function, an aggregate,
/// a table function, or a member of an extension that this node loads or the database installed. The
/// built-in part is checked here, against this build; the installed part is checked under the schema
/// lock and again in the replicated apply, which orders it against a concurrent
/// <c>CREATE EXTENSION</c>.</para>
///
/// <para><b>Privileges.</b> The statement authorizer requires the <c>CREATE</c> privilege on the
/// database for <c>CREATE FUNCTION</c>. Replacing, dropping and giving away a function need its owner
/// or a superuser, and that check runs here, under the schema lock, against the record the statement
/// changes: a replace changes what every view and CHECK that calls the function means. The creator
/// owns a new function; a replace keeps the owner.</para>
///
/// <para><b>Dependents.</b> <c>DROP FUNCTION</c>, and a replace that changes the parameter types or the
/// result type, are refused while a column default, a CHECK constraint, a view or a materialized view
/// calls the function. A replace that keeps the signature is allowed, and those objects see the new
/// body at their next evaluation.</para>
///
/// <para><b>Runs on the schema leader</b>, under the database's DDL semaphore, like extension, view
/// and sequence DDL.</para>
/// </summary>
internal sealed class FunctionDdlService
{
    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    private readonly SqlParserCache sqlParserCache;

    private readonly Auth.StatementAuthorizer authorizer;

    private readonly CamusDB.Core.Auth.AuthService? authService;

    public FunctionDdlService(
        ExecutorContext context, CatalogsManager catalogs, SqlParserCache sqlParserCache, Auth.StatementAuthorizer authorizer, CamusDB.Core.Auth.AuthService? authService)
    {
        this.context = context;
        this.catalogs = catalogs;
        this.sqlParserCache = sqlParserCache;
        this.authorizer = authorizer;
        this.authService = authService;
    }

    public async Task CreateAsync(DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        InlineFunctionRuntime runtime = RequireRuntime();

        if (!runtime.Enabled)
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionsDisabled,
                "CREATE FUNCTION is not enabled on this node; set inline_functions_enabled to true on every node and restart them");

        bool replace = ast.nodeType == NodeType.CreateOrReplaceFunction;
        InlineFunctionDefinition function = BuildRecord(ast, ticket);

        RequireNameFreeOfBuiltIns(function.Name);

        await RequireSchemaLeaderAsync(database, "CREATE FUNCTION").ConfigureAwait(false);

        // Before any lock: the compile takes milliseconds to seconds, and a refused body must not
        // have held the schema of the database for that time.
        runtime.Validate(function, ticket.CancellationToken);

        await InDdlGateAsync(database, () => catalogs.CreateOrReplaceFunctionAsync(
            database,
            function,
            replace,
            checkNew: schema =>
            {
                if (InlineFunctionDeltaApplier.ExtensionThatDefines(schema, function.Name) is { } extension)
                    throw new CamusDBException(
                        CamusDBErrorCodes.FunctionAlreadyExists,
                        $"Cannot create function '{function.Name}': the installed extension '{extension.Name}' defines a function of that name");

                if (runtime.MaxFunctionsPerDatabase > 0 && schema.Functions.Count >= runtime.MaxFunctionsPerDatabase)
                    throw new CamusDBException(
                        CamusDBErrorCodes.SchemaLimitExceeded,
                        $"Database '{database.Name}' would exceed the maximum of {runtime.MaxFunctionsPerDatabase} inline functions per database (max_inline_functions_per_database)");
            },
            checkReplace: (schema, existing, next) =>
            {
                authorizer.RequireFunctionOwnership(database, existing, ticket, "replace");

                if (existing.HasSameSignature(next))
                    return;

                List<string> dependents = ExtensionDependencyFinder.FindCallers(schema, existing.Name, Parse);

                if (dependents.Count > 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.FunctionHasDependents,
                        $"Cannot change the parameter types or the result type of function '{existing.Name}' " +
                        $"({InlineFunctionSignature.Describe(existing)}) because these objects call it: {string.Join(", ", dependents)}");
            })).ConfigureAwait(false);
    }

    public async Task DropAsync(DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        string name = RequireName(ast);
        bool ifExists = ast.yytext == "if exists";

        await RequireSchemaLeaderAsync(database, "DROP FUNCTION").ConfigureAwait(false);

        // IF EXISTS only forgives a function that does not exist. Ownership and dependents still refuse.
        if (ifExists && !database.Schema.Functions.ContainsKey(name))
            return;

        try
        {
            await InDdlGateAsync(database, () => catalogs.DropFunctionAsync(database, name, (schema, existing) =>
            {
                authorizer.RequireFunctionOwnership(database, existing, ticket, "drop");

                List<string> dependents = ExtensionDependencyFinder.FindCallers(schema, existing.Name, Parse);

                if (dependents.Count > 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.FunctionHasDependents,
                        $"Cannot drop function '{existing.Name}' because these objects call it: {string.Join(", ", dependents)}");
            })).ConfigureAwait(false);
        }
        catch (CamusDBException exception) when (ifExists && exception.Code == CamusDBErrorCodes.FunctionNotFound)
        {
            // A concurrent DROP FUNCTION removed it between the check above and the proposal.
        }
    }

    public async Task AlterOwnerAsync(DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        string name = RequireName(ast);
        string newOwnerName = ast.rightAst!.yytext!;

        await RequireSchemaLeaderAsync(database, "ALTER FUNCTION").ConfigureAwait(false);

        // The new owner must exist now, with an id: the ownership check compares ids, so a name
        // alone would leave a function that only a superuser can change.
        UserRecord? newOwner = authService is null
            ? null
            : await authService.TryGetUserAsync(newOwnerName).ConfigureAwait(false);

        if (authService is not null && (newOwner is null || string.IsNullOrEmpty(newOwner.Id)))
            throw new CamusDBException(
                CamusDBErrorCodes.UserDoesNotExist,
                $"User '{newOwnerName}' does not exist, or predates user ids and cannot own a function");

        HLCTimestamp now = Now();

        await InDdlGateAsync(database, () => catalogs.SetFunctionOwnerAsync(
            database, name, newOwner?.Name ?? newOwnerName, newOwner?.Id, now,
            (_, existing) => authorizer.RequireFunctionOwnership(database, existing, ticket, "change the owner of"))).ConfigureAwait(false);
    }

    // ─── The record ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds the record of the function the statement defines. Checks the shape of the statement:
    /// a plain name, the one supported language, and parameter and result types from the accepted
    /// set. The compiler checks the body and the parameter names.
    /// </summary>
    private InlineFunctionDefinition BuildRecord(NodeAst ast, ExecuteSQLTicket ticket)
    {
        string name = RequireName(ast);
        string language = ast.extendedThree?.yytext ?? "";

        if (!string.Equals(language, InlineFunctionDefinition.CSharp, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Language '{language}' is not supported for CREATE FUNCTION; the only language is {InlineFunctionDefinition.CSharp}");

        (ColumnType returnType, ColumnType? returnElement) = ReadType(ast.extendedOne!, name, "the result");

        List<InlineFunctionParameter> parameters = [];

        // The parameter list is left-deep: unwind it with a loop, last parameter first.
        for (NodeAst? node = ast.rightAst; node is not null;)
        {
            NodeAst parameter = node.nodeType == NodeType.FunctionParameterList ? node.rightAst! : node;
            string parameterName = parameter.leftAst!.yytext ?? "";
            (ColumnType type, ColumnType? element) = ReadType(parameter.rightAst!, name, $"parameter '{parameterName}'");

            parameters.Add(new InlineFunctionParameter { Name = parameterName, Type = type, ElementType = element });
            node = node.nodeType == NodeType.FunctionParameterList ? node.leftAst : null;
        }

        parameters.Reverse();

        string source = ast.extendedTwo?.yytext ?? "";
        HLCTimestamp now = Now();

        // Null when authentication is off; then no ownership check applies either.
        Principal? principal = ticket.Principal;


        return new InlineFunctionDefinition
        {
            Name = name,
            Parameters = parameters,
            ReturnType = returnType,
            ReturnElementType = returnElement,
            NullInput = ast.yytext == "called" ? InlineFunctionNullInput.Called : InlineFunctionNullInput.ReturnsNull,
            Language = InlineFunctionDefinition.CSharp,
            Source = source,
            SourceSha256 = InlineFunctionRuntime.SourceHash(source),
            LanguageVersion = InlineFunctionRuntime.LanguageVersion,
            RuleRevision = InlineFunctionAllowList.Revision,
            Owner = principal?.UserName,
            OwnerId = principal?.UserId,
            CreatedAt = now,
            UpdatedAt = now,
        };
    }

    private static (ColumnType Type, ColumnType? Element) ReadType(NodeAst node, string function, string what)
    {
        (ColumnType Type, ColumnType? Element)? read = node.nodeType switch
        {
            NodeType.TypeInteger64 => (ColumnType.Integer64, null),
            NodeType.TypeFloat64 => (ColumnType.Float64, null),
            NodeType.TypeFloat32 => (ColumnType.Float32, null),
            NodeType.TypeBool => (ColumnType.Bool, null),
            NodeType.TypeString => (ColumnType.String, null),
            NodeType.TypeUuid => (ColumnType.Uuid, null),
            NodeType.TypeBytes => (ColumnType.Bytes, null),
            NodeType.TypeDateTime => (ColumnType.DateTime, null),
            NodeType.TypeNumeric => (ColumnType.Numeric, null),
            NodeType.TypeArray when node.leftAst is { nodeType: not NodeType.TypeArray } element && ReadType(element, function, what) is { Element: null } scalar =>
                (ColumnType.Array, scalar.Type),
            _ => null,
        };

        if (read is not { } type || !InlineFunctionSignature.IsAccepted(type.Type, type.Element))
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function}' is refused: the type of {what} is not accepted; the accepted types are {InlineFunctionSignature.AcceptedTypes}");

        return type;
    }

    private static string RequireName(NodeAst ast)
    {
        string? name = ast.leftAst?.yytext;

        if (!InlineFunctionSignature.IsPlainName(name))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid function name '{name}': a function name is a plain identifier (a letter or underscore, then letters, digits and underscores)");

        return name!;
    }

    /// <summary>
    /// Refuses a name that this build gives a meaning of its own: a built-in scalar function, a
    /// reserved aggregate name, a built-in table function, or a member of an extension this node loads
    /// (installed in the database or not: a later <c>CREATE EXTENSION</c> must not find its name taken).
    /// </summary>
    private void RequireNameFreeOfBuiltIns(string name)
    {
        string lowered = name.ToLowerInvariant();
        string? taken = null;

        if (ScalarFunctionEvaluator.IsBuiltInScalarFunction(lowered))
            taken = "a built-in function";
        else if (Queries.AggregateFunctionCatalog.IsReservedName(lowered))
            taken = "an aggregate";
        else if (TableFunctionRegistry.IsBuiltInTableFunction(lowered))
            taken = "a built-in table function";
        else
        {
            foreach (LoadedExtension extension in context.Extensions.Extensions)
            {
                if (extension.DefinesCallable(lowered))
                {
                    taken = $"a function of the extension '{extension.Name}'";
                    break;
                }
            }
        }

        if (taken is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.FunctionAlreadyExists,
                $"Cannot create function '{name}': the name is already {taken}. Functions of every kind share one name space");
    }

    // ─── Plumbing ────────────────────────────────────────────────────────────────────────────────

    private InlineFunctionRuntime RequireRuntime() =>
        context.ExtensionScopes.InlineFunctions
            ?? throw new CamusDBException(CamusDBErrorCodes.InlineFunctionsDisabled, "This engine was built without support for inline functions");

    private NodeAst Parse(string sql) => SQLParserProcessor.Parse(sql, sqlParserCache);

    /// <summary>
    /// Runs a proposal under the database's DDL semaphore, as the table DDL does, so the dependency
    /// check of a drop or a replace and a concurrent <c>CREATE TABLE</c> or <c>CREATE VIEW</c> that
    /// calls the function are not built against the same schema.
    /// </summary>
    private static async Task InDdlGateAsync(DatabaseDescriptor database, Func<Task> proposal)
    {
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await proposal().ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    private async Task RequireSchemaLeaderAsync(DatabaseDescriptor database, string statement)
    {
        if (!context.IsClusterMode)
            return;

        if (await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            return;

        string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"{statement} must be executed by schema leader '{leader}' for database '{database.Name}'");
    }

    private HLCTimestamp Now() =>
        context.SharedNode is { } node
            ? node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId())
            : default;
}
