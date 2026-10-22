/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Runs <c>CALL name(args)</c>: resolves the procedure among the extensions installed in the database, evaluates
/// the arguments, and runs the procedure inside the statement's transaction.
///
/// <para><b>Arguments</b> are evaluated like the arguments of a function call in a FROM-less SELECT:
/// against no row, with the statement's parameters, so <c>CALL p(@x, 1 + 2, now())</c> works. Each
/// value is handed over as the plain CLR value of the extension value mapping. The count is checked
/// against the procedure's declared range before the procedure runs.</para>
///
/// <para><b>Transaction.</b> The procedure's SQL runs through an <see cref="ExtensionSqlSession"/> over
/// the caller's transaction, so everything the <c>CALL</c> wrote commits or rolls back as one
/// statement. An exception from the procedure ends the statement as
/// <see cref="CamusDBErrorCodes.ProcedureFailed"/>; the transport then rolls the transaction back. A
/// <see cref="CamusDBException"/> raised by the procedure's own SQL keeps its code.</para>
///
/// <para>The <c>CALL</c> reports, as its affected-row count, the total number of rows its SQL changed.
/// It returns no result set in this version.</para>
/// </summary>
internal static class ProcedureRunner
{
    public static async Task<int> CallAsync(CommandExecutor executor, DatabaseDescriptor database, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string name = ast.leftAst?.yytext ?? "";

        // Resolved against the extensions installed in this database, as this node loads them; a
        // procedure of an extension that is only available here, not installed, is not found.
        if (!executor.ExtensionScopes.Resolve(database).TryGetProcedure(name, out ExtensionProcedure procedure))
            throw new CamusDBException(CamusDBErrorCodes.ProcedureNotFound, $"No extension installed in database '{database.Name}' defines the procedure '{name}'");

        IReadOnlyList<ColumnValue> values = ScalarFunctionArguments.EvaluateArgumentList(
            ast.rightAst,
            EmptyRow,
            ticket.Parameters,
            rowNameResolver: null,
            static (expression, row, parameters, resolver) => SQLExecutorBaseCreator.EvalExpr(expression, row, parameters, resolver));

        if (values.Count < procedure.MinArguments || values.Count > procedure.MaxArguments)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Procedure '{name}' expects {DescribeArity(procedure)} argument(s) but received {values.Count}");

        object?[] arguments = new object?[values.Count];
        for (int i = 0; i < values.Count; i++)
            arguments[i] = ExtensionValueMapping.ToObject(values[i]);

        ExtensionSqlSession session = new(executor, database.Name, ticket.TxnState, ticket.Principal);

        await RunAsync(procedure, new ProcedureContext(name, arguments, session), session).ConfigureAwait(false);

        return (int)Math.Min(session.AffectedRows, int.MaxValue);
    }

    private static readonly IReadOnlyDictionary<string, ColumnValue> EmptyRow = new Dictionary<string, ColumnValue>();

    private static async Task RunAsync(ExtensionProcedure procedure, ProcedureContext context, ExtensionSqlSession session)
    {
        ExtensionNesting.Enter("Procedure '" + procedure.Name + "'");

        try
        {
            await procedure.Instance.ExecuteAsync(context).ConfigureAwait(false);

            // The procedure caught an error from its own SQL and returned; see ExtensionSqlSession.
            if (session.Failure is { } failure)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionCaughtSqlError,
                    $"Procedure '{procedure.Name}' returned after {failure}; without savepoints the CALL cannot continue");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (CamusDBException ex) when (ex.Code != CamusDBErrorCodes.SchemaCatchingUp)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.ProcedureFailed,
                $"Procedure '{procedure.Name}' failed: {ex.Message}",
                ex);
        }
    }

    private static string DescribeArity(ExtensionProcedure procedure) =>
        procedure.MinArguments == procedure.MaxArguments ? procedure.MinArguments.ToString()
        : procedure.MaxArguments == int.MaxValue ? "at least " + procedure.MinArguments
        : procedure.MinArguments + " to " + procedure.MaxArguments;

    private sealed class ProcedureContext : IProcedureContext
    {
        public ProcedureContext(string procedureName, IReadOnlyList<object?> arguments, ISqlSession sql)
        {
            ProcedureName = procedureName;
            Arguments = arguments;
            Sql = sql;
        }

        public string ProcedureName { get; }

        public IReadOnlyList<object?> Arguments { get; }

        public ISqlSession Sql { get; }

        // A statement that writes is not cancellable once it has started writing; see the contract.
        public CancellationToken CancellationToken => CancellationToken.None;
    }
}
