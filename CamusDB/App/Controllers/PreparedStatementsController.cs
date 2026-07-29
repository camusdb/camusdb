
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// The REST lifecycle for prepared statements: register a statement once, then execute it by handle
/// through the normal SQL endpoints with values sent positionally.
///
/// <para>These are separate endpoints rather than a mode of the execution routes because preparing
/// and executing are genuinely different operations with different lifetimes — one mints a handle,
/// the other spends it, and a client that conflates them gains nothing.</para>
///
/// <para>A handle is local to this node and to the caller's principal; see
/// <see cref="PreparedStatementRegistry"/> for why REST cannot borrow the gRPC stream-scoped model.
/// Clients must treat <c>CADB0520</c> (HTTP 404) on execution as routine and re-prepare.</para>
/// </summary>
[ApiController]
public sealed class PreparedStatementsController : CommandsController
{
    private readonly PreparedStatementRegistry preparedStatements;

    public PreparedStatementsController(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        PreparedStatementRegistry preparedStatements,
        ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {
        this.preparedStatements = preparedStatements;
    }

    /// <summary>
    /// Parses and registers a statement, replying with its handle and the parameter names in binding
    /// order.
    ///
    /// <para>Registration performs no privilege check: authorization runs on execution against the
    /// executing request's principal, exactly as for an inline statement, so preparing reveals
    /// nothing beyond whether the SQL parses. A statement that does not parse fails here rather than
    /// surprising whichever execution happens to be first.</para>
    /// </summary>
    [HttpPost]
    [Route("/prepare-sql-statement")]
    public async Task<JsonResult> PrepareSQLStatement()
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        try
        {
            PrepareStatementRequest? request =
                await JsonSerializer.DeserializeAsync<PrepareStatementRequest>(Request.Body, jsonOptions).ConfigureAwait(false);

            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "PrepareSQLStatement request is not valid");

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            (string handle, PreparedStatement statement) =
                preparedStatements.Prepare(principal, request.DatabaseName ?? "", request.Sql ?? "");

            return new JsonResult(new PrepareStatementResponse("ok", handle, statement.ParameterNames)
            {
                ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds,
            });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);

            return new JsonResult(new PrepareStatementResponse("failed", e.Code, e.Message)
            {
                ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds,
            })
            { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new PrepareStatementResponse("failed", "CA0000", e.Message)
            {
                ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds,
            })
            { StatusCode = 500 };
        }
    }

    /// <summary>
    /// Releases a prepared statement. Idempotent: closing a handle that is unknown, already closed,
    /// or long expired reports success, because the caller's requested end state holds either way
    /// and a client cleaning up should not have to tell those apart. Skipping close entirely is also
    /// safe — the idle reaper collects what a client abandons.
    /// </summary>
    [HttpPost]
    [Route("/close-sql-statement")]
    public async Task<JsonResult> CloseSQLStatement()
    {
        try
        {
            CloseStatementRequest? request =
                await JsonSerializer.DeserializeAsync<CloseStatementRequest>(Request.Body, jsonOptions).ConfigureAwait(false);

            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "CloseSQLStatement request is not valid");

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            preparedStatements.Close(principal, request.StatementId ?? "");

            return new JsonResult(new CloseStatementResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);

            return new JsonResult(new CloseStatementResponse("failed", e.Code, e.Message))
            { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CloseStatementResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
