
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.App.Services;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal endpoint that executes a peer coordinator's span-scan fragment and streams the
/// surviving rows back as NDJSON (<see cref="QueryFragmentWireLine"/> per line). Authenticated
/// exclusively by the node secret — the <c>/internal/</c> middleware rule; never by client
/// tokens.
///
/// <para>Cancellation and zombie prevention: execution is bound to
/// <see cref="HttpContext.RequestAborted"/>, so a coordinator that cancels, crashes, or loses
/// its connection tears down the remote scan with the request — no fragment outlives its
/// caller. Failures before the first row surface as a plain HTTP 500; failures after
/// streaming has started (the status is already sent) surface as a terminal error frame the
/// transport converts back into an exception.</para>
/// </summary>
[ApiController]
public sealed class QueryFragmentController : CommandsController
{
    private static readonly byte[] newline = "\n"u8.ToArray();

    public QueryFragmentController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger,
        CamusDBOptions options)
        : base(executor, transactions, logger, options)
    {
    }

    [HttpPost]
    [Route("/internal/query-fragment")]
    public async Task ExecuteFragment()
    {
        CancellationToken cancellationToken = HttpContext.RequestAborted;

        QueryFragmentRequest? request;

        try
        {
            request = await JsonSerializer.DeserializeAsync<QueryFragmentRequest>(
                Request.Body, cancellationToken: cancellationToken);
        }
        catch (JsonException)
        {
            request = null;
        }

        if (request is null)
        {
            Response.StatusCode = StatusCodes.Status400BadRequest;
            await Response.WriteAsync("unreadable query-fragment request", cancellationToken);
            return;
        }

        Response.ContentType = "application/x-ndjson";

        try
        {
            await foreach (QueryFragmentRow row in executor.ExecuteQueryFragment(request, cancellationToken))
            {
                await JsonSerializer.SerializeAsync(
                    Response.Body,
                    new QueryFragmentWireLine { RowIdHex = row.RowIdHex, Data = row.Data },
                    cancellationToken: cancellationToken);
                await Response.Body.WriteAsync(newline, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Coordinator went away; nothing to report to nobody.
        }
        catch (Exception ex)
        {
            if (!Response.HasStarted)
            {
                Response.StatusCode = StatusCodes.Status500InternalServerError;
                await Response.WriteAsync(ex.Message, CancellationToken.None);
                return;
            }

            // Rows already streamed: the status is committed, so report through an error frame.
            await JsonSerializer.SerializeAsync(
                Response.Body,
                new QueryFragmentWireLine { Error = ex.Message },
                cancellationToken: CancellationToken.None);
            await Response.Body.WriteAsync(newline, CancellationToken.None);
        }
    }
}
