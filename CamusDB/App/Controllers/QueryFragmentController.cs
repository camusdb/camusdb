/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.Pipelines;
using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.App.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Net.Http.Headers;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal endpoint that executes a peer coordinator's span-scan fragment and streams the
/// surviving rows back, one frame per row (<see cref="QueryFragmentWireCodec"/>). The
/// encoding is negotiated: a coordinator that lists the binary media type in <c>Accept</c>
/// gets binary frames; anything else — including a coordinator on an older build that sends
/// no <c>Accept</c> — gets NDJSON. Frames are written straight to the response pipe and
/// flushed one at a time, so the first row leaves as soon as it exists regardless of encoding.
/// Authenticated exclusively by the node secret — the <c>/internal/</c> middleware rule;
/// never by client tokens.
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
    private static readonly JsonWriterOptions NdjsonWriterOptions = new() { SkipValidation = true };

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

        bool binary = AcceptsBinary();
        Response.ContentType = binary ? QueryFragmentWireCodec.BinaryContentType : QueryFragmentWireCodec.NdjsonContentType;

        PipeWriter body = Response.BodyWriter;

        // One writer for the whole stream: Reset() between frames keeps its buffers and its
        // output target, so the per-row cost is the frame's bytes and nothing else.
        using Utf8JsonWriter? json = binary ? null : new Utf8JsonWriter(body, NdjsonWriterOptions);

        try
        {
            await foreach (QueryFragmentRow row in executor.ExecuteQueryFragment(request, cancellationToken))
            {
                if (binary)
                    QueryFragmentWireCodec.WriteBinaryFrame(body, row);
                else
                    WriteNdjsonLine(json!, body, row, error: null);

                await body.FlushAsync(cancellationToken);
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
            if (binary)
                QueryFragmentWireCodec.WriteBinaryError(body, ex.Message);
            else
                WriteNdjsonLine(json!, body, row: null, error: ex.Message);

            await body.FlushAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Emits one JSON object plus its newline into the pipe. The writer is flushed (its bytes
    /// committed to the pipe) and then reset so the next call starts a fresh top-level value.
    /// </summary>
    private static void WriteNdjsonLine(Utf8JsonWriter json, PipeWriter body, QueryFragmentRow? row, string? error)
    {
        if (row is not null)
            QueryFragmentWireCodec.WriteNdjsonFrame(json, row);
        else
            QueryFragmentWireCodec.WriteNdjsonError(json, error!);

        json.Flush();
        json.Reset();

        body.GetSpan(1)[0] = (byte)'\n';
        body.Advance(1);
    }

    /// <summary>
    /// True when the coordinator listed the binary media type in <c>Accept</c>. Parsed as a
    /// header list rather than a substring match so a quality-zero entry or a media type that
    /// merely shares a prefix cannot select it.
    /// </summary>
    private bool AcceptsBinary()
    {
        if (!MediaTypeHeaderValue.TryParseList(Request.Headers.Accept, out IList<MediaTypeHeaderValue>? accepted))
            return false;

        for (int i = 0; i < accepted.Count; i++)
        {
            MediaTypeHeaderValue media = accepted[i];

            if (media.Quality is 0)
                continue;

            if (media.MediaType.Equals(QueryFragmentWireCodec.BinaryContentType, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
