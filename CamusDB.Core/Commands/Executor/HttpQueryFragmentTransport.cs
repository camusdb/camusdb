
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Config;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// One NDJSON line of the fragment response stream: either a surviving row or a terminal
/// error frame. The server cannot change the HTTP status once rows have started streaming,
/// so a mid-stream failure arrives as an <see cref="Error"/> line instead; the client throws
/// on it and the coordinator's local-resume fallback takes over.
/// </summary>
public sealed class QueryFragmentWireLine
{
    public string? RowIdHex { get; set; }

    public byte[]? Data { get; set; }

    /// <summary>Partial-aggregate cells (see <see cref="Models.Queries.QueryFragmentRow.CellsJson"/>).</summary>
    public string? Cells { get; set; }

    public string? Error { get; set; }
}

/// <summary>
/// Production <see cref="IQueryFragmentTransport"/>: POSTs the fragment request to the target
/// node's <c>/internal/query-fragment</c> endpoint (node-secret authenticated, exactly like
/// the settings forwarder) and streams the NDJSON response. No transparent retries — the
/// coordinator owns retry/fallback, and a fragment is not idempotent mid-stream. The
/// <see cref="HttpClient"/> must have an infinite timeout: a fragment stream lives as long as
/// the scan it feeds, and cancellation (which aborts the request, and with it the remote
/// execution) is the only legitimate way to end it early.
/// </summary>
public sealed class HttpQueryFragmentTransport : IQueryFragmentTransport
{
    private readonly HttpClient httpClient;

    private readonly PeerEndpointResolver resolver;

    private readonly string? nodeSecret;

    public HttpQueryFragmentTransport(HttpClient httpClient, PeerEndpointResolver resolver, string? nodeSecret)
    {
        httpClient.Timeout = Timeout.InfiniteTimeSpan;
        this.httpClient = httpClient;
        this.resolver = resolver;
        this.nodeSecret = nodeSecret;
    }

    public async IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentAsync(
        string targetRaftEndpoint,
        QueryFragmentRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Uri target = new(resolver.Resolve(targetRaftEndpoint), "/internal/query-fragment");

        using HttpRequestMessage message = new(HttpMethod.Post, target)
        {
            Content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json"),
        };

        if (!string.IsNullOrEmpty(nodeSecret))
            message.Headers.TryAddWithoutValidation("X-Camus-Node-Secret", nodeSecret);

        using HttpResponseMessage response = await httpClient
            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            string body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query fragment on '{targetRaftEndpoint}' failed with HTTP {(int)response.StatusCode}: {body}");
        }

        await using Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using StreamReader reader = new(stream, Encoding.UTF8);

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (line.Length == 0)
                continue;

            QueryFragmentWireLine? parsed = JsonSerializer.Deserialize<QueryFragmentWireLine>(line);

            if (parsed is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Query fragment on '{targetRaftEndpoint}' returned an unreadable frame");

            if (parsed.Error is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Query fragment on '{targetRaftEndpoint}' failed mid-stream: {parsed.Error}");

            if (parsed.Cells is not null)
            {
                yield return new QueryFragmentRow(null, null, parsed.Cells);
                continue;
            }

            if (parsed.RowIdHex is null || parsed.Data is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Query fragment on '{targetRaftEndpoint}' returned an incomplete row frame");

            yield return new QueryFragmentRow(parsed.RowIdHex, parsed.Data);
        }
    }
}
