/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using CamusDB.Core.Config;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>Body of a forwarded cluster-setting change. <c>Value</c> null means reset.</summary>
public sealed record ClusterSettingsForwardRequest(string Key, string? Value);

/// <summary>
/// Leader's answer to a forwarded change: <c>ok</c>, <c>not-leader</c> (caller re-resolves and
/// retries), or <c>failed</c> with the engine error code and message to rethrow at the origin.
/// </summary>
public sealed class ClusterSettingsForwardResponse
{
    public string? Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }
}

/// <summary>
/// Production forwarder carrying a cluster-setting change from a non-leader node to the
/// settings-partition leader over the authenticated <c>/internal/</c> route, mirroring
/// <see cref="HttpSchemaDdlForwarder"/>. Changes are last-writer-wins by Raft commit order, so a
/// retried forward that applied twice is harmless and no operation-id replay cache is needed.
///
/// <para>The node-secret header is attached on every request when a secret is configured: the
/// receiving node's middleware refuses <c>/internal/*</c> without it once authentication is on,
/// so a forwarder that omitted it would work in development and fail closed in production.</para>
/// </summary>
public sealed class HttpClusterSettingsForwarder : IClusterSettingsForwarder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private readonly HttpClient httpClient;

    private readonly Func<string, Uri> endpointResolver;

    private readonly string nodeSecret;

    private readonly ILogger logger;

    public HttpClusterSettingsForwarder(
        HttpClient httpClient,
        Func<string, Uri> endpointResolver,
        string nodeSecret,
        ILogger logger)
    {
        this.httpClient = httpClient;
        this.endpointResolver = endpointResolver;
        this.nodeSecret = nodeSecret;
        this.logger = logger;
    }

    public async Task<bool?> ForwardChangeAsync(string leader, string key, string? value, CancellationToken cancellationToken)
    {
        Uri endpoint = new(endpointResolver(leader), "/internal/cluster-settings/apply");

        HttpResponseMessage response;

        try
        {
            using HttpRequestMessage request = new(HttpMethod.Post, endpoint)
            {
                Content = JsonContent.Create(new ClusterSettingsForwardRequest(key, value), options: JsonOptions),
            };

            if (!string.IsNullOrEmpty(nodeSecret))
                request.Headers.TryAddWithoutValidation("X-Camus-Node-Secret", nodeSecret);

            response = await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is HttpRequestException || (e is TaskCanceledException && !cancellationToken.IsCancellationRequested))
        {
            // Transport failure to one endpoint is indistinguishable from a stale leader; report
            // not-leader so the caller re-resolves and retries rather than failing the statement.
            logger.LogWarning("Cluster-settings forward to {Endpoint} failed: {Message}", endpoint, e.Message);
            return null;
        }

        using (response)
        {
            if (response.StatusCode == HttpStatusCode.ServiceUnavailable)
                return null;

            ClusterSettingsForwardResponse? body = await response.Content
                .ReadFromJsonAsync<ClusterSettingsForwardResponse>(JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            if (body is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Empty response from cluster-settings leader at {endpoint}");

            return body.Status switch
            {
                "ok" => true,
                "not-leader" => null,
                "failed" => throw new CamusDBException(
                    body.Code ?? CamusDBErrorCodes.InvalidInternalOperation,
                    body.Message ?? "Unknown error from cluster-settings leader"),
                _ => throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unexpected status '{body.Status}' from {endpoint}"),
            };
        }
    }
}
