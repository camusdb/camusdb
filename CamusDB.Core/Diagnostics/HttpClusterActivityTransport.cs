/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Http;
using System.Net.Http.Json;
using CamusDB.Core.Config;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Production <see cref="IClusterActivityTransport"/>: POSTs to the peer's
/// <c>/internal/activity/*</c> endpoints with the cluster's node secret, as the settings forwarder
/// and the query-fragment transport do. The bodies are small JSON documents; the peer's answer is
/// bounded by the number of statements and connections it holds open.
///
/// <para>No retries, and no timeout of its own: the caller passes a token that carries the per-peer
/// timeout, and decides what a failure means.</para>
/// </summary>
public sealed class HttpClusterActivityTransport : IClusterActivityTransport
{
    private readonly HttpClient httpClient;

    private readonly PeerEndpointResolver resolver;

    private readonly string? nodeSecret;

    public HttpClusterActivityTransport(HttpClient httpClient, PeerEndpointResolver resolver, string? nodeSecret)
    {
        // The caller's token is the only deadline, so the client's own default must not cut it short.
        httpClient.Timeout = Timeout.InfiniteTimeSpan;
        this.httpClient = httpClient;
        this.resolver = resolver;
        this.nodeSecret = nodeSecret;
    }

    public async Task<List<QueryActivityRow>> ListQueriesAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken)
        => await PostAsync<ClusterActivityWire.ListRequest, List<QueryActivityRow>>(
            targetRaftEndpoint, ClusterActivityWire.QueriesPath, new ClusterActivityWire.ListRequest(viewer), cancellationToken).ConfigureAwait(false);

    public async Task<List<ConnectionActivityRow>> ListConnectionsAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken)
        => await PostAsync<ClusterActivityWire.ListRequest, List<ConnectionActivityRow>>(
            targetRaftEndpoint, ClusterActivityWire.ConnectionsPath, new ClusterActivityWire.ListRequest(viewer), cancellationToken).ConfigureAwait(false);

    public async Task<QueryCancelOutcome> CancelAsync(string targetRaftEndpoint, string queryId, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        ClusterActivityWire.CancelResponse response = await PostAsync<ClusterActivityWire.CancelRequest, ClusterActivityWire.CancelResponse>(
            targetRaftEndpoint, ClusterActivityWire.CancelPath, new ClusterActivityWire.CancelRequest(queryId, viewer), cancellationToken).ConfigureAwait(false);

        return response.Outcome;
    }

    private async Task<TResponse> PostAsync<TRequest, TResponse>(
        string targetRaftEndpoint, string path, TRequest body, CancellationToken cancellationToken)
    {
        Uri target = new(resolver.Resolve(targetRaftEndpoint), path);

        using HttpRequestMessage message = new(HttpMethod.Post, target) { Content = JsonContent.Create(body) };

        if (!string.IsNullOrEmpty(nodeSecret))
            message.Headers.TryAddWithoutValidation("X-Camus-Node-Secret", nodeSecret);

        using HttpResponseMessage response = await httpClient.SendAsync(message, cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            string text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Activity request to '{targetRaftEndpoint}' failed with HTTP {(int)response.StatusCode}: {text}");
        }

        return await response.Content.ReadFromJsonAsync<TResponse>(cancellationToken).ConfigureAwait(false)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Activity request to '{targetRaftEndpoint}' returned an empty body");
    }
}
