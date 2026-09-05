/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Config;

/// <summary>
/// Maps a peer's Raft endpoint (the engine's canonical node identity, e.g. <c>host:7070</c>)
/// to its HTTP base URI, from the parallel <c>peers</c>/<c>http_peers</c> config lists. This
/// is the single resolver shared by every CamusDB node-to-node channel (schema-DDL forwarding,
/// cluster-settings forwarding, query fragments) — it existed as two inline copies in the host
/// wiring before the third channel forced the extraction.
///
/// <para>When <c>http_peers</c> is populated and its count matches <c>peers</c>, each entry is
/// the exact HTTP address for that node. Otherwise the resolver falls back to the uniform-port
/// heuristic: same host as the Raft endpoint, this node's HTTP port. A populated map that
/// misses a lookup logs a warning — that shape means a <c>peers</c> entry does not byte-match
/// the format Raft reports, and silently falling back would route internal traffic to the
/// wrong address.</para>
///
/// <para><b>The scheme is configuration, not a constant.</b> Two of the three channels this feeds
/// attach the cluster's shared node secret as a header, so a hard-coded <c>http://</c> would put that
/// secret on the wire in the clear on every forwarded operation, with no way for an operator to
/// change it — the token HMAC and the constant-time comparisons elsewhere buy nothing against an
/// observer who can simply read the secret off the network. An <c>http_peers</c> entry may carry its
/// own scheme and wins outright; everything else follows <c>peer_tls_enabled</c>.</para>
/// </summary>
public sealed class PeerEndpointResolver
{
    private readonly Dictionary<string, Uri> peerEndpointMap = [];

    private readonly int httpPort;

    private readonly string scheme;

    private readonly ILogger<ICamusDB> logger;

    /// <param name="peers">Raft endpoints, the canonical node identities.</param>
    /// <param name="httpPeers">Parallel HTTP addresses; an entry may carry its own scheme.</param>
    /// <param name="httpPort">This node's HTTP port, used by the uniform-port fallback.</param>
    /// <param name="peerTlsEnabled">Whether to reach peers over HTTPS when the address names no scheme.</param>
    /// <param name="logger">Logs a map miss, which means the peer lists do not agree with Raft.</param>
    public PeerEndpointResolver(
        IReadOnlyList<string> peers,
        IReadOnlyList<string> httpPeers,
        int httpPort,
        bool peerTlsEnabled,
        ILogger<ICamusDB> logger)
    {
        this.httpPort = httpPort;
        this.logger = logger;
        this.scheme = peerTlsEnabled ? "https" : "http";

        if (httpPeers.Count == peers.Count && httpPeers.Count > 0)
        {
            for (int i = 0; i < peers.Count; i++)
                peerEndpointMap[peers[i]] = new Uri(WithScheme(httpPeers[i]));
        }
    }

    public Uri Resolve(string raftEndpoint)
    {
        if (peerEndpointMap.TryGetValue(raftEndpoint, out Uri? mapped))
            return mapped;

        if (peerEndpointMap.Count > 0)
            logger.LogWarning(
                "Raft endpoint '{RaftEndpoint}' not found in http_peers map (keys: {Keys}); " +
                "falling back to uniform-port heuristic. If this is unexpected, verify that " +
                "each peers entry byte-matches the format Raft reports (host:raftPort).",
                raftEndpoint,
                string.Join(", ", peerEndpointMap.Keys));

        string host = raftEndpoint.Contains(':') ? raftEndpoint.Split(':')[0] : raftEndpoint;
        return new Uri($"{scheme}://{host}:{httpPort}");
    }

    /// <summary>
    /// Prefixes the configured scheme onto a peer address that does not already name one.
    ///
    /// <para>An address that names its own scheme is left alone, so a single peer reachable
    /// differently from the rest can be written out in full rather than forcing the whole cluster onto
    /// one setting. The test is for <c>"://"</c> rather than a specific prefix, so an address written
    /// with any scheme survives instead of becoming <c>http://https://host</c>.</para>
    /// </summary>
    private string WithScheme(string httpPeer)
        => httpPeer.Contains("://", StringComparison.Ordinal) ? httpPeer : $"{scheme}://{httpPeer}";
}
