
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
/// </summary>
public sealed class PeerEndpointResolver
{
    private readonly Dictionary<string, Uri> peerEndpointMap = [];

    private readonly int httpPort;

    private readonly ILogger<ICamusDB> logger;

    public PeerEndpointResolver(
        IReadOnlyList<string> peers,
        IReadOnlyList<string> httpPeers,
        int httpPort,
        ILogger<ICamusDB> logger)
    {
        this.httpPort = httpPort;
        this.logger = logger;

        if (httpPeers.Count == peers.Count && httpPeers.Count > 0)
        {
            for (int i = 0; i < peers.Count; i++)
                peerEndpointMap[peers[i]] = new Uri($"http://{httpPeers[i]}");
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
        return new Uri($"http://{host}:{httpPort}");
    }
}
