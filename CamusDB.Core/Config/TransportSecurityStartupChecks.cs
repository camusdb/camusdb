/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Reports the configurations that leave this node's cluster traffic readable on the wire.
///
/// <para>It exists for the reason <see cref="KeyRangeSplitStartupChecks"/> exists: each of these
/// states starts the node successfully and then keeps working, so nothing else would ever mention
/// them. The difference is only visible to somebody watching the network, which is precisely the
/// person who will not tell the operator.</para>
///
/// <para>What is at stake is concrete. Two of the three node-to-node HTTP channels attach the shared
/// node secret as a header, and the Raft and Kahuna gRPC services carry replicated row data. Over a
/// plaintext link, an observer reads both — and holding the node secret makes the constant-time
/// comparison that guards the peer routes irrelevant, because they have the real value.</para>
///
/// <para>Nothing here fires for a single-node deployment or a loopback cluster: there is no peer
/// traffic to observe, and warning about a development setup is how a check becomes noise that
/// operators learn to scroll past.</para>
/// </summary>
public static class TransportSecurityStartupChecks
{
    /// <summary>
    /// Returns one message per condition that leaves peer traffic in the clear, empty when there is
    /// nothing to report. Each message names the setting that closes it, because a warning an operator
    /// cannot act on is worse than none.
    ///
    /// <para>More than one can hold at once and each is returned, since the HTTP channels and the gRPC
    /// listeners are closed by different settings — reporting only the first would send an operator
    /// through one restart per cause.</para>
    /// </summary>
    public static IReadOnlyList<string> Inspect(ConfigDefinition config)
    {
        List<string> warnings = [];

        if (!config.IsClusterMode || IsLoopbackCluster(config))
            return warnings;

        if (!config.PeerTlsEnabled && !HasExplicitHttpsPeer(config))
            warnings.Add(
                "This node is in cluster mode and reaches its peers over plaintext HTTP. The shared " +
                "node secret is sent as a header on schema-DDL and cluster-settings forwarding, so " +
                "anyone who can observe peer traffic can read it. Set peer_tls_enabled: true in " +
                "config.yml (and serve HTTPS on the peers' HTTP port), or give each http_peers entry " +
                "an explicit https:// address.");

        if (string.IsNullOrEmpty(config.RaftCertificate))
            warnings.Add(
                "This node is in cluster mode with no raft_certificate, so the Raft port serves " +
                "plaintext HTTP/2 (h2c). Replicated row data and consensus traffic cross the network " +
                "in the clear. Set raft_certificate in config.yml for an exposed cluster.");

        if (config.GrpcEnabled && string.IsNullOrEmpty(config.GrpcCertificate) && string.IsNullOrEmpty(config.RaftCertificate))
            warnings.Add(
                "The client-facing gRPC port has no certificate (neither grpc_certificate nor " +
                "raft_certificate is set), so it serves plaintext HTTP/2 (h2c) and bearer tokens " +
                "presented over it are readable on the wire. Set grpc_certificate in config.yml.");

        return warnings;
    }

    /// <summary>
    /// Whether every peer address names the loopback interface, which makes this a single-host
    /// cluster whose traffic never reaches a network. Judged from the peer list rather than this
    /// node's own host, because it is the traffic between nodes that is at issue.
    /// </summary>
    private static bool IsLoopbackCluster(ConfigDefinition config)
    {
        if (config.Peers.Count == 0)
            return true;

        foreach (string peer in config.Peers)
        {
            if (!IsLoopbackHost(HostOf(peer)))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Whether at least one <c>http_peers</c> entry already names <c>https</c> for itself. Such an
    /// entry overrides <c>peer_tls_enabled</c> in <see cref="PeerEndpointResolver"/>, so a deployment
    /// that spells every peer out in full is not misconfigured and must not be warned at.
    /// </summary>
    private static bool HasExplicitHttpsPeer(ConfigDefinition config)
    {
        foreach (string httpPeer in config.HttpPeers)
        {
            if (httpPeer.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static string HostOf(string endpoint)
    {
        int colon = endpoint.LastIndexOf(':');
        return colon > 0 ? endpoint[..colon] : endpoint;
    }

    /// <summary>
    /// Whether a host names the local machine. <c>localhost</c> is matched by name as well as by
    /// address, because a peer list generally spells it that way and parsing it as an address fails.
    /// </summary>
    private static bool IsLoopbackHost(string host)
    {
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            return true;

        return IPAddress.TryParse(host, out IPAddress? address) && IPAddress.IsLoopback(address);
    }
}
