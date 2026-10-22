/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Refuses to start a cluster node whose <see cref="ConfigDefinition.Peers"/> list names the node's own
/// Raft address (<see cref="ConfigDefinition.RaftHost"/>:<see cref="ConfigDefinition.RaftPort"/>).
///
/// <para>The peer list becomes Kommander's static discovery set unchanged, and Kommander uses that set as
/// the peer list of every partition until the first membership roster is committed. Kommander treats its
/// own endpoint in a peer list as corruption: the startup handshake of partition 0 throws
/// "Corrupted nodes", the partition executor stops for good, and the node stays up with no system
/// partition, so the cluster never forms and only a log line says why. The roster that Kommander seeds
/// later does strip the local endpoint, but no roster is ever seeded, because partition 0 is already
/// dead. Failing here names the entry instead.</para>
///
/// <para>The comparison is literal, as Kommander's is: the host matches case-insensitively and the port
/// numerically, but no name is resolved, so <c>localhost:7070</c> does not match a node whose
/// <c>raft_host</c> is <c>127.0.0.1</c>. A join seed list (<see cref="ConfigDefinition.JoinExisting"/>)
/// is the same list and feeds the same discovery set, so it is checked too.</para>
///
/// <para>This runs on the final configuration rather than in <see cref="ConfigDefinition.Validate"/>,
/// because the file is validated before command-line flags apply. A file that lists
/// <c>localhost:7070</c> as a peer is correct for a node started with <c>--raft-port=7072</c>, and the
/// file-time check would see only the default port and refuse it.</para>
/// </summary>
public static class PeerListStartupCheck
{
    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.InvalidConfig"/> when a cluster-mode configuration lists the
    /// node's own Raft address among its peers. Call it after every configuration layer is applied.
    /// </summary>
    public static void Validate(ConfigDefinition config)
    {
        if (!config.IsClusterMode)
            return;

        ReadOnlySpan<char> localHost = TrimBrackets(config.RaftHost);

        foreach (string peer in config.Peers)
        {
            int colon = peer.LastIndexOf(':');
            if (colon <= 0)
                continue;

            if (!int.TryParse(peer.AsSpan(colon + 1), out int port) || port != config.RaftPort)
                continue;

            if (!TrimBrackets(peer.AsSpan(0, colon)).Equals(localHost, StringComparison.OrdinalIgnoreCase))
                continue;

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'peers' lists '{peer}', which is this node's own Raft address " +
                $"('raft_host' {config.RaftHost}, 'raft_port' {config.RaftPort}). List only the other nodes " +
                "of the cluster in 'peers' (--initial-cluster), and drop the matching entry from 'http_peers' " +
                "(--http-peers) so the two lists stay parallel.");
        }
    }

    private static ReadOnlySpan<char> TrimBrackets(ReadOnlySpan<char> host) =>
        host.Length >= 2 && host[0] == '[' && host[^1] == ']' ? host[1..^1] : host;
}
