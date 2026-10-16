/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Refuses to start a node that has authentication on and peers to talk to, but no way to authenticate
/// those peers.
///
/// <para>The authentication layer fronts every route, the Raft and Kahuna gRPC services included. Those
/// services carry no user token, so with authentication on the only credential they can show is a
/// verified peer certificate, which exists only when <see cref="ConfigDefinition.RaftMutualTlsEnabled"/>
/// is on. Without it every consensus call from a peer is refused. The node starts, answers its health
/// probe as not ready, and the cluster never forms, with nothing but a stream of rejected calls in the
/// logs. Failing at startup names the setting instead.</para>
///
/// <para>A cluster-mode node with no peers is allowed: a single-node cluster has no peer traffic to
/// refuse. Authentication is checked here rather than in <see cref="ConfigDefinition.Validate"/> because
/// it comes from the environment, which is applied after the file is validated.</para>
/// </summary>
public static class ClusterAuthenticationStartupCheck
{
    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.InvalidConfig"/> when authentication is on, the node has
    /// peers, and peer certificates are not checked.
    /// </summary>
    public static void Validate(ConfigDefinition config, bool authenticationEnabled)
    {
        if (!authenticationEnabled || !config.IsClusterMode || config.Peers.Count == 0 || config.RaftMutualTlsEnabled)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidConfig,
            "Authentication is enabled on a node with cluster peers, but 'raft_mutual_tls_enabled' is off. " +
            "Peers send Raft and Kahuna traffic without a user token, so this node would refuse all of it and " +
            "the cluster would never form. Set 'raft_mutual_tls_enabled: true' and 'raft_peer_ca' in config.yml, " +
            "and give each node a 'raft_certificate' that allows client authentication.");
    }
}
