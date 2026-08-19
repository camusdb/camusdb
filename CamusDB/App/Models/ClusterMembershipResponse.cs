/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// The committed membership roster. Every node answers with the same committed roster (it is a
/// consensus record, not a per-node peer list); only <c>LocalRole</c> differs by responder.
/// </summary>
public sealed class ClusterMembershipResponse
{
    public long MembershipVersion { get; set; }

    public List<ClusterMemberModel> Members { get; set; } = new();

    public string LocalRole { get; set; } = "";

    public bool Initialized { get; set; }
}

/// <summary>One roster entry: a node's raft endpoint, id, role, and the version it joined at.</summary>
public sealed class ClusterMemberModel
{
    public string Endpoint { get; set; } = "";

    public int NodeId { get; set; }

    public string Role { get; set; } = "";

    public long JoinedVersion { get; set; }
}
