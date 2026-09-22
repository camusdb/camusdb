/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Asks the local node, which must currently lead <c>PartitionId</c>, to hand that partition's
/// leadership to the replica at <c>TargetEndpoint</c> (the consensus endpoint as the placement
/// table lists it, e.g. <c>10.0.0.3:7072</c>). The target must host the partition as a voter.
/// </summary>
public sealed class ClusterTransferLeadershipRequest
{
    public int PartitionId { get; set; }

    public string TargetEndpoint { get; set; } = "";
}
