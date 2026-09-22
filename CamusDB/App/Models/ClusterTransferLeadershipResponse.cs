/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Outcome of <c>POST /v1/cluster/transfer-leadership</c>. <c>Success</c> means the target was
/// observed leading the partition before the call returned; otherwise <c>Status</c> carries the
/// consensus layer's verdict (<c>NodeIsNotLeader</c> when this node does not lead the partition,
/// <c>LeaderAlreadyElected</c> when leadership moved elsewhere during the handover,
/// <c>Refused</c> when the request never reached consensus) and <c>Reason</c> says why.
/// </summary>
public sealed class ClusterTransferLeadershipResponse
{
    public bool Success { get; set; }

    public string Status { get; set; } = "";

    public int PartitionId { get; set; }

    public string TargetEndpoint { get; set; } = "";

    public string? Reason { get; set; }
}
