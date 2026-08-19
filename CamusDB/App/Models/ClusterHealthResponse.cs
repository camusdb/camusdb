/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Readiness probe answer for the local node. <c>Ready</c> requires cluster initialization to have
/// completed AND a serving roster role — a node can answer HTTP long before it can serve a single
/// key/value request. <c>HostedPartitions</c> is informational only: under a replication factor a
/// node hosting zero data partitions is still ready (it serves every key by forwarding), so probes
/// must never gate on it.
/// </summary>
public sealed class ClusterHealthResponse
{
    public bool Ready { get; set; }

    public bool Initialized { get; set; }

    public string LocalRole { get; set; } = "";

    public int HostedPartitions { get; set; }
}
