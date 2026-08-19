/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Per-partition replication-factor override. <c>ReplicationFactor</c> 0 clears the override so
/// the partition inherits the global factor again. The change adjusts the placement target only —
/// the rebalancer moves replicas toward it on later passes.
/// </summary>
public sealed class SetReplicationFactorRequest
{
    public int PartitionId { get; set; }

    public int ReplicationFactor { get; set; }
}
