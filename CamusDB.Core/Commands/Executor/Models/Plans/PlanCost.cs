
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Cost estimate for a single physical plan node.
///
/// All fields count logical I/O units. KV point lookups, range-scan entries, and primary-store
/// row fetches each cost 1.0 unit — they are treated as equally expensive at this stage because
/// the underlying KV store's read amplification is not yet profiled (future work will differentiate
/// random vs. sequential I/O once real access-pattern data is available). In-memory rows
/// (sort, group, distinct) are cheap at 0.1 unit each.
///
/// <c>NetworkFactor</c> is the estimated bytes-shipped cost for remote partitions.
/// Non-zero only when key-range sharding is on and <c>ClusterPartitionCount &gt; 1</c>.
///
/// <c>Total</c> is the weighted sum used by <see cref="Controllers.Queries.CostEstimator"/>
/// to compare competing physical alternatives.
/// </summary>
public readonly struct PlanCost
{
    /// <summary>Estimated output cardinality of this node.</summary>
    public long EstimatedRows { get; init; }

    /// <summary>Number of KV point lookups (e.g. unique-index → primary row fetch).</summary>
    public long KvPointLookups { get; init; }

    /// <summary>Number of KV range-scan entries consumed (index leaf pages).</summary>
    public long KvRangeScanEntries { get; init; }

    /// <summary>Primary-store row fetches needed after index navigation (non-covering index).</summary>
    public long RowFetchesAfterIndex { get; init; }

    /// <summary>Rows materialised in memory (sort buffer, hash-group, distinct hash set).</summary>
    public long InMemoryRows { get; init; }

    /// <summary>
    /// Network shipping cost: <c>remoteRows × rowWidthBytes × NetWeight</c>.
    /// Non-zero only when key-range sharding is on and <see cref="CamusDBConfig.ClusterPartitionCount"/>
    /// &gt; 1. Zero for single-node deployments and for non-scan (pipeline) nodes.
    /// </summary>
    public double NetworkFactor { get; init; }

    // Weight rationale:
    //   Point lookup      = 1.0  (random I/O to primary store)
    //   Range-scan entry  = 1.0  (sequential index read, same weight as primary scan)
    //   Row fetch after index = 1.0 (random I/O to primary, like a point lookup)
    //   In-memory row     = 0.1  (cheap; avoids overweighting sort/group costs)
    /// <summary>Weighted total used for plan-choice comparisons.</summary>
    public double Total =>
        KvPointLookups * 1.0
        + KvRangeScanEntries * 1.0
        + RowFetchesAfterIndex * 1.0
        + InMemoryRows * 0.1
        + NetworkFactor;
}
