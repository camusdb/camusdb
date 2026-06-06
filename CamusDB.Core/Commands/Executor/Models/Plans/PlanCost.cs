
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Cost estimate for a single physical plan node (R9).
///
/// All fields count logical I/O units; a KV point lookup and a KV range-scan entry are both
/// one unit, but they have different weights because random point lookups are heavier than
/// sequential range reads. Row fetches from the primary store after an index scan are weighted
/// like point lookups. In-memory rows (sort, group, distinct) are cheap but non-zero.
///
/// <c>NetworkFactor</c> is always 0 in the current single-partition deployment; reserved for
/// distributed sharding.
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

    /// <summary>Reserved for distributed plans; always 0.0 today.</summary>
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
