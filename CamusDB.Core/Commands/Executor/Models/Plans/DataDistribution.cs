
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Describes how a plan node's output data is distributed across the cluster.
/// Populated on scan leaves by the planner; null on all other nodes.
///
/// Staleness contract: reflects <see cref="CamusDB.Core.CamusDBOptions.KeyRangeShardingEnabled"/>
/// at plan-build time. Leadership transitions are not tracked — this is an approximate
/// snapshot suitable for cost estimates only. Re-planning is triggered by schema changes,
/// not by Raft leadership changes.
/// </summary>
public sealed class DataDistribution
{
    // ── Well-known singletons ─────────────────────────────────────────────

    /// <summary>
    /// All data is local to the executing node / coordinator.
    /// Used when key-range sharding is off, or for point lookups that return ≤ 1 row.
    /// </summary>
    public static readonly DataDistribution Gathered = new(DataDistributionKind.Gathered, null);

    /// <summary>
    /// Data is replicated to every node in the cluster.
    /// No CamusDB table uses this today; reserved for future read-replica / system-catalog use.
    /// </summary>
    public static readonly DataDistribution Replicated = new(DataDistributionKind.Replicated, null);

    // ── Instance ──────────────────────────────────────────────────────────

    public DataDistributionKind Kind { get; }

    /// <summary>
    /// The columns that determine the partition assignment when <see cref="Kind"/> is
    /// <see cref="DataDistributionKind.Partitioned"/>. Null for <c>Gathered</c> and
    /// <c>Replicated</c>.
    /// </summary>
    public string[]? KeyColumns { get; }

    private DataDistribution(DataDistributionKind kind, string[]? keyColumns)
    {
        Kind = kind;
        KeyColumns = keyColumns;
    }

    /// <summary>Creates a <see cref="DataDistributionKind.Partitioned"/> value.</summary>
    public static DataDistribution PartitionedBy(string[] keyColumns)
        => new(DataDistributionKind.Partitioned, keyColumns);

    /// <inheritdoc/>
    public override string ToString() => Kind switch
    {
        DataDistributionKind.Gathered    => "gathered",
        DataDistributionKind.Replicated  => "replicated",
        DataDistributionKind.Partitioned => $"partitioned({string.Join(",", KeyColumns ?? [])})",
        _                                => "unknown",
    };
}

/// <summary>Discriminant for <see cref="DataDistribution"/>.</summary>
public enum DataDistributionKind
{
    /// <summary>Single-node / coordinator; no network cost for this data path.</summary>
    Gathered,

    /// <summary>Range-sharded by <see cref="DataDistribution.KeyColumns"/>; may span multiple leaders.</summary>
    Partitioned,

    /// <summary>Replicated to every node; reads are always local.</summary>
    Replicated,
}
