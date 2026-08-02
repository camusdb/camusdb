
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Returns <see cref="DataDistribution"/> values for plan scan leaves.
///
/// Answers are derived from <see cref="CamusDBOptions.KeyRangeShardingEnabled"/> and the
/// index/table key-column schema. This tells the cost model WHICH COLUMNS partition the data,
/// but NOT how many partitions a given key range spans or whether those partitions are remote.
///
/// The live Kahuna range-map read ("for range R, how many partitions, is any remote?") is
/// deferred, to produce a real <c>NetworkFactor</c> in <see cref="PlanCost"/>. There is no
/// point reading the range map if the cost model cannot yet act on it.
///
/// Staleness contract: reflects the sharding flag at plan-build time. Raft leadership
/// changes are NOT tracked; the planner re-plans on schema changes, not on leader elections
/// (leadership is dynamic and best handled at the routing / execution layer).
/// </summary>
internal sealed class PlacementReader
{
    /// <summary>Shared singleton; stateless once constructed.</summary>
    public static readonly PlacementReader Instance = new();

    // ── Public API ────────────────────────────────────────────────────────

    /// <summary>
    /// Distribution for a full primary-row table scan (TableScanNode with PrimaryRows source).
    /// Gathered when sharding is off; Partitioned by PK key columns when sharding is on.
    /// </summary>
    public DataDistribution GetPrimaryRowScanDistribution(TableDescriptor table, CamusDBOptions options)
    {
        if (!options.KeyRangeShardingEnabled)
            return DataDistribution.Gathered;

        string[] pkCols = GetPkColumns(table);
        return pkCols.Length > 0
            ? DataDistribution.PartitionedBy(pkCols)
            : DataDistribution.Gathered;
    }

    /// <summary>
    /// Distribution for an index range scan or forced-index full scan.
    /// Gathered when sharding is off; Partitioned by the index key columns when sharding is on.
    /// </summary>
    public DataDistribution GetIndexScanDistribution(TableIndexSchema index, CamusDBOptions options)
    {
        if (!options.KeyRangeShardingEnabled)
            return DataDistribution.Gathered;

        return index.Columns is { Length: > 0 }
            ? DataDistribution.PartitionedBy(index.Columns)
            : DataDistribution.Gathered;
    }

    /// <summary>
    /// Distribution for a unique-index point lookup or IN-list scan.
    /// Always Gathered: a point lookup returns at most 1 row, which is pulled to the coordinator
    /// regardless of which partition holds it. IN-list scans are a collection of such lookups.
    /// </summary>
    public static DataDistribution GetLookupDistribution() => DataDistribution.Gathered;

    // ── Remote-fraction for NetworkFactor computation ────────────────────

    /// <summary>
    /// Returns the fraction of a scan's output rows that cross a network boundary.
    ///
    /// Model (approximate/cached):
    ///   - Sharding off OR partitions ≤ 1 → 0.0 (everything local, NetworkFactor = 0).
    ///   - Otherwise → <c>(N-1)/N</c> where N = <see cref="CamusDBOptions.ClusterPartitionCount"/>.
    ///     This treats all non-local partitions as equally likely to hold any given row
    ///     and the executing node as leader of exactly one partition. It is a rough
    ///     approximation; precise per-range partition assignment requires reading Kahuna's
    ///     internal range-map (deferred to a future Kahuna API).
    ///
    /// Staleness contract: reflects the startup partition count, not live Raft state.
    /// </summary>
    public static double GetRemoteFraction(CamusDBOptions options)
    {
        if (!options.KeyRangeShardingEnabled)
            return 0.0;

        int n = options.ClusterPartitionCount;
        return n <= 1 ? 0.0 : (n - 1.0) / n;
    }

    // ── Private helpers ───────────────────────────────────────────────────

    private static string[] GetPkColumns(TableDescriptor table)
    {
        if (table.Indexes is null)
            return [];

        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (kv.Key == "~pk" && kv.Value.Columns is { Length: > 0 })
                return kv.Value.Columns;
        }

        return [];
    }
}
