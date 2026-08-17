
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
    /// <para><b>Cluster mode:</b> read from the live placement snapshot
    /// (<see cref="Storage.Kv.EmbeddedKahuna.GetPlacement"/>): the fraction of the scanned
    /// key space's spans whose partition leader is not this node (unknown leaders count as
    /// remote). The key space is the one the leaf actually scans — the index bucket for index
    /// scans, the row bucket otherwise. The snapshot is TTL-cached and advisory; staleness
    /// mis-costs a plan, never breaks one.</para>
    ///
    /// <para><b>Standalone:</b> there is no cluster to read, so the declared-topology
    /// approximation is kept: <c>(N-1)/N</c> with N =
    /// <see cref="CamusDBOptions.ClusterPartitionCount"/>. This is what lets single-node
    /// planner tests and cost experiments model a sharded layout that does not physically
    /// exist yet; a real cluster ignores the declared count in favor of the live map.</para>
    ///
    /// Only called for Partitioned scan leaves (sharding on) — the caller gates on
    /// <see cref="DataDistributionKind.Partitioned"/>, so the sharding-off answer stays 0.
    /// </summary>
    public static double GetRemoteFraction(
        DatabaseDescriptor database,
        TableDescriptor? table,
        PhysicalPlanNode node,
        CamusDBOptions options)
    {
        if (!options.KeyRangeShardingEnabled)
            return 0.0;

        if (table is null || !database.Kahuna.IsClusterMode)
        {
            int n = options.ClusterPartitionCount;
            return n <= 1 ? 0.0 : (n - 1.0) / n;
        }

        string keySpace = node switch
        {
            IndexRangeScanNode { Index.Id: { } indexId } => table.Store.IndexKeySpace(indexId),
            TableScanNode { Source: TableScanSource.ForcedIndex, Index: { Id: { } indexId } } => table.Store.IndexKeySpace(indexId),
            _ => table.Store.RowKeySpace,
        };

        return database.Kahuna.GetPlacement(keySpace).RemoteLeaderFraction;
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
