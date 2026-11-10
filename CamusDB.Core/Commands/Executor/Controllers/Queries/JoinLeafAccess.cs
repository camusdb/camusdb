/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The one decision between an index leaf and a table-scan leaf of a join. The join-order search
/// (<see cref="JoinEnumerator"/>) costs a leaf and the tree builder (<see cref="JoinQueryPlanner"/>)
/// builds it; both must ask here, or the search costs a leaf shape the builder then refuses.
///
/// <para><b>keepBoundedLeaf.</b> True for a statement whose scans hold predicate locks
/// (<see cref="QueryPlanner.ScanHoldsPredicateLocks"/>). A table-scan leaf locks the whole row
/// range of its table, so the join conflicts with every write to that table, while an index leaf
/// locks the range or the values its filter names. The breakeven rule compares read costs only and
/// gives up the index on a small table or for a filter that selects most rows, so it does not
/// apply to the leaves <see cref="QueryPlanner.KeepsBoundedScan"/> protects (an equality prefix,
/// or a table at or below the lock escalation threshold), nor to an IN-list leaf, which names its
/// rows as an equality does. A pure range leaf on a larger table keeps the breakeven rule.</para>
/// </summary>
internal static class JoinLeafAccess
{
    /// <summary>True when the leaf must be a table scan and not the bounded range scan <paramref name="candidate"/>.</summary>
    internal static bool PrefersFullScan(
        IndexRangeScanNode candidate,
        long tableRows,
        bool keepBoundedLeaf,
        CamusDBOptions options,
        StatisticsManager stats,
        DatabaseDescriptor database,
        TableDescriptor table)
        => !(keepBoundedLeaf && QueryPlanner.KeepsBoundedScan(candidate.EqualityPrefixLength, tableRows, options))
            && CostEstimator.ShouldPreferFullScan(candidate, tableRows, options, stats, database, table);

    /// <summary>True when the leaf must be a table scan and not the IN-list seeks that return <paramref name="listRows"/> rows.</summary>
    internal static bool ListPrefersFullScan(long listRows, long breakevenRows, bool keepBoundedLeaf)
        => !keepBoundedLeaf && listRows >= breakevenRows;
}
