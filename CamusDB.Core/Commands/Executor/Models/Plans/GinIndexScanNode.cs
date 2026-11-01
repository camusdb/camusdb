/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Reads the candidate rows of a jsonb predicate from a GIN index (<see cref="IndexType.Inverted"/>).
/// Each requirement of <see cref="Query"/> is one entry scan (an exact entry, or every entry under a
/// prefix); the rows of one group are united, and the groups are intersected.
///
/// <para><b>The scan returns a superset.</b> GIN entries lose array positions and structure, so a
/// candidate row can still fail the operator. The scan absorbs no conjunct: the planner keeps the full
/// predicate in the execution filter, and the executor checks every fetched row against it. A wrong or
/// stale requirement can only cost extra rows, never drop a matching one, as long as
/// <see cref="JsonbGinEntries"/> never asks for an entry a matching row may not hold.</para>
///
/// <para>Phantom protection: the executor takes a range lock over each entry range it scans. A new
/// or changed row that matches the predicate writes an entry into every group's ranges, so it meets
/// the lock of the first group the scan read.</para>
/// </summary>
public sealed class GinIndexScanNode : PhysicalPlanNode
{
    public TableIndexSchema Index { get; }

    /// <summary>The jsonb column the index covers.</summary>
    public string ColumnName { get; }

    /// <summary>The entries a candidate row must hold: an AND of OR groups.</summary>
    internal JsonbGinEntries.Query Query { get; }

    public override bool CanDecomposeToLocalPlusMerge => true;

    internal GinIndexScanNode(TableIndexSchema index, string columnName, JsonbGinEntries.Query query)
    {
        Index = index;
        ColumnName = columnName;
        Query = query;
    }

    /// <summary>The number of entry scans the node runs.</summary>
    public int RequirementCount
    {
        get
        {
            int count = 0;
            foreach (IReadOnlyList<JsonbGinEntries.Requirement> group in Query.Groups)
                count += group.Count;
            return count;
        }
    }
}
