
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Runtime counters collected during EXPLAIN ANALYZE execution.
/// All fields are updated by the executor while rows flow through the operator;
/// they are read-only after the cursor has been drained.
/// </summary>
public sealed class PlanNodeStats
{
    /// <summary>
    /// Rows fetched and decoded from storage by a scan operator (pre-filter count).
    /// Always ≥ <see cref="RowsEmitted"/>. For unique-index point lookups this is 0 or 1.
    /// Zero for non-scan operators (they read from an upstream cursor, not from storage).
    /// </summary>
    public long RowsRead;

    /// <summary>Rows emitted (yielded) by this operator to its parent (post-filter count).</summary>
    public long RowsEmitted;

    /// <summary>
    /// Wall-clock milliseconds spent executing this operator and all operators below it.
    /// Populated only on the plan root after the cursor is fully drained; null on all other
    /// nodes (not measured — emitted as NULL in EXPLAIN ANALYZE output rather than 0.0 which
    /// would imply zero cost).
    /// </summary>
    public double? ElapsedMs;

    /// <summary>KV point-lookups issued (unique-index lookups).</summary>
    public long KvPointLookups;

    /// <summary>KV scan entries visited (table or range-index scans).</summary>
    public long KvScanEntries;
}
