
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// The lock step of <c>SELECT … FOR UPDATE</c>: it takes the exclusive row lock of each candidate row,
/// reads the row again at its latest committed head, re-checks the WHERE clause on that read, and emits
/// the re-read row. It sits above the filter, the semi-joins and the sort, and below the projection
/// and the LIMIT, so the order of operations is scan → filter → sort → <b>lock</b> → re-read → re-check
/// → project → limit.
///
/// <para><b>It locks only rows it emits, in pages.</b> It pulls candidates in pages of at most
/// <see cref="CamusDBOptions.IndexScanFetchBatchSize"/> and sizes each page to the rows the LIMIT (plus
/// OFFSET) still needs, so the last page never locks a row past the LIMIT. A row that the re-check
/// drops, that a concurrent commit deleted, or that SKIP LOCKED skips does not count, and the next page
/// asks only for the rows still missing. The rows a re-check drops stay locked until the transaction
/// ends; the page sizing keeps their number small.</para>
///
/// <para><b>Nothing below it may cut rows by the LIMIT.</b> A skipped or re-checked row must not use
/// up the LIMIT, so the planner turns off the scan row limit, the per-span survivor cap and the
/// bounded (top-k) sort for a plan with this node.</para>
/// </summary>
public sealed class LockRowsNode : PhysicalPlanNode
{
    /// <summary>What the step does with a row another transaction holds.</summary>
    internal RowLockWaitPolicy WaitPolicy { get; init; }

    /// <summary>
    /// <c>limit + offset</c> when both are known at plan time, or null to emit until the input ends.
    /// The step stops pulling candidates once it has emitted this many rows.
    /// </summary>
    public long? RowBudget { get; init; }

    public LockRowsNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
