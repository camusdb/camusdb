/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// The reference of a recursive CTE to itself, in the FROM of its recursive term. It reads the rows
/// that the previous step of the recursion made.
///
/// <para><b>Never a top-level source.</b> Like <see cref="TableFunctionSource"/>, this record only ever
/// sits as the <see cref="SelectQuery.Source"/> of the inner shell of a <see cref="DerivedTableSource"/>,
/// so every relation-kind consumer treats the work table as one more derived table. Only the query
/// creator, the binder and the derived-table executor know what is inside. Read it through
/// <see cref="DerivedTableSource.WorkTable"/>.</para>
///
/// <para><b>No storage.</b> The rows are the rows of the previous step, which the executor keeps. The
/// source opens no table and takes no lock, so it needs no privilege of its own.</para>
/// </summary>
/// <param name="Table">The work table that the recursive UNION around this source fills.</param>
public sealed record WorkTableSource(RecursiveWorkTable Table) : QuerySource;

/// <summary>
/// The work table of one recursive CTE, for one execution of a statement. It links the recursive UNION
/// that fills it (<see cref="SetOperationSource.WorkTable"/>) to the <see cref="WorkTableSource"/> in the
/// recursive term that reads it.
///
/// <para><b>Per execution.</b> The query creator makes one for each work-table reference it builds, and
/// the UNION around the reference takes the same instance, so it is not shared between two executions.
/// The bound-query cache keeps no query that has a derived table (see <see cref="BoundQuerySlot"/>), so no
/// later execution reads this state. A change that lets that cache keep such a query must make a new
/// work table for each execution.</para>
///
/// <para><b>Who writes what.</b> The binder sets <see cref="Columns"/> and <see cref="RowKeys"/> when it
/// binds the non-recursive term, before it binds the recursive term, which reads them. The executor sets
/// <see cref="Rows"/> before each run of the recursive term, and one run at a time reads them
/// (<see cref="Enter"/>).</para>
/// </summary>
public sealed class RecursiveWorkTable
{
    private int running;

    public RecursiveWorkTable(string name)
    {
        Name = name;
    }

    /// <summary>The name of the CTE, for EXPLAIN and errors.</summary>
    public string Name { get; }

    /// <summary>
    /// The CTE's column-alias list, or null. The query creator sets it from the UNION's
    /// <see cref="SQLParser.NodeType.RecursiveCte"/> marker, because the recursive term reads the columns
    /// by those names.
    /// </summary>
    public IReadOnlyList<string>? ColumnAliases { get; internal set; }

    /// <summary>
    /// The non-recursive term of the UNION that took this work table. A scope analysis can bind the
    /// recursive term of a query model it made itself, with no bind of the UNION around it; the binder then
    /// binds this term for its schema to learn the columns (see <see cref="Columns"/>).
    /// </summary>
    public SelectQuery? NonRecursiveTerm { get; internal set; }

    /// <summary>True once a recursive UNION took this work table, so a UNION around it does not take it too.</summary>
    internal bool IsAttached { get; set; }

    /// <summary>
    /// The columns of the work table, with the names that the recursive term reads and the types of the
    /// non-recursive term. Null until the binder binds the non-recursive term. The bind of the UNION sets
    /// them again from its own bind of that term, which is the one that runs.
    /// </summary>
    public IReadOnlyList<DerivedColumnSchema>? Columns { get; internal set; }

    /// <summary>
    /// The renames from the key of a kept cell (the UNION's output key) to the key of its column in
    /// <see cref="Columns"/>, or null when the two are the same. See <see cref="BoundDerivedTableSource.InnerRowKeys"/>.
    /// </summary>
    public (string InnerKey, string RowKey)[]? RowKeys { get; internal set; }

    /// <summary>The rows of the previous step, read by the current run of the recursive term.</summary>
    internal SpillableRowList? Rows { get; set; }

    /// <summary>
    /// Marks the start of one recursion. Two at the same time on one work table would read each other's
    /// rows, and the query creator makes that impossible, so a second one is an internal error.
    /// </summary>
    internal void Enter()
    {
        if (Interlocked.CompareExchange(ref running, 1, 0) != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"The recursive query '{Name}' started again while it was running");
    }

    /// <summary>Marks the end of the recursion that <see cref="Enter"/> started, and drops the rows.</summary>
    internal void Exit()
    {
        Rows = null;
        Volatile.Write(ref running, 0);
    }
}
