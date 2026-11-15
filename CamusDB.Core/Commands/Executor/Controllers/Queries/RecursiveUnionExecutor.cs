/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Runs the UNION of a recursive CTE with the work-table method, as PostgreSQL does, and streams its
/// rows.
///
/// <para><b>Steps.</b> Step 0 runs the non-recursive term. Each later step runs the recursive term
/// again, and its work-table reference reads only the rows that the step before it made. The recursion
/// ends after a step that makes no row. Every row is returned when its step makes it, so a reader that
/// stops early (a <c>LIMIT</c> over a recursion with no end) also stops the recursion.</para>
///
/// <para><b>UNION and UNION ALL.</b> UNION ALL keeps every row. UNION drops a row that any earlier
/// step, or the same step, already returned, so a walk over a graph with a cycle ends when no new row
/// appears. It keeps one copy of each returned row in memory for that check
/// (<see cref="QueryDistincter.NewDistinctSet"/>).</para>
///
/// <para><b>Memory.</b> The rows of one step are kept in a <see cref="SpillableRowList"/> for the next
/// step, under the spill settings, and the list of the step before is deleted when the next step
/// starts. So only two steps are held at a time, besides the UNION check.</para>
///
/// <para><b>Limits.</b> A recursion with no end has no other stop, because a statement has no time
/// limit. The first row of a step past <see cref="CamusDBOptions.MaxRecursiveCteIterations"/>, and a
/// row past <see cref="CamusDBOptions.MaxRecursiveCteRows"/>, fail the statement with
/// <see cref="CamusDBErrorCodes.RecursiveQueryLimitExceeded"/>. Both are read from the database's
/// options when the run starts, so a change applies to the next statement.</para>
///
/// <para><b>One run at a time.</b> The work table belongs to one execution (see
/// <see cref="RecursiveWorkTable"/>), and a run holds it from the first step to the end.</para>
/// </summary>
internal sealed class RecursiveUnionExecutor
{
    private readonly SetOperationExecutor setOperationExecutor;

    public RecursiveUnionExecutor(SetOperationExecutor setOperationExecutor)
    {
        this.setOperationExecutor = setOperationExecutor;
    }

    /// <summary>
    /// Streams the rows of the recursive UNION <paramref name="setOperation"/>. <paramref name="stats"/>
    /// gets the rows of the non-recursive term and of all steps of the recursive term, for EXPLAIN
    /// (ANALYZE). Only the non-recursive term runs the plan that EXPLAIN built in advance: the recursive
    /// term runs once per step, and a plan runs one time.
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> Execute(
        DatabaseDescriptor database,
        BoundSetOperation setOperation,
        QueryTicket outerTicket,
        SetOperationStats? stats,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        RecursiveWorkTable workTable = setOperation.Source.WorkTable!;
        RowLayout layout = RowLayout.ForColumns(setOperation.Columns.Select(static column => column.RowKey));
        QueryExecutionContext context = QueryExecutionContext.For(database, outerTicket);

        int maxSteps = context.Options.MaxRecursiveCteIterations;
        long maxRows = context.Options.MaxRecursiveCteRows;
        HashSet<QueryResultRow>? returned = setOperation.Source.All ? null : QueryDistincter.NewDistinctSet();

        SpillableRowList? previous = null;
        SpillableRowList? current = new(context);
        long rows = 0;

        workTable.Enter();

        try
        {
            // The non-recursive term runs one time, so it runs the plan that EXPLAIN built and counts its own rows.
            await foreach (QueryResultRow row in setOperationExecutor.RunOperand(
                               database, setOperation.Left, setOperation.LeftSubqueries, setOperation.LeftColumns,
                               setOperation.Columns, layout, outerTicket, stats, isLeft: true)
                               .WithCancellation(ct).ConfigureAwait(false))
            {
                if (returned is not null && !returned.Add(row))
                    continue;

                CheckRows(++rows, maxRows, workTable.Name);
                await current.AddAsync(row, ct).ConfigureAwait(false);
                yield return row;
            }

            for (int step = 1; ; step++)
            {
                await current.SealAsync(ct).ConfigureAwait(false);

                if (current.Count == 0)
                    break;

                // The rows of the step before are read; the step that starts now reads the rows of the last one.
                if (previous is not null)
                    await previous.DisposeAsync().ConfigureAwait(false);

                previous = current;
                current = null;
                workTable.Rows = previous;
                current = new SpillableRowList(context);

                bool madeRows = false;

                await foreach (QueryResultRow row in setOperationExecutor.RunOperand(
                                   database, setOperation.Right, setOperation.RightSubqueries, setOperation.RightColumns,
                                   setOperation.Columns, layout, outerTicket, stats: null, isLeft: false)
                                   .WithCancellation(ct).ConfigureAwait(false))
                {
                    if (stats is not null)
                        stats.Right.RowsEmitted++;

                    if (returned is not null && !returned.Add(row))
                        continue;

                    if (!madeRows)
                    {
                        madeRows = true;
                        CheckStep(step, maxSteps, workTable.Name);
                    }

                    CheckRows(++rows, maxRows, workTable.Name);
                    await current.AddAsync(row, ct).ConfigureAwait(false);
                    yield return row;
                }
            }
        }
        finally
        {
            workTable.Exit();

            if (previous is not null)
                await previous.DisposeAsync().ConfigureAwait(false);

            if (current is not null)
                await current.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void CheckStep(int step, int maxSteps, string name)
    {
        if (maxSteps > 0 && step > maxSteps)
            throw new CamusDBException(
                CamusDBErrorCodes.RecursiveQueryLimitExceeded,
                $"Recursive query '{name}' did not end after {maxSteps} steps (max_recursive_cte_iterations). " +
                "Add a condition that ends the recursion, use UNION to drop rows already returned, or raise the setting");
    }

    private static void CheckRows(long rows, long maxRows, string name)
    {
        if (maxRows > 0 && rows > maxRows)
            throw new CamusDBException(
                CamusDBErrorCodes.RecursiveQueryLimitExceeded,
                $"Recursive query '{name}' made more than {maxRows} rows (max_recursive_cte_rows). " +
                "Add a condition that ends the recursion, use UNION to drop rows already returned, or raise the setting");
    }
}
