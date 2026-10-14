/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The right side of one nested-loop join, read from storage once and replayed for every later
/// left row. Without it the operator re-reads and re-decodes the whole right table for each left
/// row, so a join of L left rows over R right rows costs L × R storage reads; with it the reads
/// are R and only the L × R pair comparisons remain.
///
/// <para><b>The first read is lazy and copies as it streams.</b> Nothing is read until the first
/// left row asks for the right side, so an empty left input reads nothing. That first pass yields
/// each right row to the join as it arrives and copies it into the spool at the same time, so a
/// consumer that stops early (a <c>LIMIT</c>, a cancelled query) has read only as far as it needed.
/// The spool becomes the source for later left rows only when a pass read the right side to its
/// end; a pass that stopped early leaves nothing behind, and the next one reads again.</para>
///
/// <para><b>Memory.</b> The rows go into a <see cref="SpillableRowList"/>, which writes them to a
/// spill file past the spill threshold when spilling is on. When spilling is off the spool holds
/// at most <see cref="CamusDBOptions.HashJoinMaxBuildRows"/> rows, the same cap the hash build has
/// in that mode; a right side larger than that is dropped from the spool and every later left row
/// reads it from storage again, which is the behavior without a spool.</para>
///
/// <para><b>What may be spooled.</b> Only a base-table right side whose pushed filter calls no
/// volatile function. A derived table is already materialized once per plan, and a volatile
/// filter (<c>random()</c>, <c>clock_timestamp()</c>) must be evaluated again for every pass,
/// as it was before. The filter references only the right table, so the rows it keeps do not
/// depend on the left row. Reading once instead of L times takes no lock the re-reads did not:
/// a join leaf takes no range lock, and every re-read registered the same keys.</para>
///
/// <para>One instance belongs to one execution of one join node and is used from one thread.</para>
/// </summary>
internal sealed class NestedLoopInnerSpool : IAsyncDisposable
{
    private readonly Func<IAsyncEnumerable<QueryResultRow>> readRightSide;

    private readonly QueryExecutionContext context;

    private SpillableRowList? rows;

    private bool abandoned;

    /// <param name="readRightSide">Opens a new storage read of the filtered right side.</param>
    /// <param name="context">The query's spill configuration and location.</param>
    public NestedLoopInnerSpool(Func<IAsyncEnumerable<QueryResultRow>> readRightSide, QueryExecutionContext context)
    {
        this.readRightSide = readRightSide;
        this.context = context;
    }

    /// <summary>
    /// True when the right side outgrew the in-memory cap (spilling off), so every pass reads it
    /// from storage.
    /// </summary>
    public bool Abandoned => abandoned;

    /// <summary>
    /// The right rows for one left row: replayed from the spool once a pass has completed it,
    /// otherwise read from storage (and copied, unless the spool was abandoned).
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> RowsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (rows is not null)
        {
            await foreach (QueryResultRow row in rows.EnumerateAsync(cancellationToken).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        if (abandoned)
        {
            await foreach (QueryResultRow row in readRightSide().ConfigureAwait(false))
                yield return row;

            yield break;
        }

        SpillableRowList? filling = new(context);
        int memoryCap = context.Options.SpillEnabled ? int.MaxValue : Math.Max(0, context.Options.HashJoinMaxBuildRows);
        bool completed = false;

        try
        {
            await foreach (QueryResultRow row in readRightSide().ConfigureAwait(false))
            {
                if (filling is not null)
                {
                    if (filling.Count >= memoryCap)
                    {
                        await filling.DisposeAsync().ConfigureAwait(false);
                        filling = null;
                        abandoned = true;
                    }
                    else
                    {
                        await filling.AddAsync(row, cancellationToken).ConfigureAwait(false);
                    }
                }

                yield return row;
            }

            completed = true;
        }
        finally
        {
            if (filling is not null)
            {
                if (completed)
                {
                    await filling.SealAsync(cancellationToken).ConfigureAwait(false);
                    rows = filling;
                }
                else
                {
                    await filling.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (rows is not null)
        {
            await rows.DisposeAsync().ConfigureAwait(false);
            rows = null;
        }
    }
}
