/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The unconditional join: for every left row, walk the whole right source and emit each pair the
/// ON predicate accepts. The planner picks it only when no equi-join key is available. The hash
/// join also falls back to it when a build overflows its cap and spilling is turned off, because
/// correctness there matters more than the cost.
///
/// <para>A base-table right side is read from storage once and replayed for every later left row
/// (<see cref="NestedLoopInnerSpool"/>), so the cost is one right read plus the left × right pair
/// comparisons. The right side is read again for every left row only when its filter calls a
/// volatile function, or when it outgrows the in-memory cap with spilling off; EXPLAIN (ANALYZE)
/// then shows <c>ran=nested-loop-rescan</c>.</para>
///
/// <para>The left row is qualified once per outer iteration and the join layout is built from the
/// first emitted pair, so the per-row cost is the right-side scan and the predicate, not layout
/// construction. A left outer join instead takes its layout from <see cref="OuterJoinPadding"/>,
/// which knows the right columns before any right row exists, and pads a left row whose inner
/// scan ended with no accepted pair.</para>
/// </summary>
internal sealed class NestedLoopJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    private readonly JoinLeafScanner scanner;

    public NestedLoopJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree, JoinLeafScanner scanner)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
    }

    internal async IAsyncEnumerable<QueryResultRow> ExecuteNestedLoopJoin(
        NestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        JoinRowPairer pairer = new();
        OuterJoinPadding? padding = joinNode.Kind == JoinKind.LeftOuter
            ? await OuterJoinPadding.CreateAsync(joinNode.RightSource, plan).ConfigureAwait(false)
            : null;

        await using NestedLoopInnerSpool? spool = TryCreateSpool(joinNode, plan);

        await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
        {
            IReadOnlyDictionary<string, ColumnValue> leftQualified;
            string leftAlias = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftRow);
            if (leftRow.Row is QueryRow leftQr)
            {
                qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                leftQualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
            }
            else
            {
                leftQualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
            }

            if (padding is not null)
                (joinLayout, rightOrdinalMap) = padding.Bind(leftQualified);

            bool matched = false;

            IAsyncEnumerable<QueryResultRow> rightRows = spool is not null
                ? spool.RowsAsync(plan.Ticket.CancellationToken)
                : ReadRightSide(joinNode, plan);

            await foreach (QueryResultRow rightRow in rightRows.ConfigureAwait(false))
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow.Row, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                QueryRow merged  = pairer.Pair(leftQualified, rightRow.Row, joinLayout, rightOrdinalMap);

                if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                matched = true;
                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }

            if (padding is not null && !matched)
                yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));
        }

        if (spool is { Abandoned: true } && joinNode.Stats is { } stats)
            stats.RanAs ??= "nested-loop-rescan";
    }

    /// <summary>
    /// The spool for a right side that may be read once (see <see cref="NestedLoopInnerSpool"/>),
    /// or null when every left row must walk the right side itself: a derived table, which is
    /// already materialized once per plan, or a right filter that calls a volatile function.
    /// </summary>
    private NestedLoopInnerSpool? TryCreateSpool(NestedLoopJoinNode joinNode, QueryPlan plan)
    {
        if (joinNode.RightSource.Table is null)
            return null;

        if (Functions.ScalarFunctionEvaluator.ContainsVolatileFunction(joinNode.RightExecutionFilter))
        {
            if (joinNode.Stats is { } stats)
                stats.RanAs ??= "nested-loop-rescan";

            return null;
        }

        return new NestedLoopInnerSpool(
            () => ReadRightSide(joinNode, plan),
            QueryExecutionContext.For(plan.Database, plan.Ticket));
    }

    /// <summary>
    /// Reads the right table: through its own access path when the planner gave it one
    /// (<see cref="NestedLoopJoinNode.RightPhysicalNode"/>), else with a full scan and the pushed filter.
    /// </summary>
    private IAsyncEnumerable<QueryResultRow> ReadRightSide(NestedLoopJoinNode joinNode, QueryPlan plan) =>
        joinNode.RightPhysicalNode is { } rightInput
            ? tree.ExecuteNode(rightInput, plan)
            : scanner.ScanJoinRightSource(joinNode.RightSource, joinNode.RightExecutionFilter, plan, joinNode.Stats);
}
