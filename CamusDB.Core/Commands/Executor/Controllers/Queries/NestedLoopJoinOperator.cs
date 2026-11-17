/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
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
/// <para>A <c>LATERAL</c> right side reads the left row, so it runs once for each left row with that row
/// (<see cref="JoinLeafScanner.ScanLateralRightSource"/>) and is never spooled. The planner gives such a
/// right side to this operator only.</para>
///
/// <para>With a <see cref="NestedLoopJoinNode.RangeSeek"/> the right side is not spooled: each left row
/// computes its own bounds and reads only that index range (<see cref="SeekRightSide"/>), through the
/// same leaf scan, range lock and counters as any join leaf range scan.</para>
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
            ? await OuterJoinPadding.CreateAsync(joinNode.RightSource, plan, joinNode.RightPhysicalNode).ConfigureAwait(false)
            : null;

        JoinRangeSeek? seek = joinNode.RangeSeek;
        List<AnalyzedComparison>? seekBounds = seek is null ? null : new(seek.Bounds.Count);

        await using NestedLoopInnerSpool? spool = seek is null ? TryCreateSpool(joinNode, plan) : null;

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

            // A LATERAL body of expressions only gives one row or none, with no right-side cursor.
            if (seek is null && spool is null && joinNode.RightSource.Derived is { LateralExpressions: not null } expressionBody)
            {
                QueryResultRow? evaluated = await scanner
                    .EvaluateLateralExpressionsAsync(expressionBody, joinNode.RightExecutionFilter, plan, leftQualified)
                    .ConfigureAwait(false);

                if (evaluated is { } rightRow)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow.Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                    QueryRow merged  = pairer.Pair(leftQualified, rightRow.Row, joinLayout, rightOrdinalMap);

                    if (await services.Filterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    {
                        matched = true;
                        yield return new QueryResultRow(default(ObjectIdValue), merged);
                    }
                }

                if (padding is not null && !matched)
                    yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));

                continue;
            }

            IAsyncEnumerable<QueryResultRow> rightRows = seek is not null
                ? SeekRightSide(joinNode, seek, seekBounds!, plan, leftQualified)
                : spool is not null
                    ? spool.RowsAsync(plan.Ticket.CancellationToken)
                    : joinNode.RightSource.Derived is { Lateral: not null } lateral
                        ? scanner.ScanLateralRightSource(lateral, joinNode.RightExecutionFilter, plan, leftQualified)
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
    /// Reads the right rows of one left row through the join's range seek: the bounds are evaluated
    /// against the left row, rewritten into the key column's type, combined into the tightest range by
    /// the rule a single-table range scan uses (<see cref="IndexScanSelector.TryBuildLeadingColumnRange"/>),
    /// and read as an index range scan of the right table with the pushed right filter.
    /// <list type="bullet">
    ///   <item>A NULL bound reads nothing: a comparison with NULL is never true, so no right row can
    ///   satisfy the conjunct it came from.</item>
    ///   <item>A bound of another type with no exact rewrite into the key's domain is left out, which
    ///   widens the range; the <c>ON</c> predicate still tests each pair.</item>
    ///   <item>A bound that does not evaluate against the left row alone (an error) is left out too, for
    ///   the same reason. The <c>ON</c> predicate then reports the error on the pairs it tests.</item>
    ///   <item>A range whose lower end is above its upper end reads nothing.</item>
    /// </list>
    /// </summary>
    private IAsyncEnumerable<QueryResultRow> SeekRightSide(
        NestedLoopJoinNode joinNode,
        JoinRangeSeek seek,
        List<AnalyzedComparison> bounds,
        QueryPlan plan,
        IReadOnlyDictionary<string, ColumnValue> left)
    {
        bounds.Clear();

        foreach (JoinRangeSeekBound seekBound in seek.Bounds)
        {
            ColumnValue value;
            try
            {
                value = SqlExecutor.EvalExpr(seekBound.Expression, left, plan.Ticket.Parameters);
            }
            catch (CamusDBException)
            {
                continue;
            }

            if (value.Type == ColumnType.Null)
                return EmptyRows();

            string op = seekBound.Operator;

            if (value.Type != seek.KeyType)
            {
                if (NumericBoundNormalizer.Normalize(op, value, seek.KeyType, out string newOp, out ColumnValue newValue)
                    != NumericBoundNormalizer.Outcome.Converted)
                    continue;

                (op, value) = (newOp, newValue);
            }

            bounds.Add(new AnalyzedComparison(seek.KeyColumn, op, value, seekBound.Expression));
        }

        CompositeColumnValue? from = null, to = null;
        bool fromInclusive = true, toInclusive = true;

        if (bounds.Count > 0)
        {
            IndexScanSelector.TryBuildLeadingColumnRange(bounds, out from, out fromInclusive, out to, out toInclusive);

            if (from is not null && to is not null)
            {
                int cmp = string.CompareOrdinal(KeyEncoder.Encode(from), KeyEncoder.Encode(to));
                if (cmp > 0 || (cmp == 0 && !(fromInclusive && toInclusive)))
                    return EmptyRows();
            }
        }

        IndexRangeScanNode range = new(seek.Index, from, fromInclusive, to, toInclusive)
        {
            BoundSource = joinNode.RightSource.Table,
            ExecutionFilter = joinNode.RightExecutionFilter,
        };

        return scanner.ScanBoundTableByIndexRange(range, plan, joinNode.Stats);
    }

    private static async IAsyncEnumerable<QueryResultRow> EmptyRows()
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
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
