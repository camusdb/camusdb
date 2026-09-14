/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Joins two key-ordered inputs with a two-pointer walk, emitting the cross-product of each pair
/// of equal-key runs.
///
/// <para>Two modes share the key rules. When both sides already arrive ordered — through a sort
/// node or a forced-index scan the planner chose — the streaming mode buffers only the current
/// right-side run, so memory is the size of one run rather than of both inputs. When either side
/// needs an internal sort, the materializing mode collects that side and sorts it. A side the
/// plan already ordered is never re-sorted.</para>
///
/// <para>NULL keys are excluded from both sides, which matches SQL inner-join semantics.</para>
/// </summary>
internal sealed class MergeJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    private readonly JoinLeafScanner scanner;

    public MergeJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree, JoinLeafScanner scanner)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
    }

    /// <summary>
    /// Dispatches to the streaming or materializing two-pointer merge based on whether
    /// both sides are pre-ordered. When both sides are ordered (both use a ForcedIndex scan or
    /// SortNode) the streaming path buffers only the current equal-key run on the right side —
    /// O(max right run size) memory rather than O(n + m). When one or both sides need an
    /// internal sort, the materializing path is used.
    ///
    /// NULL keys are excluded from both sides — consistent with SQL inner-join semantics.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ExecuteMergeJoin(
        MergeJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (joinNode.LeftIsOrdered && joinNode.RightIsOrdered && joinNode.RightPhysicalNode is not null)
        {
            await foreach (QueryResultRow r in StreamMergeJoin(joinNode, plan, ct).ConfigureAwait(false))
                yield return r;
            yield break;
        }

        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;

        // ── Materialise left side (qualify each row immediately) ────────────
        List<(ColumnValue[] Key, IReadOnlyDictionary<string, ColumnValue> QualifiedRow)> leftRows = new();
        RowLayout? qualifiedLeftLayout = null;

        await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
        {
            IReadOnlyDictionary<string, ColumnValue> qualified;
            string leftAlias = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftRow);
            if (leftRow.Row is QueryRow leftQr)
            {
                qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                qualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
            }
            else
            {
                qualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
            }

            ColumnValue[]? key = JoinKeyExtractor.ExtractMergeKey(qualified, joinNode.LeftKeyColumns);
            if (key is null) continue;

            leftRows.Add((key, qualified));
        }

        // Sort only when not already ordered by the plan (via SortNode or ForcedIndex scan).
        if (!joinNode.LeftIsOrdered)
            leftRows.Sort((a, b) => JoinKeyExtractor.CompareMergeKeys(a.Key, b.Key));

        // ── Materialise right side (unqualified; MergeRows qualifies at emit time) ──
        List<(ColumnValue[] Key, IReadOnlyDictionary<string, ColumnValue> Row)> rightRows = new();

        if (joinNode.RightIsOrdered && joinNode.RightPhysicalNode is not null)
        {
            await foreach (QueryResultRow rightRow in tree.ExecuteNode(joinNode.RightPhysicalNode, plan).ConfigureAwait(false))
            {
                ColumnValue[]? key = JoinKeyExtractor.ExtractMergeKey(rightRow.Row, joinNode.RightKeyColumns);
                if (key is null) continue;
                rightRows.Add((key, rightRow.Row));
            }
        }
        else
        {
            // Fallback: full scan via ScanJoinRightSource + internal sort.
            await foreach (QueryResultRow rightRow in scanner.ScanJoinRightSource(
                joinNode.RightSource, joinNode.RightExecutionFilter, plan).ConfigureAwait(false))
            {
                ColumnValue[]? key = JoinKeyExtractor.ExtractMergeKey(rightRow.Row, joinNode.RightKeyColumns);
                if (key is null) continue;
                rightRows.Add((key, rightRow.Row));
            }

            rightRows.Sort((a, b) => JoinKeyExtractor.CompareMergeKeys(a.Key, b.Key));
        }

        // ── Two-pointer sort-merge ────────────────────────────────────────────
        int li = 0;
        int ri = 0;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        while (li < leftRows.Count && ri < rightRows.Count)
        {
            int cmp = JoinKeyExtractor.CompareMergeKeys(leftRows[li].Key, rightRows[ri].Key);

            if (cmp < 0) { li++; continue; }
            if (cmp > 0) { ri++; continue; }

            // Equal keys — find the extent of both equal-key runs.
            int leftRunStart  = li;
            int rightRunStart = ri;

            while (li < leftRows.Count  && JoinKeyExtractor.CompareMergeKeys(leftRows[li].Key,  leftRows[leftRunStart].Key)   == 0) li++;
            while (ri < rightRows.Count && JoinKeyExtractor.CompareMergeKeys(rightRows[ri].Key, rightRows[rightRunStart].Key) == 0) ri++;

            // Emit cross-product of left[leftRunStart..li) × right[rightRunStart..ri).
            for (int l = leftRunStart; l < li; l++)
            {
                for (int r = rightRunStart; r < ri; r++)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftRows[l].QualifiedRow, rightRows[r].Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRows[r].Row, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(
                        leftRows[l].QualifiedRow, rightRows[r].Row, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
    }

    /// <summary>
    /// Streaming two-pointer merge for pre-ordered inputs. Advances left and right enumerators
    /// in lockstep; on equal keys buffers only the right equal-key run, then iterates all left
    /// rows with that key one at a time emitting the cross-product. Memory = O(right run size)
    /// instead of O(n + m), making the cost model's InMemoryRows = 0 claim hold.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> StreamMergeJoin(
        MergeJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        await using IAsyncEnumerator<QueryResultRow> leftEnum  =
            tree.ExecuteNode(joinNode.Input!, plan).GetAsyncEnumerator(ct);
        await using IAsyncEnumerator<QueryResultRow> rightEnum =
            tree.ExecuteNode(joinNode.RightPhysicalNode!, plan).GetAsyncEnumerator(ct);

        bool leftHasMore  = await leftEnum.MoveNextAsync().ConfigureAwait(false);
        bool rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false);

        while (leftHasMore && rightHasMore)
        {
            // Qualify current left row and extract its key.
            string leftAlias = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftEnum.Current);
            IReadOnlyDictionary<string, ColumnValue> leftQualified;
            if (leftEnum.Current.Row is QueryRow leftQr)
            {
                qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                leftQualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
            }
            else
            {
                leftQualified = QueryRowMerger.QualifyRow(leftEnum.Current.Row, leftAlias);
            }
            ColumnValue[]? leftKey = JoinKeyExtractor.ExtractMergeKey(leftQualified, joinNode.LeftKeyColumns);
            if (leftKey is null)
            {
                leftHasMore = await leftEnum.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            ColumnValue[]? rightKey = JoinKeyExtractor.ExtractMergeKey(rightEnum.Current.Row, joinNode.RightKeyColumns);
            if (rightKey is null)
            {
                rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            int cmp = JoinKeyExtractor.CompareMergeKeys(leftKey, rightKey);
            if (cmp < 0) { leftHasMore  = await leftEnum.MoveNextAsync().ConfigureAwait(false);  continue; }
            if (cmp > 0) { rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false); continue; }

            // Equal keys — buffer the right equal-key run.
            ColumnValue[] runKey = leftKey;
            List<IReadOnlyDictionary<string, ColumnValue>> rightRun = [];
            while (rightHasMore)
            {
                ColumnValue[]? rk = JoinKeyExtractor.ExtractMergeKey(rightEnum.Current.Row, joinNode.RightKeyColumns);
                if (rk is null || JoinKeyExtractor.CompareMergeKeys(rk, runKey) != 0) break;
                rightRun.Add(rightEnum.Current.Row);
                rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false);
            }

            // Emit all left rows with the same key × the buffered right run.
            // Current left row is already qualified; loop advances after each emission.
            while (true)
            {
                foreach (IReadOnlyDictionary<string, ColumnValue> rightRow in rightRun)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, rightRow, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                leftHasMore = await leftEnum.MoveNextAsync().ConfigureAwait(false);
                if (!leftHasMore) break;

                string la = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftEnum.Current);
                if (leftEnum.Current.Row is QueryRow nextQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(nextQr.Layout, la);
                    leftQualified = QueryRowMerger.QualifyRowAsQueryRow(nextQr, qualifiedLeftLayout);
                }
                else
                {
                    leftQualified = QueryRowMerger.QualifyRow(leftEnum.Current.Row, la);
                }
                ColumnValue[]? lk = JoinKeyExtractor.ExtractMergeKey(leftQualified, joinNode.LeftKeyColumns);
                if (lk is null || JoinKeyExtractor.CompareMergeKeys(lk, runKey) != 0) break;
            }
        }
    }
}
