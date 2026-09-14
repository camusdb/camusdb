/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Joins by probing an index on the right table once per left row, instead of re-scanning it.
/// The planner picks it when the join key is the leading column of a right-side index.
///
/// <para>Two probe shapes share one outer loop. A unique index answers with at most one row and
/// needs no buffer. A non-unique index walks an equality range, and its row fetches are paged
/// through one <see cref="JoinLeafRowPage"/> that lives for the whole join: rebuilding the buffer
/// and its decode plan for every outer row would be pure overhead on the common one-match
/// probe. The page is flushed before each probe returns, so it never spans two left rows.</para>
///
/// <para>One <see cref="RightDecodeState"/> is created per execution and threaded through every
/// probe, so the join owns exactly one row-decode plan and one alias-qualified layout no matter
/// which fetch path a row takes.</para>
/// </summary>
internal sealed class IndexNestedLoopJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    private readonly JoinLeafScanner scanner;

    public IndexNestedLoopJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree, JoinLeafScanner scanner)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
    }

    internal async IAsyncEnumerable<QueryResultRow> ExecuteIndexNestedLoopJoin(
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        RightDecodeState rightDecodeState = new();

        // A non-unique probe pages its primary-row fetches through one buffer that lives for the whole
        // join, not one per outer row: the buffer and its decode plan would otherwise be rebuilt for
        // every probe, which is pure overhead for the common one-match probe. The page shares
        // rightDecodeState so the join owns exactly one decode plan and one qualified layout.
        JoinLeafRowPage? probePage = joinNode.UseUniqueLookup
            ? null
            : new JoinLeafRowPage(services, plan, joinNode.RightSource, joinNode.RightExecutionFilter, rightDecodeState);

        await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
        {
            // Qualify the left row: reuse the source Values array when it is a QueryRow to avoid
            // a per-row Dictionary allocation — only the layout (key names) changes.
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

            if (!leftQualified.TryGetValue(joinNode.LeftLookupColumn, out ColumnValue? lookupValue) || lookupValue is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join lookup column '{joinNode.LeftLookupColumn}' is missing from left row");
            }

            CompositeColumnValue lookupKey = new(new[] { lookupValue });

            await foreach (QueryResultRow rightRow in ProbeRightIndex(
                joinNode,
                lookupKey,
                plan,
                rightDecodeState,
                probePage).ConfigureAwait(false))
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow.Row, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, rightRow.Row, joinLayout, rightOrdinalMap);

                if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    /// <summary>
    /// Resolves the right-side rows for one outer row. A unique index answers with at most one row
    /// through <see cref="LookupUniqueRightRow"/>; a non-unique index walks its equality range through
    /// <see cref="ScanMultiIndexRightRows"/>, which needs the join-wide <paramref name="probePage"/>
    /// (never null for a non-unique probe, see <see cref="ExecuteIndexNestedLoopJoin"/>).
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ProbeRightIndex(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        RightDecodeState decodeState,
        JoinLeafRowPage? probePage)
    {
        if (joinNode.UseUniqueLookup)
        {
            await foreach (QueryResultRow row in LookupUniqueRightRow(joinNode, lookupKey, plan, decodeState).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        if (probePage is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A non-unique index nested-loop probe requires a row page");
        }

        await foreach (QueryResultRow row in ScanMultiIndexRightRows(joinNode, lookupKey, plan, probePage).ConfigureAwait(false))
            yield return row;
    }

    private async IAsyncEnumerable<QueryResultRow> LookupUniqueRightRow(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        RightDecodeState decodeState)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        QueryDependencyCollector? deps = plan.DepCollector;

        // Unique index probe: record the index bucket range (membership check) and schema.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        ObjectIdValue? rowId = await table.Store.LookupUnique(plan.Ticket.TxnState, joinNode.Index.KvId, lookupKey, plan.Ticket.CancellationToken).ConfigureAwait(false);

        if (rowId is null)
            yield break;

        // Record the row point dep whether or not it passes the execution filter — a later
        // update to a non-indexed column could change what the join returns.
        deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

        QueryResultRow? row = await scanner.LoadRightRow(source, rowId.Value, joinNode.RightExecutionFilter, plan, decodeState).ConfigureAwait(false);

        if (row is QueryResultRow loadedRow)
            yield return loadedRow;
    }

    /// <summary>
    /// Non-unique index probe for one outer row: walks the index equality range for the lookup key
    /// and fetches the matching primary rows one page at a time through <paramref name="probePage"/>
    /// instead of one round trip per index entry.
    /// <para>
    /// The page is owned by the whole join execution, but it is flushed before this method returns,
    /// so a page never spans two outer rows and the rows of one outer row still stream in index order.
    /// The first-key comparison and the ASC/DESC stop rule run before an id is buffered, and the row
    /// point dependency is recorded before the fetch, exactly as the per-entry fetch did.
    /// </para>
    /// <para>
    /// Cancellation is checked on every index entry here and on every row inside the page flush,
    /// because the storage enumerator does not observe the token on its own.
    /// </para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanMultiIndexRightRows(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        JoinLeafRowPage probePage)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(table, joinNode.Index);
        ColumnValue lookupValue = lookupKey.Values[0];
        QueryDependencyCollector? deps = plan.DepCollector;
        CancellationToken cancellationToken = plan.Ticket.CancellationToken;

        // Non-unique index equality probe: the index bucket range catches phantom inserts for
        // this key. Per-row point deps are recorded below as each row id is buffered.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        // The probe reads its stop rule from the index's DECODED order: an ascending first column
        // streams ascending (past = greater), a descending one streams descending (past = smaller).
        // Breaking on `> 0` for a DESC column would never fire, silently downgrading every probe to
        // a full index-tail walk.
        bool firstColumnDescending = joinNode.Index.DirectionAt(0) == OrderType.Descending;

        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            joinNode.Index.KvId,
            keyTypes,
            lookupKey,
            to: null,
            unique: false, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            int cmp = key.Values[0].CompareTo(lookupValue);

            if (firstColumnDescending ? cmp < 0 : cmp > 0)
                break;

            if (cmp != 0)
                continue;

            // Recorded before the fetch, per row id: a later update to a non-indexed projected column
            // must invalidate this result even if the residual filter later rejects the row.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            probePage.Add(rowId);

            if (!probePage.IsFull)
                continue;

            await foreach (QueryResultRow row in probePage.FlushAsync(cancellationToken).ConfigureAwait(false))
                yield return row;
        }

        // The tail of this outer row's matches. Flushing here, not on the next probe, is what keeps a
        // page from spanning two outer rows.
        if (probePage.Count > 0)
        {
            await foreach (QueryResultRow row in probePage.FlushAsync(cancellationToken).ConfigureAwait(false))
                yield return row;
        }
    }
}
