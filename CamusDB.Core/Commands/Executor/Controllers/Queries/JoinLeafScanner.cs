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
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Reads the rows of one join leaf — a base table, an index walk, an IN-list probe, or a
/// materialized derived table — and hands them to whichever join operator asked. It is the only
/// one place a join leaf reads storage, so a change to how a leaf scans, decodes, or records its
/// dependencies has one home.
///
/// <para>The join strategies that only consume rows — nested loop, hash, merge, and Grace hash —
/// read every row through this class and touch neither <see cref="KvTableStore"/> nor
/// <see cref="RowEncoder"/> themselves. Three other classes do still read storage directly,
/// because each drives a different access pattern:
/// <see cref="IndexNestedLoopJoinOperator"/> walks an index per outer row,
/// <see cref="BroadcastHashProbeExecutor"/> and <see cref="JoinFragmentProbeExecutor"/> scan one
/// placement span each, and <see cref="JoinLeafRowPage"/> performs the batched row fetch this
/// class and the index probe share.</para>
///
/// <para><b>Dependency-collection scaffolding:</b> every scan here reads <c>plan.DepCollector</c>
/// and calls <c>deps?.RecordRange / RecordPoint / RecordSchema</c>. Those calls are currently
/// no-ops, because a join plan never assigns a collector (see <see cref="QueryJoinExecutor"/> for
/// why the join path bypasses the result cache). They are kept deliberately: when the
/// multi-keyspace cache fence lands, the join cache path assigns a collector and this
/// instrumentation starts recording with no further change here.</para>
///
/// <para>Nothing in this class is thread-safe. A scan builds its layouts and its decode state
/// lazily and reuses them across rows, so one scan belongs to one consumer.</para>
/// </summary>
internal sealed class JoinLeafScanner
{
    private readonly JoinExecutionServices services;

    private readonly DerivedTableExecutor derivedTableExecutor;

    public JoinLeafScanner(JoinExecutionServices services, DerivedTableExecutor derivedTableExecutor)
    {
        this.services = services;
        this.derivedTableExecutor = derivedTableExecutor;
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanJoinRightSource(
        BoundJoinRightSource source,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        if (source.Table is not null)
        {
            await foreach (QueryResultRow row in ScanBoundTable(source.Table, executionFilter, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        await foreach (QueryResultRow row in ScanDerivedTable(source.Derived!, executionFilter, plan).ConfigureAwait(false))
            yield return row;
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanBoundTable(
        BoundTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        IReadOnlySet<string>? requiredColumns = null)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        IReadOnlySet<string>? required = requiredColumns ?? JoinAliasMetadata.GetRequiredColumnsForAlias(plan, source.Alias);
        QueryDependencyCollector? deps = plan.DepCollector;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        deps?.RecordRange(table.Store.RowKeySpace);
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
            plan.Ticket.TxnState, cancellationToken: plan.Ticket.CancellationToken).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                txId,
                rowId,
                data,
            services.Options,
                required,
                JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias),
                rowDecodeState).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

                if (!await services.Filterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(rowId, row);
        }
    }

    /// <summary>
    /// Scans a table in join-key order by walking its secondary index from the first entry to
    /// the last. Used by <see cref="MergeJoinOperator.ExecuteMergeJoin"/> when the planner detected free ordering
    /// on the right side (a <see cref="TableScanSource.ForcedIndex"/> node). Row ids are collected
    /// in index order and each page is fetched in one batch call (see <see cref="JoinLeafRowPage"/>);
    /// deleted or empty rows are skipped.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanBoundTableByIndex(
        BoundTableSource source,
        TableIndexSchema index,
        NodeAst? executionFilter,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        TableDescriptor table = source.Table;
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(table, index);
        QueryDependencyCollector? deps = plan.DepCollector;

        bool unique = index.Type == IndexType.Unique;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        using CancellationTokenSource? linked = LinkEnumeratorCancellation(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        JoinLeafRowPage page = new(services, plan, source, executionFilter);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            index.KvId,
            keyTypes,
            from: null, to: null, unique: unique, cancellationToken: scanToken).ConfigureAwait(false))
        {
            scanToken.ThrowIfCancellationRequested();

            // Recorded before the fetch, per row id, exactly as the per-entry path did: a later update
            // to a non-indexed projected column must invalidate this result even if the row is skipped.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            page.Add(rowId);

            if (!page.IsFull)
                continue;

            await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                yield return row;
        }

        if (page.Count > 0)
        {
            await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                yield return row;
        }
    }

    /// <summary>
    /// Executes a bounded index range scan for a join leaf node, fetching the primary rows one page
    /// at a time (see <see cref="JoinLeafRowPage"/>) and applying the residual execution filter (if
    /// any). Used when <see cref="JoinQueryPlanner"/> chose an index range scan for this leaf.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanBoundTableByIndexRange(
        IndexRangeScanNode rangeNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        BoundTableSource source = rangeNode.BoundSource!;
        TableDescriptor table = source.Table;
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(table, rangeNode.Index);
        QueryDependencyCollector? deps = plan.DepCollector;

        bool unique = rangeNode.Index.Type == IndexType.Unique;

        // Index range scan: the index bucket range covers membership phantoms; per-row point
        // deps cover updates to non-indexed projected columns.
        deps?.RecordRange(table.Store.IndexKeySpace(rangeNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        using CancellationTokenSource? linked = LinkEnumeratorCancellation(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        JoinLeafRowPage page = new(services, plan, source, rangeNode.ExecutionFilter);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            rangeNode.Index.KvId,
            keyTypes,
            from: rangeNode.FromBound,
            to: rangeNode.ToBound,
            fromInclusive: rangeNode.FromInclusive,
            toInclusive: rangeNode.ToInclusive,
            unique: unique, cancellationToken: scanToken).ConfigureAwait(false))
        {
            scanToken.ThrowIfCancellationRequested();

            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            page.Add(rowId);

            if (!page.IsFull)
                continue;

            await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                yield return row;
        }

        if (page.Count > 0)
        {
            await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                yield return row;
        }
    }

    /// <summary>
    /// Executes an IN-list index scan for a join leaf node.
    /// Unique indexes perform one <c>LookupUnique</c> per value; non-unique indexes perform one
    /// equality range scan per value. Duplicate row IDs are suppressed across all values, on the
    /// index side, before a row id is buffered. The surviving ids are fetched one page at a time
    /// (see <see cref="JoinLeafRowPage"/>) and the residual
    /// <see cref="IndexInListScanNode.ExecutionFilter"/> (if any) is applied after each row is
    /// decoded, using the alias-qualified row.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanBoundTableByInList(
        IndexInListScanNode inListNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        BoundTableSource source = inListNode.BoundSource!;
        TableDescriptor table = source.Table;
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(table, inListNode.Index);
        bool isUnique = inListNode.Index.Type == IndexType.Unique;
        HashSet<ObjectIdValue> seen = new();
        QueryDependencyCollector? deps = plan.DepCollector;

        // IN-list scan: record the index bucket once (covers all per-value range probes) and schema.
        deps?.RecordRange(table.Store.IndexKeySpace(inListNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        using CancellationTokenSource? linked = LinkEnumeratorCancellation(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        // One page spans the whole IN list: ids are appended in list order (and, per value, in index
        // order), so a page can hold the tail of one value's matches and the head of the next. Order
        // and the index-side `seen` dedup are unaffected — both happen before an id is buffered.
        JoinLeafRowPage page = new(services, plan, source, inListNode.ExecutionFilter);

        foreach (ColumnValue value in inListNode.Values)
        {
            scanToken.ThrowIfCancellationRequested();

            CompositeColumnValue lookupKey = new(new[] { value });

            if (isUnique)
            {
                ObjectIdValue? rowId = await table.Store.LookupUnique(
                    plan.Ticket.TxnState, inListNode.Index.KvId, lookupKey, scanToken).ConfigureAwait(false);

                if (rowId is null || !seen.Add(rowId.Value))
                    continue;

                deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

                page.Add(rowId.Value);

                if (!page.IsFull)
                    continue;

                await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                    yield return row;
            }
            else
            {
                // Non-unique equality scan: use successor as exclusive upper bound when available,
                // else inclusive exact-match [v, v].
                CompositeColumnValue? upperBound = BuildInListScanUpperBound(table, inListNode.Index, lookupKey);
                CompositeColumnValue toBound = upperBound ?? lookupKey;
                bool toInclusive = upperBound is null;

                await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
                    plan.Ticket.TxnState, inListNode.Index.KvId, keyTypes,
                    lookupKey, toBound, unique: false,
                    fromInclusive: true, toInclusive: toInclusive,
                    maxRows: null, cancellationToken: scanToken).ConfigureAwait(false))
                {
                    scanToken.ThrowIfCancellationRequested();

                    if (!seen.Add(rowId))
                        continue;

                    deps?.RecordPoint(table.Store.RowPointKey(rowId));

                    page.Add(rowId);

                    if (!page.IsFull)
                        continue;

                    await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                        yield return row;
                }
            }
        }

        if (page.Count > 0)
        {
            await foreach (QueryResultRow row in page.FlushAsync(scanToken).ConfigureAwait(false))
                yield return row;
        }
    }

    /// <summary>
    /// Links the token an <c>await foreach</c> consumer supplied through <c>WithCancellation</c> with
    /// the ticket's own cancellation token, and returns <see langword="null"/> when there is nothing to
    /// link — the consumer passed no token, or the same one the ticket already carries. A join leaf
    /// must observe both: the ticket token cancels the whole statement, while the enumerator token
    /// cancels just this enumeration.
    /// </summary>
    private static CancellationTokenSource? LinkEnumeratorCancellation(QueryPlan plan, CancellationToken cancellationToken)
    {
        CancellationToken ticketToken = plan.Ticket.CancellationToken;

        if (!cancellationToken.CanBeCanceled || cancellationToken == ticketToken)
            return null;

        return CancellationTokenSource.CreateLinkedTokenSource(ticketToken, cancellationToken);
    }

    private static CompositeColumnValue? BuildInListScanUpperBound(
        TableDescriptor table,
        TableIndexSchema index,
        CompositeColumnValue lookupKey)
    {
        if (lookupKey.Values.Length == 0)
            return null;

        ColumnValue[] upperValues = new ColumnValue[lookupKey.Values.Length];
        Array.Copy(lookupKey.Values, upperValues, lookupKey.Values.Length - 1);

        string lastColumn = index.Columns[lookupKey.Values.Length - 1];
        TableColumnSchema? column = table.Schema.Columns?.Find(c => string.Equals(c.Name, lastColumn, StringComparison.OrdinalIgnoreCase));
        ColumnType columnType = column?.Type ?? ColumnType.String;
        ColumnValue lastValue = lookupKey.Values[^1];
        ColumnValue? nextValue = IndexScanSelector.NextSortValue(columnType, lastValue);

        if (nextValue is null)
            return null;

        upperValues[^1] = nextValue;
        return new CompositeColumnValue(upperValues);
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanDerivedTable(
        BoundDerivedTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!plan.DerivedMaterializations.TryGetValue(source, out SpillableRowList? rows))
        {
            rows = await derivedTableExecutor
                .MaterializeAsync(plan.Database, source, plan.Ticket, executionFilter)
                .ConfigureAwait(false);
            plan.DerivedMaterializations[source] = rows;
        }

        await foreach (QueryResultRow row in rows.EnumerateAsync(ct).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Fetches a single right-side row by its row id, applies the residual execution filter
    /// (if any), and returns the row. Returns <see langword="null"/> when the row is missing or
    /// fails the filter.
    /// <para>
    /// When <paramref name="decodeState"/> is provided, <see cref="RowEncoder.DecodeToQueryRowAsync"/>
    /// reuses its <see cref="RightDecodeState.DecodeState"/> to avoid rebuilding the row-decode plan
    /// on each call, and the residual-filter qualification uses
    /// <see cref="QueryRowMerger.QualifyRowAsQueryRow"/> with a cached
    /// <see cref="RightDecodeState.QualifiedLayout"/> — eliminating the per-row
    /// <c>Dictionary&lt;string,ColumnValue&gt;</c> allocation of
    /// <see cref="QueryRowMerger.QualifyRow"/>.
    /// </para>
    /// </summary>
    internal async Task<QueryResultRow?> LoadRightRow(
        BoundTableSource source,
        ObjectIdValue rowId,
        NodeAst? executionFilter,
        QueryPlan plan,
        RightDecodeState? decodeState = null)
    {
        ReadOnlyMemory<byte>? dataOpt = await source.Table.Store.GetRow(plan.Ticket.TxnState, rowId, plan.Ticket.CancellationToken).ConfigureAwait(false);

        if (dataOpt is null || dataOpt.Value.Length == 0)
            return null;

        ReadOnlyMemory<byte> data = dataOpt.Value;
        QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
            source.Table.Schema,
            plan.Ticket.TxnState.TransactionId,
            rowId,
            data,
            services.Options,
            JoinAliasMetadata.GetRequiredColumnsForAlias(plan, source.Alias),
            JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias),
            decodeState?.DecodeState).ConfigureAwait(false);

        if (executionFilter is not null)
        {
            // Qualify the row for filter evaluation: reuse the alias-prefixed layout built from
            // the first right row (stored in decodeState) so BuildQualifiedLayout and its
            // FrozenDictionary construction happen at most once per join node.
            if (decodeState is not null)
            {
                decodeState.QualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, decodeState.QualifiedLayout);
                if (!await services.Filterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    return null;
            }
            else
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);
                if (!await services.Filterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    return null;
            }
        }

        return new QueryResultRow(rowId, row);
    }
}
