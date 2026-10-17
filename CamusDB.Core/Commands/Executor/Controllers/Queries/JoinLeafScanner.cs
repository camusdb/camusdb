/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
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
/// <para><b>Runtime counters:</b> every read method takes an optional <see cref="PlanNodeStats"/>,
/// the counters of the plan node the read is charged to. A leaf scan passes its own node's; a join
/// operator reading its right side passes the join node's, because the right side has no node of
/// its own (see <see cref="PlanNodeStats"/>). The argument is null unless the statement runs under
/// EXPLAIN (ANALYZE). The statement's slow-query-log probe (<c>plan.Ticket.Probe</c>) is counted at
/// the same sites, so a join reports the rows it read exactly as a single-table scan does.</para>
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

    /// <summary>
    /// Reads the right side of a join: a full scan of a base table, or the materialized rows of a
    /// derived table. <paramref name="stats"/> is the join node's counters (null outside
    /// EXPLAIN (ANALYZE)); a nested-loop join calls this once per left row, so its counters show
    /// every re-read.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanJoinRightSource(
        BoundJoinRightSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        PlanNodeStats? stats = null)
    {
        if (source.Table is not null)
        {
            await foreach (QueryResultRow row in ScanBoundTable(source.Table, executionFilter, plan, stats: stats).ConfigureAwait(false))
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
        IReadOnlySet<string>? requiredColumns = null,
        PlanNodeStats? stats = null)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        IReadOnlySet<string>? required = requiredColumns ?? JoinAliasMetadata.GetRequiredColumnsForAlias(plan, source.Alias);
        QueryDependencyCollector? deps = plan.DepCollector;
        CamusDB.Core.Diagnostics.StatementProbe? probe = plan.Ticket.Probe;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        deps?.RecordRange(table.Store.RowKeySpace);
        deps?.RecordRange(table.Store.LargeValueKeySpace);
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
            plan.Ticket.TxnState, cancellationToken: plan.Ticket.CancellationToken,
            largeValues: LargeValueFetch.Columns(table.Schema, required)).ConfigureAwait(false))
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

            probe?.AddRowRead();
            if (stats is not null)
            {
                stats.KvScanEntries++;
                stats.RowsRead++;
            }

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
        PlanNodeStats? stats = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        TableDescriptor table = source.Table;
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(table, index);
        QueryDependencyCollector? deps = plan.DepCollector;

        bool unique = index.Type == IndexType.Unique;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        using CancellationTokenSource? linked = JoinCancellation.Link(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        JoinLeafRowPage page = new(services, plan, source, executionFilter, stats: stats);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            index.KvId,
            keyTypes,
            from: null, to: null, unique: unique, cancellationToken: scanToken).ConfigureAwait(false))
        {
            scanToken.ThrowIfCancellationRequested();

            if (stats is not null)
                stats.KvScanEntries++;

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
        PlanNodeStats? stats = null,
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

        using CancellationTokenSource? linked = JoinCancellation.Link(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        JoinLeafRowPage page = new(services, plan, source, rangeNode.ExecutionFilter, stats: stats);

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

            if (stats is not null)
                stats.KvScanEntries++;

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
        PlanNodeStats? stats = null,
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

        using CancellationTokenSource? linked = JoinCancellation.Link(plan, cancellationToken);
        CancellationToken scanToken = linked?.Token ?? plan.Ticket.CancellationToken;

        // One page spans the whole IN list: ids are appended in list order (and, per value, in index
        // order), so a page can hold the tail of one value's matches and the head of the next. Order
        // and the index-side `seen` dedup are unaffected — both happen before an id is buffered.
        JoinLeafRowPage page = new(services, plan, source, inListNode.ExecutionFilter, stats: stats);

        foreach (ColumnValue value in inListNode.Values)
        {
            scanToken.ThrowIfCancellationRequested();

            CompositeColumnValue lookupKey = new(new[] { value });

            if (isUnique)
            {
                ObjectIdValue? rowId = await table.Store.LookupUnique(
                    plan.Ticket.TxnState, inListNode.Index.KvId, lookupKey, scanToken).ConfigureAwait(false);

                if (stats is not null)
                    stats.KvPointLookups++;

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
                // Non-unique equality scan over one IN value. The helper picks a range that is
                // non-empty in the index's own key order, which a descending column reverses.
                (CompositeColumnValue toBound, bool toInclusive) = IndexScanSelector.EqualitySeekUpperBound(table, inListNode.Index, lookupKey);

                await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
                    plan.Ticket.TxnState, inListNode.Index.KvId, keyTypes,
                    lookupKey, toBound, unique: false,
                    fromInclusive: true, toInclusive: toInclusive,
                    maxRows: null, cancellationToken: scanToken).ConfigureAwait(false))
                {
                    scanToken.ThrowIfCancellationRequested();

                    if (stats is not null)
                        stats.KvScanEntries++;

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

    internal async IAsyncEnumerable<QueryResultRow> ScanDerivedTable(
        BoundDerivedTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // A function in FROM is streamed, never materialized: its rows cost no I/O to make again,
        // and a series can be far larger than memory (LIMIT 10 over a billion-row series must return
        // at once). A join that re-reads it simply generates it again.
        if (source.Source.Function is { } call)
        {
            await foreach (QueryResultRow row in ScanTableFunction(source, call, executionFilter, plan, ct).ConfigureAwait(false))
                yield return row;

            yield break;
        }

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
    /// Streams the rows of a function in <c>FROM</c> (see <see cref="TableFunctionSource"/>). The
    /// arguments are evaluated once, with the statement's parameters, before the first row. Nothing
    /// here touches storage, so the scan takes no lock and records nothing in the read set.
    /// <para>The pushed-down filter is applied per row the same way a materialized derived table
    /// applies it, so a predicate on the series column filters before the join sees the row.
    /// Cancellation is checked every <see cref="CancellationCheckInterval"/> rows, because a filter that
    /// rejects every row would otherwise loop without ever reaching a consumer that checks it.</para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ScanTableFunction(
        BoundDerivedTableSource source,
        TableFunctionSource call,
        NodeAst? executionFilter,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct)
    {
        TableFunction function = TableFunctionRegistry.Resolve(call.FunctionName, call.Arguments.Count);

        ColumnValue[] arguments = TableFunctionEvaluation.EvaluateArguments(call, plan.Ticket.Parameters);

        RowLayout layout = RowLayout.ForColumns([call.ColumnName]);

        bool qualifyForFilter = executionFilter is not null
                                && plan.Ticket.RowNameResolver is { } resolver
                                && resolver.UsesQualifiedRowKeys();
        RowLayout? qualifiedLayout = qualifyForFilter ? QueryRowMerger.BuildQualifiedLayout(layout, source.Alias) : null;

        CancellationToken statementToken = plan.Ticket.CancellationToken;
        int untilCheck = CancellationCheckInterval;

        foreach (ColumnValue value in function.Generate(arguments))
        {
            if (--untilCheck == 0)
            {
                untilCheck = CancellationCheckInterval;
                statementToken.ThrowIfCancellationRequested();
                ct.ThrowIfCancellationRequested();
            }

            QueryRow row = new(default, layout, [value]);

            if (executionFilter is not null)
            {
                IReadOnlyDictionary<string, ColumnValue> evalRow = qualifiedLayout is not null
                    ? QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout)
                    : row;

                if (!await services.Filterer.MeetWhereAsync(executionFilter, evalRow, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(default, row);
        }
    }

    /// <summary>How many generated rows a function scan produces between two cancellation checks.</summary>
    private const int CancellationCheckInterval = 1024;
}
