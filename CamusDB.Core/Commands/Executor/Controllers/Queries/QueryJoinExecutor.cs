
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
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes inner joins via nested-loop iteration over bound table sources.
///
/// <para><b>Cache bypass:</b> join queries deliberately ignore any <c>{cache=name}</c> hint on
/// the ticket. Caching a multi-table result requires the generation fence to snapshot the row
/// keyspace for <em>every</em> table in the plan, not just one. Until that multi-keyspace fence
/// is implemented (deferred), routing join results through
/// <see cref="QueryExecutor.QueryWithCache"/> would silently under-fence the non-primary tables
/// and risk serving a stale result as if it were fresh. The dispatch in
/// <c>CommandExecutor.ExecuteSQLQuery</c> routes <c>IsMultiSource</c> queries here before
/// any cache probe is performed.</para>
///
/// <para><b>Dependency-collection scaffolding:</b> each scan method in this class reads
/// <c>plan.DepCollector</c> and calls <c>deps?.RecordRange / RecordPoint / RecordSchema</c>.
/// These calls are currently no-ops (<c>DepCollector</c> is never assigned on a join plan) but
/// are deliberately kept in place: when the multi-keyspace fence is added the join cache path
/// can assign a collector and the instrumentation will fire without any further changes here.</para>
/// </summary>
internal sealed class QueryJoinExecutor
{
    private readonly QueryExecutor queryExecutor;

    private readonly DerivedTableExecutor derivedTableExecutor;

    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryDistincter queryDistincter = new();

    private readonly StatisticsManager? _stats;

    /// <summary>Configuration of the engine this executor belongs to.</summary>
    private CamusDBOptions _options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => _options = next;

    private readonly PlanCache? _planCache;

    /// <summary>Engine logger; null only in tests that build the executor bare (broadcast then stays off).</summary>
    private readonly ILogger<ICamusDB>? logger;

    /// <summary>Node-to-node fragment channel; null in standalone mode. Required for broadcast joins.</summary>
    private readonly IQueryFragmentTransport? fragmentTransport;

    /// <summary>Coordinator-side distributed counters; null only in tests that build the executor bare.</summary>
    private readonly DistributedQueryMetrics? distributedMetrics;

    public QueryJoinExecutor(QueryExecutor queryExecutor, CamusDBOptions options, StatisticsManager? stats = null, PlanCache? planCache = null,
        ILogger<ICamusDB>? logger = null, IQueryFragmentTransport? fragmentTransport = null, DistributedQueryMetrics? distributedMetrics = null)
    {
        this.queryExecutor = queryExecutor;
        _stats = stats;
        _options = options;
        _planCache = planCache;
        this.logger = logger;
        this.fragmentTransport = fragmentTransport;
        this.distributedMetrics = distributedMetrics;
        derivedTableExecutor = new DerivedTableExecutor(queryExecutor, this);
    }

    /// <summary>
    /// Entry point for multi-source (join) queries. Any <c>{cache=name}</c> hint on
    /// <paramref name="ticket"/> is intentionally not consulted here — see the class summary
    /// for the reason. Cache bypass is enforced at the call site in
    /// <c>CommandExecutor.ExecuteSQLQuery</c> by routing <c>IsMultiSource</c> queries here
    /// rather than through <c>QueryExecutor.Query</c>.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket)
    {
        JoinQueryPlanner planner = new(_stats, _planCache, _options);
        QueryPlan plan = planner.GetPlan(database, bound, ticket);

        IAsyncEnumerable<QueryResultRow> cursor = ExecuteJoinTree(plan.Root, plan);

        if (plan.ExecutionFilter is not null)
            cursor = ApplyWhere(cursor, plan.ExecutionFilter, plan);

        cursor = QueryPostScanPipeline.Apply(
            plan.Database,
            ticket,
            cursor,
            queryFilterer,
            querySorter,
            queryAggregator,
            queryProjector,
            queryDistincter,
            queryLimiter
    );

        return WithDerivedMaterializationCleanup(cursor, plan);
    }

    /// <summary>
    /// Wraps <paramref name="cursor"/> so that derived-table <see cref="SpillableRowList"/>
    /// instances stored in the plan are disposed after the cursor is fully consumed,
    /// cancelled, or throws. The try/finally guarantees cleanup under all exit paths including
    /// early-cancel — derived-table spill files are never leaked regardless of how the caller
    /// terminates the enumeration.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> WithDerivedMaterializationCleanup(
        IAsyncEnumerable<QueryResultRow> cursor,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
                yield return row;
        }
        finally
        {
            await plan.DisposeMaterializationsAsync().ConfigureAwait(false);
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteJoinTree(
        PhysicalPlanNode node,
        QueryPlan plan)
    {
        switch (node)
        {
            case IndexNestedLoopJoinNode indexJoinNode:
            {
                await foreach (QueryResultRow row in ExecuteIndexNestedLoopJoin(indexJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case HashJoinNode hashJoinNode:
            {
                await foreach (QueryResultRow row in ExecuteHashJoin(hashJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case MergeJoinNode mergeJoinNode:
            {
                await foreach (QueryResultRow row in ExecuteMergeJoin(mergeJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case NestedLoopJoinNode joinNode:
            {
                await foreach (QueryResultRow row in ExecuteNestedLoopJoin(joinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case SortNode { Input: not null } sortNode when sortNode.OrderBy is { Count: > 0 }:
            {
                IAsyncEnumerable<QueryResultRow> sorted = querySorter.SortByKeys(ExecuteJoinTree(sortNode.Input!, plan), sortNode.OrderBy, QueryExecutionContext.For(plan.Database, plan.Ticket));
                
                await foreach (QueryResultRow row in sorted.ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case IndexRangeScanNode { BoundSource: not null } rangeLeaf:
            {
                await foreach (QueryResultRow row in ScanBoundTableByIndexRange(rangeLeaf, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case IndexInListScanNode { BoundSource: not null } inListLeaf:
            {
                await foreach (QueryResultRow row in ScanBoundTableByInList(inListLeaf, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case TableScanNode { BoundSource: not null, Source: TableScanSource.ForcedIndex, Index: not null } indexScanNode:
            {
                await foreach (QueryResultRow row in ScanBoundTableByIndex(
                    indexScanNode.BoundSource, indexScanNode.Index, indexScanNode.ExecutionFilter, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case TableScanNode { BoundSource: not null } scanNode:
            {
                await foreach (QueryResultRow row in ScanBoundTable(
                    scanNode.BoundSource,
                    scanNode.ExecutionFilter,
                    plan,
                    scanNode.RequiredColumns).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
            {
                await foreach (QueryResultRow row in ScanDerivedTable(
                    derivedScanNode.BoundSource,
                    derivedScanNode.ExecutionFilter,
                    plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unsupported join plan node: {node.GetType().Name}");
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteIndexNestedLoopJoin(
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        RightDecodeState rightDecodeState = new();

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            // Qualify the left row: reuse the source Values array when it is a QueryRow to avoid
            // a per-row Dictionary allocation — only the layout (key names) changes.
            IReadOnlyDictionary<string, ColumnValue> leftQualified;
            string leftAlias = ResolveLeftAlias(joinNode.Input!, leftRow);
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
                rightDecodeState).ConfigureAwait(false))
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow.Row, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, rightRow.Row, joinLayout, rightOrdinalMap);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteNestedLoopJoin(
        NestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            IReadOnlyDictionary<string, ColumnValue> leftQualified;
            string leftAlias = ResolveLeftAlias(joinNode.Input!, leftRow);
            if (leftRow.Row is QueryRow leftQr)
            {
                qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                leftQualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
            }
            else
            {
                leftQualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
            }

            await foreach (QueryResultRow rightRow in ScanJoinRightSource(
                joinNode.RightSource,
                joinNode.RightExecutionFilter,
                plan).ConfigureAwait(false))
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow.Row, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, rightRow.Row, joinLayout, rightOrdinalMap);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ProbeRightIndex(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        RightDecodeState decodeState)
    {
        if (joinNode.UseUniqueLookup)
        {
            await foreach (QueryResultRow row in LookupUniqueRightRow(joinNode, lookupKey, plan, decodeState).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        await foreach (QueryResultRow row in ScanMultiIndexRightRows(joinNode, lookupKey, plan, decodeState).ConfigureAwait(false))
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
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        ObjectIdValue? rowId = await table.Store.LookupUnique(plan.Ticket.TxnState, joinNode.Index.KvId, lookupKey, plan.Ticket.CancellationToken).ConfigureAwait(false);

        if (rowId is null)
            yield break;

        // Record the row point dep whether or not it passes the execution filter — a later
        // update to a non-indexed column could change what the join returns.
        deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

        QueryResultRow? row = await LoadRightRow(source, rowId.Value, joinNode.RightExecutionFilter, plan, decodeState).ConfigureAwait(false);

        if (row is QueryResultRow loadedRow)
            yield return loadedRow;
    }

    private async IAsyncEnumerable<QueryResultRow> ScanMultiIndexRightRows(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        RightDecodeState decodeState)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, joinNode.Index);
        ColumnValue lookupValue = lookupKey.Values[0];
        QueryDependencyCollector? deps = plan.DepCollector;

        // Non-unique index equality probe: the index bucket range catches phantom inserts for
        // this key. Per-row point deps are recorded below as each row is fetched.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            joinNode.Index.KvId,
            keyTypes,
            lookupKey,
            to: null,
            unique: false, cancellationToken: plan.Ticket.CancellationToken).ConfigureAwait(false))
        {
            int cmp = key.Values[0].CompareTo(lookupValue);

            // Early-exit past the probe value in the index's DECODED order: an ascending first
            // column streams ascending (past = greater), a descending one streams descending
            // (past = smaller). Breaking on `> 0` for a DESC column would never fire, silently
            // downgrading every probe to a full index-tail walk.
            bool firstColumnDescending = joinNode.Index.DirectionAt(0) == OrderType.Descending;
            if (firstColumnDescending ? cmp < 0 : cmp > 0)
                break;

            if (cmp != 0)
                continue;

            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            QueryResultRow? row = await LoadRightRow(source, rowId, joinNode.RightExecutionFilter, plan, decodeState).ConfigureAwait(false);

            if (row is QueryResultRow loadedRow)
                yield return loadedRow;
        }
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
    private async Task<QueryResultRow?> LoadRightRow(
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
            _options,
            GetRequiredColumnsForAlias(plan, source.Alias),
            GetTableSchemaVersionForAlias(plan, source.Alias),
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
                if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    return null;
            }
            else
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);
                if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    return null;
            }
        }

        return new QueryResultRow(rowId, row);
    }

    private static string ResolveLeftAlias(PhysicalPlanNode leftNode, QueryResultRow leftRow)
    {
        switch (leftNode)
        {
            case TableScanNode { BoundSource: not null } scanNode:
                return scanNode.BoundSource.Alias;

            case IndexRangeScanNode { BoundSource: not null } rangeScanNode:
                return rangeScanNode.BoundSource.Alias;

            case IndexInListScanNode { BoundSource: not null } inListScanNode:
                return inListScanNode.BoundSource.Alias;

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
                return derivedScanNode.BoundSource.Alias;

            // Left side may be wrapped in a SortNode; delegate to its inner scan.
            case SortNode { Input: not null } sortNode:
                return ResolveLeftAlias(sortNode.Input!, leftRow);

            default:
                break;
        }

        foreach (KeyValuePair<string, ColumnValue> entry in leftRow.Row)
        {
            if (!QueryRowMerger.IsQualifiedKey(entry.Key))
                continue;

            int dotIndex = entry.Key.IndexOf('.');
            return entry.Key[..dotIndex];
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Could not resolve alias for nested join left row");
    }

    private async IAsyncEnumerable<QueryResultRow> ScanJoinRightSource(
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

    private async IAsyncEnumerable<QueryResultRow> ScanBoundTable(
        BoundTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        IReadOnlySet<string>? requiredColumns = null)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        IReadOnlySet<string>? required = requiredColumns ?? GetRequiredColumnsForAlias(plan, source.Alias);
        QueryDependencyCollector? deps = plan.DepCollector;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        deps?.RecordRange(table.Store.RowKeySpace);
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

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
            _options,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias),
                rowDecodeState).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

                if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(rowId, row);
        }
    }

    /// <summary>
    /// Scans a table in join-key order by walking its secondary index from the first entry to
    /// the last. Used by <see cref="ExecuteMergeJoin"/> when the planner detected free ordering
    /// on the right side (a <see cref="TableScanSource.ForcedIndex"/> node). One row-fetch is
    /// issued per index entry; deleted or empty rows are skipped.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ScanBoundTableByIndex(
        BoundTableSource source,
        TableIndexSchema index,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);
        IReadOnlySet<string>? required = GetRequiredColumnsForAlias(plan, source.Alias);
        QueryDependencyCollector? deps = plan.DepCollector;

        bool unique = index.Type == IndexType.Unique;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            index.KvId,
            keyTypes,
            from: null, to: null, unique: unique, cancellationToken: plan.Ticket.CancellationToken).ConfigureAwait(false))
        {
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            ReadOnlyMemory<byte>? dataOpt = await table.Store.GetRow(plan.Ticket.TxnState, rowId, plan.Ticket.CancellationToken).ConfigureAwait(false);

            if (dataOpt is null || dataOpt.Value.Length == 0)
                continue;

            ReadOnlyMemory<byte> data = dataOpt.Value;
            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                txId,
                rowId,
                data,
            _options,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias),
                rowDecodeState).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

                if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(rowId, row);
        }
    }

    /// <summary>
    /// Executes a bounded index range scan for a join leaf node, fetching the primary row for
    /// each matching index entry and applying the residual execution filter (if any).
    /// Used when <see cref="JoinQueryPlanner"/> chose an index range scan for this leaf.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ScanBoundTableByIndexRange(
        IndexRangeScanNode rangeNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        BoundTableSource source = rangeNode.BoundSource!;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, rangeNode.Index);
        IReadOnlySet<string>? required = GetRequiredColumnsForAlias(plan, source.Alias);
        QueryDependencyCollector? deps = plan.DepCollector;

        bool unique = rangeNode.Index.Type == IndexType.Unique;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        // Index range scan: the index bucket range covers membership phantoms; per-row point
        // deps cover updates to non-indexed projected columns.
        deps?.RecordRange(table.Store.IndexKeySpace(rangeNode.Index.KvId));
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            rangeNode.Index.KvId,
            keyTypes,
            from: rangeNode.FromBound,
            to: rangeNode.ToBound,
            fromInclusive: rangeNode.FromInclusive,
            toInclusive: rangeNode.ToInclusive,
            unique: unique, cancellationToken: plan.Ticket.CancellationToken).ConfigureAwait(false))
        {
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            ReadOnlyMemory<byte>? dataOpt = await table.Store.GetRow(plan.Ticket.TxnState, rowId, plan.Ticket.CancellationToken).ConfigureAwait(false);

            if (dataOpt is null || dataOpt.Value.Length == 0)
                continue;

            ReadOnlyMemory<byte> data = dataOpt.Value;
            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                txId,
                rowId,
                data,
            _options,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias),
                rowDecodeState).ConfigureAwait(false);

            if (rangeNode.ExecutionFilter is not null)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);
                if (!await queryFilterer.MeetWhereAsync(rangeNode.ExecutionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(rowId, row);
        }
    }

    /// <summary>
    /// Executes an IN-list index scan for a join leaf node.
    /// Unique indexes perform one <c>LookupUnique</c> per value; non-unique indexes perform one
    /// equality range scan per value. Duplicate row IDs are suppressed across all values.
    /// The residual <see cref="IndexInListScanNode.ExecutionFilter"/> (if any) is applied after
    /// each row is fetched, using the alias-qualified row.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ScanBoundTableByInList(
        IndexInListScanNode inListNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        BoundTableSource source = inListNode.BoundSource!;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, inListNode.Index);
        IReadOnlySet<string>? required = GetRequiredColumnsForAlias(plan, source.Alias);
        bool isUnique = inListNode.Index.Type == IndexType.Unique;
        HashSet<ObjectIdValue> seen = new();
        QueryDependencyCollector? deps = plan.DepCollector;
        RowEncoder.RowDecodeState rowDecodeState = new();
        RowLayout? qualifiedLayout = null;

        // IN-list scan: record the index bucket once (covers all per-value range probes) and schema.
        deps?.RecordRange(table.Store.IndexKeySpace(inListNode.Index.KvId));
        deps?.RecordSchema(table.Id, GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        foreach (ColumnValue value in inListNode.Values)
        {
            CompositeColumnValue lookupKey = new(new[] { value });

            if (isUnique)
            {
                ObjectIdValue? rowId = await table.Store.LookupUnique(
                    plan.Ticket.TxnState, inListNode.Index.KvId, lookupKey, plan.Ticket.CancellationToken).ConfigureAwait(false);

                if (rowId is null || !seen.Add(rowId.Value))
                    continue;

                deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

                ReadOnlyMemory<byte>? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId.Value, plan.Ticket.CancellationToken).ConfigureAwait(false);
                if (data is null || data.Value.Length == 0)
                    continue;

                QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema, txId, rowId.Value, data.Value,
            _options,
                    required,
                    GetTableSchemaVersionForAlias(plan, source.Alias),
                    rowDecodeState).ConfigureAwait(false);

                if (inListNode.ExecutionFilter is not null)
                {
                    qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                    IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);
                    if (!await queryFilterer.MeetWhereAsync(
                            inListNode.ExecutionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                        continue;
                }

                yield return new QueryResultRow(rowId.Value, row);
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
                    maxRows: null, cancellationToken: plan.Ticket.CancellationToken).ConfigureAwait(false))
                {
                    if (!seen.Add(rowId))
                        continue;

                    deps?.RecordPoint(table.Store.RowPointKey(rowId));

                    ReadOnlyMemory<byte>? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId, plan.Ticket.CancellationToken).ConfigureAwait(false);
                    if (data is null || data.Value.Length == 0)
                        continue;

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema, txId, rowId, data.Value,
            _options,
                        required,
                        GetTableSchemaVersionForAlias(plan, source.Alias),
                        rowDecodeState).ConfigureAwait(false);

                    if (inListNode.ExecutionFilter is not null)
                    {
                        qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                        IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);
                        if (!await queryFilterer.MeetWhereAsync(
                                inListNode.ExecutionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                            continue;
                    }

                    yield return new QueryResultRow(rowId, row);
                }
            }
        }
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

    private async IAsyncEnumerable<QueryResultRow> ScanDerivedTable(
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

    private async IAsyncEnumerable<QueryResultRow> ApplyWhere(
        IAsyncEnumerable<QueryResultRow> cursor,
        NodeAst where,
        QueryPlan plan)
    {
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (await queryFilterer.MeetWhereAsync(where, row.Row, plan.Ticket, plan.Database).ConfigureAwait(false))
                yield return row;
        }
    }

    private static IReadOnlySet<string>? GetRequiredColumnsForAlias(QueryPlan plan, string alias)
    {
        if (plan.RequiredColumnsByAlias?.TryGetValue(alias, out IReadOnlySet<string>? required) == true)
            return required;

        return plan.ScanRequiredColumns;
    }

    private static int GetTableSchemaVersionForAlias(QueryPlan plan, string alias)
    {
        return plan.TableSchemaVersionByAlias.TryGetValue(alias, out int version)
            ? version
            : plan.TableSchemaVersion;
    }

    private static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
        {
            string colName = index.Columns[i];
            TableColumnSchema? col = table.Schema.Columns?.Find(c => string.Equals(c.Name, colName, StringComparison.OrdinalIgnoreCase));
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }

    // ── Broadcast Hash Join ───────────────────────────────────────────────────

    /// <summary>
    /// Everything the broadcast probe needs beyond the join node itself: the probe leaf's
    /// identity and placement, the build rows flattened into an indexable array (in bucket
    /// enumeration order — remote match indices point into it), and the reusable fragment
    /// join spec with the build rows already wire-encoded once.
    /// </summary>
    private sealed class BroadcastJoinPlan
    {
        public required TableDescriptor ProbeTable { get; init; }

        public required string ProbeAlias { get; init; }

        public required IReadOnlySet<string>? ProbeRequiredColumns { get; init; }

        public required int ProbeSchemaVersion { get; init; }

        public required NodeAst? ProbeFilter { get; init; }

        public required TablePlacement Placement { get; init; }

        public required IReadOnlyList<IReadOnlyDictionary<string, ColumnValue>> FlatBuildRows { get; init; }

        public required QueryFragmentJoinSpec Spec { get; init; }
    }

    /// <summary>One probe row crossing a broadcast span channel; null <see cref="Matches"/> means "probe on the consumer" (local span or fallback), non-null means the remote already filtered, probed, and ON-checked.</summary>
    private readonly record struct BroadcastProbeItem(ObjectIdValue RowId, QueryRow Row, int[]? Matches);

    /// <summary>
    /// Decides whether this hash join's probe runs as broadcast fragments, and prepares the
    /// shipping plan when it does. Every condition falls back to the standard local probe —
    /// declining is never an error. Gates: distributed execution on, a transport, a positive
    /// broadcast cap the build's <b>actual</b> row count fits under, a plain primary-row base
    /// table on the probe side (the left leaf when the right side was built; the right table
    /// source when the left subtree was built), a transaction whose reads need no session
    /// folding and takes no exclusive predicate locks, no dependency collector, shippable ON
    /// and probe-filter predicates, and a multi-span key-range placement with at least one
    /// non-local leader (an all-local placement gains nothing from shipping the build).
    /// </summary>
    private BroadcastJoinPlan? TryPrepareBroadcastJoin(
        HashJoinNode joinNode,
        QueryPlan plan,
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> hashTable,
        HashJoinBuildSide buildSide)
    {
        if (fragmentTransport is null || logger is null)
            return null;

        CamusDBOptions liveOptions = plan.Database.Options;

        if (!liveOptions.DistributedQueryExecutionEnabled || liveOptions.BroadcastJoinMaxBuildRows <= 0)
            return null;

        // The probe side must be a plain primary-row base table so its keyspace can be
        // span-fragmented: the left scan leaf when the right side was built, or the right
        // table source when the left subtree was built (a derived right source cannot be
        // span-scanned remotely).
        TableDescriptor probeTable;
        string probeAlias;
        NodeAst? probeFilter;
        IReadOnlySet<string>? probeRequired;

        if (buildSide == HashJoinBuildSide.Right)
        {
            if (joinNode.Input is not TableScanNode { Source: TableScanSource.PrimaryRows, BoundSource: { } probeSource } probeScan)
                return null;

            probeTable = probeSource.Table;
            probeAlias = probeSource.Alias;
            probeFilter = probeScan.ExecutionFilter;
            probeRequired = probeScan.RequiredColumns ?? GetRequiredColumnsForAlias(plan, probeAlias);
        }
        else
        {
            if (joinNode.BuildSource.Table is not { } rightSource)
                return null;

            probeTable = rightSource.Table;
            probeAlias = rightSource.Alias;
            probeFilter = joinNode.BuildExecutionFilter;
            probeRequired = GetRequiredColumnsForAlias(plan, probeAlias);
        }

        if (plan.Ticket.TxnState.FoldReads || plan.Ticket.ExclusivePredicateLocks || plan.DepCollector is not null)
            return null;

        if (joinNode.OnPredicate is null || !FragmentFilterShippability.IsShippable(joinNode.OnPredicate))
            return null;

        if (probeFilter is not null && !FragmentFilterShippability.IsShippable(probeFilter))
            return null;

        EmbeddedKahuna kahuna = plan.Database.Kahuna;
        if (!kahuna.IsClusterMode)
            return null;

        TablePlacement placement = kahuna.GetPlacement(probeTable.Store.RowKeySpace);
        if (!placement.IsKeyRange || placement.Spans.Count < 2 || placement.AllLeadersLocal)
            return null;

        foreach (PlacementSpan span in placement.Spans)
        {
            if (!GatherNode.TryParseRowIdBound(span.StartKey, out _) || !GatherNode.TryParseRowIdBound(span.EndKey, out _))
                return null;
        }

        // Flatten the build in bucket enumeration order. Inter-bucket order is irrelevant
        // (buckets only group equal keys); what matters is that equal-key rows keep their
        // bucket order, so the remote's rebuilt buckets match this coordinator's exactly and
        // match indices reproduce the same merge order.
        int buildCount = 0;
        foreach (List<IReadOnlyDictionary<string, ColumnValue>> bucket in hashTable.Values)
            buildCount += bucket.Count;

        if (buildCount == 0 || buildCount > liveOptions.BroadcastJoinMaxBuildRows)
            return null;

        List<IReadOnlyDictionary<string, ColumnValue>> flat = new(buildCount);
        string[] encoded = new string[buildCount];

        foreach (List<IReadOnlyDictionary<string, ColumnValue>> bucket in hashTable.Values)
        {
            foreach (IReadOnlyDictionary<string, ColumnValue> row in bucket)
            {
                encoded[flat.Count] = QueryExecutor.EncodeCells(row);
                flat.Add(row);
            }
        }

        return new BroadcastJoinPlan
        {
            ProbeTable = probeTable,
            ProbeAlias = probeAlias,
            ProbeRequiredColumns = probeRequired,
            ProbeSchemaVersion = GetTableSchemaVersionForAlias(plan, probeAlias),
            ProbeFilter = probeFilter,
            Placement = placement,
            FlatBuildRows = flat,
            Spec = new QueryFragmentJoinSpec
            {
                BuildIsLeft = buildSide == HashJoinBuildSide.Left,
                ProbeAlias = probeAlias,
                ProbeKeyColumns = joinNode.ProbeKeyColumns.ToArray(),
                BuildAlias = joinNode.BuildSource.Alias,
                BuildKeyColumns = joinNode.BuildKeyColumns.ToArray(),
                BuildRows = encoded,
                OnPredicateJson = NodeAstWireCodec.Serialize(joinNode.OnPredicate),
                ProbeFilterJson = probeFilter is not null ? NodeAstWireCodec.Serialize(probeFilter) : null,
            },
        };
    }

    /// <summary>
    /// The broadcast probe: one worker per placement span — remote spans ship the build and
    /// receive only matched probe rows (raw bytes + match indices), local spans and fallbacks
    /// stream unfiltered probe rows — drained strictly in span order so output is
    /// byte-identical to the sequential probe (spans partition the row keyspace, and the
    /// coordinator merges every pair against its <b>own</b> build dictionaries; the remote
    /// only ever contributes indices). Filtering, probing, merging, and ON evaluation all run
    /// on this consumer thread — the filterer and lazily built layouts are not thread-safe —
    /// while workers do the fetch and decode. A remote failure resumes that span locally
    /// after the last delivered probe row; frames are per probe row, so nothing is duplicated
    /// or lost mid-bucket.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ExecuteBroadcastHashProbe(
        HashJoinNode joinNode,
        QueryPlan plan,
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> hashTable,
        BroadcastJoinPlan broadcast)
    {
        TableDescriptor table = broadcast.ProbeTable;
        DatabaseDescriptor database = plan.Database;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        HLCTimestamp readTs = plan.Ticket.TxnState.ReadTimestamp;
        CamusDBOptions options = database.Options;
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.BuildSource.Alias;
        IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
        IReadOnlyList<PlacementSpan> spans = broadcast.Placement.Spans;

        // The session must exist before workers read tx.TransactionId concurrently.
        await plan.Ticket.TxnState.EnsureSessionStartedAsync(CancellationToken.None).ConfigureAwait(false);

        // Linked to the request: every probe worker reads and writes through this token, so the
        // link stops the local scans and the remote fragments together on a client disconnect.
        using CancellationTokenSource cts =
            CancellationTokenSource.CreateLinkedTokenSource(plan.Ticket.CancellationToken);

        var channels = new Channel<BroadcastProbeItem>[spans.Count];
        var workers = new Task[spans.Count];

        for (int i = 0; i < spans.Count; i++)
        {
            channels[i] = Channel.CreateBounded<BroadcastProbeItem>(
                new BoundedChannelOptions(256) { SingleReader = true, SingleWriter = true });

            PlacementSpan span = spans[i];

            // Bounds were validated during preparation; a failure here means the placement
            // snapshot changed shape underneath us — fail closed via the channel.
            if (!GatherNode.TryParseRowIdBound(span.StartKey, out ObjectIdValue? fromRowId)
                || !GatherNode.TryParseRowIdBound(span.EndKey, out ObjectIdValue? untilRowId))
            {
                channels[i].Writer.TryComplete(new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Placement span boundary is not a row-id key: " + (span.StartKey ?? span.EndKey)));
                workers[i] = Task.CompletedTask;
                continue;
            }

            bool remote = !span.LeaderIsLocal && span.LeaderEndpoint is not null;

            workers[i] = remote
                ? RunRemoteProbeSpanAsync(channels[i].Writer, span, fromRowId, untilRowId)
                : RunLocalProbeSpanAsync(channels[i].Writer, fromRowId, untilRowId, afterRowId: null);
        }

        // Shared merge state, consumer-thread only. The two build sides differ only in which
        // argument of the merge the probe row occupies and where its keys are read from:
        // build-right probes are qualified left rows (keys via ProbeKeyColumns, merged as the
        // left argument); build-left probes are bare right rows (keys via BuildKeyColumns,
        // merged as the right argument, qualified by the merge itself).
        bool buildIsLeft = broadcast.Spec.BuildIsLeft;
        IReadOnlyList<string> localProbeKeys = buildIsLeft ? joinNode.BuildKeyColumns : probeKeys;
        RowLayout? qualifiedProbeLayout = null;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        try
        {
            for (int i = 0; i < spans.Count; i++)
            {
                await foreach (BroadcastProbeItem item in channels[i].Reader.ReadAllAsync(plan.Ticket.CancellationToken).ConfigureAwait(false))
                {
                    qualifiedProbeLayout ??= QueryRowMerger.BuildQualifiedLayout(item.Row.Layout, broadcast.ProbeAlias);
                    IReadOnlyDictionary<string, ColumnValue> qualifiedProbe = QueryRowMerger.QualifyRowAsQueryRow(item.Row, qualifiedProbeLayout);

                    // Key extraction reads the same view the standard probe reads: the
                    // qualified row for a left-side probe, the bare row for a right-side one.
                    IReadOnlyDictionary<string, ColumnValue> keySource = buildIsLeft ? item.Row : qualifiedProbe;

                    if (item.Matches is null)
                    {
                        // Local span (or fallback): the standard probe, on this thread.
                        if (broadcast.ProbeFilter is not null
                            && !await queryFilterer.MeetWhereAsync(broadcast.ProbeFilter, qualifiedProbe, ticket, database).ConfigureAwait(false))
                            continue;

                        ColumnValue[] probeKeyValues = new ColumnValue[localProbeKeys.Count];
                        bool hasNull = false;

                        for (int k = 0; k < localProbeKeys.Count; k++)
                        {
                            if (!keySource.TryGetValue(localProbeKeys[k], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                            { hasNull = true; break; }
                            probeKeyValues[k] = kv;
                        }

                        if (hasNull) continue;

                        if (!hashTable.TryGetValue(new CompositeColumnValue(probeKeyValues), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                            continue;

                        foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
                        {
                            QueryRow merged = MergePair(buildRow, item.Row, qualifiedProbe);

                            if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, database).ConfigureAwait(false))
                                continue;

                            yield return new QueryResultRow(default(ObjectIdValue), merged);
                        }
                    }
                    else
                    {
                        // Remote span: filter, probe, and ON check already ran at the data —
                        // re-running would double-evaluate. Merge against our own build rows.
                        foreach (int matchIndex in item.Matches)
                        {
                            if ((uint)matchIndex >= (uint)broadcast.FlatBuildRows.Count)
                                throw new CamusDBException(
                                    CamusDBErrorCodes.InvalidInternalOperation,
                                    $"Broadcast join fragment returned match index {matchIndex} outside the shipped build ({broadcast.FlatBuildRows.Count} rows)");

                            yield return new QueryResultRow(
                                default(ObjectIdValue),
                                MergePair(broadcast.FlatBuildRows[matchIndex], item.Row, qualifiedProbe));
                        }
                    }
                }

                await workers[i].ConfigureAwait(false);
            }
        }
        finally
        {
            cts.Cancel();

            foreach (Channel<BroadcastProbeItem> channel in channels)
            {
                while (channel.Reader.TryRead(out _)) { }
            }

            foreach (Task worker in workers)
            {
                try
                {
                    await worker.ConfigureAwait(false);
                }
                catch
                {
                    // Consumer is exiting; a worker fault was either already thrown from its
                    // channel or belongs to an abandoned span.
                }
            }
        }

        // The merge argument order is the standard probe's for each build side: build-right
        // merges (qualified probe, bare build); build-left merges (qualified build, bare
        // probe). The lazily built layout/ordinal-map pair is shared for the whole gather —
        // consumer-thread only.
        QueryRow MergePair(
            IReadOnlyDictionary<string, ColumnValue> buildRow,
            QueryRow bareProbe,
            IReadOnlyDictionary<string, ColumnValue> qualifiedProbe)
        {
            if (buildIsLeft)
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(buildRow, bareProbe, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(bareProbe, rightAlias, joinLayout);
                return QueryRowMerger.MergeRowsAsQueryRow(buildRow, bareProbe, joinLayout, rightOrdinalMap);
            }

            joinLayout      ??= QueryRowMerger.BuildJoinLayout(qualifiedProbe, buildRow, rightAlias);
            rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, rightAlias, joinLayout);
            return QueryRowMerger.MergeRowsAsQueryRow(qualifiedProbe, buildRow, joinLayout, rightOrdinalMap);
        }

        async Task RunRemoteProbeSpanAsync(
            ChannelWriter<BroadcastProbeItem> writer,
            PlacementSpan span,
            ObjectIdValue? fromRowId,
            ObjectIdValue? untilRowId)
        {
            // Probe rows already delivered from this span; frames are per probe row (all of a
            // row's matches ship together), so resuming locally AFTER this row on failure
            // duplicates and loses nothing.
            ObjectIdValue? lastEmitted = null;
            long shippedThisSpan = 0;

            if (distributedMetrics is not null)
                Interlocked.Increment(ref distributedMetrics.FragmentsDispatched);

            try
            {
                QueryFragmentRequest request = new()
                {
                    FragmentId = Guid.NewGuid().ToString("n"),
                    DatabaseName = database.Name,
                    DatabaseId = database.Id,
                    TableName = table.Name,
                    TableId = table.Id,
                    SchemaVersion = broadcast.ProbeSchemaVersion,
                    FromRowIdHex = fromRowId?.ToString(),
                    UntilRowIdHex = untilRowId?.ToString(),
                    ReadTsNode = readTs.N,
                    ReadTsPhysical = readTs.L,
                    ReadTsCounter = readTs.C,
                    RequiredColumns = broadcast.ProbeRequiredColumns?.ToArray(),
                    Join = broadcast.Spec,
                };

                RowEncoder.RowDecodeState decodeState = new();

                await foreach (QueryFragmentRow fragmentRow in fragmentTransport!
                    .ExecuteFragmentAsync(span.LeaderEndpoint!, request, cts.Token).ConfigureAwait(false))
                {
                    if (fragmentRow.Stats is not null)
                        continue;

                    if (fragmentRow.MatchIndices is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            "Broadcast join fragment returned a frame without match indices");

                    if (fragmentRow.RowIdHex is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            "Broadcast join fragment returned a row frame without a row id");

                    ObjectIdValue rowId = ObjectId.ToValue(fragmentRow.RowIdHex);

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema,
                        txId,
                        rowId,
                        fragmentRow.Data,
                        options,
                        broadcast.ProbeRequiredColumns,
                        broadcast.ProbeSchemaVersion,
                        decodeState).ConfigureAwait(false);

                    await writer.WriteAsync(new BroadcastProbeItem(rowId, row, fragmentRow.MatchIndices), cts.Token).ConfigureAwait(false);
                    lastEmitted = rowId;
                    shippedThisSpan++;
                }

                writer.Complete();
            }
            catch (OperationCanceledException)
            {
                writer.TryComplete();
            }
            catch (Exception ex)
            {
                Log.LogRemoteFragmentFellBackToLocal(logger!, span.LeaderEndpoint!, ex.Message);
                database.Kahuna.InvalidatePlacement(table.Store.RowKeySpace);

                if (distributedMetrics is not null)
                    Interlocked.Increment(ref distributedMetrics.FragmentFallbacks);

                await RunLocalProbeSpanAsync(writer, fromRowId, untilRowId, afterRowId: lastEmitted).ConfigureAwait(false);
            }
            finally
            {
                if (distributedMetrics is not null && shippedThisSpan > 0)
                    Interlocked.Add(ref distributedMetrics.RowsShippedIn, shippedThisSpan);
            }
        }

        async Task RunLocalProbeSpanAsync(
            ChannelWriter<BroadcastProbeItem> writer,
            ObjectIdValue? fromRowId,
            ObjectIdValue? untilRowId,
            ObjectIdValue? afterRowId)
        {
            try
            {
                RowEncoder.RowDecodeState decodeState = new();

                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    plan.Ticket.TxnState,
                    afterRowId: afterRowId,
                    cancellationToken: cts.Token,
                    untilRowId: untilRowId,
                    fromRowId: fromRowId).ConfigureAwait(false))
                {
                    if (data.Length == 0)
                        continue;

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema,
                        txId,
                        rowId,
                        data,
                        options,
                        broadcast.ProbeRequiredColumns,
                        broadcast.ProbeSchemaVersion,
                        decodeState).ConfigureAwait(false);

                    await writer.WriteAsync(new BroadcastProbeItem(rowId, row, null), cts.Token).ConfigureAwait(false);
                }

                writer.Complete();
            }
            catch (OperationCanceledException)
            {
                writer.TryComplete();
            }
            catch (Exception ex)
            {
                writer.TryComplete(ex);
            }
        }
    }

    /// <summary>
    /// Serves a peer coordinator's broadcast-join probe fragment: rebuilds the buckets from
    /// the shipped build rows (each keeping its index into the shipped array), scans the probe
    /// span at the fragment's snapshot, and for each probe row that passes the probe filter
    /// and has at least one candidate whose merged pair satisfies the full ON predicate,
    /// ships one frame — the probe row's raw bytes plus its match indices. Runs through this
    /// class rather than the scan path because match semantics must be exactly the local hash
    /// probe's: same key comparer, same NULL-key exclusion, same merged-row ON evaluation via
    /// <see cref="QueryRowMerger"/>.
    /// </summary>
    internal async IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentJoinProbe(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryFragmentJoinSpec join,
        ObjectIdValue? fromRowId,
        ObjectIdValue? untilRowId,
        int schemaVersion,
        IReadOnlySet<string>? requiredColumns,
        KvTransaction snapshotTx,
        bool wantStats = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Which columns key which side depends on the shipped build side: a right-side build
        // ships bare rows keyed by BuildKeyColumns and probes qualified left rows via
        // ProbeKeyColumns; a left-side build ships qualified rows keyed by ProbeKeyColumns
        // and probes bare right rows via BuildKeyColumns — exactly the coordinator's own
        // key-extraction rules for each shape.
        string[] bucketKeyColumns = join.BuildIsLeft ? join.ProbeKeyColumns : join.BuildKeyColumns;
        string[] probeKeyColumns = join.BuildIsLeft ? join.BuildKeyColumns : join.ProbeKeyColumns;

        // Rebuild the buckets in shipped order so equal-key rows keep the coordinator's
        // bucket order — match indices must reproduce the coordinator's merge order exactly.
        Dictionary<CompositeColumnValue, List<(int Index, Dictionary<string, ColumnValue> Row)>> buckets =
            new(CompositeColumnValueComparer.Instance);

        for (int i = 0; i < join.BuildRows.Length; i++)
        {
            Dictionary<string, ColumnValue> buildRow = QueryExecutor.ParseCells(join.BuildRows[i]);

            ColumnValue[] keyValues = new ColumnValue[bucketKeyColumns.Length];
            bool hasNull = false;

            for (int k = 0; k < bucketKeyColumns.Length; k++)
            {
                if (!buildRow.TryGetValue(bucketKeyColumns[k], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                { hasNull = true; break; }
                keyValues[k] = kv;
            }

            if (hasNull)
                continue;

            CompositeColumnValue key = new(keyValues);
            if (!buckets.TryGetValue(key, out List<(int Index, Dictionary<string, ColumnValue> Row)>? bucket))
            { bucket = []; buckets[key] = bucket; }

            bucket.Add((i, buildRow));
        }

        NodeAst onPredicate = NodeAstWireCodec.Deserialize(join.OnPredicateJson);
        NodeAst? probeFilter = join.ProbeFilterJson is null ? null : NodeAstWireCodec.Deserialize(join.ProbeFilterJson);

        QueryTicket ticket = new(
            txnState: snapshotTx,
            databaseName: database.Name,
            tableName: table.Name,
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null,
            cancellationToken: cancellationToken);

        RowEncoder.RowDecodeState decodeState = new();
        RowLayout? qualifiedLayout = null;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        List<int> matches = [];
        long scanned = 0, shipped = 0;

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
            snapshotTx,
            cancellationToken: cancellationToken,
            untilRowId: untilRowId,
            fromRowId: fromRowId).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            scanned++;

            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                snapshotTx.TransactionId,
                rowId,
                data,
                database.Options,
                requiredColumns,
                schemaVersion,
                decodeState).ConfigureAwait(false);

            qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, join.ProbeAlias);
            IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

            if (probeFilter is not null
                && !await queryFilterer.MeetWhereAsync(probeFilter, qualified, ticket, database).ConfigureAwait(false))
                continue;

            // Same key-source rule as the coordinator: qualified row for a left-side probe,
            // bare row for a right-side one.
            IReadOnlyDictionary<string, ColumnValue> keySource = join.BuildIsLeft ? row : qualified;

            ColumnValue[] probeKeyValues = new ColumnValue[probeKeyColumns.Length];
            bool hasNull = false;

            for (int k = 0; k < probeKeyColumns.Length; k++)
            {
                if (!keySource.TryGetValue(probeKeyColumns[k], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                { hasNull = true; break; }
                probeKeyValues[k] = kv;
            }

            if (hasNull)
                continue;

            if (!buckets.TryGetValue(new CompositeColumnValue(probeKeyValues), out List<(int Index, Dictionary<string, ColumnValue> Row)>? bucket))
                continue;

            matches.Clear();

            foreach ((int index, Dictionary<string, ColumnValue> buildRow) in bucket)
            {
                QueryRow merged;

                if (join.BuildIsLeft)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(buildRow, row, join.BuildAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(row, join.BuildAlias, joinLayout);
                    merged = QueryRowMerger.MergeRowsAsQueryRow(buildRow, row, joinLayout, rightOrdinalMap);
                }
                else
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(qualified, buildRow, join.BuildAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, join.BuildAlias, joinLayout);
                    merged = QueryRowMerger.MergeRowsAsQueryRow(qualified, buildRow, joinLayout, rightOrdinalMap);
                }

                if (await queryFilterer.MeetWhereAsync(onPredicate, merged, ticket, database).ConfigureAwait(false))
                    matches.Add(index);
            }

            if (matches.Count == 0)
                continue;

            shipped++;
            yield return new QueryFragmentRow(rowId.ToString(), data.ToArray(), MatchIndices: matches.ToArray());
        }

        if (wantStats)
            yield return new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(scanned, shipped));
    }

    // ── Hash Join ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Materialises the build side into a hash table keyed on the equi-join columns.
    /// Rows whose join key contains any NULL value are excluded.
    ///
    /// Returns null to signal that the caller should route to a fallback:
    /// <list type="bullet">
    ///   <item>When <see cref="CamusDBOptions.SpillEnabled"/> is true, the effective cap is
    ///     <see cref="CamusDBOptions.SpillEffectiveThreshold"/>; overflow routes to
    ///     <see cref="GraceHashJoinAsync"/>.</item>
    ///   <item>When spill is disabled, the cap is <see cref="CamusDBOptions.HashJoinMaxBuildRows"/>;
    ///     overflow routes to a full nested-loop scan.</item>
    /// </list>
    ///
    /// On cap overflow the build scan is abandoned; the fallback re-scans the build side from
    /// scratch (~2× cost on this rare path). This is intentional: correctness over complexity.
    ///
    /// When <paramref name="buildSide"/> is <see cref="HashJoinBuildSide.Right"/> the
    /// right source (<see cref="HashJoinNode.BuildSource"/>) is scanned and keyed by
    /// <see cref="HashJoinNode.BuildKeyColumns"/> (unqualified column names). Stored rows
    /// are unqualified — <c>MergeRows</c> qualifies them at probe time.
    ///
    /// When <paramref name="buildSide"/> is <see cref="HashJoinBuildSide.Left"/> the left
    /// subtree (<see cref="PhysicalPlanNode.Input"/>) is scanned, each row is qualified
    /// immediately, and keyed by <see cref="HashJoinNode.ProbeKeyColumns"/> (qualified
    /// column names such as <c>o.id</c>). Stored rows are already qualified — at probe
    /// time they are passed directly as the left arg to <c>MergeRows</c>.
    /// </summary>
    private async Task<Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>>?> BuildHashTable(
        HashJoinNode joinNode,
        QueryPlan plan,
        HashJoinBuildSide buildSide)
    {
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> table =
            new(CompositeColumnValueComparer.Instance);

        // With spill enabled, cap at SpillEffectiveThreshold so the Grace path is triggered
        // at the configured spill boundary instead of the (much larger) legacy NLJ cap.
        // With spill disabled, honour the legacy HashJoinMaxBuildRows cap.
        QueryExecutionContext context = QueryExecutionContext.For(plan.Database, plan.Ticket);

        int buildCap = context.Options.SpillEnabled
            ? context.Options.SpillEffectiveThreshold
            : context.Options.HashJoinMaxBuildRows;

        int rowCount = 0;

        if (buildSide == HashJoinBuildSide.Right)
        {
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;

            await foreach (QueryResultRow row in ScanJoinRightSource(
                joinNode.BuildSource, joinNode.BuildExecutionFilter, plan).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return null;

                ColumnValue[] keyValues = new ColumnValue[buildKeys.Count];
                bool hasNull = false;

                for (int i = 0; i < buildKeys.Count; i++)
                {
                    if (!row.Row.TryGetValue(buildKeys[i], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                    { hasNull = true; break; }
                    keyValues[i] = kv;
                }

                if (hasNull) continue;

                CompositeColumnValue key = new(keyValues);
                if (!table.TryGetValue(key, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                { bucket = []; table[key] = bucket; }

                bucket.Add(row.Row);
                rowCount++;
            }
        }
        else
        {
            // BuildSide.Left: materialise the left subtree; rows are qualified immediately so
            // they can be passed directly as the "left" arg to MergeRowsAsQueryRow during probe.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
            RowLayout? qualifiedLeftLayout = null;

            await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return null;

                IReadOnlyDictionary<string, ColumnValue> qualified;
                string leftAlias = ResolveLeftAlias(joinNode.Input!, leftRow);
                if (leftRow.Row is QueryRow leftQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                    qualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
                }
                else
                {
                    qualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
                }

                ColumnValue[] keyValues = new ColumnValue[probeKeys.Count];
                bool hasNull = false;

                for (int i = 0; i < probeKeys.Count; i++)
                {
                    if (!qualified.TryGetValue(probeKeys[i], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                    { hasNull = true; break; }
                    keyValues[i] = kv;
                }

                if (hasNull) continue;

                CompositeColumnValue key = new(keyValues);
                if (!table.TryGetValue(key, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                { bucket = []; table[key] = bucket; }

                bucket.Add(qualified);
                rowCount++;
            }
        }

        return table;
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteHashJoin(
        HashJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        HashJoinBuildSide buildSide = joinNode.BuildSide;

        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>>? hashTable =
            await BuildHashTable(joinNode, plan, buildSide).ConfigureAwait(false);

        if (hashTable is null)
        {
            // Build cap exceeded. When spilling is enabled, route to Grace/hybrid hash join
            // so large builds are partitioned to disk instead of falling back to O(n·m) NLJ.
            // Both paths re-scan the build side from scratch (~2× cost on this rare path).
            if (plan.Database.Options.SpillEnabled)
            {
                await foreach (QueryResultRow row in GraceHashJoinAsync(joinNode, plan, ct).ConfigureAwait(false))
                    yield return row;
                yield break;
            }

            // Spill disabled — fall back to nested-loop for correctness.
            NestedLoopJoinNode fallback = new(joinNode.Input!, joinNode.BuildSource, joinNode.OnPredicate!)
            {
                RightExecutionFilter = joinNode.BuildExecutionFilter,
            };

            await foreach (QueryResultRow row in ExecuteNestedLoopJoin(fallback, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        string rightAlias = joinNode.BuildSource.Alias;
        QueryTicket ticket = plan.Ticket;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        if (buildSide == HashJoinBuildSide.Right)
        {
            // Broadcast probe: decided here, after the build exists, so it gates on the
            // build's ACTUAL row count — never a cardinality estimate (interior-node
            // estimates are constants on multi-way joins). When eligible, the small build
            // side is shipped to each remote probe span's leader and only matched probe
            // rows come back; otherwise the standard sequential probe below runs unchanged.
            BroadcastJoinPlan? broadcast = TryPrepareBroadcastJoin(joinNode, plan, hashTable, HashJoinBuildSide.Right);

            if (broadcast is not null)
            {
                await foreach (QueryResultRow row in ExecuteBroadcastHashProbe(joinNode, plan, hashTable, broadcast).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            // Standard path: probe = left subtree, build = right source.
            // Hash table rows are unqualified; MergeRowsAsQueryRow qualifies them with rightAlias.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;

            await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
            {
                IReadOnlyDictionary<string, ColumnValue> leftQualified;
                string leftAlias = ResolveLeftAlias(joinNode.Input!, leftRow);
                if (leftRow.Row is QueryRow leftQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                    leftQualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
                }
                else
                {
                    leftQualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
                }

                ColumnValue[] probeKeyValues = new ColumnValue[probeKeys.Count];
                bool hasNull = false;

                for (int i = 0; i < probeKeys.Count; i++)
                {
                    if (!leftQualified.TryGetValue(probeKeys[i], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                    { hasNull = true; break; }
                    probeKeyValues[i] = kv;
                }

                if (hasNull) continue;

                CompositeColumnValue probeKey = new(probeKeyValues);
                if (!hashTable.TryGetValue(probeKey, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, buildRow, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, buildRow, joinLayout, rightOrdinalMap);

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
        else
        {
            // Same broadcast opportunity with the sides flipped: the built (left) side is the
            // small one, so it ships and the RIGHT table's spans probe remotely. This is the
            // shape real statistics usually produce (small side planned left / built).
            BroadcastJoinPlan? broadcast = TryPrepareBroadcastJoin(joinNode, plan, hashTable, HashJoinBuildSide.Left);

            if (broadcast is not null)
            {
                await foreach (QueryResultRow row in ExecuteBroadcastHashProbe(joinNode, plan, hashTable, broadcast).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            // Build-left path: build = left subtree (stored qualified in hash table),
            // probe = right source (scanned unqualified).
            // MergeRowsAsQueryRow(leftQualifiedBuildRow, rightUnqualifiedProbeRow, rightAlias) is the
            // same call shape as the standard path — output column naming is identical.
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;

            await foreach (QueryResultRow rightRow in ScanJoinRightSource(
                joinNode.BuildSource, joinNode.BuildExecutionFilter, plan).ConfigureAwait(false))
            {
                ColumnValue[] probeKeyValues = new ColumnValue[buildKeys.Count];
                bool hasNull = false;

                for (int i = 0; i < buildKeys.Count; i++)
                {
                    if (!rightRow.Row.TryGetValue(buildKeys[i], out ColumnValue? kv) || kv.Type == ColumnType.Null)
                    { hasNull = true; break; }
                    probeKeyValues[i] = kv;
                }

                if (hasNull) continue;

                CompositeColumnValue probeKey = new(probeKeyValues);
                if (!hashTable.TryGetValue(probeKey, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (IReadOnlyDictionary<string, ColumnValue> leftBuildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftBuildRow, rightRow.Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftBuildRow, rightRow.Row, joinLayout, rightOrdinalMap);

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
    }

    // ── Merge Join ────────────────────────────────────────────────────────────

    /// <summary>
    /// Dispatches to the streaming or materializing two-pointer merge based on whether
    /// both sides are pre-ordered. When both sides are ordered (both use a ForcedIndex scan or
    /// SortNode) the streaming path buffers only the current equal-key run on the right side —
    /// O(max right run size) memory rather than O(n + m). When one or both sides need an
    /// internal sort, the materializing path is used.
    ///
    /// NULL keys are excluded from both sides — consistent with SQL inner-join semantics.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ExecuteMergeJoin(
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

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            IReadOnlyDictionary<string, ColumnValue> qualified;
            string leftAlias = ResolveLeftAlias(joinNode.Input!, leftRow);
            if (leftRow.Row is QueryRow leftQr)
            {
                qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                qualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
            }
            else
            {
                qualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
            }

            ColumnValue[]? key = ExtractMergeKey(qualified, joinNode.LeftKeyColumns);
            if (key is null) continue;

            leftRows.Add((key, qualified));
        }

        // Sort only when not already ordered by the plan (via SortNode or ForcedIndex scan).
        if (!joinNode.LeftIsOrdered)
            leftRows.Sort((a, b) => CompareMergeKeys(a.Key, b.Key));

        // ── Materialise right side (unqualified; MergeRows qualifies at emit time) ──
        List<(ColumnValue[] Key, IReadOnlyDictionary<string, ColumnValue> Row)> rightRows = new();

        if (joinNode.RightIsOrdered && joinNode.RightPhysicalNode is not null)
        {
            await foreach (QueryResultRow rightRow in ExecuteJoinTree(joinNode.RightPhysicalNode, plan).ConfigureAwait(false))
            {
                ColumnValue[]? key = ExtractMergeKey(rightRow.Row, joinNode.RightKeyColumns);
                if (key is null) continue;
                rightRows.Add((key, rightRow.Row));
            }
        }
        else
        {
            // Fallback: full scan via ScanJoinRightSource + internal sort.
            await foreach (QueryResultRow rightRow in ScanJoinRightSource(
                joinNode.RightSource, joinNode.RightExecutionFilter, plan).ConfigureAwait(false))
            {
                ColumnValue[]? key = ExtractMergeKey(rightRow.Row, joinNode.RightKeyColumns);
                if (key is null) continue;
                rightRows.Add((key, rightRow.Row));
            }

            rightRows.Sort((a, b) => CompareMergeKeys(a.Key, b.Key));
        }

        // ── Two-pointer sort-merge ────────────────────────────────────────────
        int li = 0;
        int ri = 0;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        while (li < leftRows.Count && ri < rightRows.Count)
        {
            int cmp = CompareMergeKeys(leftRows[li].Key, rightRows[ri].Key);

            if (cmp < 0) { li++; continue; }
            if (cmp > 0) { ri++; continue; }

            // Equal keys — find the extent of both equal-key runs.
            int leftRunStart  = li;
            int rightRunStart = ri;

            while (li < leftRows.Count  && CompareMergeKeys(leftRows[li].Key,  leftRows[leftRunStart].Key)   == 0) li++;
            while (ri < rightRows.Count && CompareMergeKeys(rightRows[ri].Key, rightRows[rightRunStart].Key) == 0) ri++;

            // Emit cross-product of left[leftRunStart..li) × right[rightRunStart..ri).
            for (int l = leftRunStart; l < li; l++)
            {
                for (int r = rightRunStart; r < ri; r++)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftRows[l].QualifiedRow, rightRows[r].Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRows[r].Row, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(
                        leftRows[l].QualifiedRow, rightRows[r].Row, joinLayout, rightOrdinalMap);

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
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
            ExecuteJoinTree(joinNode.Input!, plan).GetAsyncEnumerator(ct);
        await using IAsyncEnumerator<QueryResultRow> rightEnum =
            ExecuteJoinTree(joinNode.RightPhysicalNode!, plan).GetAsyncEnumerator(ct);

        bool leftHasMore  = await leftEnum.MoveNextAsync().ConfigureAwait(false);
        bool rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false);

        while (leftHasMore && rightHasMore)
        {
            // Qualify current left row and extract its key.
            string leftAlias = ResolveLeftAlias(joinNode.Input!, leftEnum.Current);
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
            ColumnValue[]? leftKey = ExtractMergeKey(leftQualified, joinNode.LeftKeyColumns);
            if (leftKey is null)
            {
                leftHasMore = await leftEnum.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            ColumnValue[]? rightKey = ExtractMergeKey(rightEnum.Current.Row, joinNode.RightKeyColumns);
            if (rightKey is null)
            {
                rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false);
                continue;
            }

            int cmp = CompareMergeKeys(leftKey, rightKey);
            if (cmp < 0) { leftHasMore  = await leftEnum.MoveNextAsync().ConfigureAwait(false);  continue; }
            if (cmp > 0) { rightHasMore = await rightEnum.MoveNextAsync().ConfigureAwait(false); continue; }

            // Equal keys — buffer the right equal-key run.
            ColumnValue[] runKey = leftKey;
            List<IReadOnlyDictionary<string, ColumnValue>> rightRun = [];
            while (rightHasMore)
            {
                ColumnValue[]? rk = ExtractMergeKey(rightEnum.Current.Row, joinNode.RightKeyColumns);
                if (rk is null || CompareMergeKeys(rk, runKey) != 0) break;
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

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                leftHasMore = await leftEnum.MoveNextAsync().ConfigureAwait(false);
                if (!leftHasMore) break;

                string la = ResolveLeftAlias(joinNode.Input!, leftEnum.Current);
                if (leftEnum.Current.Row is QueryRow nextQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(nextQr.Layout, la);
                    leftQualified = QueryRowMerger.QualifyRowAsQueryRow(nextQr, qualifiedLeftLayout);
                }
                else
                {
                    leftQualified = QueryRowMerger.QualifyRow(leftEnum.Current.Row, la);
                }
                ColumnValue[]? lk = ExtractMergeKey(leftQualified, joinNode.LeftKeyColumns);
                if (lk is null || CompareMergeKeys(lk, runKey) != 0) break;
            }
        }
    }

    // ── Grace / hybrid hash join ─────────────────────────────────────────────

    /// <summary>
    /// Qualifies every row produced by <paramref name="inputNode"/> so column names carry the
    /// source alias (e.g. "order_id" → "o.order_id"). Used when the left subtree must be
    /// partitioned before the join phase, where it would normally be qualified inline.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> QualifyStreamAsync(
        PhysicalPlanNode inputNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        RowLayout? qualifiedLayout = null;

        await foreach (QueryResultRow row in ExecuteJoinTree(inputNode, plan).WithCancellation(ct).ConfigureAwait(false))
        {
            string alias = ResolveLeftAlias(inputNode, row);
            IReadOnlyDictionary<string, ColumnValue> qualified;
            if (row.Row is QueryRow qr)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(qr.Layout, alias);
                qualified = QueryRowMerger.QualifyRowAsQueryRow(qr, qualifiedLayout);
            }
            else
            {
                qualified = QueryRowMerger.QualifyRow(row.Row, alias);
            }
            yield return new QueryResultRow(row.RowId, qualified);
        }
    }


    /// <summary>Maximum recursion depth for Grace hash-join repartitioning before forcing an
    /// in-memory load regardless of partition size (last resort for extreme single-key skew).</summary>
    private const int MaxGraceHashDepth = 2;

    /// <summary>
    /// Reads all rows from a spill file lazily via a <see cref="SpillRunReader"/>.
    /// Returns an empty sequence when the file is empty.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> ReadSpillFileAsync(
        string path,
        int maxFrameBytes,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, maxFrameBytes, ct: ct).ConfigureAwait(false);
        if (reader is null) yield break;
        await using (reader)
        {
            while (!reader.IsExhausted)
            {
                yield return reader.Current;
                if (!await reader.AdvanceAsync(ct).ConfigureAwait(false)) break;
            }
        }
    }

    /// <summary>
    /// Partitions every row in <paramref name="input"/> into <paramref name="K"/> spill files
    /// using a seed-mixed hash of the join-key columns (see <see cref="PartitionIndex"/>).
    /// Rows with any NULL key column are silently dropped — consistent with SQL inner-join
    /// NULL exclusion. Returns the <paramref name="K"/> file paths in partition order.
    /// </summary>
    private static async Task<string[]> PartitionStreamToFilesAsync(
        SpillScope scope,
        int K,
        int seed,
        IAsyncEnumerable<QueryResultRow> input,
        IReadOnlyList<string> keyColumns,
        CancellationToken ct)
    {
        FileStream[] writers = new FileStream[K];
        string[] paths = new string[K];

        for (int i = 0; i < K; i++)
            paths[i] = scope.OpenWriter(out writers[i]);

        try
        {
            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                ColumnValue[]? keyVals = ExtractMergeKey(row.Row, keyColumns);
                if (keyVals is null) continue;

                int p = PartitionIndex(keyVals, K, seed);
                SpillRowCodec.EncodeToStream(writers[p], row);
            }

            for (int i = 0; i < K; i++)
            {
                await writers[i].FlushAsync(ct).ConfigureAwait(false);
                writers[i].Close();
                writers[i] = null!;
            }

            return paths;
        }
        catch
        {
            for (int i = 0; i < K; i++)
                try { writers[i]?.Close(); } catch { }
            throw;
        }
    }

    /// <summary>
    /// Maps a composite join key to a partition bucket in [0, <paramref name="partitionCount"/>).
    /// <paramref name="seed"/> lets recursive calls use a statistically independent bucketing so
    /// that keys colliding at depth N are redistributed at depth N+1.
    ///
    /// The seed is folded in with a multiplicative mix and a murmur-style finalizer before the
    /// modulo so that each level's bucketing is independent across all bits — not just the high
    /// bits as XOR-then-mod would be for power-of-two bucket counts. This means that two keys
    /// sharing a bucket at depth 0 will land in different buckets at depth 1 (unless they are
    /// actually equal, in which case they are inseparable and the depth-limit load-all backstop
    /// in <see cref="JoinPartitionAsync"/> is the correct resolution).
    /// </summary>
    internal static int PartitionIndex(ColumnValue[] keyValues, int partitionCount, int seed)
    {
        uint h = (uint)CompositeColumnValueComparer.Instance.GetHashCode(keyValues.AsSpan());
        h ^= (uint)seed * 0x9E3779B1u;   // fold level seed in with a multiplier
        h *= 0x85EBCA6Bu; h ^= h >> 13;  // murmur-style bit-mixing finalizer
        h *= 0xC2B2AE35u; h ^= h >> 16;
        return (int)(h % (uint)partitionCount);
    }

    /// <summary>
    /// Grace/hybrid hash join entry point. Partitions both build and probe sides into
    /// <see cref="CamusDBOptions.SpillMergeFanIn"/> spill files each (keyed by join-key hash),
    /// then joins each partition pair. Partitions whose build side exceeds
    /// <see cref="CamusDBOptions.SpillEffectiveThreshold"/> are recursively re-partitioned up to
    /// <see cref="MaxGraceHashDepth"/> levels deep; beyond that depth the entire partition is
    /// loaded into the hash table regardless (correctness backstop for extreme single-key skew).
    ///
    /// <para>
    /// All temporary partition files are written into a <see cref="SpillScope"/> that is
    /// disposed (and all files deleted) in the <c>finally</c> block, covering normal
    /// completion, cancellation, and exception paths.
    /// </para>
    ///
    /// <para>
    /// Side normalization:
    /// <list type="bullet">
    ///   <item><see cref="HashJoinBuildSide.Right"/>: build = unqualified right scan; probe = qualified left scan.</item>
    ///   <item><see cref="HashJoinBuildSide.Left"/>: build = qualified left scan; probe = unqualified right scan.</item>
    /// </list>
    /// In both cases the probe-to-build match at emit time is <c>MergeRows(leftQualified, rightUnqualified, alias)</c>.
    /// </para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> GraceHashJoinAsync(
        HashJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        int K = plan.Database.Options.SpillMergeFanIn;

        if (_stats is not null)
            _stats.HashJoinGracePathCount++;

        IAsyncEnumerable<QueryResultRow> buildStream;
        IReadOnlyList<string> buildKeyColumns;
        IAsyncEnumerable<QueryResultRow> probeStream;
        IReadOnlyList<string> probeKeyColumns;

        if (joinNode.BuildSide == HashJoinBuildSide.Right)
        {
            buildStream     = ScanJoinRightSource(joinNode.BuildSource, joinNode.BuildExecutionFilter, plan);
            buildKeyColumns = joinNode.BuildKeyColumns;
            probeStream     = QualifyStreamAsync(joinNode.Input!, plan, ct);
            probeKeyColumns = joinNode.ProbeKeyColumns;
        }
        else
        {
            buildStream     = QualifyStreamAsync(joinNode.Input!, plan, ct);
            buildKeyColumns = joinNode.ProbeKeyColumns;
            probeStream     = ScanJoinRightSource(joinNode.BuildSource, joinNode.BuildExecutionFilter, plan);
            probeKeyColumns = joinNode.BuildKeyColumns;
        }

        plan.Ticket.Probe?.NoteSpill();
        SpillScope scope = SpillFileManager.CreateScope(QueryExecutionContext.For(plan.Database, plan.Ticket).SpillDirectory);

        try
        {
            string[] buildFiles = await PartitionStreamToFilesAsync(scope, K, seed: 0, buildStream, buildKeyColumns, ct).ConfigureAwait(false);
            string[] probeFiles = await PartitionStreamToFilesAsync(scope, K, seed: 0, probeStream, probeKeyColumns, ct).ConfigureAwait(false);

            for (int p = 0; p < K; p++)
            {
                await foreach (QueryResultRow row in JoinPartitionAsync(
                    joinNode, plan, scope, buildFiles[p], probeFiles[p],
                    buildKeyColumns, probeKeyColumns, seed: 0, depth: 0, ct).ConfigureAwait(false))
                    yield return row;
            }
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Joins a single partition pair by loading the build partition into an in-memory hash table
    /// and streaming the probe partition against it.
    ///
    /// <para>
    /// When the build partition count exceeds <see cref="CamusDBOptions.SpillEffectiveThreshold"/>
    /// and the recursion depth allows it, both files are re-read with a perturbed hash seed to
    /// split into sub-partitions. When the depth limit is reached (extreme skew where a single
    /// key dominates and cannot be split further), the method falls back to a nested-loop join
    /// over the partition files — O(|probe|×|build|) reads but O(1) memory — instead of
    /// materialising the entire skewed build partition into a hash table.
    /// </para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> JoinPartitionAsync(
        HashJoinNode joinNode,
        QueryPlan plan,
        SpillScope scope,
        string buildFile,
        string probeFile,
        IReadOnlyList<string> buildKeyColumns,
        IReadOnlyList<string> probeKeyColumns,
        int seed,
        int depth,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = plan.Database.Options.SpillEffectiveThreshold;
        int K = plan.Database.Options.SpillMergeFanIn;
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.BuildSource.Alias;

        // Load build partition into an in-memory hash table, stopping early whenever
        // the threshold is exceeded regardless of depth — the depth determines the
        // overflow strategy, not whether we detect the overflow.
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> hashTable =
            new(CompositeColumnValueComparer.Instance);
        int buildCount = 0;
        bool overflow = false;

        await using IAsyncEnumerator<QueryResultRow> buildEnum =
            ReadSpillFileAsync(buildFile, _options.SpillMaxFrameBytes, ct).GetAsyncEnumerator(ct);

        while (await buildEnum.MoveNextAsync().ConfigureAwait(false))
        {
            QueryResultRow buildRow = buildEnum.Current;
            ColumnValue[]? keyVals = ExtractMergeKey(buildRow.Row, buildKeyColumns);
            if (keyVals is null) continue;

            if (buildCount >= threshold)
            {
                overflow = true;
                break;
            }

            CompositeColumnValue key = new(keyVals);
            if (!hashTable.TryGetValue(key, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
            { bucket = []; hashTable[key] = bucket; }
            bucket.Add(buildRow.Row);
            buildCount++;
        }

        if (overflow)
        {
            if (depth < MaxGraceHashDepth)
            {
                // Re-read both files with a new hash seed to distribute rows across K sub-partitions.
                int newSeed = seed + 1;
                string[] subBuildFiles = await PartitionStreamToFilesAsync(
                    scope, K, newSeed, ReadSpillFileAsync(buildFile, _options.SpillMaxFrameBytes, ct), buildKeyColumns, ct).ConfigureAwait(false);
                string[] subProbeFiles = await PartitionStreamToFilesAsync(
                    scope, K, newSeed, ReadSpillFileAsync(probeFile, _options.SpillMaxFrameBytes, ct), probeKeyColumns, ct).ConfigureAwait(false);

                for (int p = 0; p < K; p++)
                {
                    await foreach (QueryResultRow row in JoinPartitionAsync(
                        joinNode, plan, scope, subBuildFiles[p], subProbeFiles[p],
                        buildKeyColumns, probeKeyColumns, newSeed, depth + 1, ct).ConfigureAwait(false))
                        yield return row;
                }
            }
            else
            {
                // Recursion depth limit reached and the partition still exceeds the threshold
                // (extreme single-key skew — the key cannot be split further). Fall back to a
                // nested-loop join over the partition files: O(|probe|×|build|) reads, O(1) memory.
                if (_stats is not null)
                    _stats.HashJoinNljPartitionFallbackCount++;

                await foreach (QueryResultRow row in NestedLoopPartitionJoinAsync(
                    joinNode, buildFile, probeFile, buildKeyColumns, probeKeyColumns, ticket, plan, ct)
                    .ConfigureAwait(false))
                    yield return row;
            }
            yield break;
        }

        if (hashTable.Count == 0) yield break;

        // Probe phase: stream probe partition against the loaded hash table.
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        await foreach (QueryResultRow probeRow in ReadSpillFileAsync(probeFile, _options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
        {
            ColumnValue[]? probeKeyVals = ExtractMergeKey(probeRow.Row, probeKeyColumns);
            if (probeKeyVals is null) continue;

            CompositeColumnValue probeKey = new(probeKeyVals);
            if (!hashTable.TryGetValue(probeKey, out List<IReadOnlyDictionary<string, ColumnValue>>? bucket)) continue;

            foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
            {
                // Probe rows are the qualified left-side; build rows are the unqualified right-side
                // (and vice-versa for BuildSide.Left) — see GraceHashJoinAsync side normalization.
                IReadOnlyDictionary<string, ColumnValue> leftQ  = joinNode.BuildSide == HashJoinBuildSide.Right ? probeRow.Row : buildRow;
                IReadOnlyDictionary<string, ColumnValue> rightR = joinNode.BuildSide == HashJoinBuildSide.Right ? buildRow : probeRow.Row;
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQ, rightR, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightR, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQ, rightR, joinLayout, rightOrdinalMap);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    /// <summary>
    /// Nested-loop join over two partition files, used as the bounded-memory backstop when
    /// the recursion depth limit is reached and a skewed partition still exceeds the threshold.
    /// For each probe row, the build file is streamed from the beginning and matching rows are
    /// emitted. Memory is O(1): neither file is materialised.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> NestedLoopPartitionJoinAsync(
        HashJoinNode joinNode,
        string buildFile,
        string probeFile,
        IReadOnlyList<string> buildKeyColumns,
        IReadOnlyList<string> probeKeyColumns,
        QueryTicket ticket,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct)
    {
        string rightAlias = joinNode.BuildSource.Alias;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        await foreach (QueryResultRow probeRow in ReadSpillFileAsync(probeFile, _options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
        {
            ColumnValue[]? probeKeyVals = ExtractMergeKey(probeRow.Row, probeKeyColumns);
            if (probeKeyVals is null) continue;

            await foreach (QueryResultRow buildRow in ReadSpillFileAsync(buildFile, _options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
            {
                ColumnValue[]? buildKeyVals = ExtractMergeKey(buildRow.Row, buildKeyColumns);
                if (buildKeyVals is null) continue;

                if (!CompositeColumnValueComparer.Instance.Equals(probeKeyVals.AsSpan(), buildKeyVals.AsSpan()))
                    continue;

                IReadOnlyDictionary<string, ColumnValue> leftQ  = joinNode.BuildSide == HashJoinBuildSide.Right ? probeRow.Row : buildRow.Row;
                IReadOnlyDictionary<string, ColumnValue> rightR = joinNode.BuildSide == HashJoinBuildSide.Right ? buildRow.Row : probeRow.Row;
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQ, rightR, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightR, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQ, rightR, joinLayout, rightOrdinalMap);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    /// <summary>
    /// Extracts the merge-join key values from a row.
    /// Returns null when any key column is absent or NULL (the row is excluded).
    /// </summary>
    private static ColumnValue[]? ExtractMergeKey(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<string> keyColumns)
    {
        ColumnValue[] key = new ColumnValue[keyColumns.Count];

        for (int i = 0; i < keyColumns.Count; i++)
        {
            if (!row.TryGetValue(keyColumns[i], out ColumnValue? v) || v.Type == ColumnType.Null)
                return null;

            key[i] = v;
        }

        return key;
    }

    /// <summary>
    /// Lexicographic comparison of two parallel key arrays using <see cref="ColumnValue.CompareTo"/>.
    /// Both arrays must have the same length (guaranteed by construction).
    ///
    /// Compares by <c>Type</c> ordinal first: <see cref="ColumnValue.CompareTo"/> throws on mismatched
    /// types, so a cross-type equi-join key (e.g. <c>a.intCol = b.strCol</c>) would otherwise crash the
    /// merge. Ordering by type first makes mismatched types sort deterministically and never
    /// compare-equal — so such a join simply yields no matches, matching the hash-join comparer's
    /// graceful "different type ⇒ not equal" behaviour. (NULL keys are already excluded upstream.)
    /// </summary>
    private static int CompareMergeKeys(ColumnValue[] x, ColumnValue[] y)
    {
        for (int i = 0; i < x.Length; i++)
        {
            int typeCmp = ((int)x[i].Type).CompareTo((int)y[i].Type);
            if (typeCmp != 0) return typeCmp;

            int c = x[i].CompareTo(y[i]);
            if (c != 0) return c;
        }

        return 0;
    }

    /// <summary>
    /// Value-based equality comparer for <see cref="CompositeColumnValue"/> hash table keys.
    ///
    /// Neither <see cref="CompositeColumnValue"/> nor <see cref="ColumnValue"/> overrides
    /// <c>Equals</c>/<c>GetHashCode</c> (both only implement <see cref="IComparable"/>), so
    /// the default dictionary comparer uses reference equality and would match nothing. This
    /// comparer hashes and compares by <c>Type + payload</c>, consistent with
    /// <see cref="ColumnValue.CompareTo"/> for non-NULL values. NULL keys are excluded before
    /// they reach the table so the comparer does not need null-equality semantics.
    /// </summary>
    /// <summary>
    /// Carries the two pieces of mutable state that the index-nested-loop right-side probe
    /// needs to reuse across iterations of the outer left-row loop:
    /// <list type="bullet">
    ///   <item><see cref="DecodeState"/> — the per-probe <see cref="RowEncoder.RowDecodeState"/>,
    ///   keyed by stored schema version; holds the precomputed row-decode plan built by
    ///   <see cref="RowEncoder.DecodeToQueryRowAsync"/> for each version encountered, so the plan
    ///   (layout plus per-column read/skip steps) is built at most once per version instead of on
    ///   every right-row decode.</item>
    ///   <item><see cref="QualifiedLayout"/> — the alias-prefixed layout built from the first
    ///   right <see cref="QueryRow"/>; reused on every subsequent right-row qualify so
    ///   <see cref="QueryRowMerger.BuildQualifiedLayout"/> is called at most once per join node
    ///   rather than once per row.</item>
    /// </list>
    /// One instance is created per <see cref="ExecuteIndexNestedLoopJoin"/> call and passed
    /// through <see cref="ProbeRightIndex"/> into <see cref="LoadRightRow"/>.
    /// </summary>
    private sealed class RightDecodeState
    {
        internal readonly RowEncoder.RowDecodeState DecodeState = new();
        internal RowLayout? QualifiedLayout;
    }

    private sealed class CompositeColumnValueComparer : IEqualityComparer<CompositeColumnValue>
    {
        public static readonly CompositeColumnValueComparer Instance = new();

        public bool Equals(CompositeColumnValue? x, CompositeColumnValue? y)
        {
            if (x is null && y is null) return true;
            if (x is null || y is null) return false;
            return Equals(x.Values, y.Values);
        }

        /// <summary>
        /// Span-based key equality — lets callers compare join keys without wrapping either operand in a
        /// throwaway <see cref="CompositeColumnValue"/>. Identical semantics to the object overload
        /// (type match plus <see cref="ColumnValue.CompareTo"/> == 0 per position).
        /// </summary>
        public bool Equals(ReadOnlySpan<ColumnValue> x, ReadOnlySpan<ColumnValue> y)
        {
            if (x.Length != y.Length) return false;

            for (int i = 0; i < x.Length; i++)
            {
                ColumnValue a = x[i];
                ColumnValue b = y[i];

                if (a.Type != b.Type) return false;
                if (a.CompareTo(b) != 0) return false;
            }

            return true;
        }

        public int GetHashCode(CompositeColumnValue obj) => GetHashCode(obj.Values);

        /// <summary>
        /// Span-based key hash — lets callers (partition routing, probe) hash a just-extracted key array
        /// without allocating a <see cref="CompositeColumnValue"/> wrapper. Byte-identical mixing to the
        /// object overload, so a span-hashed row routes/probes the same as a materialized key.
        /// </summary>
        public int GetHashCode(ReadOnlySpan<ColumnValue> values)
        {
            HashCode h = new();

            foreach (ColumnValue v in values)
            {
                h.Add((int)v.Type);

                switch (v.Type)
                {
                    case ColumnType.String:
                    case ColumnType.Id:
                        h.Add(v.StrValue, StringComparer.Ordinal);
                        break;

                    case ColumnType.Integer64:
                    case ColumnType.Date:
                    case ColumnType.DateTime:
                        h.Add(v.LongValue);
                        break;

                    case ColumnType.Float64:
                        h.Add(v.FloatValue);
                        break;

                    case ColumnType.Float32:
                        h.Add((float)v.FloatValue);
                        break;

                    case ColumnType.Bool:
                        h.Add(v.BoolValue);
                        break;

                    case ColumnType.Bytes:
                        if (v.BytesValue is not null)
                            foreach (byte b in v.BytesValue)
                                h.Add(b);
                        break;

                    case ColumnType.Uuid:
                        // Hash both halves; the low half alone would collide all UUIDs sharing it.
                        h.Add(v.UuidHigh);
                        h.Add(v.LongValue);
                        break;
                }
            }

            return h.ToHashCode();
        }
    }
}
