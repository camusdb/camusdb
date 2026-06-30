
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes inner joins via nested-loop iteration over bound table sources.
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
    private readonly PlanCache? _planCache;

    public QueryJoinExecutor(QueryExecutor queryExecutor, StatisticsManager? stats = null, PlanCache? planCache = null)
    {
        this.queryExecutor = queryExecutor;
        _stats = stats;
        _planCache = planCache;
        derivedTableExecutor = new DerivedTableExecutor(queryExecutor, this);
    }

    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket)
    {
        JoinQueryPlanner planner = new(_stats, _planCache);
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
            queryLimiter);

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
                IAsyncEnumerable<QueryResultRow> sorted = querySorter.SortByKeys(
                    ExecuteJoinTree(sortNode.Input!, plan), sortNode.OrderBy);
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

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> leftQualified = QueryRowMerger.QualifyRow(
                leftRow.Row,
                ResolveLeftAlias(joinNode.Input!, leftRow));

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
                plan).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(
                    leftQualified,
                    rightRow.Row,
                    rightAlias);

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

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> leftQualified = QueryRowMerger.QualifyRow(
                leftRow.Row,
                ResolveLeftAlias(joinNode.Input!, leftRow));

            await foreach (QueryResultRow rightRow in ScanJoinRightSource(
                joinNode.RightSource,
                joinNode.RightExecutionFilter,
                plan).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(
                    leftQualified,
                    rightRow.Row,
                    rightAlias);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ProbeRightIndex(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        if (joinNode.UseUniqueLookup)
        {
            await foreach (QueryResultRow row in LookupUniqueRightRow(joinNode, lookupKey, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        await foreach (QueryResultRow row in ScanMultiIndexRightRows(joinNode, lookupKey, plan).ConfigureAwait(false))
            yield return row;
    }

    private async IAsyncEnumerable<QueryResultRow> LookupUniqueRightRow(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;

        ObjectIdValue? rowId = await table.Store.LookupUnique(plan.Ticket.TxnState, joinNode.Index.Name, lookupKey).ConfigureAwait(false);

        if (rowId is null)
            yield break;

        QueryResultRow? row = await LoadRightRow(source, rowId.Value, joinNode.RightExecutionFilter, plan).ConfigureAwait(false);

        if (row is QueryResultRow loadedRow)
            yield return loadedRow;
    }

    private async IAsyncEnumerable<QueryResultRow> ScanMultiIndexRightRows(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, joinNode.Index);
        ColumnValue lookupValue = lookupKey.Values[0];

        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            joinNode.Index.Name,
            keyTypes,
            lookupKey,
            to: null,
            unique: false).ConfigureAwait(false))
        {
            if (key.Values[0].CompareTo(lookupValue) > 0)
                break;

            if (key.Values[0].CompareTo(lookupValue) != 0)
                continue;

            QueryResultRow? row = await LoadRightRow(source, rowId, joinNode.RightExecutionFilter, plan).ConfigureAwait(false);

            if (row is QueryResultRow loadedRow)
                yield return loadedRow;
        }
    }

    private async Task<QueryResultRow?> LoadRightRow(
        BoundTableSource source,
        ObjectIdValue rowId,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        byte[]? data = await source.Table.Store.GetRow(plan.Ticket.TxnState, rowId).ConfigureAwait(false);

        if (data is null || data.Length == 0)
            return null;

        Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
            source.Table.Schema,
            plan.Ticket.TxnState.TransactionId,
            rowId,
            data,
            GetRequiredColumnsForAlias(plan, source.Alias),
            GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

        if (executionFilter is not null)
        {
            Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);

            if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                return null;
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

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(plan.Ticket.TxnState).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);

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

        bool unique = index.Type == IndexType.Unique;

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            index.Name,
            keyTypes,
            from: null, to: null, unique: unique).ConfigureAwait(false))
        {
            byte[]? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId).ConfigureAwait(false);

            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);

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

        bool unique = rangeNode.Index.Type == IndexType.Unique;

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            rangeNode.Index.Name,
            keyTypes,
            from: rangeNode.FromBound,
            to: rangeNode.ToBound,
            fromInclusive: rangeNode.FromInclusive,
            toInclusive: rangeNode.ToInclusive,
            unique: unique).ConfigureAwait(false))
        {
            byte[]? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId).ConfigureAwait(false);

            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

            if (rangeNode.ExecutionFilter is not null)
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);
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

        foreach (ColumnValue value in inListNode.Values)
        {
            CompositeColumnValue lookupKey = new(new[] { value });

            if (isUnique)
            {
                ObjectIdValue? rowId = await table.Store.LookupUnique(
                    plan.Ticket.TxnState, inListNode.Index.Name, lookupKey).ConfigureAwait(false);

                if (rowId is null || !seen.Add(rowId.Value))
                    continue;

                byte[]? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId.Value).ConfigureAwait(false);
                if (data is null || data.Length == 0)
                    continue;

                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                    table.Schema, txId, rowId.Value, data,
                    required,
                    GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

                if (inListNode.ExecutionFilter is not null)
                {
                    Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);
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

                await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
                    plan.Ticket.TxnState, inListNode.Index.Name, keyTypes,
                    lookupKey, toBound, unique: false,
                    fromInclusive: true, toInclusive: toInclusive,
                    maxRows: null).ConfigureAwait(false))
                {
                    if (!seen.Add(rowId))
                        continue;

                    byte[]? data = await table.Store.GetRow(plan.Ticket.TxnState, rowId).ConfigureAwait(false);
                    if (data is null || data.Length == 0)
                        continue;

                    Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                        table.Schema, txId, rowId, data,
                        required,
                        GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

                    if (inListNode.ExecutionFilter is not null)
                    {
                        Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);
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
        TableColumnSchema? column = table.Schema.Columns?.Find(c => c.Name == lastColumn);
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
            TableColumnSchema? col = table.Schema.Columns?.Find(c => c.Name == colName);
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }

    // ── Hash Join ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Materialises the build side into a hash table keyed on the equi-join columns.
    /// Returns null when the build exceeds <see cref="CamusDBConfig.HashJoinMaxBuildRows"/>
    /// (signals the caller to fall back to nested-loop).
    /// Rows whose join key contains any NULL value are excluded.
    ///
    /// Note: on cap overflow the build scan is abandoned after reading up to
    /// <c>HashJoinMaxBuildRows</c> rows; the fallback (Grace hash join or nested-loop) then
    /// re-scans the build side from scratch (~2× cost on this rare path). This is intentional:
    /// correctness over complexity. When <see cref="CamusDBConfig.SpillEnabled"/> is <c>true</c>,
    /// the caller routes the overflow to <see cref="GraceHashJoinAsync"/> instead of nested-loop.
    /// </summary>
    /// <summary>
    /// Materialises the build side into a hash table keyed on the equi-join columns.
    /// Returns null when the build exceeds <see cref="CamusDBConfig.HashJoinMaxBuildRows"/>
    /// (signals the caller to fall back to nested-loop).
    /// Rows whose join key contains any NULL value are excluded.
    ///
    /// Note: on cap overflow the build scan is abandoned after reading up to
    /// <c>HashJoinMaxBuildRows</c> rows; the nested-loop fallback then re-scans the
    /// build side from scratch, so the build table is read up to ~2× in that rare path.
    /// This is intentional: correctness over complexity. Disk spilling is out of scope.
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
    private async Task<Dictionary<CompositeColumnValue, List<Dictionary<string, ColumnValue>>>?> BuildHashTable(
        HashJoinNode joinNode,
        QueryPlan plan,
        HashJoinBuildSide buildSide)
    {
        Dictionary<CompositeColumnValue, List<Dictionary<string, ColumnValue>>> table =
            new(CompositeColumnValueComparer.Instance);

        int rowCount = 0;

        if (buildSide == HashJoinBuildSide.Right)
        {
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;

            await foreach (QueryResultRow row in ScanJoinRightSource(
                joinNode.BuildSource, joinNode.BuildExecutionFilter, plan).ConfigureAwait(false))
            {
                if (rowCount >= CamusDBConfig.HashJoinMaxBuildRows) return null;

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
                if (!table.TryGetValue(key, out List<Dictionary<string, ColumnValue>>? bucket))
                { bucket = []; table[key] = bucket; }

                bucket.Add(row.Row);
                rowCount++;
            }
        }
        else
        {
            // BuildSide.Left: materialise the left subtree; rows are qualified immediately so
            // they can be passed directly as the "left" arg to MergeRows during probe.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;

            await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
            {
                if (rowCount >= CamusDBConfig.HashJoinMaxBuildRows) return null;

                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(
                    leftRow.Row, ResolveLeftAlias(joinNode.Input!, leftRow));

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
                if (!table.TryGetValue(key, out List<Dictionary<string, ColumnValue>>? bucket))
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

        Dictionary<CompositeColumnValue, List<Dictionary<string, ColumnValue>>>? hashTable =
            await BuildHashTable(joinNode, plan, buildSide).ConfigureAwait(false);

        if (hashTable is null)
        {
            // Build cap exceeded. When spilling is enabled, route to Grace/hybrid hash join
            // so large builds are partitioned to disk instead of falling back to O(n·m) NLJ.
            // Both paths re-scan the build side from scratch (~2× cost on this rare path).
            if (CamusDBConfig.SpillEnabled)
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

        if (buildSide == HashJoinBuildSide.Right)
        {
            // Standard path: probe = left subtree, build = right source.
            // Hash table rows are unqualified; MergeRows qualifies them with rightAlias.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;

            await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> leftQualified = QueryRowMerger.QualifyRow(
                    leftRow.Row,
                    ResolveLeftAlias(joinNode.Input!, leftRow));

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
                if (!hashTable.TryGetValue(probeKey, out List<Dictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (Dictionary<string, ColumnValue> buildRow in bucket)
                {
                    Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(leftQualified, buildRow, rightAlias);

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
        else
        {
            // Build-left path: build = left subtree (stored qualified in hash table),
            // probe = right source (scanned unqualified).
            // MergeRows(leftQualifiedBuildRow, rightUnqualifiedProbeRow, rightAlias) is the
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
                if (!hashTable.TryGetValue(probeKey, out List<Dictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (Dictionary<string, ColumnValue> leftBuildRow in bucket)
                {
                    Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(leftBuildRow, rightRow.Row, rightAlias);

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
        List<(ColumnValue[] Key, Dictionary<string, ColumnValue> QualifiedRow)> leftRows = new();

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(
                leftRow.Row, ResolveLeftAlias(joinNode.Input!, leftRow));

            ColumnValue[]? key = ExtractMergeKey(qualified, joinNode.LeftKeyColumns);
            if (key is null) continue;

            leftRows.Add((key, qualified));
        }

        // Sort only when not already ordered by the plan (via SortNode or ForcedIndex scan).
        if (!joinNode.LeftIsOrdered)
            leftRows.Sort((a, b) => CompareMergeKeys(a.Key, b.Key));

        // ── Materialise right side (unqualified; MergeRows qualifies at emit time) ──
        List<(ColumnValue[] Key, Dictionary<string, ColumnValue> Row)> rightRows = new();

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
                    Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(
                        leftRows[l].QualifiedRow, rightRows[r].Row, rightAlias);

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
            Dictionary<string, ColumnValue> leftQualified =
                QueryRowMerger.QualifyRow(leftEnum.Current.Row, leftAlias);
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
            List<Dictionary<string, ColumnValue>> rightRun = [];
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
                foreach (Dictionary<string, ColumnValue> rightRow in rightRun)
                {
                    Dictionary<string, ColumnValue> merged =
                        QueryRowMerger.MergeRows(leftQualified, rightRow, rightAlias);

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                leftHasMore = await leftEnum.MoveNextAsync().ConfigureAwait(false);
                if (!leftHasMore) break;

                string la = ResolveLeftAlias(joinNode.Input!, leftEnum.Current);
                leftQualified = QueryRowMerger.QualifyRow(leftEnum.Current.Row, la);
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
        await foreach (QueryResultRow row in ExecuteJoinTree(inputNode, plan).WithCancellation(ct).ConfigureAwait(false))
        {
            string alias = ResolveLeftAlias(inputNode, row);
            Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row.Row, alias);
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
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, ct).ConfigureAwait(false);
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
        uint h = (uint)CompositeColumnValueComparer.Instance.GetHashCode(new CompositeColumnValue(keyValues));
        h ^= (uint)seed * 0x9E3779B1u;   // fold level seed in with a multiplier
        h *= 0x85EBCA6Bu; h ^= h >> 13;  // murmur-style bit-mixing finalizer
        h *= 0xC2B2AE35u; h ^= h >> 16;
        return (int)(h % (uint)partitionCount);
    }

    /// <summary>
    /// Grace/hybrid hash join entry point. Partitions both build and probe sides into
    /// <see cref="CamusDBConfig.SpillMergeFanIn"/> spill files each (keyed by join-key hash),
    /// then joins each partition pair. Partitions whose build side exceeds
    /// <see cref="CamusDBConfig.SpillEffectiveThreshold"/> are recursively re-partitioned up to
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
        int K = CamusDBConfig.SpillMergeFanIn;

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

        SpillScope scope = SpillFileManager.CreateScope(CamusDBConfig.DataDirectory);

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
    /// and streaming the probe partition against it. When the build partition count exceeds the
    /// threshold and the recursion depth allows it, both files are re-read with a perturbed hash
    /// seed to split into sub-partitions. When the depth limit is reached (extreme skew where a
    /// single key dominates and cannot be split further), all remaining build rows are loaded
    /// regardless of the threshold — correctness is preserved at the cost of a larger in-memory
    /// footprint for that partition.
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
        int threshold = CamusDBConfig.SpillEffectiveThreshold;
        int K = CamusDBConfig.SpillMergeFanIn;
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.BuildSource.Alias;

        // Load build partition into an in-memory hash table, stopping early when the threshold
        // is reached and recursion is still possible. The partial load is discarded and both
        // files are re-read cleanly for repartitioning.
        Dictionary<CompositeColumnValue, List<Dictionary<string, ColumnValue>>> hashTable =
            new(CompositeColumnValueComparer.Instance);
        int buildCount = 0;
        bool overflow = false;

        await using IAsyncEnumerator<QueryResultRow> buildEnum =
            ReadSpillFileAsync(buildFile, ct).GetAsyncEnumerator(ct);

        while (await buildEnum.MoveNextAsync().ConfigureAwait(false))
        {
            QueryResultRow buildRow = buildEnum.Current;
            ColumnValue[]? keyVals = ExtractMergeKey(buildRow.Row, buildKeyColumns);
            if (keyVals is null) continue;

            if (buildCount >= threshold && depth < MaxGraceHashDepth)
            {
                overflow = true;
                break;
            }

            CompositeColumnValue key = new(keyVals);
            if (!hashTable.TryGetValue(key, out List<Dictionary<string, ColumnValue>>? bucket))
            { bucket = []; hashTable[key] = bucket; }
            bucket.Add(buildRow.Row);
            buildCount++;
        }

        if (overflow)
        {
            // Re-read both files with a new hash seed to distribute rows across K sub-partitions.
            int newSeed = seed + 1;
            string[] subBuildFiles = await PartitionStreamToFilesAsync(
                scope, K, newSeed, ReadSpillFileAsync(buildFile, ct), buildKeyColumns, ct).ConfigureAwait(false);
            string[] subProbeFiles = await PartitionStreamToFilesAsync(
                scope, K, newSeed, ReadSpillFileAsync(probeFile, ct), probeKeyColumns, ct).ConfigureAwait(false);

            for (int p = 0; p < K; p++)
            {
                await foreach (QueryResultRow row in JoinPartitionAsync(
                    joinNode, plan, scope, subBuildFiles[p], subProbeFiles[p],
                    buildKeyColumns, probeKeyColumns, newSeed, depth + 1, ct).ConfigureAwait(false))
                    yield return row;
            }
            yield break;
        }

        if (hashTable.Count == 0) yield break;

        // Probe phase: stream probe partition against the loaded hash table.
        await foreach (QueryResultRow probeRow in ReadSpillFileAsync(probeFile, ct).ConfigureAwait(false))
        {
            ColumnValue[]? probeKeyVals = ExtractMergeKey(probeRow.Row, probeKeyColumns);
            if (probeKeyVals is null) continue;

            CompositeColumnValue probeKey = new(probeKeyVals);
            if (!hashTable.TryGetValue(probeKey, out List<Dictionary<string, ColumnValue>>? bucket)) continue;

            foreach (Dictionary<string, ColumnValue> buildRow in bucket)
            {
                // Build rows are always the qualified left-side; probe rows are always the
                // unqualified right-side — regardless of which physical side is BuildSide.
                Dictionary<string, ColumnValue> merged = joinNode.BuildSide == HashJoinBuildSide.Right
                    ? QueryRowMerger.MergeRows(probeRow.Row, buildRow, rightAlias)
                    : QueryRowMerger.MergeRows(buildRow, probeRow.Row, rightAlias);

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
    private sealed class CompositeColumnValueComparer : IEqualityComparer<CompositeColumnValue>
    {
        public static readonly CompositeColumnValueComparer Instance = new();

        public bool Equals(CompositeColumnValue? x, CompositeColumnValue? y)
        {
            if (x is null && y is null) return true;
            if (x is null || y is null) return false;
            if (x.Values.Length != y.Values.Length) return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                ColumnValue a = x.Values[i];
                ColumnValue b = y.Values[i];

                if (a.Type != b.Type) return false;
                if (a.CompareTo(b) != 0) return false;
            }

            return true;
        }

        public int GetHashCode(CompositeColumnValue obj)
        {
            HashCode h = new();

            foreach (ColumnValue v in obj.Values)
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
                }
            }

            return h.ToHashCode();
        }
    }
}
