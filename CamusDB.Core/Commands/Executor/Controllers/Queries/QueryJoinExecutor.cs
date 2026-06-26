
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
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using System.Runtime.CompilerServices;

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

    public QueryJoinExecutor(QueryExecutor queryExecutor, StatisticsManager? stats = null)
    {
        this.queryExecutor = queryExecutor;
        _stats = stats;
        derivedTableExecutor = new DerivedTableExecutor(queryExecutor, this);
    }

    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket)
    {
        JoinQueryPlanner planner = new(_stats);
        QueryPlan plan = planner.GetPlan(database, bound, ticket);

        IAsyncEnumerable<QueryResultRow> cursor = ExecuteJoinTree(plan.Root, plan);

        if (plan.ExecutionFilter is not null)
            cursor = ApplyWhere(cursor, plan.ExecutionFilter, plan);

        return QueryPostScanPipeline.Apply(
            plan.Database,
            ticket,
            cursor,
            queryFilterer,
            querySorter,
            queryAggregator,
            queryProjector,
            queryDistincter,
            queryLimiter);
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
                    ExecuteJoinTree(sortNode.Input, plan), sortNode.OrderBy);
                await foreach (QueryResultRow row in sorted.ConfigureAwait(false))
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

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
                return derivedScanNode.BoundSource.Alias;

            // Left side may be wrapped in a SortNode; delegate to its inner scan.
            case SortNode { Input: not null } sortNode:
                return ResolveLeftAlias(sortNode.Input, leftRow);

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

    private async IAsyncEnumerable<QueryResultRow> ScanDerivedTable(
        BoundDerivedTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        if (!plan.DerivedMaterializations.TryGetValue(source, out List<Dictionary<string, ColumnValue>>? rows))
        {
            rows = await derivedTableExecutor
                .MaterializeAsync(plan.Database, source, plan.Ticket, executionFilter)
                .ConfigureAwait(false);
            plan.DerivedMaterializations[source] = rows;
        }

        foreach (Dictionary<string, ColumnValue> row in rows)
            yield return new QueryResultRow(default(ObjectIdValue), row);
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
    /// <c>HashJoinMaxBuildRows</c> rows; the nested-loop fallback then re-scans the
    /// build side from scratch, so the build table is read up to ~2× in that rare path.
    /// This is intentional: correctness over complexity. Disk spilling is out of scope.
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
            // Build cap exceeded — fall back to nested-loop for correctness.
            // The nested-loop will re-scan the build side from scratch (~2× cost on this rare path).
            NestedLoopJoinNode fallback = new(joinNode.Input!, joinNode.BuildSource, joinNode.OnPredicate)
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

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
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

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
    }

    // ── Merge Join ────────────────────────────────────────────────────────────

    /// <summary>
    /// Materialises both sides, sorts them on the equi-join key(s), then walks two pointers
    /// in lockstep. On equal keys, buffers the full right-side run of matching rows and emits
    /// the cross-product with every left row in the equal-key group (one-to-many correctness).
    /// Residual <see cref="MergeJoinNode.OnPredicate"/> is applied per emitted pair.
    ///
    /// NULL keys are excluded from both sides — consistent with SQL inner-join semantics.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> ExecuteMergeJoin(
        MergeJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
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
            // Stream the right physical node (SortNode or ForcedIndex scan) — already ordered.
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

                    if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
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
