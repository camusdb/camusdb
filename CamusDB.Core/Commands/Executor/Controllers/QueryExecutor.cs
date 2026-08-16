
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using Kahuna;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly ILogger<ICamusDB> logger;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>Configuration of the engine this executor belongs to.</summary>
    internal CamusDBOptions Options => options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot and forwards it to the planners that
    /// captured their own copy at construction. Each query pins the planner's snapshot once when
    /// it plans, so an in-flight query keeps the configuration it started with and the new value
    /// takes effect for the next query.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        options = next;
        queryPlanner.ApplyOptions(next);
        queryJoinExecutor.ApplyOptions(next);

        // The plan cache latched its cap at construction; re-cap it (trimming immediately when the
        // cap shrank) so a runtime change bounds memory now, not only on future inserts.
        PlanCache.SetMaxEntries(next.PlanCacheMaxEntries);
    }

    private readonly QueryPlanner queryPlanner;

    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator;

    private readonly QueryProjector queryProjector = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryDistincter queryDistincter = new();

    private readonly SemiJoinExecutor semiJoinExecutor = new();

    private readonly QueryJoinExecutor queryJoinExecutor;

    private readonly QueryScanner queryScanner;

    internal readonly PlanCache PlanCache;

    /// <summary>
    /// Kahuna instance used by the strict-validation path to probe point and range deps.
    /// Null when running in test contexts that do not inject a shared node; strict validation
    /// fails closed (evicts the entry, serves live rows) when this is null.
    /// </summary>
    private readonly IKahuna? _kahuna;

    public QueryExecutor(ILogger<ICamusDB> logger, CamusDBOptions options, StatisticsManager? stats = null, IKahuna? kahuna = null)
    {
        this.logger = logger;
        this.options = options;
        _kahuna = kahuna;
        PlanCache = new PlanCache(options.PlanCacheMaxEntries);
        queryPlanner = new QueryPlanner(stats, PlanCache, options);
        queryAggregator = new QueryAggregator(stats);
        queryJoinExecutor = new QueryJoinExecutor(this, options, stats, PlanCache);
        this.queryScanner = new(logger);
    }

    /// <param name="metaOut">
    /// Optional holder populated with cache resolution metadata after the returned cursor is
    /// fully drained. Pass a <see cref="CacheMetadataHolder"/> instance when the caller needs
    /// to surface cache status in the response envelope; pass <c>null</c> for internal callers
    /// (DML, DDL, subquery) that do not need the metadata.
    /// </param>
    public IAsyncEnumerable<QueryResultRow> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket, CacheMetadataHolder? metaOut = null)
    {
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        // Only autocommit reads are cache-eligible (IsReadOnly ensures no writes). Two
        // kinds are admitted:
        //
        // 1. Synthetic zero-snapshot reads (TransactionId == Zero): CreateReadOnly() and
        //    CreateSnapshotReadOnly(T). These have no Kahuna identity and read at the latest
        //    committed value per key — the standard single-node / hash-mode fast path.
        //
        // 2. Promoted single-statement autocommit reads (TransactionId != Zero, but
        //    ReadTimestamp == Zero): BeginReadOnlyAsync(promote: true) under key-range
        //    sharding. These mint a real Kahuna transaction identity so a shared range lock
        //    can be held for phantom-free reads, but they do not pin a read snapshot
        //    (ReadTimestamp stays Zero). Semantically they are equivalent to a zero-snapshot
        //    autocommit read for caching: both read at the current latest committed state,
        //    and both are single-statement (no cross-statement snapshot to protect).
        //    Admitting them keeps the result cache effective in key-range sharding mode;
        //    excluding them would silently disable caching for every HTTP autocommit SELECT
        //    in that mode.
        //
        // Excluded: explicit Serializable RO transactions (TransactionId != Zero AND
        //    ReadTimestamp != Zero), opened via BeginAsync(Serializable, ReadOnly). These
        //    pin a specific read snapshot; serving a cached result from a different snapshot
        //    would violate their isolation guarantee.
        if (ticket.CacheHint is { } hint
                && ticket.TxnState.IsReadOnly
                && (ticket.TxnState.TransactionId == HLCTimestamp.Zero
                    || ticket.TxnState.ReadTimestamp == HLCTimestamp.Zero))
        {
            if (HasVolatileFunction(ticket))
            {
                if (metaOut is not null)
                {
                    metaOut.Status = QueryCacheStatus.Bypass;
                    metaOut.BypassReason = QueryCacheBypassReason.NonDeterministic;
                    metaOut.CacheName = hint.CacheName;
                }
                return ExecuteQueryPlanInternal(plan);
            }

            if (database.Cache is { } cache)
                return QueryWithCache(database, plan, hint, cache, metaOut);

            // Cache is disabled (database.Cache == null). Report CacheDisabled so the client
            // can distinguish "hint ignored due to config" from a non-hinted query where
            // cacheName is absent entirely.
            if (metaOut is not null)
            {
                metaOut.Status = QueryCacheStatus.Bypass;
                metaOut.BypassReason = QueryCacheBypassReason.CacheDisabled;
                metaOut.CacheName = hint.CacheName;
            }
        }
        else if (ticket.CacheHint is { CacheName: { } txCacheName } && metaOut is not null)
        {
            // The hint was present but the gate rejected the transaction: either a write
            // transaction (IsReadOnly == false) or an explicit Serializable RO transaction
            // with a pinned read snapshot (TransactionId != Zero && ReadTimestamp != Zero).
            // Execute live without caching and surface the bypass so the caller can
            // distinguish "hint seen but ineligible" from "query had no hint at all."
            metaOut.CacheName = txCacheName;
            metaOut.Status = QueryCacheStatus.Bypass;
            metaOut.BypassReason = QueryCacheBypassReason.InTransaction;
        }

        return ExecuteQueryPlanInternal(plan);
    }

    /// <summary>
    /// Executes a cached-read path for autocommit SELECTs with a <c>{cache=name}</c> hint.
    /// Admitted are synthetic zero-snapshot reads (<see cref="KvTransaction.TransactionId"/> ==
    /// <see cref="HLCTimestamp.Zero"/>) and promoted single-statement autocommit reads
    /// (<see cref="KvTransaction.TransactionId"/> != Zero but <see cref="KvTransaction.ReadTimestamp"/>
    /// == Zero). Excluded are explicit Serializable RO transactions, which have a pinned read
    /// snapshot (<see cref="KvTransaction.ReadTimestamp"/> != Zero); serving them a cached result
    /// from a different snapshot would violate their isolation guarantee.
    ///
    /// <para><b>Single-table only.</b> The generation fence snapshots only the primary table's row
    /// keyspace. Multi-source (join) plans must never be routed here — they require all involved
    /// tables' row keyspaces to be fenced, or a write to a non-primary table would go undetected.
    /// This invariant is enforced by routing <c>IsMultiSource</c> queries to
    /// <c>QueryJoinExecutor.ExecuteJoinQuery</c> before this path is reachable (see
    /// <c>CommandExecutor.ExecuteSQLQuery</c>).</para>
    ///
    /// <para>On a cache hit the stored rows are yielded immediately. On a miss, the real plan
    /// executes while a <see cref="QueryDependencyCollector"/> captures range/schema deps; after the
    /// drain the entry is published through the generation fence (see <see cref="CachePublishGate"/>).
    /// If a write is in-flight for the table's row keyspace the live rows are served without
    /// publishing to avoid a stale-hit window.</para>
    ///
    /// <para><b>Single-flight is a completion notification only.</b> Concurrent callers for the same
    /// fingerprint wait for the owner and then re-run the same probe every other reader runs; they
    /// never receive the owner's materialized rows. Otherwise a write committing between the owner's
    /// publish and its signal would leave a waiter — a query that began after that invalidation —
    /// serving rows older than a committed write, and skipping strict/schema validation while doing
    /// it. A waiter whose re-probe misses simply executes live.</para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> QueryWithCache(
        DatabaseDescriptor database,
        QueryPlan plan,
        CacheHintOptions hint,
        IQueryResultCache cache,
        CacheMetadataHolder? metaOut = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Belt-and-suspenders: the dispatch in CommandExecutor ensures this is never called for
        // multi-source plans, but guard here so a future misrouting fails loudly.
        System.Diagnostics.Debug.Assert(
            plan.Table is not null,
            "QueryWithCache must only be called for single-table plans; join plans bypass the cache.");

        string fingerprint = ResultFingerprintBuilder.Build(
            database.Id, hint.CacheName, plan.QueryShapeId,
            plan.Ticket.Parameters, plan.SchemaDeps, hint);

        // Tracks that a strict entry was found but evicted as stale; live re-execution follows.
        // Used to override the final status to StaleRevalidated rather than Miss.
        bool wasStaleRevalidated = false;

        // ── Cache probe ───────────────────────────────────────────────────────
        // Fetch entry plus its deps, validate them, and return the entry only if it is still
        // serveable. A stale entry is evicted here so the next probe skips re-validation, and the
        // caller falls through to live execution.
        //
        // This runs both on the initial probe and again for a single-flight waiter that the owner
        // woke: a waiter must reach a served entry through exactly this path — same eviction, same
        // strict/schema validation — never through a result object handed over by the owner, which
        // could already have been invalidated by a committed write.
        async ValueTask<CachedQueryResult?> ProbeCacheAsync()
        {
            (CachedQueryResult Result, QueryDependencySet Deps)? hitWithDeps =
                await cache.TryGetWithDepsAsync(database.Id, hint.CacheName, fingerprint, ct).ConfigureAwait(false);

            if (hitWithDeps is not { } hd)
                return null;

            // Strict: validate every dep against current KV state, failing closed when Kahuna is
            // unavailable or the probe limit is exceeded. Non-strict: validate schema deps in-memory,
            // a cheap lock-free check that closes the DDL publish window — an in-flight miss that
            // published after a DROP/ALTER invalidation is caught on the very next probe rather than
            // replayed until TTL.
            bool valid = hd.Result.HintIsStrict
                ? _kahuna is not null
                    && await StrictValidator.ValidateAsync(hd.Result, hd.Deps, database, _kahuna, ct).ConfigureAwait(false)
                : SchemaDepsCurrent(hd.Deps, database);

            if (valid)
                return hd.Result;

            cache.InvalidateEntry(database.Id, hd.Result.CacheName, fingerprint);
            wasStaleRevalidated = true;
            return null;
        }

        void RecordHitMetadata(CachedQueryResult served)
        {
            if (metaOut is null)
                return;

            metaOut.Status = QueryCacheStatus.Hit;
            metaOut.CacheName = hint.CacheName;
            metaOut.CachedAtHlc = served.CachedAt;
            metaOut.AgeMs = served.CreatedAtMs == 0
                ? null : Environment.TickCount64 - served.CreatedAtMs;
        }

        CachedQueryResult? hit = await ProbeCacheAsync().ConfigureAwait(false);
        if (hit is not null)
        {
            RecordHitMetadata(hit);
            foreach (QueryResultRow row in hit.Rows)
                yield return row;
            yield break;
        }

        // ── Strict-with-no-snapshot bypass ────────────────────────────────────
        // For strict entries, T_cache must be a real HLC snapshot so that
        // LastModified comparisons are meaningful. CreateReadOnly() uses
        // HLCTimestamp.Zero as the read timestamp — storing a strict entry at Zero
        // would make validation always fail (all rows have LastModified > Zero).
        // Serve live rows without populating the cache in this case.
        bool canPublishStrict = !hint.IsStrict
            || plan.Ticket.TxnState.ReadTimestamp != HLCTimestamp.Zero;

        if (!canPublishStrict)
        {
            if (metaOut is not null)
            {
                metaOut.Status = QueryCacheStatus.Bypass;
                metaOut.CacheName = hint.CacheName;
            }
            await foreach (QueryResultRow row in ExecuteQueryPlanInternal(plan).WithCancellation(ct).ConfigureAwait(false))
                yield return row;
            yield break;
        }

        // ── Single-flight gate ────────────────────────────────────────────────
        // If another concurrent request is already computing this fingerprint, block until it
        // finishes and then take its published entry from the cache — avoiding redundant plan
        // execution under a thundering-herd cold-cache burst. If the wait times out, the owner
        // fails, or the published entry is already gone, fall through and execute independently.
        //
        // The gate is entered BEFORE SnapshotGenerations so a waiter that ends up serving a cached
        // entry never needs to take a generation snapshot. The owner takes the snapshot and executes
        // normally in the code that follows.
        SingleFlightSlot sfSlot = cache.EnterSingleFlight(fingerprint);
        bool isSingleFlightOwner = sfSlot.IsOwner;

        if (!isSingleFlightOwner)
        {
            // We are a waiter. Block until the owner signals completion or the timeout elapses.
            bool ownerPublished = await sfSlot.WaitAsync(
                options.QueryResultCacheSingleFlightWaitMs, ct).ConfigureAwait(false);

            if (ownerPublished)
            {
                // The owner stored an entry — but between its store and this wakeup a write may have
                // committed and evicted it, and this query began after that invalidation. So re-probe
                // the cache rather than trusting the owner's materialized rows: serve only what the
                // probe still finds and validates, and execute live otherwise.
                CachedQueryResult? sfHit = await ProbeCacheAsync().ConfigureAwait(false);

                if (sfHit is not null)
                {
                    RecordHitMetadata(sfHit);
                    foreach (QueryResultRow row in sfHit.Rows)
                        yield return row;
                    yield break;
                }
            }

            // Timeout, owner failure, or an entry that no longer exists: execute independently. We
            // are not the single-flight owner, so we must NOT call ExitSingleFlight. The owner will
            // signal when done.
        }

        // ── Owner liveness guard ──────────────────────────────────────────────
        // The code from here to the inner try/finally (SnapshotGenerations, HasInFlightWrite,
        // runner setup) is in-memory and rarely throws, but if it does and we are the owner,
        // the TCS stays in _inFlight permanently — every future request for this fingerprint
        // becomes a waiter and blocks for SingleFlightWaitMs on a dead TCS. The outer
        // try/finally below is the safety net: ExitSingleFlight is idempotent (the second call
        // from the inner finally is a no-op because _inFlight.Remove returns false), so
        // composing both is always correct.
        try
        {

        // ── Generation snapshot ───────────────────────────────────────────────
        // Snapshots the row keyspace for the single table in the plan. Every INSERT/UPDATE/DELETE
        // on this table bumps this bucket's generation, so the fence catches any concurrent write
        // during the query's execution — including writes that go through a secondary index scan.
        string rowBucket = plan.Table.Store.RowKeySpace;
        string[] keyspaces = [rowBucket];

        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations(keyspaces);
        if (cache.PublishGate.HasInFlightWrite(keyspaces))
        {
            // An in-flight write is detected: serve live rows without caching. If we are the
            // single-flight owner, wake waiters immediately with null so they execute
            // independently rather than blocking until our timeout.
            if (isSingleFlightOwner)
                cache.ExitSingleFlight(fingerprint, published: false);

            if (metaOut is not null)
            {
                metaOut.Status = QueryCacheStatus.Bypass;
                metaOut.BypassReason = QueryCacheBypassReason.InFlightWrite;
                metaOut.CacheName = hint.CacheName;
            }
            await foreach (QueryResultRow row in ExecuteQueryPlanInternal(plan).WithCancellation(ct).ConfigureAwait(false))
                yield return row;
            yield break;
        }

        var depCollector = new QueryDependencyCollector(options);
        plan.DepCollector = depCollector;

        CachedQueryResult pending = new(
            CacheName: hint.CacheName,
            DatabaseId: database.Id,
            Rows: [],
            ResultFingerprint: fingerprint,
            CachedAt: plan.Ticket.TxnState.ReadTimestamp,
            Status: QueryCacheStatus.Miss,
            HintTtlMs: hint.TtlMs,
            HintIsStrict: hint.IsStrict);

        CachedQueryRunner runner = new(cache, pending, token, options, ct);
        runner.DepCollector = depCollector;

        try
        {
            await foreach (QueryResultRow row in runner.DrainAsync(ExecuteQueryPlanInternal(plan)).WithCancellation(ct).ConfigureAwait(false))
                yield return row;
        }
        finally
        {
            (QueryCacheStatus finalStatus, QueryCacheBypassReason finalReason) = await runner.FinalizeAsync().ConfigureAwait(false);

            if (isSingleFlightOwner)
            {
                // Signal waiters: true when an entry was stored, so they re-probe the cache instead
                // of duplicating this execution; false on any failure (generation fence rejected,
                // byte cap exceeded, drain cancelled) so they execute independently. Only the flag
                // crosses over — the rows a waiter serves must come from its own validated probe.
                cache.ExitSingleFlight(fingerprint, published: finalStatus == QueryCacheStatus.Miss);
            }

            if (metaOut is not null)
            {
                // A stale strict hit that required live re-execution maps to StaleRevalidated
                // rather than Miss/EvictedBeforePublish so the caller can distinguish "fresh miss"
                // from "revalidation that could not re-publish". Bypass means the drain itself
                // failed, in which case rows were never served and the revalidation signal is moot.
                metaOut.Status = wasStaleRevalidated && finalStatus != QueryCacheStatus.Bypass
                    ? QueryCacheStatus.StaleRevalidated
                    : finalStatus;
                metaOut.BypassReason = finalReason;
                metaOut.CacheName = hint.CacheName;
            }
        }

        } // end outer try
        finally
        {
            // Safety net: if an exception escaped the inner finally without calling
            // ExitSingleFlight (e.g. a throw in SnapshotGenerations or runner setup), and
            // we are the owner, wake waiters with null so they execute independently.
            // No-op when ExitSingleFlight was already called (idempotent _inFlight.Remove).
            if (isSingleFlightOwner)
                cache.ExitSingleFlight(fingerprint, published: false);
        }
    }

    /// <summary>
    /// Executes a pre-built <see cref="QueryPlan"/> directly.
    /// Used by EXPLAIN ANALYZE to run a plan that already has <see cref="QueryPlan.CollectRuntimeStats"/> set.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> ExecuteQueryPlan(QueryPlan plan)
        => ExecuteQueryPlanInternal(plan);

    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket) =>
        queryJoinExecutor.ExecuteJoinQuery(database, bound, ticket);

    private IAsyncEnumerable<QueryResultRow> ExecuteQueryPlanInternal(QueryPlan plan)
    {
        if (plan.Root is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Query plan root is null");

        bool collectStats = plan.CollectRuntimeStats;
        if (collectStats)
        {
            foreach (PhysicalPlanNode stepNode in plan.StepNodes)
                stepNode.Stats = new();
        }

        IAsyncEnumerable<QueryResultRow> cursor = ExecutePlanNode(plan, plan.Root, collectStats);
        plan.DataCursor = cursor;
        return cursor;
    }

    /// <summary>
    /// Recursively composes the execution cursor for a single-table physical plan subtree.
    /// This walks <see cref="QueryPlan.Root"/> directly (the same tree the join executor and
    /// EXPLAIN consume) instead of the flattened <see cref="QueryPlan.Steps"/> list, so operators
    /// that exist only as tree nodes (e.g. future exchange/gather operators) can execute without
    /// a step-list encoding. <see cref="FilterNode"/> produces no operator of its own: row
    /// filtering runs inside the scan via <see cref="QueryPlan.ExecutionFilter"/>. Join node
    /// types never reach this method — multi-source plans execute through
    /// <see cref="QueryJoinExecutor"/>.
    /// </summary>
    private IAsyncEnumerable<QueryResultRow> ExecutePlanNode(QueryPlan plan, PhysicalPlanNode node, bool collectStats)
    {
        // Row filtering is evaluated inside the scan leaf (QueryPlan.ExecutionFilter); the
        // filter node contributes no operator, matching the step list which emits no step for it.
        if (node is FilterNode)
        {
            if (node.Input is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Filter node has no input");

            return ExecutePlanNode(plan, node.Input, collectStats);
        }

        IAsyncEnumerable<QueryResultRow>? input = node.Input is null ? null : ExecutePlanNode(plan, node.Input, collectStats);

        IAsyncEnumerable<QueryResultRow> cursor;

        switch (node)
        {
            case TableScanNode tableScan when tableScan.Source == TableScanSource.PrimaryRows:
                Trace(QueryPlanStepType.FullScanFromTableIndex);
                cursor = queryScanner.ScanUsingTableIndex(plan, queryFilterer);
                break;

            case TableScanNode tableScan when tableScan.Source == TableScanSource.ForcedIndex:
                Trace(QueryPlanStepType.FullScanFromIndex);
                cursor = queryScanner.ScanUsingIndex(plan, queryFilterer, tableScan.Index);
                break;

            case IndexLookupNode indexLookup:
                Trace(QueryPlanStepType.QueryFromIndex);
                cursor = QueryUsingIndex(plan, indexLookup.Index, indexLookup.LookupKey, columnValue: null);
                break;

            case IndexRangeScanNode rangeScan:
                Trace(QueryPlanStepType.RangeScanFromIndex);
                cursor = QueryUsingRangeIndex(plan, rangeScan.Index, rangeScan.FromBound, rangeScan.FromInclusive, rangeScan.ToBound, rangeScan.ToInclusive);
                break;

            case IndexInListScanNode inListScan:
                Trace(QueryPlanStepType.InListScanFromIndex);
                cursor = QueryUsingInListIndex(plan, inListScan);
                break;

            case SortNode:
                Trace(QueryPlanStepType.SortBy);
                cursor = querySorter.SortResultset(plan.Ticket, RequireInput(input), QueryExecutionContext.For(plan.Database));
                break;

            case AggregateNode aggregateNode:
                Trace(QueryPlanStepType.Aggregate);
                cursor = aggregateNode.IsStreamingGroupBy
                    ? queryAggregator.AggregateStreamingGrouped(plan.Ticket, RequireInput(input), QueryExecutionContext.For(plan.Database))
                    : queryAggregator.AggregateResultset(plan.Ticket, RequireInput(input), QueryExecutionContext.For(plan.Database));
                break;

            case HavingFilterNode:
                Trace(QueryPlanStepType.HavingFilter);
                cursor = queryFilterer.FilterHavingResultset(plan.Database, plan.Ticket, RequireInput(input));
                break;

            case DistinctNode distinctNode:
                Trace(QueryPlanStepType.Distinct);
                cursor = distinctNode.IsStreaming
                    ? queryDistincter.StreamingDistinctRows(RequireInput(input))
                    : queryDistincter.DistinctResultset(plan.Ticket, RequireInput(input), QueryExecutionContext.For(plan.Database));
                break;

            case ProjectNode:
                Trace(QueryPlanStepType.ReduceToProjections);
                cursor = queryProjector.ProjectResultset(plan.Ticket, RequireInput(input));
                break;

            case LimitNode:
                Trace(QueryPlanStepType.Limit);
                cursor = queryLimiter.LimitResultset(plan.Ticket, RequireInput(input));
                break;

            case SemiJoinNode semiJoinNode:
                Trace(QueryPlanStepType.SemiJoinProbe);
                cursor = semiJoinExecutor.ExecuteAsync(RequireInput(input), semiJoinNode, plan.Ticket);
                break;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Unsupported plan node in single-table execution: " + node.GetType().Name);
        }

        // Stats are initialized only for nodes present in StepNodes, so nodes without a step
        // (filter) are never wrapped — same coverage as the retired step-list walk.
        if (collectStats && node.Stats is not null)
            cursor = CountRowsEmitted(cursor, node.Stats);

        return cursor;

        void Trace(QueryPlanStepType stepType)
        {
            if (options.QueryTracingEnabled)
                Log.LogExecutingQueryStep(logger, stepType);
        }

        static IAsyncEnumerable<QueryResultRow> RequireInput(IAsyncEnumerable<QueryResultRow>? input)
        {
            if (input is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

            return input;
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> CountRowsEmitted(
        IAsyncEnumerable<QueryResultRow> source,
        PlanNodeStats stats)
    {
        await foreach (QueryResultRow row in source.ConfigureAwait(false))
        {
            stats.RowsEmitted++;
            yield return row;
        }
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingIndex(
        QueryPlan plan,
        TableIndexSchema? index,
        CompositeColumnValue? lookupKey,
        ColumnValue? columnValue
    )
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's unique index");

        CompositeColumnValue key = lookupKey ?? new CompositeColumnValue(new[] { columnValue! });

        if (columnValue is null && lookupKey is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid lookup key");

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(plan, index, key);

        if (!IndexScanSelector.SupportsExactEqualityPrefixUpperBound(plan.Table, index.Columns, key.Values.Length))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Non-unique index lookup requires a bounded scan plan");

        CompositeColumnValue? upperBound = BuildPrefixScanUpperBound(plan.Table, index, key);

        return QueryUsingRangeIndex(plan, index, key, fromInclusive: true, upperBound, toInclusive: false);
    }

    private static CompositeColumnValue? BuildPrefixScanUpperBound(
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
        ColumnValue? nextValue = NextSortValue(columnType, lastValue);

        if (nextValue is null)
            return null;

        upperValues[^1] = nextValue;
        return new CompositeColumnValue(upperValues);
    }

    private static ColumnValue? NextSortValue(ColumnType columnType, ColumnValue value)
    {
        return IndexScanSelector.NextSortValue(columnType, value);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingUniqueIndex(
        QueryPlan plan,
        TableIndexSchema index,
        CompositeColumnValue lookupKey
    )
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;
        QueryDependencyCollector? deps = plan.DepCollector;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, table.Schema.Version, table.Schema.ContentsGeneration);

        if (plan.IndexOnly)
        {
            // Covering path: fetch the rowId together with the entry's stored/payload (INCLUDE) tuple
            // in one probe. Key columns come from the lookup key; included columns from the tuple.
            (ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)? covering =
                await table.Store.LookupUniqueCovering(ticket.TxnState, index.KvId, lookupKey).ConfigureAwait(false);

            if (scanStats is not null)
                scanStats.KvPointLookups++;

            if (covering is null)
                yield break;

            QueryScanner.IndexOnlyLayout coveredLayout = QueryScanner.BuildIndexOnlyLayout(table, plan.ScanRequiredColumns, index);
            ColumnValue[] values = QueryScanner.SynthesizeCoveredValues(coveredLayout, lookupKey, covering.Value.includeTuple.Span);
            QueryRow coveredRow = new(covering.Value.rowId, coveredLayout.Layout, values);
            if (await queryFilterer.MeetPlanFilterAsync(plan, coveredRow).ConfigureAwait(false))
                yield return new(covering.Value.rowId, coveredRow);
            yield break;
        }

        ObjectIdValue? rowId = await table.Store.LookupUnique(ticket.TxnState, index.KvId, lookupKey).ConfigureAwait(false);

        if (scanStats is not null)
            scanStats.KvPointLookups++;

        if (rowId is null)
            yield break;

        ReadOnlyMemory<byte>? data = await table.Store.GetRow(ticket.TxnState, rowId.Value).ConfigureAwait(false);
        if (data is null || data.Value.Length == 0)
            yield break;

        if (scanStats is not null)
            scanStats.RowsRead++;

        ObjectIdValue resolvedRowId = rowId.Value;
        deps?.RecordPoint(table.Store.RowPointKey(resolvedRowId));

        // Layout-backed decode (same policy as QueryScanner's scans): the filterer takes the
        // ordinal fast path over a QueryRow, and slot/borrowed backing avoids materializing
        // cells the filter rejects or the projection never reads.
        RowEncoder.RowDecodeState decodeState = new()
        {
            SlotBackedDecode = options.SlotBackedDecode || plan.ExecutionFilter is not null,
            BorrowedDecode = QueryScanner.ShouldUseBorrowedDecode(plan),
        };

        QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
            table.Schema,
            txId,
            resolvedRowId,
            data.Value,
            options,
            plan.ScanRequiredColumns,
            plan.TableSchemaVersion,
            decodeState).ConfigureAwait(false);

        if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
            yield return new(resolvedRowId, row);
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndex(
        QueryPlan plan,
        TableIndexSchema? index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive)
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's index for range scan");

        bool unique = index.Type == IndexType.Unique;
        return QueryUsingRangeIndexInternal(plan, index, fromBound, fromInclusive, toBound, toInclusive, unique);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndexInternal(
        QueryPlan plan,
        TableIndexSchema index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive,
        bool unique)
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;
        QueryDependencyCollector? deps = plan.DepCollector;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, table.Schema.Version, table.Schema.ContentsGeneration);

        // Acquire a range lock scoped to [fromBound, toBound]. Shared for SELECT; Exclusive for
        // UPDATE/DELETE so concurrent Serializable+RW readers cannot enter the mutated sub-range.
        await table.Store.AcquireBoundedIndexRangeLockAsync(
            ticket.TxnState, index.KvId,
            fromBound, fromInclusive, toBound, toInclusive,
            unique, exclusive: plan.Ticket.ExclusivePredicateLocks,
            keyColumnCount: index.Columns.Length).ConfigureAwait(false);

        if (plan.IndexOnly)
        {
            // Covering path: every needed column is present in the decoded index key or is the
            // primary-key column. Synthesize rows directly from the scan entry; no GetRow call.
            QueryScanner.IndexOnlyLayout covered = QueryScanner.BuildIndexOnlyLayout(table, plan.ScanRequiredColumns, index);

            await foreach ((CompositeColumnValue decodedKey, ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple) in table.Store.ScanIndex(
                ticket.TxnState,
                index.KvId,
                keyTypes,
                fromBound,
                toBound,
                unique,
                fromInclusive,
                toInclusive,
                maxRows: plan.ScanRowLimit))
            {
                if (scanStats is not null)
                    scanStats.KvScanEntries++;

                ColumnValue[] values = QueryScanner.SynthesizeCoveredValues(covered, decodedKey, includeTuple.Span);
                QueryRow queryRow = new(rowId, covered.Layout, values);

                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    yield return new(rowId, queryRow);
            }
            yield break;
        }

        // Non-covering path: buffer row ids per page and fetch in one batch call per page.
        // Index order is preserved: the page is filled in scan order and batch results are
        // indexed positionally, so rows are decoded and yielded in scan order. A missing row
        // (null from the batch) is silently skipped — the same contract as the per-entry path.
        // Layout-backed decode with a per-scan plan cache (same policy as QueryScanner's scans):
        // every row at the same stored schema version shares one decode plan, the filterer takes
        // the ordinal fast path over a QueryRow, and slot/borrowed backing avoids materializing
        // cells the filter rejects or the projection never reads.
        RowEncoder.RowDecodeState decodeState = new()
        {
            SlotBackedDecode = options.SlotBackedDecode || plan.ExecutionFilter is not null,
            BorrowedDecode = QueryScanner.ShouldUseBorrowedDecode(plan),
        };
        int batchSize = options.IndexScanFetchBatchSize;
        List<ObjectIdValue> pageBuf = new(batchSize);

        async IAsyncEnumerable<QueryResultRow> flushPageAsync(List<ObjectIdValue> page)
        {
            ReadOnlyMemory<byte>?[] batchResult = await table.Store.GetRowsBatch(
                ticket.TxnState, page).ConfigureAwait(false);
            for (int i = 0; i < page.Count; i++)
            {
                ObjectIdValue batchRowId = page[i];
                ReadOnlyMemory<byte>? data = batchResult[i];
                if (data is null || data.Value.Length == 0)
                    continue;
                deps?.RecordPoint(table.Store.RowPointKey(batchRowId));
                if (scanStats is not null) scanStats.RowsRead++;
                QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema, txId, batchRowId, data.Value,
                    options,
                    plan.ScanRequiredColumns, plan.TableSchemaVersion, decodeState).ConfigureAwait(false);
                if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                    yield return new(batchRowId, row);
            }
        }

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            ticket.TxnState,
            index.KvId,
            keyTypes,
            fromBound,
            toBound,
            unique,
            fromInclusive,
            toInclusive,
            maxRows: plan.ScanRowLimit))
        {
            if (scanStats is not null)
                scanStats.KvScanEntries++;

            pageBuf.Add(rowId);

            if (pageBuf.Count < batchSize)
                continue;

            await foreach (QueryResultRow r in flushPageAsync(pageBuf))
                yield return r;
            pageBuf.Clear();
        }

        if (pageBuf.Count > 0)
            await foreach (QueryResultRow r in flushPageAsync(pageBuf))
                yield return r;
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingInListIndex(QueryPlan plan, IndexInListScanNode inListNode)
        => QueryUsingInListIndexInternal(plan, inListNode);

    /// <summary>
    /// Executes an IN-list scan by performing one index seek per distinct value and unioning
    /// the results. Duplicate row IDs (repeated values or overlapping non-unique entries)
    /// are suppressed so each matched row is emitted at most once.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> QueryUsingInListIndexInternal(
        QueryPlan plan,
        IndexInListScanNode inListNode)
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        TableIndexSchema index = inListNode.Index;
        bool isUnique = index.Type == IndexType.Unique;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);
        QueryDependencyCollector? deps = plan.DepCollector;

        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, table.Schema.Version, table.Schema.ContentsGeneration);

        HashSet<ObjectIdValue> seen = new();
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;

        // Layout-backed decode with a per-scan plan cache shared by every seek in the IN list
        // (same policy as QueryScanner's scans): rows at the same stored schema version share one
        // decode plan, the filterer takes the ordinal fast path over a QueryRow, and slot/borrowed
        // backing avoids materializing cells the filter rejects or the projection never reads.
        RowEncoder.RowDecodeState decodeState = new()
        {
            SlotBackedDecode = options.SlotBackedDecode || plan.ExecutionFilter is not null,
            BorrowedDecode = QueryScanner.ShouldUseBorrowedDecode(plan),
        };

        // For a non-unique IN-list scan, acquire a range lock covering [min, max] of the IN values.
        // Shared for SELECT; Exclusive for UPDATE/DELETE. Unique IN-list uses per-key point
        // lookups (LookupUnique), so no range lock is needed.
        if (!isUnique)
        {
            CompositeColumnValue? minVal = null, maxVal = null;
            foreach (ColumnValue v in inListNode.Values)
            {
                CompositeColumnValue cv = new(v);
                if (minVal is null || cv.CompareTo(minVal) < 0) minVal = cv;
                if (maxVal is null || cv.CompareTo(maxVal) > 0) maxVal = cv;
            }
            await table.Store.AcquireBoundedIndexRangeLockAsync(
                ticket.TxnState, index.KvId,
                minVal, true, maxVal, true,
                unique: false, exclusive: plan.Ticket.ExclusivePredicateLocks).ConfigureAwait(false);
        }

        foreach (ColumnValue value in inListNode.Values)
        {
            CompositeColumnValue lookupKey = new(new[] { value });

            if (isUnique)
            {
                ObjectIdValue? rowId = await table.Store.LookupUnique(ticket.TxnState, index.KvId, lookupKey).ConfigureAwait(false);

                if (scanStats is not null)
                    scanStats.KvPointLookups++;

                if (rowId is null || !seen.Add(rowId.Value))
                    continue;

                ReadOnlyMemory<byte>? data = await table.Store.GetRow(ticket.TxnState, rowId.Value).ConfigureAwait(false);
                if (data is null || data.Value.Length == 0)
                    continue;

                deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

                if (scanStats is not null)
                    scanStats.RowsRead++;

                QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema, txId, rowId.Value, data.Value,
                    options,
                    plan.ScanRequiredColumns, plan.TableSchemaVersion, decodeState).ConfigureAwait(false);

                if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                    yield return new(rowId.Value, row);
            }
            else
            {
                // Non-unique equality scan.
                // Types with a computable successor (Integer64, Float64, Bool): [v, successor(v)) exclusive.
                // String/Id: no total successor — use exact-match [v, v] inclusive instead.
                // ScanIndex appends a high sentinel ("￿") to endKey for non-unique, so
                // all "{encodedKey}{rowIdHex}" entries for key==v are captured by the raw scan,
                // and the decoded-key bounds filter trims to exactly key==v.
                CompositeColumnValue? upperBound = BuildPrefixScanUpperBound(table, index, lookupKey);
                CompositeColumnValue toBound = upperBound ?? lookupKey;
                bool toInclusive = upperBound is null; // inclusive only for exact-match (no successor)

                await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
                    ticket.TxnState, index.KvId, keyTypes,
                    lookupKey, toBound, unique: false,
                    fromInclusive: true, toInclusive: toInclusive,
                    maxRows: null).ConfigureAwait(false))
                {
                    if (scanStats is not null)
                        scanStats.KvScanEntries++;

                    if (!seen.Add(rowId))
                        continue;

                    ReadOnlyMemory<byte>? data = await table.Store.GetRow(ticket.TxnState, rowId).ConfigureAwait(false);
                    if (data is null || data.Value.Length == 0)
                        continue;

                    deps?.RecordPoint(table.Store.RowPointKey(rowId));

                    if (scanStats is not null)
                        scanStats.RowsRead++;

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema, txId, rowId, data.Value,
                        options,
                        plan.ScanRequiredColumns, plan.TableSchemaVersion, decodeState).ConfigureAwait(false);

                    if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                        yield return new(rowId, row);
                }
            }
        }
    }

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryByIdTicket ticket
    )
    {
        if (!table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        ObjectIdValue? rowId = await table.Store.LookupUnique(ticket.TxnState, index.KvId, new CompositeColumnValue(new[] { columnId })).ConfigureAwait(false);
        if (rowId is null)
            yield break;

        ReadOnlyMemory<byte>? data = await table.Store.GetRow(ticket.TxnState, rowId.Value).ConfigureAwait(false);
        if (data is null || data.Value.Length == 0)
            yield break;

        yield return await RowEncoder.DecodeAsync(
            table.Schema,
            txId,
            rowId.Value,
            data.Value,
            visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);
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

    /// <summary>
    /// Returns <c>true</c> when any projection, WHERE, HAVING, or GROUP BY expression in
    /// <paramref name="ticket"/> contains a call to a volatile scalar function. Such queries
    /// must not be served from or stored in the result cache because repeated executions are
    /// expected to return different rows.
    /// </summary>
    private static bool HasVolatileFunction(QueryTicket ticket)
    {
        if (ticket.Projection is { } projections)
        {
            foreach (NodeAst proj in projections)
            {
                if (ScalarFunctionEvaluator.ContainsVolatileFunction(proj))
                    return true;
            }
        }

        if (ScalarFunctionEvaluator.ContainsVolatileFunction(ticket.Where))
            return true;

        if (ScalarFunctionEvaluator.ContainsVolatileFunction(ticket.Having))
            return true;

        if (ticket.GroupBy is { } groupBy)
        {
            foreach (NodeAst expr in groupBy)
            {
                if (ScalarFunctionEvaluator.ContainsVolatileFunction(expr))
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns <c>true</c> if every schema dep in <paramref name="deps"/> still matches the
    /// current in-memory schema of <paramref name="database"/>: the table exists and its
    /// <see cref="TableSchema.Version"/> equals the version recorded at cache time.
    ///
    /// <para><b>This is a concurrency-only defense, not the primary schema fence.</b> The
    /// schema version is already folded into the result fingerprint (see
    /// <c>ResultFingerprintBuilder.Build</c>), and the planner always stamps the fingerprint
    /// from the current live schema version. So in a single-threaded sequence a schema change
    /// yields a different fingerprint — a natural miss — and this method is never reached; and
    /// a fingerprint hit implies the versions already match, so this returns <c>true</c>. It
    /// cannot evict on any non-concurrent path.</para>
    ///
    /// <para>It earns its keep as defense-in-depth for two states the fingerprint fence alone
    /// does not cover. First, a TOCTOU race: a committing DDL can bump the version and run its
    /// <c>InvalidateByTableId</c> in the window <em>after</em> the planner stamped the
    /// fingerprint at the old version and <em>after</em> this probe already fetched the entry,
    /// but before its rows are yielded — re-reading the live version here evicts rather than
    /// serving a snapshot a concurrent commit just invalidated. Second, an entry whose stored
    /// deps and fingerprint disagree (e.g. one that published after its <c>InvalidateByTableId</c>
    /// already fired, so it is absent from the dependency index and a later
    /// <c>InvalidateByTableId</c> cannot find it) — this re-check still catches it on the next
    /// probe. The eviction logic is covered deterministically by
    /// <c>TestQueryResultCacheSchemaFence.SchemaDepsCurrent_StaleDepVersion_EvictsEntryOnHit</c>
    /// (via a test injection seam), and the primary version/id fingerprint fence that makes this
    /// path almost never necessary is covered by the <c>Fingerprint_*</c> tests in the same
    /// fixture.</para>
    /// </summary>
    private static bool SchemaDepsCurrent(QueryDependencySet deps, DatabaseDescriptor database)
    {
        foreach ((string tableId, int expectedVersion, long expectedContents) in deps.SchemaDeps)
        {
            TableSchema? schema = null;
            foreach (TableSchema s in database.Schema.Tables.Values)
            {
                if (s.Id == tableId) { schema = s; break; }
            }
            if (schema is null)
                return false;  // table was dropped
            if (schema.Version != expectedVersion)
                return false;  // schema changed (column add/drop/rename, index add/drop)
            if (schema.ContentsGeneration != expectedContents)
                return false;  // a materialized-view refresh replaced the rows these were read from
        }
        return true;
    }
}
