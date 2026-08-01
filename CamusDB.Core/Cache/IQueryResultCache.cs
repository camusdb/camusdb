
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Cache;

/// <summary>
/// Result rows materialized from a cacheable SELECT together with the metadata needed to
/// serve them on a subsequent hit and validate them under strict mode.
/// </summary>
/// <param name="CacheName">Logical family name supplied by the query hint.</param>
/// <param name="DatabaseId">Identifies the database so entries from different databases never collide.</param>
/// <param name="Rows">Fully materialized result set; never partial.</param>
/// <param name="ResultFingerprint">Stable, collision-resistant key that encodes query shape, typed parameters, schema versions, and cache options.</param>
/// <param name="CachedAt">HLC timestamp at which the result was computed; used by strict validation.</param>
/// <param name="Status">Status to surface in the response envelope for this entry.</param>
/// <param name="HintTtlMs">Per-query TTL from the <c>{cache=…, ttl=…}</c> hint, or <c>null</c> to use <see cref="CamusDBOptions.QueryResultCacheDefaultTtlMs"/>.</param>
/// <param name="HintIsStrict">Whether the hint requested strict (read-your-writes) validation. Carried on the entry so revalidation can apply the same strictness the original query requested.</param>
/// <param name="CreatedAtMs">Monotonic tick count (<see cref="Environment.TickCount64"/>) stamped when the entry is stored. Used to compute <c>ageMs</c> in the response envelope. Zero for entries created before this field was added.</param>
public sealed record CachedQueryResult(
    string CacheName,
    string DatabaseId,
    IReadOnlyList<QueryResultRow> Rows,
    string ResultFingerprint,
    HLCTimestamp CachedAt,
    QueryCacheStatus Status,
    int? HintTtlMs = null,
    bool HintIsStrict = false,
    long CreatedAtMs = 0);

/// <summary>
/// Core contract for the per-node in-memory query result cache.
///
/// <para><b>Safety invariants that every implementation must uphold:</b></para>
/// <list type="bullet">
///   <item><description>
///     <b>No partial publish.</b> An entry is only ever stored after the query enumeration
///     completed fully and the autocommit read transaction finalized successfully. A
///     cancelled or faulted enumeration must never leave an entry in the cache.
///   </description></item>
///   <item><description>
///     <b>Dependency-complete or bypass.</b> An entry is only stored if its dependency set
///     fully covers every storage fact that could change the result. If the dependency cap
///     is exceeded and no safe coarser promotion is available, the entry must be bypassed
///     rather than stored with an incomplete dep set.
///   </description></item>
///   <item><description>
///     <b>False invalidation is allowed.</b> Entries may be invalidated more aggressively
///     than strictly necessary (e.g. a write to one row invalidates entries for the whole
///     table range). This is correct but may reduce hit rate.
///   </description></item>
///   <item><description>
///     <b>Stale same-node hit forbidden after commit boundary.</b> Once a write transaction
///     commits on this node, no subsequent probe for a keyspace that overlaps the write may
///     return an entry that pre-dates the write. This property is enforced by
///     <see cref="CachePublishGate"/>: the gate is consulted both at probe time (in-flight
///     check) and at publish time (generation check).
///   </description></item>
/// </list>
///
/// <para>Cross-node writes are <em>not</em> covered by these invariants in v1; entries
/// from writes on other nodes may be served until TTL expiry or strict revalidation.</para>
/// </summary>
public interface IQueryResultCache
{
    /// <summary>
    /// The shared publish gate that write transactions must call into before and after
    /// committing. Exposing it here lets <see cref="CachedQueryRunner"/> access the gate
    /// without taking a direct dependency on the concrete cache implementation.
    /// </summary>
    CachePublishGate PublishGate { get; }

    /// <summary>
    /// Probes the cache for a result matching <paramref name="fingerprint"/>.
    ///
    /// <para>Returns a non-null <see cref="CachedQueryResult"/> on a cache hit. The caller
    /// must check <see cref="CachedQueryResult.Status"/> — a
    /// <see cref="QueryCacheStatus.Hit"/> means rows can be returned immediately; a
    /// <see cref="QueryCacheStatus.StaleRevalidated"/> means the caller should re-execute
    /// and then call <see cref="TryPublishAsync"/> with the fresh result.</para>
    ///
    /// <para>Returns <c>null</c> on a miss or bypass (e.g. in-flight write detected).</para>
    /// </summary>
    Task<CachedQueryResult?> TryGetAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to publish <paramref name="result"/> to the cache after the query has been
    /// fully materialized and the read transaction finalized.
    ///
    /// <para><b>Preconditions the caller must satisfy before invoking this method:</b></para>
    /// <list type="number">
    ///   <item><description>The enumeration completed without exception or cancellation.</description></item>
    ///   <item><description>The read transaction committed (or finalized as a no-op for zero-snapshot reads).</description></item>
    ///   <item><description>
    ///     The caller holds a <see cref="CacheGenerationToken"/> obtained before execution
    ///     began and passes it as <paramref name="generationToken"/>.
    ///   </description></item>
    /// </list>
    ///
    /// <para><b>Implementation requirement — atomic publish:</b> implementations must insert
    /// the entry into the cache dictionary by calling
    /// <see cref="CachePublishGate.TryPublishUnderGeneration"/>, which holds the same internal
    /// lock as <see cref="CachePublishGate.CommitWrite"/>. Calling
    /// <see cref="CachePublishGate.CanPublish"/> and then inserting separately is a TOCTOU
    /// race: <see cref="CachePublishGate.CommitWrite"/> can bump the generation and run its
    /// invalidation callback between the check and the insert, leaving a stale entry that
    /// survives until TTL.</para>
    ///
    /// <para>Pass <see cref="QueryDependencySet.Empty"/> when no dep collector was attached
    /// (uncached paths). The implementation must bypass publish rather than store an incomplete
    /// dep set if <see cref="QueryDependencyCollector.CapExceeded"/> was true.</para>
    ///
    /// <para>Returns the status and bypass reason to surface to the caller:
    /// <c>(Miss, None)</c> if the entry was stored;
    /// <c>(EvictedBeforePublish, OversizedResult)</c> if a per-entry or global byte/row cap
    /// rejected it; <c>(EvictedBeforePublish, InFlightWrite)</c> if the generation fence
    /// rejected it (a write committed into the same keyspace during query execution).</para>
    /// </summary>
    Task<(QueryCacheStatus Status, QueryCacheBypassReason Reason)> TryPublishAsync(
        CachedQueryResult result,
        CacheGenerationToken generationToken,
        QueryDependencySet? deps = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes the cache for a result matching <paramref name="fingerprint"/> and, on a hit,
    /// also returns the stored <see cref="QueryDependencySet"/> so the caller can run strict
    /// validation before serving cached rows. Returns <c>null</c> on miss, expired entry, or
    /// in-flight write bypass — identical miss semantics to <see cref="TryGetAsync"/>.
    ///
    /// <para>Used by the strict-validation read path, which needs deps to probe point and range
    /// deps against current KV state. Non-strict reads should use <see cref="TryGetAsync"/>
    /// to avoid the additional tuple allocation.</para>
    /// </summary>
    Task<(CachedQueryResult Result, QueryDependencySet Deps)?> TryGetWithDepsAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a single entry by its exact fingerprint. Used by the strict-validation path
    /// when a hit is detected as stale — evicting it avoids re-validating the same stale
    /// entry on every subsequent probe until TTL expiry.
    ///
    /// <para>A no-op if the entry does not exist (was already evicted by a concurrent write
    /// or TTL sweep). Idempotent.</para>
    /// </summary>
    void InvalidateEntry(string databaseId, string cacheName, string fingerprint);

    /// <summary>
    /// Invalidates all entries whose dependency sets overlap any key in
    /// <paramref name="modifiedKeys"/>. Called by the write path after a successful commit
    /// (typically from <see cref="CachedQueryRunner"/> or directly from
    /// <c>KvTransactionsManager.CommitAsync</c> via the injected cache hook).
    ///
    /// <para>This is distinct from the generation-fence in <see cref="CachePublishGate"/>:
    /// the gate blocks <em>new</em> publishes; this method removes <em>existing</em> entries
    /// that are now stale. Both must be called on every commit.</para>
    /// </summary>
    void InvalidateByModifiedKeys(IReadOnlyCollection<(string key, KeyValueDurability durability)> modifiedKeys);

    /// <summary>
    /// Invalidates all entries whose <c>schemaDeps</c> reference <paramref name="tableId"/>
    /// at any schema version. Called on DDL changes (drop table, alter column, drop index, etc.).
    /// </summary>
    void InvalidateByTableId(string databaseId, string tableId);

    /// <summary>
    /// Invalidates every entry belonging to <paramref name="databaseId"/>. Called on
    /// database drop or explicit <c>EVICT CACHE ALL</c>.
    /// </summary>
    void InvalidateDatabase(string databaseId);

    /// <summary>
    /// Invalidates all entries with the given <paramref name="cacheName"/> under
    /// <paramref name="databaseId"/>. Called by explicit <c>EVICT CACHE '<name>'</c>.
    /// </summary>
    void InvalidateCacheName(string databaseId, string cacheName);

    /// <summary>
    /// Attempts to register <paramref name="fingerprint"/> as the single in-flight execution.
    /// The first concurrent caller for a given fingerprint becomes the <em>owner</em>
    /// (<see cref="SingleFlightSlot.IsOwner"/> == <c>true</c>); all subsequent concurrent
    /// callers become <em>waiters</em> (<c>IsOwner</c> == <c>false</c>) and should call
    /// <see cref="SingleFlightSlot.WaitAsync"/> to block until the owner publishes an entry.
    ///
    /// <para>The owner <b>must</b> call <see cref="ExitSingleFlight"/> exactly once — on
    /// success or failure — to wake waiters and remove the in-flight registration. Failing to
    /// call <see cref="ExitSingleFlight"/> leaves waiters blocked until their timeout.</para>
    ///
    /// <para>Implementations that do not track in-flight state (e.g. <see cref="NullQueryResultCache"/>)
    /// always return a slot with <see cref="SingleFlightSlot.IsOwner"/> == <c>true</c> so the
    /// caller always executes the plan — single-flight is simply a no-op.</para>
    /// </summary>
    SingleFlightSlot EnterSingleFlight(string fingerprint);

    /// <summary>
    /// Signals all waiters for <paramref name="fingerprint"/> and removes the in-flight
    /// registration. Must be called exactly once by the owner after
    /// <see cref="EnterSingleFlight"/> returned a slot with <see cref="SingleFlightSlot.IsOwner"/>
    /// == <c>true</c>.
    ///
    /// <para>Pass <paramref name="published"/> == <c>true</c> when the owner successfully
    /// materialized and stored an entry, so waiters re-probe the cache instead of duplicating the
    /// execution. Pass <c>false</c> when the owner failed (exception, cancellation, generation fence
    /// rejected, byte cap exceeded) — waiters will then execute the plan independently.</para>
    ///
    /// <para>The signal is deliberately a flag rather than the result object: an entry published a
    /// moment ago can be evicted by a write committing before waiters wake, so only a fresh probe
    /// through the normal read path can decide whether serving it is still correct.</para>
    ///
    /// <para>A no-op when <paramref name="fingerprint"/> is not registered (e.g. after
    /// <see cref="NullQueryResultCache"/> returned an always-owner slot).</para>
    /// </summary>
    void ExitSingleFlight(string fingerprint, bool published);
}
