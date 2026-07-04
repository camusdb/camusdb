
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// Describes how a cached SELECT was resolved for a single request.
/// Surfaced to callers in the query response envelope so clients can
/// reason about cache behaviour without inspecting timing.
/// </summary>
public enum QueryCacheStatus
{
    /// <summary>
    /// A valid, fully-materialized entry was found and returned without touching storage.
    /// The result is guaranteed to reflect all committed writes on this node up to the point
    /// the entry was published; cross-node writes are bounded by TTL or strict validation.
    /// </summary>
    Hit,

    /// <summary>
    /// No valid entry existed; the query executed against live storage and the result was
    /// published to the cache after full materialization and transaction finalization.
    /// </summary>
    Miss,

    /// <summary>
    /// The cache was consulted but the request was not eligible for caching or could not be
    /// served from the cache. The query executed against live storage. No entry was published.
    /// See the accompanying <c>cacheReason</c> field for why the cache was bypassed.
    /// </summary>
    Bypass,

    /// <summary>
    /// A strict-mode entry was found but failed dependency validation (a row was updated,
    /// deleted, or inserted into a scanned range since the entry was computed). The query was
    /// re-executed against live storage and a fresh entry published if the new result fits.
    /// </summary>
    StaleRevalidated,

    /// <summary>
    /// The query executed successfully and rows were materialized, but the entry could not be
    /// published. Three distinct conditions map to this single value: (1) the result exceeded
    /// the per-entry row or byte cap, (2) the global byte cap was still exceeded after LRU
    /// eviction, or (3) a write committed into an overlapping keyspace between query-start and
    /// publish time (generation-fence rejection). The live rows were returned to the caller.
    ///
    /// <para>A future observability pass will split these into distinct sub-reasons surfaced in the
    /// <c>cacheReason</c> response field. For now callers cannot distinguish the three cases from
    /// the status value alone.</para>
    /// </summary>
    EvictedBeforePublish,
}

/// <summary>
/// Reason recorded on a <see cref="QueryCacheStatus.Bypass"/> or
/// <see cref="QueryCacheStatus.EvictedBeforePublish"/> response.
/// </summary>
public enum QueryCacheBypassReason
{
    None,

    /// <summary>The request is inside an explicit open transaction, which must read live storage.</summary>
    InTransaction,

    /// <summary>The query contains a non-deterministic function (wall-clock, random, volatile session state).</summary>
    NonDeterministic,

    /// <summary>More than one cache hint appeared in the SELECT; v1 does not support multi-hint queries.</summary>
    MultipleCacheHints,

    /// <summary>The cache is disabled via configuration.</summary>
    CacheDisabled,

    /// <summary>The cache name or option values were syntactically invalid.</summary>
    InvalidCacheOptions,

    /// <summary>The result exceeded the configured per-entry row or byte cap; rows were returned from live storage.</summary>
    OversizedResult,

    /// <summary>The dependency set exceeded the configured cap and could not be safely promoted to a coarser range.</summary>
    DependencyLimitExceeded,

    /// <summary>The schema changed between plan time and publish time; the entry was not stored.</summary>
    SchemaChanged,

    /// <summary>
    /// A write committed into an overlapping keyspace between query-start and publish time.
    /// The live rows were returned; the next miss will repopulate with the updated data.
    /// </summary>
    InFlightWrite,

    /// <summary>Strict-validation exceeded the configured key/scan limit; treated as a miss.</summary>
    StrictValidationLimit,

    /// <summary>The statement type is not eligible for result caching (e.g. DML, DDL, EXPLAIN).</summary>
    UnsupportedStatementType,
}
