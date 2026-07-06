
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Cache;

/// <summary>
/// Mutable side-channel that <c>QueryExecutor.QueryWithCache</c> populates with cache resolution
/// metadata once the row cursor has been fully drained. The controller creates one instance per
/// autocommit SELECT, passes it to <see cref="CamusDB.Core.CommandsExecutor.CommandExecutor.ExecuteSQLQuery"/>,
/// and reads from it after the <c>await foreach</c> completes to set the response envelope fields.
///
/// <para><b>Lifecycle:</b> the holder starts with <see cref="QueryCacheStatus.Bypass"/> / no other
/// fields set. On the non-cache path (no hint, join query, explicit transaction) it stays in this
/// default state and callers should omit cache fields from the response. On the cache path the holder
/// is populated in the iterator's <c>finally</c> block or at each <c>yield break</c> site — the
/// iterator owns the write side; the controller owns the read side after the cursor is consumed.</para>
///
/// <para><b>Thread safety:</b> not thread-safe; one instance per request.</para>
/// </summary>
public sealed class CacheMetadataHolder
{
    /// <summary>
    /// How the cache resolved this request. Defaults to <see cref="QueryCacheStatus.Bypass"/>
    /// so un-populated holders are safe to inspect.
    /// </summary>
    public QueryCacheStatus Status { get; internal set; } = QueryCacheStatus.Bypass;

    /// <summary>
    /// Why the cache was bypassed or the entry was not stored. Relevant only when
    /// <see cref="Status"/> is <see cref="QueryCacheStatus.Bypass"/> or
    /// <see cref="QueryCacheStatus.EvictedBeforePublish"/>.
    /// </summary>
    public QueryCacheBypassReason BypassReason { get; internal set; } = QueryCacheBypassReason.None;

    /// <summary>
    /// HLC timestamp at which the served cache entry was computed. Non-null only on a cache hit.
    /// </summary>
    public HLCTimestamp? CachedAtHlc { get; internal set; }

    /// <summary>
    /// Approximate wall-clock age of the cache entry in milliseconds. Non-null only on a cache
    /// hit where <see cref="CachedQueryResult.CreatedAtMs"/> was stamped.
    /// </summary>
    public long? AgeMs { get; internal set; }

    /// <summary>
    /// Logical cache family name from the query hint. Non-null when the cache path was entered
    /// (regardless of whether the result was a hit, miss, or stale revalidation).
    /// </summary>
    public string? CacheName { get; internal set; }

    /// <summary>
    /// Converts <see cref="QueryCacheStatus"/> to the lowercase-kebab string value surfaced in
    /// the JSON response envelope.
    /// </summary>
    public static string ToStatusString(QueryCacheStatus status) => status switch
    {
        QueryCacheStatus.Hit                 => "hit",
        QueryCacheStatus.Miss                => "miss",
        QueryCacheStatus.StaleRevalidated    => "stale-revalidated",
        QueryCacheStatus.EvictedBeforePublish => "evicted-before-publish",
        _                                    => "bypass",
    };

    /// <summary>
    /// Converts <see cref="QueryCacheBypassReason"/> to the lowercase-kebab string value
    /// surfaced in the JSON response envelope. Returns <c>null</c> for <see cref="QueryCacheBypassReason.None"/>.
    /// </summary>
    public static string? ToBypassReasonString(QueryCacheBypassReason reason) => reason switch
    {
        QueryCacheBypassReason.InTransaction         => "in-transaction",
        QueryCacheBypassReason.NonDeterministic      => "non-deterministic",
        QueryCacheBypassReason.MultipleCacheHints    => "multiple-cache-hints",
        QueryCacheBypassReason.CacheDisabled         => "cache-disabled",
        QueryCacheBypassReason.InvalidCacheOptions   => "invalid-cache-options",
        QueryCacheBypassReason.OversizedResult       => "oversized-result",
        QueryCacheBypassReason.DependencyLimitExceeded => "dependency-limit",
        QueryCacheBypassReason.SchemaChanged         => "schema-changed",
        QueryCacheBypassReason.InFlightWrite         => "in-flight-write",
        QueryCacheBypassReason.StrictValidationLimit => "strict-validation-limit",
        QueryCacheBypassReason.UnsupportedStatementType => "unsupported-statement-type",
        QueryCacheBypassReason.Join                  => "join",
        QueryCacheBypassReason.InnerHint             => "inner-hint",
        _                                            => null,
    };
}
