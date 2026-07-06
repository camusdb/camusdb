
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
/// A no-op <see cref="IQueryResultCache"/> used when <c>query_result_cache_enabled</c> is
/// <c>false</c> (the default) or when a test or component does not need caching.
///
/// All probes miss, all publishes silently succeed with a <see cref="QueryCacheStatus.Bypass"/>
/// status, and invalidation is a no-op. The <see cref="PublishGate"/> is a real
/// <see cref="CachePublishGate"/> instance so write paths can always call into it safely
/// without a null check, but since nothing ever publishes, the gate's generation counters are
/// irrelevant.
/// </summary>
public sealed class NullQueryResultCache : IQueryResultCache
{
    public static readonly NullQueryResultCache Instance = new();

    public CachePublishGate PublishGate { get; } = new();

    public Task<CachedQueryResult?> TryGetAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default)
        => Task.FromResult<CachedQueryResult?>(null);

    public Task<(CachedQueryResult Result, QueryDependencySet Deps)?> TryGetWithDepsAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default)
        => Task.FromResult<(CachedQueryResult, QueryDependencySet)?>(null);

    public void InvalidateEntry(string databaseId, string cacheName, string fingerprint) { }

    public Task<(QueryCacheStatus Status, QueryCacheBypassReason Reason)> TryPublishAsync(
        CachedQueryResult result,
        CacheGenerationToken generationToken,
        QueryDependencySet? deps = null,
        CancellationToken cancellationToken = default)
        => Task.FromResult((QueryCacheStatus.Bypass, QueryCacheBypassReason.None));

    public void InvalidateByModifiedKeys(IReadOnlyCollection<(string key, KeyValueDurability durability)> modifiedKeys) { }

    public void InvalidateByTableId(string databaseId, string tableId) { }

    public void InvalidateDatabase(string databaseId) { }

    public void InvalidateCacheName(string databaseId, string cacheName) { }

    /// <summary>
    /// Always returns an owner slot — no single-flight tracking occurs when the cache is
    /// disabled. Every caller executes the plan independently, which is the correct no-op
    /// behavior for the null object.
    /// </summary>
    public SingleFlightSlot EnterSingleFlight(string fingerprint)
        => new SingleFlightSlot(isOwner: true, Task.FromResult<CachedQueryResult?>(null));

    /// <summary>No-op: the null cache never registers an in-flight slot.</summary>
    public void ExitSingleFlight(string fingerprint, CachedQueryResult? result) { }
}
