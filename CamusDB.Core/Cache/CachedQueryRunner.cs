
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Cache;

/// <summary>
/// Wraps an autocommit SELECT execution and enforces the no-partial-publish invariant:
/// an entry is only published to <see cref="IQueryResultCache"/> when <em>all</em> of the
/// following hold true simultaneously:
/// <list type="number">
///   <item><description>The enumeration drained to completion without throwing or being cancelled.</description></item>
///   <item><description>The read transaction finalized successfully (caller signals this via <see cref="FinalizeAsync"/>).</description></item>
///   <item><description>The generation fence still holds — the store happens through
///     <see cref="CachePublishGate.TryPublishUnderGeneration"/> (invoked by the cache's
///     <see cref="IQueryResultCache.TryPublishAsync"/>), which atomically re-checks the token
///     minted before execution and inserts only if no overlapping write committed meanwhile.</description></item>
/// </list>
///
/// If any step fails — cancellation, an operator exception, a transaction abort, a cap breach,
/// or a generation-fence rejection — the live rows are returned to the caller and no entry is
/// stored. The caller never needs to decide whether to publish; that decision is entirely
/// encapsulated here.
///
/// <para><b>Usage pattern:</b></para>
/// <code>
///   var token = cache.PublishGate.SnapshotGenerations(keyspaces);
///   var runner = new CachedQueryRunner(cache, result, token, ct);
///   await foreach (var row in runner.DrainAsync())
///       yield return row;
///   await runner.FinalizeAsync();  // called only after full drain
/// </code>
///
/// <para><b>Concurrency:</b> one instance per request; not thread-safe.</para>
/// </summary>
public sealed class CachedQueryRunner
{
    private readonly IQueryResultCache _cache;
    private readonly CachedQueryResult _pendingResult;
    private readonly CacheGenerationToken _token;
    private readonly CancellationToken _ct;

    /// <summary>
    /// Optional dep collector attached to the query plan. Populated by the scan path during
    /// <see cref="DrainAsync"/> and forwarded to <see cref="IQueryResultCache.TryPublishAsync"/>
    /// in <see cref="FinalizeAsync"/>. Null means no dep capture (uncached or non-dep path).
    /// </summary>
    internal QueryDependencyCollector? DepCollector { get; set; }

    private bool _drainCompleted;

    public CachedQueryRunner(
        IQueryResultCache cache,
        CachedQueryResult pendingResult,
        CacheGenerationToken token,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(pendingResult);
        ArgumentNullException.ThrowIfNull(token);

        _cache         = cache;
        _pendingResult = pendingResult;
        _token         = token;
        _ct            = ct;
    }

    /// <summary>
    /// Materializes <paramref name="source"/> row by row, accumulating into the pending entry.
    /// If enumeration is cancelled or throws, <see cref="_drainCompleted"/> stays <c>false</c>
    /// and <see cref="FinalizeAsync"/> will not publish — enforcing the no-partial-publish invariant.
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> DrainAsync(IAsyncEnumerable<QueryResultRow> source)
    {
        await foreach (QueryResultRow row in source.WithCancellation(_ct).ConfigureAwait(false))
        {
            yield return row;
        }

        // Only reached when the enumeration completed without cancellation or exception.
        _drainCompleted = true;
    }

    /// <summary>
    /// Called by the caller after the read transaction has finalized successfully.
    /// Attempts to publish the accumulated entry — but only when <see cref="DrainAsync"/>
    /// completed fully and the generation fence allows it.
    ///
    /// <para>If <see cref="DrainAsync"/> was cancelled or threw, this is a no-op: the
    /// <c>_drainCompleted</c> flag is <c>false</c> and publish is skipped.</para>
    ///
    /// <para>Callers should still invoke this on every non-exception path (e.g. in a
    /// <c>finally</c> block after committing the read transaction) — it is safe to call
    /// when drain did not complete.</para>
    /// </summary>
    public async Task<QueryCacheStatus> FinalizeAsync()
    {
        if (!_drainCompleted)
            return QueryCacheStatus.Bypass;

        // If the dep collector hit a cap, bypass rather than storing an incomplete dep set.
        QueryDependencySet? deps = DepCollector is not null
            ? (DepCollector.CapExceeded ? null : DepCollector.Build())
            : null;

        if (DepCollector is not null && DepCollector.CapExceeded)
            return QueryCacheStatus.EvictedBeforePublish;

        return await _cache.TryPublishAsync(_pendingResult, _token, deps, _ct).ConfigureAwait(false);
    }
}
