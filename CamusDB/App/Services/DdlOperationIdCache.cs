
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Services;

/// <summary>
/// Two-layer idempotency cache for DDL forwarding operations.
///
/// <b>Layer 1 — completed result cache</b>: stores the successful response for
/// a stable <c>operationId</c> for <see cref="Ttl"/>.  Sequential retries (the
/// forwarder retried after the response was lost in transit) hit this layer and
/// receive the cached response without re-executing the DDL.  Only "ok" responses
/// are stored; errors are never cached so a corrected retry always re-executes.
///
/// <b>Layer 2 — in-flight set</b>: a <see cref="TaskCompletionSource{T}"/> per
/// operationId, registered before execution begins.  A concurrent duplicate
/// (same operationId, e.g. because the HTTP client timed out while the leader
/// was still executing a long index backfill) finds the in-flight TCS and
/// <c>await</c>s it instead of running the DDL a second time.  When the original
/// completes, the result is stored in Layer 1 and the TCS is completed; the
/// duplicate unblocks and returns the same response.  If the original fails, the
/// TCS is faulted so the duplicate propagates the same error.
///
/// <b>Residual race</b>: between the <c>TryGetOrReserve</c> result-cache check
/// and the <c>GetOrAdd</c> in-flight registration, the original could finish
/// <em>and</em> have its TCS removed.  The retry would then register its own TCS
/// and re-execute.  This window is sub-millisecond and extremely unlikely in
/// practice; the SchemaDdlSemaphore on the leader ensures idempotency at the apply
/// layer regardless.
/// </summary>
public class DdlOperationIdCache
{
    internal static readonly TimeSpan Ttl = TimeSpan.FromSeconds(120);
    private static readonly TimeSpan EvictionInterval = TimeSpan.FromSeconds(60);

    private sealed record Entry(SchemaDdlForwardResponse Response, DateTime ExpiresAt);

    private readonly ConcurrentDictionary<string, Entry> resultCache = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<SchemaDdlForwardResponse?>> inFlight = new();
    private DateTime lastEviction = DateTime.UtcNow;

    /// <summary>
    /// Checks both cache layers for <paramref name="operationId"/>.
    ///
    /// Returns a non-null <see cref="Task{T}"/> when the caller must NOT execute
    /// the DDL itself:
    /// <list type="bullet">
    ///   <item>Completed Task — result is cached (sequential retry); <c>await</c>
    ///     returns immediately.</item>
    ///   <item>Pending Task — another call is currently executing the same opId
    ///     (concurrent duplicate); <c>await</c> blocks until that call finishes,
    ///     then returns its result.  If the original faulted, the await throws
    ///     that exception.</item>
    /// </list>
    ///
    /// Returns <c>null</c> when this is the first caller for the operationId.
    /// The caller <b>must</b> execute the DDL and then call
    /// <see cref="SetAndComplete"/> on success or <see cref="FaultAndRelease"/>
    /// on failure.
    /// </summary>
    public virtual Task<SchemaDdlForwardResponse?>? TryGetOrReserve(string operationId)
    {
        // Layer 1: completed result cache.
        if (resultCache.TryGetValue(operationId, out Entry? entry) && entry.ExpiresAt > DateTime.UtcNow)
            return Task.FromResult<SchemaDdlForwardResponse?>(entry.Response);

        // Layer 2: in-flight registration.  GetOrAdd is atomic: only one TCS
        // is stored per key even under concurrent callers.
        var ourTcs = new TaskCompletionSource<SchemaDdlForwardResponse?>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var storedTcs = inFlight.GetOrAdd(operationId, ourTcs);

        if (ReferenceEquals(ourTcs, storedTcs))
            return null; // We're the owner — caller should execute.

        return storedTcs.Task; // Concurrent duplicate — await the owner's result.
    }

    /// <summary>
    /// Records a successful response, unblocks any concurrent waiters, and
    /// removes the in-flight entry.
    ///
    /// The result cache is written <em>before</em> removing the TCS from
    /// in-flight so a racing <see cref="TryGetOrReserve"/> always finds the
    /// result before the TCS disappears, closing the window where a sequential
    /// retry could slip past both layers.
    /// </summary>
    public void SetAndComplete(string operationId, SchemaDdlForwardResponse response)
    {
        resultCache[operationId] = new Entry(response, DateTime.UtcNow + Ttl);

        if (inFlight.TryRemove(operationId, out var tcs))
            tcs.TrySetResult(response);

        MaybeEvict();
    }

    /// <summary>
    /// Faults the in-flight TCS (propagating the error to any concurrent waiters)
    /// and removes the in-flight entry.  The result is not cached so the next
    /// caller with the same operationId re-executes.
    /// </summary>
    public void FaultAndRelease(string operationId, Exception exception)
    {
        if (inFlight.TryRemove(operationId, out var tcs))
            tcs.TrySetException(exception);
    }

    private void MaybeEvict()
    {
        DateTime now = DateTime.UtcNow;
        if (now - lastEviction < EvictionInterval)
            return;

        lastEviction = now;
        foreach (string key in resultCache.Keys.ToArray())
        {
            if (resultCache.TryGetValue(key, out Entry? e) && e.ExpiresAt <= now)
                resultCache.TryRemove(key, out _);
        }
    }
}
