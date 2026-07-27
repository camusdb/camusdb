
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Core.Cache;

/// <summary>
/// Enforces the commit-safe publish ordering contract between write transactions and
/// cache entries so a same-node committed write can never be followed by a stale cache hit.
///
/// <para><b>Keyspace granularity — table-bucket, not row-key.</b>
/// Every keyspace string passed to this gate must be a coarse table-bucket identifier
/// such as <c>"{dbId}:{tableId}:r"</c> or <c>"{dbId}:{tableId}:i:{indexId}"</c>.
/// The gate's two dictionaries (<c>_generations</c> and <c>_inFlight</c>) grow
/// monotonically and are never reclaimed. Their size is bounded only when the set of
/// distinct keyspace strings is bounded — which holds at table-bucket granularity (one
/// entry per open table/index) but <em>not</em> at row-key or index-entry granularity
/// (one entry per row = unbounded leak). Never pass a per-row or per-entry key to this gate.</para>
///
/// <para><b>Required read-path call ordering.</b> The probe path must call these methods
/// in this exact order to avoid subtle races:</para>
/// <list type="number">
///   <item><description>
///     <see cref="SnapshotGenerations"/> — records the current generation for each keyspace
///     the query will scan. Must come <em>first</em> so the token captures the pre-execution
///     state of every relevant keyspace.
///   </description></item>
///   <item><description>
///     <see cref="HasInFlightWrite"/> — if <c>true</c>, bypass the cache entirely; do not
///     execute the query for cache purposes. Must come <em>after</em>
///     <see cref="SnapshotGenerations"/> so the snapshot is already taken before the check.
///     If a write marks itself in-flight between these two calls, <c>HasInFlightWrite</c>
///     returns <c>true</c> and the query is conservatively bypassed. If the write completes
///     (both <see cref="MarkWriteInFlight"/> and <see cref="CommitWrite"/>) between the two
///     calls, <c>HasInFlightWrite</c> returns <c>false</c> (in-flight mark cleared) but the
///     generation was bumped — <see cref="TryPublishUnderGeneration"/> will detect the stale
///     token and reject it.
///   </description></item>
///   <item><description>
///     Execute the query against live storage.
///   </description></item>
///   <item><description>
///     <see cref="TryPublishUnderGeneration"/> — atomically re-checks the generation and
///     stores the entry. This is the only correct publish API.
///   </description></item>
/// </list>
///
/// <para><b>Write protocol</b> (must be followed by every write path):</para>
/// <list type="number">
///   <item><description>
///     <see cref="MarkWriteInFlight"/> — call with the transaction's modified keyspaces
///     <em>before</em> <c>LocateAndCommitTransaction</c>. Concurrent probes seeing this mark
///     will bypass rather than race to publish.
///   </description></item>
///   <item><description>
///     On successful commit: <see cref="CommitWrite"/> — bumps generation, calls the
///     invalidation callback, and clears in-flight marks, all inside <c>_writeLock</c>.
///   </description></item>
///   <item><description>
///     On an <b>unknown</b> outcome — an unresolved finalize retry, a lost/errored response, a
///     cancellation or exception after a commit request was issued — <see cref="FenceAndInvalidate"/>:
///     the write may already be visible, so it must be fenced exactly like a commit.
///   </description></item>
///   <item><description>
///     Only on a <b>definite</b> non-commit (coordinator abort, confirmed rollback, or a failure
///     before any commit request): <see cref="AbortWrite"/> — clears in-flight marks without
///     bumping generations. False invalidations are acceptable; a stale hit is not.
///   </description></item>
/// </list>
///
/// <para><b>Why <c>CanPublish</c> alone is insufficient</b>: <c>CanPublish → insert</c>
/// is a check-then-act sequence. Between the check returning <c>true</c> and the insert,
/// <see cref="CommitWrite"/> can bump the generation and run the invalidation callback —
/// which finds no entry yet — after which the now-stale entry is inserted and survives
/// to TTL. <see cref="TryPublishUnderGeneration"/> eliminates this window by holding the
/// write lock across both operations.</para>
///
/// <para><b>Thread safety:</b> all public methods are safe to call concurrently from
/// multiple query and write threads/tasks. <see cref="SnapshotGenerations"/> and
/// <see cref="HasInFlightWrite"/> are lock-free advisory reads; their accuracy outside
/// the critical section is intentional and correct by monotonicity of generation counters.</para>
///
/// <para><b>False invalidation contract:</b> the gate may cause a query to re-execute when
/// no write actually changed its result (e.g. a write to a different row in the same
/// keyspace). This is acceptable. What is never acceptable is serving a result whose read
/// set overlaps a keyspace that received a committed write after the scan began.</para>
/// </summary>
public sealed class CachePublishGate
{
    // Monotonically increasing generation counter per keyspace.
    // Bumped inside _writeLock by CommitWrite; never decremented.
    // Read lock-free by SnapshotGenerations (safe by monotonicity).
    private readonly ConcurrentDictionary<string, long> _generations = new(StringComparer.Ordinal);

    // Count of active in-flight write marks per keyspace.
    // Lock-free reads by HasInFlightWrite (advisory only).
    private readonly ConcurrentDictionary<string, int> _inFlight = new(StringComparer.Ordinal);

    // Guards the critical section: generation bump + invalidation + entry insert.
    // CommitWrite and TryPublishUnderGeneration both acquire this lock so they are
    // mutually exclusive — closing the TOCTOU race between check and store.
    private readonly Lock _writeLock = new();

    /// <summary>
    /// Captures the current generation counter for every keyspace in <paramref name="keyspaces"/>.
    /// Keyspaces that have never had a committed write are recorded at generation 0, so any
    /// future commit (which sets generation ≥ 1) will be detected.
    ///
    /// <para>This read is lock-free. It is safe by monotonicity: generation counters only increase,
    /// so a snapshot taken before a write always records a value ≤ the post-write generation.
    /// <see cref="TryPublishUnderGeneration"/> re-checks inside the lock to close the race.</para>
    ///
    /// <para><b>Must be called before <see cref="HasInFlightWrite"/>.</b> See the class-level
    /// "Required read-path call ordering" section for the full ordering rationale.</para>
    ///
    /// <para><b>Keyspace granularity:</b> pass table-bucket strings only
    /// (e.g. <c>"{dbId}:{tableId}:r"</c>). Per-row or per-entry keys will cause unbounded
    /// dictionary growth. See the class-level "Keyspace granularity" note.</para>
    ///
    /// <para>Pass the returned token to <see cref="TryPublishUnderGeneration"/> when the query
    /// finishes. Do not use <see cref="CanPublish"/> as a publish gate — it is advisory only.</para>
    /// </summary>
    public CacheGenerationToken SnapshotGenerations(IReadOnlyCollection<string> keyspaces)
    {
        var snapshot = new Dictionary<string, long>(keyspaces.Count, StringComparer.Ordinal);
        foreach (string ks in keyspaces)
            snapshot[ks] = _generations.GetValueOrDefault(ks, 0L);
        return new CacheGenerationToken(snapshot);
    }

    /// <summary>
    /// Advisory check: returns <c>true</c> if any keyspace in <paramref name="keyspaces"/>
    /// currently has at least one in-flight write mark (a transaction called
    /// <see cref="MarkWriteInFlight"/> but has not yet called <see cref="CommitWrite"/> or
    /// <see cref="AbortWrite"/>). Callers should treat this as
    /// <see cref="QueryCacheBypassReason.InFlightWrite"/> and bypass the cache entirely.
    ///
    /// <para><b>Must be called after <see cref="SnapshotGenerations"/>.</b> If called before,
    /// a write that marks itself in-flight after the check (but before the snapshot) would be
    /// invisible to both checks, letting a query snapshot a generation that the write is about
    /// to supersede. Calling snapshot first means the worst case is a false bypass (the write
    /// commits between the two calls), which is acceptable.</para>
    ///
    /// <para>This check is lock-free and advisory. A write that completes entirely between
    /// <see cref="SnapshotGenerations"/> and this call is handled by the generation fence in
    /// <see cref="TryPublishUnderGeneration"/>.</para>
    /// </summary>
    public bool HasInFlightWrite(IReadOnlyCollection<string> keyspaces)
    {
        foreach (string ks in keyspaces)
        {
            if (_inFlight.TryGetValue(ks, out int count) && count > 0)
                return true;
        }
        return false;
    }

    /// <summary>
    /// Marks <paramref name="keyspaces"/> as having an in-flight write.
    /// Must be called <em>before</em> <c>LocateAndCommitTransaction</c> so concurrent probes
    /// see the mark and bypass rather than racing to publish against an uncommitted write.
    ///
    /// <para>Every call must be paired with exactly one call to either
    /// <see cref="CommitWrite"/> or <see cref="AbortWrite"/> for the same keyspaces.</para>
    /// </summary>
    public void MarkWriteInFlight(IReadOnlyCollection<string> keyspaces)
    {
        foreach (string ks in keyspaces)
            _inFlight.AddOrUpdate(ks, 1, (_, c) => c + 1);
    }

    /// <summary>
    /// Fences <paramref name="keyspaces"/> against every pre-existing cached result and clears their
    /// in-flight marks. Inside <c>_writeLock</c>, bumps the generation counter for each keyspace,
    /// invokes <paramref name="invalidate"/> (so the cache can evict stale entries while no
    /// concurrent publish can insert a new one), then clears the in-flight marks.
    ///
    /// <para>Call this whenever the write <b>may</b> have become visible — that is, both for a
    /// confirmed commit and for an <b>unknown</b> finalize outcome (an unresolved retry, a lost
    /// response, a transport error, or an exception raised after a commit request was issued). Only
    /// a <em>definite</em> non-commit may use <see cref="AbortWrite"/> instead. Fencing a write that
    /// turns out never to have committed costs a false eviction, which the cache contract permits;
    /// skipping the fence for a write that did commit serves stale rows, which it does not.</para>
    ///
    /// <para>Fencing the same keyspaces twice (e.g. once when a finalize is left unresolved and again
    /// when the same handle later resolves) is harmless: generations only move forward and evicting
    /// already-evicted entries is a no-op.</para>
    ///
    /// <para>The <paramref name="invalidate"/> callback is called <em>inside the lock</em>
    /// so eviction and generation bump are atomic with respect to
    /// <see cref="TryPublishUnderGeneration"/>. Any concurrent publish attempt that passed the
    /// advisory <see cref="HasInFlightWrite"/> check but has not yet entered
    /// <see cref="TryPublishUnderGeneration"/> will re-check the generation inside the lock
    /// and be rejected.</para>
    ///
    /// <para><b>Important:</b> the <paramref name="invalidate"/> callback must not acquire
    /// any lock that is also held by the cache's read path — doing so could deadlock if a
    /// read-path lock is held while <see cref="TryPublishUnderGeneration"/> waits for
    /// <c>_writeLock</c>. Keep the callback short.</para>
    /// </summary>
    public void FenceAndInvalidate(
        IReadOnlyCollection<string> keyspaces,
        Action<IReadOnlyCollection<string>>? invalidate = null)
    {
        lock (_writeLock)
        {
            foreach (string ks in keyspaces)
                _generations.AddOrUpdate(ks, 1L, (_, g) => g + 1);

            invalidate?.Invoke(keyspaces);

            foreach (string ks in keyspaces)
                _inFlight.AddOrUpdate(ks, 0, (_, c) => Math.Max(0, c - 1));
        }
    }

    /// <summary>
    /// Confirmed-commit spelling of <see cref="FenceAndInvalidate"/>, kept because a successful
    /// commit is the common case and reads better at the call site. Identical behavior.
    /// </summary>
    public void CommitWrite(
        IReadOnlyCollection<string> keyspaces,
        Action<IReadOnlyCollection<string>>? invalidate = null)
        => FenceAndInvalidate(keyspaces, invalidate);

    /// <summary>
    /// Called after a <b>definite</b> non-commit: a coordinator abort, a confirmed rollback, or a
    /// failure raised before any commit request reached the coordinator. Clears the in-flight marks
    /// without bumping generation counters. Such a transaction's writes were never visible, so no
    /// existing entries need evicting. False invalidations (unnecessary re-executions of concurrent
    /// misses) are acceptable.
    ///
    /// <para><b>Never use this for an unknown outcome.</b> An unresolved retry, a lost or errored
    /// response, or an exception after a commit request was issued may all follow a write that
    /// actually landed; those must go through <see cref="FenceAndInvalidate"/>.</para>
    ///
    /// <para>Does not acquire <c>_writeLock</c> — no cache entry needs to be evicted and
    /// no generation needs updating, so no mutual exclusion with
    /// <see cref="TryPublishUnderGeneration"/> is required.</para>
    /// </summary>
    public void AbortWrite(IReadOnlyCollection<string> keyspaces)
    {
        foreach (string ks in keyspaces)
            _inFlight.AddOrUpdate(ks, 0, (_, c) => Math.Max(0, c - 1));
    }

    /// <summary>
    /// Atomically re-checks the generation token and, if still valid, invokes
    /// <paramref name="storeAction"/> to insert the entry into the cache dictionary.
    ///
    /// <para>Both the re-check and the store happen inside <c>_writeLock</c>, which
    /// <see cref="CommitWrite"/> also holds when bumping generations. This mutual exclusion
    /// closes the TOCTOU race: no write can bump a generation and invalidate entries between
    /// the check and the insert.</para>
    ///
    /// <para>Returns <c>true</c> if the entry was stored; <c>false</c> if any keyspace
    /// generation moved since the snapshot (a write committed during query execution).</para>
    ///
    /// <para><b>This is the only correct way to publish a cache entry.</b> Using
    /// <see cref="CanPublish"/> followed by a separate insert is a TOCTOU race and
    /// must not be done.</para>
    /// </summary>
    public bool TryPublishUnderGeneration(CacheGenerationToken token, Action storeAction)
    {
        lock (_writeLock)
        {
            // Re-check every generation inside the lock so CommitWrite cannot interleave.
            foreach (KeyValuePair<string, long> kv in token.Generations)
            {
                long current = _generations.GetValueOrDefault(kv.Key, 0L);
                if (current != kv.Value)
                    return false;
            }

            storeAction();
            return true;
        }
    }

    /// <summary>
    /// Advisory generation check. Returns <c>true</c> only if every keyspace in
    /// <paramref name="token"/> still has the same generation it had when the token was minted.
    ///
    /// <para><b>WARNING — not sufficient for a safe publish.</b> This method is lock-free,
    /// so a concurrent <see cref="CommitWrite"/> can bump a generation and run
    /// <see cref="CommitWrite"/>'s invalidation callback between this check returning
    /// <c>true</c> and the caller inserting the entry. Use
    /// <see cref="TryPublishUnderGeneration"/> instead, which closes this race by holding
    /// the same lock as <see cref="CommitWrite"/>.</para>
    ///
    /// <para>Intended use: early-exit filtering before expensive serialization work — if
    /// <c>CanPublish</c> already returns <c>false</c>, skip serialization and skip the
    /// <see cref="TryPublishUnderGeneration"/> call entirely. A <c>true</c> result here is
    /// a necessary but not sufficient condition for a successful publish.</para>
    /// </summary>
    public bool CanPublish(CacheGenerationToken token)
    {
        foreach (KeyValuePair<string, long> kv in token.Generations)
        {
            long current = _generations.GetValueOrDefault(kv.Key, 0L);
            if (current != kv.Value)
                return false;
        }
        return true;
    }
}
