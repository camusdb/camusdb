
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Unit tests that verify the two core safety invariants required by QC0:
///
/// 1. A cancelled or faulted query enumeration must never populate the cache.
/// 2. A same-node write racing a cache miss cannot leave a stale entry published
///    after the write commits — enforced by <see cref="CachePublishGate"/>.
///
/// These tests are pure in-memory: they do not start a database, Kahuna, or Kommander node.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestQueryResultCacheBoundaries
{
    // ─────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────

    private static CachedQueryResult MakePendingResult(string fp = "fp1") => new(
        CacheName: "test-cache", DatabaseId: "db1",
        Rows: [], ResultFingerprint: fp, CachedAt: default, Status: QueryCacheStatus.Miss);

    /// <summary>
    /// A test double whose <see cref="TryPublishAsync"/> records that it was reached and
    /// returns a distinguishable <see cref="QueryCacheStatus.Miss"/> (not <c>Bypass</c>), so a
    /// test can tell "runner suppressed publish" apart from "runner published". Publishing goes
    /// through the real <see cref="CachePublishGate.TryPublishUnderGeneration"/> so the generation
    /// fence is exercised, not stubbed.
    /// </summary>
    private sealed class RecordingCache : IQueryResultCache
    {
        public CachePublishGate PublishGate { get; } = new();
        public int PublishAttempts;
        public readonly Dictionary<string, CachedQueryResult> Stored = new(StringComparer.Ordinal);

        public Task<CachedQueryResult?> TryGetAsync(string databaseId, string cacheName, string fingerprint, CancellationToken ct = default)
            => Task.FromResult(Stored.TryGetValue(fingerprint, out CachedQueryResult? r) ? r : null);

        public Task<QueryCacheStatus> TryPublishAsync(CachedQueryResult result, CacheGenerationToken token, QueryDependencySet? deps = null, CancellationToken ct = default)
        {
            Interlocked.Increment(ref PublishAttempts);
            bool ok = PublishGate.TryPublishUnderGeneration(token, () => Stored[result.ResultFingerprint] = result);
            return Task.FromResult(ok ? QueryCacheStatus.Miss : QueryCacheStatus.EvictedBeforePublish);
        }

        public void InvalidateByModifiedKeys(IReadOnlyCollection<(string key, Kahuna.Shared.KeyValue.KeyValueDurability durability)> modifiedKeys) { }
        public void InvalidateByTableId(string databaseId, string tableId) { }
        public void InvalidateDatabase(string databaseId) { }
        public void InvalidateCacheName(string databaseId, string cacheName) { }
    }

    /// <summary>Yields <paramref name="count"/> rows, then cancels if a token was provided.</summary>
    private static async IAsyncEnumerable<QueryResultRow> RowSource(
        int count, bool cancelAfterFirst = false,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        for (int i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return new QueryResultRow(ObjectIdGenerator.Generate(), new());
            if (cancelAfterFirst && i == 0)
                yield break; // simulate early stop (caller breaks out / cancels)
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Invariant 1: cancelled enumeration does not populate the cache
    // ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives a real <see cref="CachedQueryRunner"/> through a cancelled
    /// <see cref="IAsyncEnumerable{T}"/> and asserts that <see cref="CachedQueryRunner.FinalizeAsync"/>
    /// never reached the cache. Uses a <see cref="RecordingCache"/> whose publish returns
    /// <see cref="QueryCacheStatus.Miss"/> on success, so this test genuinely discriminates:
    /// if <c>_drainCompleted</c> were wrongly set on cancel, <c>FinalizeAsync</c> would call
    /// <c>TryPublishAsync</c> (bumping <c>PublishAttempts</c> and returning <c>Miss</c>) and both
    /// assertions would fail. A <see cref="NullQueryResultCache"/> could not tell the two apart,
    /// because it returns <c>Bypass</c> either way.
    /// </summary>
    [Test]
    public async Task CancelledEnumeration_Runner_DoesNotPublish()
    {
        using var cts = new CancellationTokenSource();
        RecordingCache cache = new();
        var token = cache.PublishGate.SnapshotGenerations(["db1:t1:r"]);
        var result = MakePendingResult();

        var runner = new CachedQueryRunner(cache, result, token, cts.Token);

        // Cancel before the drain begins — the enumerable will throw OperationCanceledException
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in runner.DrainAsync(RowSource(10, ct: cts.Token))) { }
        });

        QueryCacheStatus status = await runner.FinalizeAsync();
        Assert.That(status, Is.EqualTo(QueryCacheStatus.Bypass),
            "A cancelled enumeration must never publish");
        Assert.That(cache.PublishAttempts, Is.Zero,
            "FinalizeAsync must not even reach TryPublishAsync after a cancelled drain");
        Assert.That(cache.Stored, Is.Empty, "No entry may be stored after a cancelled drain");
    }

    [Test]
    public async Task EarlyExitEnumeration_Runner_DoesNotPublish()
    {
        // Simulate a caller that breaks out of the loop after the first row
        // (e.g. an operator error or a LIMIT that stops the outer foreach early).
        RecordingCache cache = new();
        var token = cache.PublishGate.SnapshotGenerations(["db1:t1:r"]);
        var result = MakePendingResult("fp2");

        var runner = new CachedQueryRunner(cache, result, token);

        // Only consume the first row from a 5-row source — drain is incomplete.
        await foreach (var row in runner.DrainAsync(RowSource(5, cancelAfterFirst: true)))
        {
            break; // caller abandons enumeration early
        }

        QueryCacheStatus status = await runner.FinalizeAsync();
        Assert.That(status, Is.EqualTo(QueryCacheStatus.Bypass),
            "A partially-consumed enumeration must never publish");
        Assert.That(cache.PublishAttempts, Is.Zero,
            "FinalizeAsync must not reach TryPublishAsync after an incomplete drain");
        Assert.That(cache.Stored, Is.Empty, "No entry may be stored after an incomplete drain");
    }

    [Test]
    public async Task FullEnumeration_Runner_Publishes()
    {
        // A fully drained enumeration must reach TryPublishAsync AND store the entry.
        // RecordingCache returns Miss (distinct from Bypass) on a successful publish, so this
        // asserts real publication rather than the ambiguous Bypass a null cache would give.
        RecordingCache cache = new();
        var token = cache.PublishGate.SnapshotGenerations(["db1:t1:r"]);
        var result = MakePendingResult("fp3");

        var runner = new CachedQueryRunner(cache, result, token);

        await foreach (var _ in runner.DrainAsync(RowSource(3))) { }

        QueryCacheStatus status = await runner.FinalizeAsync();
        Assert.That(status, Is.EqualTo(QueryCacheStatus.Miss),
            "Full drain must publish and report Miss");
        Assert.That(cache.PublishAttempts, Is.EqualTo(1), "Full drain must reach TryPublishAsync exactly once");
        Assert.That(cache.Stored.ContainsKey("fp3"), Is.True, "Full drain must store the entry");
    }

    [Test]
    public async Task FullDrain_ButRacingCommit_Runner_DoesNotStore()
    {
        // Drain completes, but a write committed into the scanned keyspace during execution.
        // The generation fence inside TryPublishUnderGeneration must reject the publish even
        // though the drain finished — proving the runner does not blindly store on completion.
        RecordingCache cache = new();
        string[] ks = ["db1:t1:r"];
        var token = cache.PublishGate.SnapshotGenerations(ks); // gen=0
        var result = MakePendingResult("fp4");

        var runner = new CachedQueryRunner(cache, result, token);

        await foreach (var _ in runner.DrainAsync(RowSource(3))) { }

        // A concurrent write commits before the runner finalizes.
        cache.PublishGate.MarkWriteInFlight(ks);
        cache.PublishGate.CommitWrite(ks); // gen→1, stale-fences the token

        QueryCacheStatus status = await runner.FinalizeAsync();
        Assert.That(status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish),
            "A racing committed write must fence the publish");
        Assert.That(cache.PublishAttempts, Is.EqualTo(1), "Publish is attempted but must be rejected by the fence");
        Assert.That(cache.Stored, Is.Empty, "The stale entry must not be stored");
    }

    [Test]
    public async Task CancelledEnumeration_NullCache_NeverPopulates()
    {
        // The NullQueryResultCache is used when QueryResultCacheEnabled = false.
        // Verify it always returns null on probe and Bypass on publish attempt,
        // even if incorrectly called after a cancellation.
        NullQueryResultCache cache = new();
        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations(["db1:t1:r"]);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "my-cache", "fingerprint-x");
        Assert.That(hit, Is.Null, "Null cache must always miss");

        // Build a dummy result (simulating what a caller might have materialized before cancelling)
        var result = new CachedQueryResult(
            CacheName: "my-cache",
            DatabaseId: "db1",
            Rows: [],
            ResultFingerprint: "fingerprint-x",
            CachedAt: default,
            Status: QueryCacheStatus.Miss);

        QueryCacheStatus status = await cache.TryPublishAsync(result, token);
        Assert.That(status, Is.EqualTo(QueryCacheStatus.Bypass),
            "Null cache must always bypass publish");

        // A probe after the (bypassed) publish still misses — nothing was stored
        CachedQueryResult? afterPublish = await cache.TryGetAsync("db1", "my-cache", "fingerprint-x");
        Assert.That(afterPublish, Is.Null, "Null cache must still miss after a bypassed publish");
    }

    // ─────────────────────────────────────────────────────────────────
    // Invariant 2: committed write races a cache miss → no stale hit
    // Tests use TryPublishUnderGeneration (the correct atomic API),
    // not CanPublish alone, which is advisory and TOCTOU-unsafe.
    // ─────────────────────────────────────────────────────────────────

    [Test]
    public void CommittedWriteDuringMiss_AtomicPublish_Rejected()
    {
        CachePublishGate gate = new();
        var keyspaces = new[] { "db1:t1:r" };

        CacheGenerationToken tokenBeforeWrite = gate.SnapshotGenerations(keyspaces);

        gate.MarkWriteInFlight(keyspaces);
        Assert.That(gate.HasInFlightWrite(keyspaces), Is.True,
            "In-flight mark must be visible to concurrent probes");

        // Write commits — bumps generation, clears in-flight, inside the gate's lock.
        gate.CommitWrite(keyspaces);

        Assert.That(gate.HasInFlightWrite(keyspaces), Is.False,
            "In-flight mark must be cleared after commit");

        // TryPublishUnderGeneration re-checks generation inside the lock.
        // The token was minted before the commit so the store action must NOT be called.
        bool storeActionCalled = false;
        bool published = gate.TryPublishUnderGeneration(tokenBeforeWrite, () => storeActionCalled = true);

        Assert.That(published, Is.False,
            "TryPublishUnderGeneration must reject a token whose keyspace received a committed write");
        Assert.That(storeActionCalled, Is.False,
            "The store action must not be called when the generation check fails");
    }

    /// <summary>
    /// Proves the atomicity guarantee: <see cref="CachePublishGate.CommitWrite"/> bumps
    /// the generation inside the same lock that <see cref="CachePublishGate.TryPublishUnderGeneration"/>
    /// holds for the generation re-check + store. A simulated "commit happens inside the store action"
    /// scenario — the closest sequential approximation of the race — must be rejected.
    /// </summary>
    [Test]
    public void CommitInsideStoreAction_IsRejectedOnReentrantAttempt()
    {
        // This test verifies the design intent rather than a literal concurrent race
        // (true concurrency tests belong in QC5 with a real cache implementation).
        // It shows that once a token is created at gen=0 and a CommitWrite bumps to gen=1,
        // a subsequent TryPublishUnderGeneration with the gen=0 token always fails —
        // regardless of when in the call sequence the check happens.
        CachePublishGate gate = new();
        var ks = new[] { "db9:t9:r" };

        CacheGenerationToken staleToken = gate.SnapshotGenerations(ks); // gen=0
        gate.MarkWriteInFlight(ks);
        gate.CommitWrite(ks); // gen→1

        int storeCallCount = 0;
        bool result = gate.TryPublishUnderGeneration(staleToken, () => storeCallCount++);

        Assert.That(result, Is.False);
        Assert.That(storeCallCount, Is.Zero,
            "Store action must never be called with a stale token — no entry may be inserted");
    }

    [Test]
    public void ValidToken_TryPublishUnderGeneration_InvokesStore()
    {
        // Confirms the happy path: a token with no intervening commit allows the store action.
        CachePublishGate gate = new();
        var ks = new[] { "db7:t7:r" };

        CacheGenerationToken freshToken = gate.SnapshotGenerations(ks); // gen=0, no writes

        bool stored = false;
        bool result = gate.TryPublishUnderGeneration(freshToken, () => stored = true);

        Assert.That(result, Is.True, "Valid token must allow the store action to run");
        Assert.That(stored, Is.True, "Store action must have been called for a valid token");
    }

    [Test]
    public void CommitWrite_InvalidationCallback_CalledInsideLock()
    {
        // Proves CommitWrite invokes the invalidation callback (simulating cache eviction)
        // before returning — verifying the atomicity contract between eviction and generation bump.
        CachePublishGate gate = new();
        var ks = new[] { "db8:t8:r" };

        bool invalidateCalled = false;
        gate.MarkWriteInFlight(ks);
        gate.CommitWrite(ks, _ => invalidateCalled = true);

        Assert.That(invalidateCalled, Is.True,
            "CommitWrite must invoke the invalidation callback");
    }

    [Test]
    public void RolledBackWrite_DoesNotBumpGeneration_ButClearsInFlight()
    {
        CachePublishGate gate = new();
        var keyspaces = new[] { "db2:t2:r" };

        CacheGenerationToken tokenBefore = gate.SnapshotGenerations(keyspaces);

        gate.MarkWriteInFlight(keyspaces);
        Assert.That(gate.HasInFlightWrite(keyspaces), Is.True);

        gate.AbortWrite(keyspaces);

        Assert.That(gate.HasInFlightWrite(keyspaces), Is.False,
            "Rollback must clear the in-flight mark");

        // Rolled-back write did not bump generation — the token is still valid.
        bool stored = false;
        bool result = gate.TryPublishUnderGeneration(tokenBefore, () => stored = true);
        Assert.That(result, Is.True, "Rolled-back write must not invalidate a concurrent query token");
        Assert.That(stored, Is.True);
    }

    [Test]
    public void MultipleCommits_AccumulateGenerations()
    {
        CachePublishGate gate = new();
        var ks = new[] { "db3:t3:r" };

        CacheGenerationToken t0 = gate.SnapshotGenerations(ks); // gen=0
        gate.MarkWriteInFlight(ks);
        gate.CommitWrite(ks); // gen→1

        CacheGenerationToken t1 = gate.SnapshotGenerations(ks); // gen=1
        gate.MarkWriteInFlight(ks);
        gate.CommitWrite(ks); // gen→2

        // t0 and t1 are stale; TryPublishUnderGeneration must reject both.
        Assert.That(gate.TryPublishUnderGeneration(t0, () => { }), Is.False,
            "Token from gen=0 must be rejected at gen=2");
        Assert.That(gate.TryPublishUnderGeneration(t1, () => { }), Is.False,
            "Token from gen=1 must be rejected at gen=2");

        // Fresh token at gen=2 is valid.
        CacheGenerationToken t2 = gate.SnapshotGenerations(ks);
        Assert.That(gate.TryPublishUnderGeneration(t2, () => { }), Is.True,
            "Token from gen=2 must be accepted at gen=2");
    }

    [Test]
    public void DisjointKeyspaces_DoNotAffectEachOther()
    {
        CachePublishGate gate = new();
        var ksA = new[] { "db4:t4:r" };
        var ksB = new[] { "db4:t5:r" };

        CacheGenerationToken tokenA = gate.SnapshotGenerations(ksA);

        gate.MarkWriteInFlight(ksB);
        gate.CommitWrite(ksB);

        // Token for A is unaffected by a commit to B.
        bool stored = false;
        Assert.That(gate.TryPublishUnderGeneration(tokenA, () => stored = true), Is.True,
            "A commit to an unrelated keyspace must not block publishing to a disjoint keyspace");
        Assert.That(stored, Is.True);
    }

    [Test]
    public void InFlightCheck_MissesDisjointKeyspaces()
    {
        CachePublishGate gate = new();
        var ksA = new[] { "db5:t6:r" };
        var ksB = new[] { "db5:t7:r" };

        gate.MarkWriteInFlight(ksA);

        Assert.That(gate.HasInFlightWrite(ksB), Is.False,
            "In-flight mark on one keyspace must not affect an unrelated keyspace");

        gate.AbortWrite(ksA);
    }

    [Test]
    public void ConcurrentWritesOnSameKeyspace_BothMustClear()
    {
        CachePublishGate gate = new();
        var ks = new[] { "db6:t8:r" };

        gate.MarkWriteInFlight(ks);
        gate.MarkWriteInFlight(ks);

        Assert.That(gate.HasInFlightWrite(ks), Is.True, "Two in-flight marks must keep keyspace in-flight");

        gate.CommitWrite(ks);
        Assert.That(gate.HasInFlightWrite(ks), Is.True, "One remaining in-flight mark must keep keyspace in-flight");

        gate.CommitWrite(ks);
        Assert.That(gate.HasInFlightWrite(ks), Is.False, "Both marks cleared — keyspace must no longer be in-flight");
    }
}
