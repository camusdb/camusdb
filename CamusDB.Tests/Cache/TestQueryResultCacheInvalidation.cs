
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Tests for commit-safe local cache invalidation.
/// Covers the <see cref="CachePublishGate"/> write protocol
/// (mark in-flight → CommitWrite / AbortWrite), key-bucket extraction,
/// and <see cref="QueryResultCache.InvalidateByModifiedKeys"/> /
/// <see cref="QueryResultCache.InvalidateByTableId"/> /
/// <see cref="QueryResultCache.InvalidateDatabase"/> paths.
///
/// All tests are pure in-memory — no Kahuna, no embedded node.
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheInvalidation
{
    // ───────────────────────────────────────────────────────────���─────────────
    // Helpers
    // ──────────────────────────────────────────────────────��──────────────────

    private static CachedQueryResult MakeResult(string dbId, string fp, IReadOnlyList<QueryResultRow>? rows = null) =>
        new(CacheName: "test", DatabaseId: dbId, Rows: rows ?? [], ResultFingerprint: fp,
            CachedAt: default(HLCTimestamp), Status: QueryCacheStatus.Miss);

    private static (string key, KeyValueDurability durability) RowKey(string dbId, string tableId, string rowId) =>
        ($"{dbId}:{tableId}:r/{rowId}", KeyValueDurability.Persistent);

    private static (string key, KeyValueDurability durability) IndexKey(string dbId, string tableId, string indexId, string encodedKey) =>
        ($"{dbId}:{tableId}:i:{indexId}/{encodedKey}", KeyValueDurability.Persistent);

    private static (string key, KeyValueDurability durability) SchemaKey(string dbId) =>
        ($"{dbId}/meta/schema", KeyValueDurability.Persistent);

    private static CacheGenerationToken SnapshotToken(QueryResultCache cache, params string[] keyspaces) =>
        cache.PublishGate.SnapshotGenerations(keyspaces);

    /// <summary>
    /// Publishes an entry with <paramref name="keyspaces"/> registered as range deps
    /// so <see cref="QueryResultCache.InvalidateByModifiedKeys"/> can find it.
    /// </summary>
    private static async Task<string> PublishEntry(QueryResultCache cache, string dbId, string fp, string[] keyspaces)
    {
        CacheGenerationToken token = SnapshotToken(cache, keyspaces);
        QueryDependencySet deps = new(keyspaces, [], []);
        await cache.TryPublishAsync(MakeResult(dbId, fp), token, deps);
        return fp;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. CachePublishGate write protocol
    // ���───────────────────────���────────────────────────────────────────────────

    /// <summary>
    /// MarkWriteInFlight sets HasInFlightWrite so concurrent probes bypass. CommitWrite bumps
    /// the generation, making any pre-commit token stale. A fresh token taken after CommitWrite
    /// is valid again.
    /// </summary>
    [Test]
    public void PublishGate_InFlightKeyspace_SetsHasInFlightAndCommitMakesTokenStale()
    {
        var gate = new CachePublishGate();
        var keyspaces = new[] { "db1:tbl1:r" };

        // Snapshot before the write starts
        CacheGenerationToken preWriteToken = gate.SnapshotGenerations(keyspaces);

        Assert.That(gate.HasInFlightWrite(keyspaces), Is.False,
            "HasInFlightWrite must return false before any mark");

        gate.MarkWriteInFlight(keyspaces);

        Assert.That(gate.HasInFlightWrite(keyspaces), Is.True,
            "HasInFlightWrite must return true after MarkWriteInFlight");

        // A pre-write token is still valid while in-flight (generation has not changed yet)
        bool stillValidWhileInFlight = gate.TryPublishUnderGeneration(preWriteToken, () => { });
        Assert.That(stillValidWhileInFlight, Is.True,
            "Token taken before MarkWriteInFlight is still valid until CommitWrite bumps the generation");

        bool wasInvalidated = false;
        gate.CommitWrite(keyspaces, ks => wasInvalidated = true);

        Assert.That(wasInvalidated, Is.True, "CommitWrite must call the invalidation action");
        Assert.That(gate.HasInFlightWrite(keyspaces), Is.False,
            "HasInFlightWrite must return false after CommitWrite clears in-flight marks");

        // The pre-write token is now stale because CommitWrite bumped the generation
        bool staleAfterCommit = gate.TryPublishUnderGeneration(preWriteToken, () => { });
        Assert.That(staleAfterCommit, Is.False,
            "Token taken before CommitWrite must be rejected after the generation is bumped");

        // A fresh token after commit is admitted
        CacheGenerationToken freshToken = gate.SnapshotGenerations(keyspaces);
        bool freshAdmitted = gate.TryPublishUnderGeneration(freshToken, () => { });
        Assert.That(freshAdmitted, Is.True, "Token taken after CommitWrite must be admitted");
    }

    [Test]
    public void PublishGate_AbortWrite_ClearsInFlightWithoutBumpingGeneration()
    {
        var gate = new CachePublishGate();
        var keyspaces = new[] { "db1:tbl2:r" };

        CacheGenerationToken token = gate.SnapshotGenerations(keyspaces);
        gate.MarkWriteInFlight(keyspaces);
        gate.AbortWrite(keyspaces);

        // After abort the generation is unchanged — the token taken before mark is still valid
        bool admitted = gate.TryPublishUnderGeneration(token, () => { });
        Assert.That(admitted, Is.True,
            "AbortWrite must not bump the generation; a pre-mark token must still be valid");
    }

    // ─────────���────────────────────────────────────────────────────��──────────
    // 2. InvalidateByModifiedKeys — range and point dep matching
    // ────────────────────────────────────────────────────────��────────────────

    [Test]
    public async Task InvalidateByModifiedKeys_RowWrite_EvictsRangeDepEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);
        string bucket = "db1:tbl1:r";
        string fp = await PublishEntry(cache, "db1", "fp-range", [bucket]);

        // Simulate a row insert in the same bucket
        cache.InvalidateByModifiedKeys([(RowKey("db1", "tbl1", "000000000000000000000001"))]);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Null, "A write to a row key must evict entries with a matching range dep");
    }

    [Test]
    public async Task InvalidateByModifiedKeys_IndexWrite_EvictsIndexRangeDepEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);
        string indexBucket = "db1:tbl1:i:idx001";
        string fp = await PublishEntry(cache, "db1", "fp-index-range", [indexBucket]);

        cache.InvalidateByModifiedKeys([(IndexKey("db1", "tbl1", "idx001", "encoded-key-abc"))]);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Null, "A write to an index key must evict entries with a matching index range dep");
    }

    [Test]
    public async Task InvalidateByModifiedKeys_DifferentTable_PreservesEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);
        string bucket = "db1:tbl1:r";
        string fp = await PublishEntry(cache, "db1", "fp-tbl1", [bucket]);

        // Write to a different table
        cache.InvalidateByModifiedKeys([(RowKey("db1", "tbl2", "000000000000000000000001"))]);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Not.Null, "A write to a different table must not evict unrelated entries");
    }

    [Test]
    public async Task InvalidateByModifiedKeys_SchemaMetaKey_DoesNotEvictRangeEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);
        string bucket = "db1:tbl1:r";
        string fp = await PublishEntry(cache, "db1", "fp-range", [bucket]);

        // Schema key does not match the row bucket pattern
        cache.InvalidateByModifiedKeys([(SchemaKey("db1"))]);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Not.Null, "Schema meta keys must not match row/index bucket patterns");
    }

    // ─────────���─────────────────────────────���──────────────────────���──────────
    // 3. InvalidateByTableId — schema dep eviction
    // ───────────────────���──────────────────────────────��──────────────────────

    [Test]
    public async Task InvalidateByTableId_EvictsSchemaDepEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        QueryDependencySet deps = new([], [], [("tblid001", 1)]);
        CacheGenerationToken token = SnapshotToken(cache);
        string fp = "fp-schema-dep";
        await cache.TryPublishAsync(MakeResult("db1", fp), token, deps);

        cache.InvalidateByTableId("db1", "tblid001");

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Null, "InvalidateByTableId must evict entries with matching schema deps");
    }

    [Test]
    public async Task InvalidateByTableId_DifferentTableId_PreservesEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        QueryDependencySet deps = new([], [], [("tblid001", 1)]);
        CacheGenerationToken token = SnapshotToken(cache);
        string fp = "fp-schema-dep-keep";
        await cache.TryPublishAsync(MakeResult("db1", fp), token, deps);

        cache.InvalidateByTableId("db1", "tblid002");

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Not.Null, "InvalidateByTableId must not evict entries for a different tableId");
    }

    [Test]
    public async Task InvalidateByTableId_DifferentDatabase_PreservesEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        QueryDependencySet deps = new([], [], [("tblid001", 1)]);
        CacheGenerationToken token = SnapshotToken(cache);
        string fp = "fp-schema-dep-db2";
        await cache.TryPublishAsync(MakeResult("db2", fp), token, deps);

        cache.InvalidateByTableId("db1", "tblid001");

        CachedQueryResult? hit = await cache.TryGetAsync("db2", "test", fp);
        Assert.That(hit, Is.Not.Null, "InvalidateByTableId must be scoped to the specified database");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. InvalidateDatabase — full database eviction
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task InvalidateDatabase_EvictsAllEntriesForDatabase()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        CacheGenerationToken t = SnapshotToken(cache);
        await cache.TryPublishAsync(MakeResult("db1", "fp1"), t);
        await cache.TryPublishAsync(MakeResult("db1", "fp2"), t);
        await cache.TryPublishAsync(MakeResult("db2", "fp3"), t);

        cache.InvalidateDatabase("db1");

        Assert.That(await cache.TryGetAsync("db1", "test", "fp1"), Is.Null, "Entry fp1 in db1 must be evicted");
        Assert.That(await cache.TryGetAsync("db1", "test", "fp2"), Is.Null, "Entry fp2 in db1 must be evicted");
        Assert.That(await cache.TryGetAsync("db2", "test", "fp3"), Is.Not.Null, "Entry fp3 in db2 must be preserved");
    }

    // ─────────────────────────────────��───────────────────────────────────────
    // 5. CommitWrite invalidation callback is called with the correct keyspaces
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void CommitWrite_PassesKeyspacesToInvalidationCallback()
    {
        var gate = new CachePublishGate();
        var keyspaces = new[] { "db1:tbl1:r", "db1:tbl1:i:idx001" };
        gate.MarkWriteInFlight(keyspaces);

        IReadOnlyCollection<string>? received = null;
        gate.CommitWrite(keyspaces, ks => received = ks);

        Assert.That(received, Is.Not.Null, "CommitWrite must call the invalidation action");
        Assert.That(received, Is.EquivalentTo(keyspaces), "Invalidation callback must receive the written keyspaces");
    }

    // ────���────────────────────────���───────────────────────────────────────────
    // 6. End-to-end: CommitWrite drives cache InvalidateByModifiedKeys
    // ──────────────────────────────────��──────────────────────────────────���───

    [Test]
    public async Task CommitWrite_ThroughCache_EvictsStaleRangeEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        string bucket = "db1:tbl1:r";
        string fp = await PublishEntry(cache, "db1", "fp-e2e", [bucket]);

        // Simulate the KvTransactionsManager commit protocol:
        var modKeys = new List<(string key, KeyValueDurability durability)>
        {
            RowKey("db1", "tbl1", "000000000000000000000002"),
        };
        var keyspaces = new List<string> { bucket };

        cache.PublishGate.MarkWriteInFlight(keyspaces);
        cache.PublishGate.CommitWrite(keyspaces, _ => cache.InvalidateByModifiedKeys(modKeys));

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Null, "A committed row write must evict the cached range-dep entry");
    }

    [Test]
    public async Task AbortWrite_ThroughCache_PreservesEntry()
    {
        using var cache = new QueryResultCache(CamusDBOptions.Default, sweepIntervalMs: -1);

        string bucket = "db1:tbl1:r";
        string fp = await PublishEntry(cache, "db1", "fp-abort", [bucket]);

        var keyspaces = new List<string> { bucket };
        cache.PublishGate.MarkWriteInFlight(keyspaces);
        // Abort: no CommitWrite, so no invalidation
        cache.PublishGate.AbortWrite(keyspaces);

        CachedQueryResult? hit = await cache.TryGetAsync("db1", "test", fp);
        Assert.That(hit, Is.Not.Null, "An aborted write must not evict cached entries");
    }
}
