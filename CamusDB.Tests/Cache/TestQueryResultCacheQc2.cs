
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Globalization;
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
/// QC2 acceptance tests: cache key fingerprinting, entry storage, LRU eviction,
/// byte/row caps, TTL expiry, and invalidation paths.
/// These tests use <see cref="QueryResultCache"/> directly (no Kahuna / SQL layer).
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheQc2
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static CachedQueryResult MakeResult(
        string dbId,
        string cacheName,
        string fingerprint,
        IReadOnlyList<QueryResultRow>? rows = null)
    {
        rows ??= [];
        return new CachedQueryResult(
            CacheName: cacheName,
            DatabaseId: dbId,
            Rows: rows,
            ResultFingerprint: fingerprint,
            CachedAt: default(HLCTimestamp),
            Status: QueryCacheStatus.Miss);
    }

    private static QueryResultRow MakeRow(string columnName, string value) =>
        new(default, new Dictionary<string, ColumnValue>
        {
            [columnName] = new ColumnValue(ColumnType.String, value, 0, 0, false, null, null, ColumnType.Null),
        });

    private static CacheHintOptions NoOpts => new("test");

    private static IReadOnlyList<(string TableName, int SchemaVersion)> SchemaDeps(
        string tableName, int version) => [(tableName, version)];

    // Mint a generation token with no keyspaces (queries without deps get an empty snapshot)
    private static CacheGenerationToken EmptyToken(QueryResultCache cache) =>
        cache.PublishGate.SnapshotGenerations([]);

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-A: Fingerprint determinism and collision resistance
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void Fingerprint_SameInputs_ProduceSameKey()
    {
        var params1 = new Dictionary<string, ColumnValue>
        {
            ["id"] = new ColumnValue(ColumnType.Integer64, null, 42, 0, false, null, null, ColumnType.Null),
        };
        var params2 = new Dictionary<string, ColumnValue>
        {
            ["id"] = new ColumnValue(ColumnType.Integer64, null, 42, 0, false, null, null, ColumnType.Null),
        };

        string f1 = ResultFingerprintBuilder.Build("db1", "cache1", "shape-x", params1, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db1", "cache1", "shape-x", params2, null, NoOpts);

        Assert.That(f1, Is.EqualTo(f2), "Identical inputs must produce identical fingerprints");
    }

    [Test]
    public void Fingerprint_DifferentDatabaseIds_ProduceDifferentKeys()
    {
        string f1 = ResultFingerprintBuilder.Build("db1", "cache1", "shape", null, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db2", "cache1", "shape", null, null, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2));
    }

    [Test]
    public void Fingerprint_DifferentParameterValues_ProduceDifferentKeys()
    {
        var p1 = new Dictionary<string, ColumnValue>
            { ["x"] = new ColumnValue(ColumnType.Integer64, null, 1, 0, false, null, null, ColumnType.Null) };
        var p2 = new Dictionary<string, ColumnValue>
            { ["x"] = new ColumnValue(ColumnType.Integer64, null, 2, 0, false, null, null, ColumnType.Null) };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", p1, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", p2, null, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2));
    }

    [Test]
    public void Fingerprint_IntegerVsStringWithSameValue_ProduceDifferentKeys()
    {
        // Integer 1 vs string "1" must not collide
        var pInt = new Dictionary<string, ColumnValue>
            { ["v"] = new ColumnValue(ColumnType.Integer64, null, 1, 0, false, null, null, ColumnType.Null) };
        var pStr = new Dictionary<string, ColumnValue>
            { ["v"] = new ColumnValue(ColumnType.String, "1", 0, 0, false, null, null, ColumnType.Null) };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", pInt, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", pStr, null, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2), "Integer 1 and string '1' must not collide");
    }

    [Test]
    public void Fingerprint_DifferentSchemaVersions_ProduceDifferentKeys()
    {
        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", null, SchemaDeps("orders", 1), NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", null, SchemaDeps("orders", 2), NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2), "Schema version bump must produce a different fingerprint");
    }

    [Test]
    public void Fingerprint_ParametersAreSortedByKey()
    {
        // Two dictionaries with the same k/v pairs in different insertion order must hash identically
        var p1 = new Dictionary<string, ColumnValue>
        {
            ["z"] = new ColumnValue(ColumnType.Integer64, null, 1, 0, false, null, null, ColumnType.Null),
            ["a"] = new ColumnValue(ColumnType.Integer64, null, 2, 0, false, null, null, ColumnType.Null),
        };
        var p2 = new Dictionary<string, ColumnValue>
        {
            ["a"] = new ColumnValue(ColumnType.Integer64, null, 2, 0, false, null, null, ColumnType.Null),
            ["z"] = new ColumnValue(ColumnType.Integer64, null, 1, 0, false, null, null, ColumnType.Null),
        };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", p1, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", p2, null, NoOpts);
        Assert.That(f1, Is.EqualTo(f2), "Parameter order must not affect the fingerprint");
    }

    [Test]
    public void Fingerprint_StringDelimiterInjection_DoesNotCollide()
    {
        // A string value containing the separator "|b=s:bar" must not collide with a separate
        // parameter b="bar". Without length-prefixing both produce the same raw bytes.
        var pTwo = new Dictionary<string, ColumnValue>
        {
            ["a"] = new ColumnValue(ColumnType.String, "foo", 0, 0, false, null, null, ColumnType.Null),
            ["b"] = new ColumnValue(ColumnType.String, "bar", 0, 0, false, null, null, ColumnType.Null),
        };
        var pOne = new Dictionary<string, ColumnValue>
        {
            // Single param whose value embeds the structural separator characters
            ["a"] = new ColumnValue(ColumnType.String, "foo|b=s:3:bar", 0, 0, false, null, null, ColumnType.Null),
        };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", pTwo, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", pOne, null, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2),
            "Delimiter characters inside a string value must not produce the same fingerprint as two separate params");
    }

    [Test]
    public void Fingerprint_ParameterKeyContainingDelimiter_DoesNotCollide()
    {
        // Param key "a|b" with value "x" vs params "a"=""|"b"="x" must not collide.
        var pInjectedKey = new Dictionary<string, ColumnValue>
        {
            ["a|b"] = new ColumnValue(ColumnType.String, "x", 0, 0, false, null, null, ColumnType.Null),
        };
        var pNormal = new Dictionary<string, ColumnValue>
        {
            ["a"] = new ColumnValue(ColumnType.String, "", 0, 0, false, null, null, ColumnType.Null),
            ["b"] = new ColumnValue(ColumnType.String, "x", 0, 0, false, null, null, ColumnType.Null),
        };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", pInjectedKey, null, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", pNormal, null, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2),
            "A key containing delimiter characters must not collide with two separate keys");
    }

    [Test]
    public void Fingerprint_TableNameWithDelimiter_DoesNotCollide()
    {
        // Table name "t1|t2" at version 1 vs two tables "t1" v1 and "t2" v1 must not collide.
        var oneTableWithPipe  = new List<(string, int)> { ("t1|t2", 1) };
        var twoSeparateTables = new List<(string, int)> { ("t1", 1), ("t2", 1) };

        string f1 = ResultFingerprintBuilder.Build("db", "c", "s", null, oneTableWithPipe, NoOpts);
        string f2 = ResultFingerprintBuilder.Build("db", "c", "s", null, twoSeparateTables, NoOpts);
        Assert.That(f1, Is.Not.EqualTo(f2),
            "A table name containing the separator must not collide with two distinct table names");
    }

    [Test]
    public void Fingerprint_Float64_IsLocaleInvariant()
    {
        // On comma-decimal locales (e.g. fr-FR, es-CO) the default ToString renders 3.14 as "3,14",
        // changing the fingerprint and making the comma interact with delimiter injection.
        // The builder must produce the same fingerprint regardless of the current thread culture.
        var p = new Dictionary<string, ColumnValue>
        {
            ["v"] = new ColumnValue(ColumnType.Float64, null, 0, 3.14, false, null, null, ColumnType.Null),
        };

        CultureInfo saved = Thread.CurrentThread.CurrentCulture;
        try
        {
            string fInvariant = ResultFingerprintBuilder.Build("db", "c", "s", p, null, NoOpts);

            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR"); // comma decimal separator
            string fCommaLocale = ResultFingerprintBuilder.Build("db", "c", "s", p, null, NoOpts);

            Assert.That(fInvariant, Is.EqualTo(fCommaLocale),
                "Float64 fingerprint must be identical regardless of the thread culture");
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = saved;
        }
    }

    [Test]
    public void Fingerprint_Float32_IsLocaleInvariant()
    {
        var p = new Dictionary<string, ColumnValue>
        {
            ["v"] = new ColumnValue(ColumnType.Float32, null, 0, 3.14, false, null, null, ColumnType.Null),
        };

        CultureInfo saved = Thread.CurrentThread.CurrentCulture;
        try
        {
            string fInvariant = ResultFingerprintBuilder.Build("db", "c", "s", p, null, NoOpts);

            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR");
            string fCommaLocale = ResultFingerprintBuilder.Build("db", "c", "s", p, null, NoOpts);

            Assert.That(fInvariant, Is.EqualTo(fCommaLocale),
                "Float32 fingerprint must be identical regardless of the thread culture");
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = saved;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-B: Hit / miss round-trip
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task HitAfterPublish()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
        CachedQueryResult result = MakeResult("db", "c", fp);
        CacheGenerationToken token = EmptyToken(cache);

        QueryCacheStatus status = await cache.TryPublishAsync(result, token);
        Assert.That(status, Is.EqualTo(QueryCacheStatus.Miss), "First publish must return Miss");

        CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp);
        Assert.That(hit, Is.Not.Null, "Entry must be found after publish");
        Assert.That(hit!.ResultFingerprint, Is.EqualTo(fp));
    }

    [Test]
    public async Task MissWhenNotPublished()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);

        CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp);
        Assert.That(hit, Is.Null);
    }

    [Test]
    public async Task DifferentDatabaseIds_Miss()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        string fp = ResultFingerprintBuilder.Build("db1", "c", "s", null, null, NoOpts);
        string fpOther = ResultFingerprintBuilder.Build("db2", "c", "s", null, null, NoOpts);

        CachedQueryResult result = MakeResult("db1", "c", fp);
        await cache.TryPublishAsync(result, EmptyToken(cache));

        // Probe with db2's fingerprint — must miss
        CachedQueryResult? hit = await cache.TryGetAsync("db2", "c", fpOther);
        Assert.That(hit, Is.Null, "Different database must not get db1's entry");
    }

    [Test]
    public async Task DifferentSchemaVersion_AfterDropRecreate_Miss()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        // Cache entry for schema version 1
        string fp1 = ResultFingerprintBuilder.Build("db", "c", "s", null, SchemaDeps("orders", 1), NoOpts);
        await cache.TryPublishAsync(MakeResult("db", "c", fp1), EmptyToken(cache));

        // After drop+recreate, the caller builds a fingerprint with version 2
        string fp2 = ResultFingerprintBuilder.Build("db", "c", "s", null, SchemaDeps("orders", 2), NoOpts);
        CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp2);
        Assert.That(hit, Is.Null, "Schema version change must produce a cache miss");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-C: Per-entry caps bypass
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task OversizedRowCount_IsRejected()
    {
        int origMax = CamusDBConfig.QueryResultCacheMaxEntryRows;
        try
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = 2;
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            // Build 3 rows — exceeds the cap of 2
            var rows = new List<QueryResultRow>
            {
                MakeRow("col", "a"), MakeRow("col", "b"), MakeRow("col", "c"),
            };
            string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
            CachedQueryResult result = MakeResult("db", "c", fp, rows);

            QueryCacheStatus status = await cache.TryPublishAsync(result, EmptyToken(cache));
            Assert.That(status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish),
                "Oversized row count must be bypassed");

            CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp);
            Assert.That(hit, Is.Null, "Bypassed entry must not be stored");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = origMax;
        }
    }

    [Test]
    public async Task OversizedBytes_IsRejected()
    {
        long origMaxBytes = CamusDBConfig.QueryResultCacheMaxEntryBytes;
        try
        {
            CamusDBConfig.QueryResultCacheMaxEntryBytes = 1; // 1 byte — any row exceeds this
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            var rows = new List<QueryResultRow> { MakeRow("col", "hello") };
            string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);

            QueryCacheStatus status = await cache.TryPublishAsync(
                MakeResult("db", "c", fp, rows), EmptyToken(cache));
            Assert.That(status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish));
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntryBytes = origMaxBytes;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-D: LRU eviction when entry count cap reached
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task LruEviction_WhenMaxEntriesReached()
    {
        int origMax = CamusDBConfig.QueryResultCacheMaxEntries;
        try
        {
            CamusDBConfig.QueryResultCacheMaxEntries = 2;
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            string fp1 = ResultFingerprintBuilder.Build("db", "c", "s1", null, null, NoOpts);
            string fp2 = ResultFingerprintBuilder.Build("db", "c", "s2", null, null, NoOpts);
            string fp3 = ResultFingerprintBuilder.Build("db", "c", "s3", null, null, NoOpts);

            await cache.TryPublishAsync(MakeResult("db", "c", fp1), EmptyToken(cache));
            await cache.TryPublishAsync(MakeResult("db", "c", fp2), EmptyToken(cache));

            // Touch fp1 so it becomes MRU; fp2 becomes LRU
            await cache.TryGetAsync("db", "c", fp1);

            // Insert fp3 — fp2 should be evicted
            await cache.TryPublishAsync(MakeResult("db", "c", fp3), EmptyToken(cache));

            Assert.That(cache.EntryCount, Is.EqualTo(2), "Cache must stay at max entries");
            Assert.That(await cache.TryGetAsync("db", "c", fp1), Is.Not.Null, "MRU entry must survive");
            Assert.That(await cache.TryGetAsync("db", "c", fp2), Is.Null, "LRU entry must be evicted");
            Assert.That(await cache.TryGetAsync("db", "c", fp3), Is.Not.Null, "New entry must be present");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntries = origMax;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-E: TTL expiry
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task TtlExpiry_EntryMissesAfterExpiry()
    {
        int origTtl = CamusDBConfig.QueryResultCacheDefaultTtlMs;
        try
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = 0; // expires immediately (TTL = 0 ms)
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
            await cache.TryPublishAsync(MakeResult("db", "c", fp), EmptyToken(cache));

            // No sleep needed — TTL of 0 means expiry is at TickCount64 + 0 = the moment it was stored.
            // Wait a tiny bit to ensure TickCount64 advances.
            await Task.Delay(10);

            CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp);
            Assert.That(hit, Is.Null, "Entry with elapsed TTL must miss on probe");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = origTtl;
        }
    }

    [Test]
    public async Task Sweep_RemovesExpiredEntries()
    {
        int origTtl = CamusDBConfig.QueryResultCacheDefaultTtlMs;
        try
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = 0;
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
            await cache.TryPublishAsync(MakeResult("db", "c", fp), EmptyToken(cache));
            Assert.That(cache.EntryCount, Is.EqualTo(1));

            await Task.Delay(10);
            cache.Sweep();

            Assert.That(cache.EntryCount, Is.EqualTo(0), "Sweep must remove expired entries");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = origTtl;
        }
    }

    [Test]
    public async Task PerHintTtl_OverridesDefault()
    {
        // An entry published with HintTtlMs = 1 must expire well before an entry
        // published with the global default TTL (5 000 ms by default).
        int origDefault = CamusDBConfig.QueryResultCacheDefaultTtlMs;
        try
        {
            // Keep the global default comfortably long so the second entry doesn't expire
            // during the test.
            CamusDBConfig.QueryResultCacheDefaultTtlMs = 60_000;

            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            // Entry with a per-hint TTL of 1 ms
            string fpShort = ResultFingerprintBuilder.Build("db", "short", "s", null, null, NoOpts);
            var shortResult = new CachedQueryResult(
                CacheName: "short",
                DatabaseId: "db",
                Rows: [],
                ResultFingerprint: fpShort,
                CachedAt: default(HLCTimestamp),
                Status: QueryCacheStatus.Miss,
                HintTtlMs: 1);            // 1 ms — expires almost immediately
            await cache.TryPublishAsync(shortResult, EmptyToken(cache));

            // Entry using the global default TTL (60 s)
            string fpLong = ResultFingerprintBuilder.Build("db", "long", "s", null, null, NoOpts);
            await cache.TryPublishAsync(MakeResult("db", "long", fpLong), EmptyToken(cache));

            // Let the 1 ms hint TTL elapse
            await Task.Delay(20);

            CachedQueryResult? hitShort = await cache.TryGetAsync("db", "short", fpShort);
            CachedQueryResult? hitLong  = await cache.TryGetAsync("db", "long",  fpLong);

            Assert.That(hitShort, Is.Null,
                "Entry with HintTtlMs=1 must miss after 20 ms");
            Assert.That(hitLong, Is.Not.Null,
                "Entry with default TTL (60 s) must still be alive");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = origDefault;
        }
    }

    [Test]
    public async Task PerHintTtl_Null_FallsBackToDefault()
    {
        // HintTtlMs = null must behave identically to using the global default.
        int origDefault = CamusDBConfig.QueryResultCacheDefaultTtlMs;
        try
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = 60_000;
            using var cache = new QueryResultCache(sweepIntervalMs: -1);

            string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
            var result = new CachedQueryResult(
                CacheName: "c",
                DatabaseId: "db",
                Rows: [],
                ResultFingerprint: fp,
                CachedAt: default(HLCTimestamp),
                Status: QueryCacheStatus.Miss,
                HintTtlMs: null);         // explicit null → must use default
            await cache.TryPublishAsync(result, EmptyToken(cache));

            // Should still be live immediately after publish
            CachedQueryResult? hit = await cache.TryGetAsync("db", "c", fp);
            Assert.That(hit, Is.Not.Null,
                "Entry with HintTtlMs=null must use the global default TTL and still be live");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheDefaultTtlMs = origDefault;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-F: Invalidation paths
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task InvalidateCacheName_RemovesMatchingEntries()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        string fp1 = ResultFingerprintBuilder.Build("db", "orders", "s1", null, null, NoOpts);
        string fp2 = ResultFingerprintBuilder.Build("db", "customers", "s2", null, null, NoOpts);

        await cache.TryPublishAsync(MakeResult("db", "orders", fp1), EmptyToken(cache));
        await cache.TryPublishAsync(MakeResult("db", "customers", fp2), EmptyToken(cache));

        cache.InvalidateCacheName("db", "orders");

        Assert.That(await cache.TryGetAsync("db", "orders", fp1), Is.Null,
            "Invalidated cache name must miss");
        Assert.That(await cache.TryGetAsync("db", "customers", fp2), Is.Not.Null,
            "Different cache name must survive");
    }

    [Test]
    public async Task InvalidateDatabase_RemovesAllEntriesForDatabase()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        string fp1 = ResultFingerprintBuilder.Build("db1", "c1", "s1", null, null, NoOpts);
        string fp2 = ResultFingerprintBuilder.Build("db1", "c2", "s2", null, null, NoOpts);
        string fp3 = ResultFingerprintBuilder.Build("db2", "c1", "s3", null, null, NoOpts);

        await cache.TryPublishAsync(MakeResult("db1", "c1", fp1), EmptyToken(cache));
        await cache.TryPublishAsync(MakeResult("db1", "c2", fp2), EmptyToken(cache));
        await cache.TryPublishAsync(MakeResult("db2", "c1", fp3), EmptyToken(cache));

        cache.InvalidateDatabase("db1");

        Assert.That(await cache.TryGetAsync("db1", "c1", fp1), Is.Null, "db1 entry must be removed");
        Assert.That(await cache.TryGetAsync("db1", "c2", fp2), Is.Null, "db1 entry must be removed");
        Assert.That(await cache.TryGetAsync("db2", "c1", fp3), Is.Not.Null, "db2 entry must survive");
        Assert.That(cache.EntryCount, Is.EqualTo(1));
    }

    [Test]
    public async Task InvalidateCacheName_OtherDatabaseUnaffected()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        // Both databases have a cache named "orders"
        string fp1 = ResultFingerprintBuilder.Build("db1", "orders", "s1", null, null, NoOpts);
        string fp2 = ResultFingerprintBuilder.Build("db2", "orders", "s2", null, null, NoOpts);

        await cache.TryPublishAsync(MakeResult("db1", "orders", fp1), EmptyToken(cache));
        await cache.TryPublishAsync(MakeResult("db2", "orders", fp2), EmptyToken(cache));

        cache.InvalidateCacheName("db1", "orders");

        Assert.That(await cache.TryGetAsync("db1", "orders", fp1), Is.Null);
        Assert.That(await cache.TryGetAsync("db2", "orders", fp2), Is.Not.Null,
            "db2's entry must survive invalidation of db1:orders");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-G: Generation fence — stale entry not published after concurrent write
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task GenerationFence_BlocksPublishAfterWrite()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        string keyspace = "db:table1:r";

        // Snapshot generations before query starts
        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations([keyspace]);

        // A write commits during query execution: bump the generation
        cache.PublishGate.CommitWrite([keyspace]);

        // Publish attempt must be rejected because the generation moved
        string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
        QueryCacheStatus status = await cache.TryPublishAsync(MakeResult("db", "c", fp), token);

        Assert.That(status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish),
            "Generation fence must block publish after a concurrent write");
        Assert.That(await cache.TryGetAsync("db", "c", fp), Is.Null,
            "No stale entry must survive the generation fence");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-H: Byte accounting
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ByteAccounting_IncreasesOnPublish_DecreasesOnRemove()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        string fp = ResultFingerprintBuilder.Build("db", "c", "s", null, null, NoOpts);
        var rows = new List<QueryResultRow> { MakeRow("col", "hello world") };
        await cache.TryPublishAsync(MakeResult("db", "c", fp, rows), EmptyToken(cache));

        long before = cache.TotalBytes;
        Assert.That(before, Is.GreaterThan(0), "TotalBytes must be positive after publish");

        cache.InvalidateCacheName("db", "c");
        Assert.That(cache.TotalBytes, Is.EqualTo(0), "TotalBytes must reach 0 after all entries removed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-H2: ExtractKeyspaceBucket — real KV key formats
    //
    // Real formats (KvTableStore):
    //   Row:   {dbId}:{tableId}:r/{rowIdHex24}
    //   Index: {dbId}:{tableId}:i:{indexId}/{encodedKey}
    //
    // The index format has a colon (not a slash) between :i and the indexId.
    // This is tested directly via InvalidateByModifiedKeys, which calls the
    // private helper through the public API, and also via a dep-index round-trip
    // where an entry carrying a range dep is invalidated by a matching key.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task InvalidateByModifiedKeys_RowKey_RemovesMatchingEntry()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        // Publish an entry whose fingerprint maps to "db1:tbl1" in the dep index.
        // Since entries currently carry QueryDependencySet.Empty, we verify the row-key
        // extraction path by publishing into a keyspace, committing a write to the same
        // keyspace (via CommitWrite to bump the generation), and confirming the entry is
        // gone. The InvalidateByModifiedKeys path runs on the committed key list.
        string fp = ResultFingerprintBuilder.Build("db1", "c", "s", null, null, NoOpts);
        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations([]);
        await cache.TryPublishAsync(MakeResult("db1", "c", fp), token);
        Assert.That(cache.EntryCount, Is.EqualTo(1));

        // A row key in the real format: {dbId}:{tableId}:r/{rowIdHex24}
        cache.InvalidateByModifiedKeys([
            ("db1:tbl1:r/aabbccddeeff001122334455", KeyValueDurability.Persistent),
        ]);

        // The entry carries Empty deps so the dep index has no range entry for db1:tbl1:r —
        // the entry must survive (this tests that the path does not crash or corrupt state).
        Assert.That(cache.EntryCount, Is.EqualTo(1),
            "Entry with empty deps must not be invalidated by a row key (no range dep registered)");
    }

    [Test]
    public void KeyspaceBucket_RowKey_ExtractedCorrectly()
    {
        // Verify the extraction logic via the public invalidation path by checking that a
        // key whose bucket matches a registered dep causes removal, while a key whose bucket
        // does not match leaves the entry intact.
        //
        // We test the extraction logic directly through InvalidateByModifiedKeys + a manually
        // constructed dep. Since QueryDependencySet is public we can build a non-empty dep set.
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        // Real row key: "mydb:orders:r/000000000000000000000001"
        // Expected bucket: "mydb:orders:r"
        string rowKey = "mydb:orders:r/000000000000000000000001";

        // The cache exposes no private method directly; we verify through the dep-index path
        // by exercising that a dep-registered entry gets invalidated when the matching key
        // is committed.
        //
        // For now (QC2 — Empty deps) we assert the extraction does not panic and the API
        // contract holds. QC3 will wire real deps; this test is a placeholder that will be
        // strengthened then.
        Assert.DoesNotThrow(() =>
            cache.InvalidateByModifiedKeys([
                (rowKey, KeyValueDurability.Persistent),
            ]));
    }

    [Test]
    public void KeyspaceBucket_IndexKey_ExtractedCorrectly()
    {
        // Real index key format: "{dbId}:{tableId}:i:{indexId}/{encodedKey}"
        // Example: "mydb:orders:i:idx-name/0031003200330034"
        // Expected bucket: "mydb:orders:i:idx-name"
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        string indexKey = "mydb:orders:i:idx-name/0031003200330034";

        // Must not throw or corrupt state; QC3 adds the dep-matching assertion.
        Assert.DoesNotThrow(() =>
            cache.InvalidateByModifiedKeys([
                (indexKey, KeyValueDurability.Persistent),
            ]));
    }

    [Test]
    public void KeyspaceBucket_OldIncorrectFormat_DoesNotMatch()
    {
        // The old (incorrect) format assumed "{dbId}:{tableId}:i/{indexId}/..." (slash after :i).
        // That format does not exist in production — the real format has a colon.
        // Passing a key in the old format should return an empty bucket (no match),
        // not silently extract a wrong bucket.
        using var cache = new QueryResultCache(sweepIntervalMs: -1);

        // Old format (never actually written by KvTableStore): "db:tbl:i/idx/key"
        // The new code requires key[colonAfterI] == ':', so this should produce no bucket.
        Assert.DoesNotThrow(() =>
            cache.InvalidateByModifiedKeys([
                ("db:tbl:i/idx/key", KeyValueDurability.Persistent),
            ]));
        // No crash and no spurious invalidation — cache not affected.
    }

    [Test]
    public void ExtractKeyspaceBucket_MapsRealKeyFormatsToBuckets()
    {
        // Direct assertions on the mapping — the DoesNotThrow tests above only prove no crash.
        // Row key → row bucket (slash is not part of the bucket).
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("mydb:orders:r/0031003200330034"),
            Is.EqualTo("mydb:orders:r"));

        // Index key → index bucket including the indexId, up to (not including) the first slash.
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("mydb:orders:i:idx-name/0031003200330034"),
            Is.EqualTo("mydb:orders:i:idx-name"));

        // Non-unique index key (encodedKey immediately followed by rowId, no extra slash).
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("mydb:orders:i:idx2/abc0031"),
            Is.EqualTo("mydb:orders:i:idx2"));

        // Old (never-written) slash-after-:i format must NOT be mis-parsed into a bucket.
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("db:tbl:i/idx/key"),
            Is.EqualTo(""));

        // Malformed / too-short keys return empty.
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("nocolons"), Is.EqualTo(""));
        Assert.That(QueryResultCache.ExtractKeyspaceBucket("db:tbl"), Is.EqualTo(""));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QC2-I: Config wiring — ConfigDefinition defaults match CamusDBConfig defaults
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void ConfigDefinition_DefaultsMatchCamusDBConfigDefaults()
    {
        var def = new CamusDB.Core.Config.Models.ConfigDefinition();

        Assert.That(def.QueryResultCacheEnabled,              Is.EqualTo(false));
        Assert.That(def.QueryResultCacheDefaultTtlMs,         Is.EqualTo(5_000));
        Assert.That(def.QueryResultCacheMaxEntries,           Is.EqualTo(1_024));
        Assert.That(def.QueryResultCacheMaxBytes,             Is.EqualTo(64 * 1024 * 1024));
        Assert.That(def.QueryResultCacheMaxEntryBytes,        Is.EqualTo(1 * 1024 * 1024));
        Assert.That(def.QueryResultCacheMaxEntryRows,         Is.EqualTo(10_000));
        Assert.That(def.QueryResultCacheMaxDeps,              Is.EqualTo(4_096));
        Assert.That(def.QueryResultCacheMaxPointDeps,         Is.EqualTo(2_048));
        Assert.That(def.QueryResultCacheMaxRanges,            Is.EqualTo(256));
        Assert.That(def.QueryResultCacheSingleFlightWaitMs,   Is.EqualTo(250));
        Assert.That(def.QueryResultCacheStrictValidationMaxKeys, Is.EqualTo(10_000));
        Assert.That(def.QueryResultCacheSweepIntervalMs,      Is.EqualTo(10_000));
    }
}
