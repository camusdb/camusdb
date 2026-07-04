/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Tests that verify cache resolution metadata is correctly populated on the
/// <see cref="CacheMetadataHolder"/> after a SELECT with a cache hint is drained.
///
/// Each test calls <see cref="CommandExecutor.ExecuteSQLQuery"/> with a holder and asserts
/// <see cref="CacheMetadataHolder.Status"/>, <see cref="CacheMetadataHolder.CacheName"/>,
/// <see cref="CacheMetadataHolder.CachedAtHlc"/>, and <see cref="CacheMetadataHolder.AgeMs"/>
/// directly — this is the same data that the HTTP controller surfaces in the JSON envelope.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheResponseMetadata : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    protected override CommandExecutor CreateCommandExecutor()
    {
        _cache = new QueryResultCache(sweepIntervalMs: -1);
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    private QueryResultCache Cache => _cache!;

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<(List<QueryResultRow> rows, CacheMetadataHolder meta)> SelectAllWithMeta(
        string dbname, CommandExecutor executor, string cacheName)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}}}",
                null), meta);
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return (rows, meta);
    }

    private static async Task<(List<QueryResultRow> rows, CacheMetadataHolder meta)> SelectAllNoHintWithMeta(
        string dbname, CommandExecutor executor)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                "SELECT * FROM orders",
                null), meta);
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return (rows, meta);
    }

    private static async Task CreateOrdersTable(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbname,
            "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL)",
            null));
    }

    private static async Task InsertOrder(string dbname, DatabaseDescriptor database, CommandExecutor executor,
        string id, int amount)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"INSERT INTO orders (id, amount) VALUES (\"{id}\", {amount})", null));
        await database.Transactions.CommitAsync(tx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Cache miss → CacheName populated, Status=Miss, no CachedAtHlc.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task CacheMiss_HolderHasMissStatus_CacheNameSet_NoHlc()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "a", 10);

        (List<QueryResultRow> rows, CacheMetadataHolder meta) = await SelectAllWithMeta(dbname, executor, "orders_all");

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That(meta.CacheName, Is.EqualTo("orders_all"));
        Assert.That(meta.CachedAtHlc, Is.Null, "No HLC on a miss — entry was just stored, not served");
        Assert.That(meta.AgeMs, Is.Null, "No age on a miss");
        Assert.That(CacheMetadataHolder.ToStatusString(meta.Status), Is.EqualTo("miss"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Cache hit → Status=Hit, CachedAtHlc set, AgeMs >= 0.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task CacheHit_HolderHasHitStatus_CachedAtHlcAndAgeMs()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "b", 20);

        // Cold miss — populates the cache.
        await SelectAllWithMeta(dbname, executor, "hit_test");

        // Warm hit — served from cache.
        (_, CacheMetadataHolder meta) = await SelectAllWithMeta(dbname, executor, "hit_test");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Hit));
        Assert.That(meta.CacheName, Is.EqualTo("hit_test"));
        Assert.That(meta.CachedAtHlc, Is.Not.Null, "HLC must be set on a hit");
        Assert.That(meta.AgeMs, Is.Not.Null, "AgeMs must be set on a hit");
        Assert.That(meta.AgeMs!.Value, Is.GreaterThanOrEqualTo(0));
        Assert.That(CacheMetadataHolder.ToStatusString(meta.Status), Is.EqualTo("hit"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. No cache hint → CacheName stays null; holder is at default Bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task NoHint_HolderCacheNameIsNull_StatusBypass()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "c", 30);

        (List<QueryResultRow> rows, CacheMetadataHolder meta) = await SelectAllNoHintWithMeta(dbname, executor);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(meta.CacheName, Is.Null, "No cache hint → CacheName must stay null");
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Miss then hit → status transitions correctly across two calls.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task MissThenHit_StatusTransitions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "d", 40);

        (_, CacheMetadataHolder miss) = await SelectAllWithMeta(dbname, executor, "transition");
        (_, CacheMetadataHolder hit) = await SelectAllWithMeta(dbname, executor, "transition");

        Assert.That(miss.Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That(hit.Status, Is.EqualTo(QueryCacheStatus.Hit));

        // After INSERT invalidation, next probe is a miss again.
        await InsertOrder(dbname, database, executor, "e", 50);
        (_, CacheMetadataHolder miss2) = await SelectAllWithMeta(dbname, executor, "transition");
        Assert.That(miss2.Status, Is.EqualTo(QueryCacheStatus.Miss));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. In-flight write: MarkWriteInFlight bypass surfaces InFlightWrite reason.
    //    We directly manipulate the publish gate to avoid a timing-sensitive
    //    concurrent-commit dependency — MarkWriteInFlight is called during CommitAsync,
    //    so we replicate that by calling the gate directly with the known keyspace.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task InFlightWrite_BypassReasonIsInFlightWrite()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "f", 60);

        // Build the row keyspace string the same way KvTableStore does: "{dbId}:{tableId}:r"
        string tableId = database.Schema.Tables["orders"].Id!;
        string rowKeySpace = $"{database.Id}:{tableId}:r";

        // Mark the keyspace as in-flight — simulates a write transaction in CommitAsync.
        Cache.PublishGate.MarkWriteInFlight([rowKeySpace]);

        (_, CacheMetadataHolder meta) = await SelectAllWithMeta(dbname, executor, "inflight_test");

        // Clear the in-flight mark so subsequent operations are unaffected.
        Cache.PublishGate.AbortWrite([rowKeySpace]);

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.InFlightWrite));
        Assert.That(meta.CacheName, Is.EqualTo("inflight_test"));
        Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("in-flight-write"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Cache-disabled: a hinted query against a disabled cache reports
    //    Status=Bypass, BypassReason=CacheDisabled, CacheName set — distinct
    //    from a non-hinted query where CacheName is null.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task HintedQuery_CacheDisabled_ReportsCacheDisabledBypassReason()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "d1", 10);

        // Build a second executor that shares the same Kahuna node but with cache: null —
        // the real configuration produced when QueryResultCacheEnabled is false.
        // database.Cache will be null, exercising the dispatch branch in Query() that
        // fires when a hint is present but no cache is wired up.
        CommandExecutor disabledExecutor = new(
            new CommandValidator(), new CatalogsManager(logger), logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
            cache: null);

        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await disabledExecutor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname, "SELECT * FROM orders{cache=disabled_test}", null), meta);
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        Assert.That(rows, Has.Count.EqualTo(1), "Rows are still returned when cache is disabled");
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.CacheDisabled),
            "A hinted query with cache disabled must report CacheDisabled, not None");
        Assert.That(meta.CacheName, Is.EqualTo("disabled_test"),
            "CacheName must be set so the client knows which hint was ignored");
        Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("cache-disabled"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. EvictedBeforePublish carries OversizedResult reason when the per-entry
    //    row cap is exceeded during a cache miss.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictedBeforePublish_RowCapExceeded_ReasonIsOversizedResult()
    {
        int origMax = CamusDBConfig.QueryResultCacheMaxEntryRows;
        try
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = 0;  // reject all

            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await CreateOrdersTable(dbname, database, executor);
            await InsertOrder(dbname, database, executor, "row1", 10);

            (_, CacheMetadataHolder meta) = await SelectAllWithMeta(dbname, executor, "cap_test");

            Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish));
            Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.OversizedResult));
            Assert.That(meta.CacheName, Is.EqualTo("cap_test"));
            Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("oversized-result"));
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = origMax;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. EvictedBeforePublish carries InFlightWrite reason when the generation
    //    fence rejects a publish because a write committed during the query.
    //    Tested directly at the TryPublishAsync layer: snapshot gen=0, bump
    //    to gen=1, then attempt publish → gate rejects with InFlightWrite.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictedBeforePublish_GenerationFence_ReasonIsInFlightWrite()
    {
        using var gate_cache = new QueryResultCache(sweepIntervalMs: -1);
        string ks = "db1:tbl1:r";
        CacheGenerationToken staleToken = gate_cache.PublishGate.SnapshotGenerations([ks]);  // gen=0
        gate_cache.PublishGate.CommitWrite([ks]);  // gen→1 — stales the snapshot token

        var result = new CachedQueryResult(
            CacheName: "gen-test", DatabaseId: "db-gen",
            Rows: [], ResultFingerprint: "gfp",
            CachedAt: default, Status: QueryCacheStatus.Miss);

        (QueryCacheStatus st, QueryCacheBypassReason rs) = await gate_cache.TryPublishAsync(result, staleToken);

        Assert.That(st, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish));
        Assert.That(rs, Is.EqualTo(QueryCacheBypassReason.InFlightWrite),
            "A generation-fence rejection must report InFlightWrite to distinguish it from a cap rejection");
        Assert.That(CacheMetadataHolder.ToBypassReasonString(rs), Is.EqualTo("in-flight-write"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. StaleRevalidated — a stale strict entry triggers re-execution and the
    //    status reflects that revalidation happened, not a plain miss.
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Computes the result-cache fingerprint that QueryWithCache will use for
    /// "SELECT * FROM orders{cache=<paramref name="cacheName"/>, strict}".
    /// Requires InternalsVisibleTo access to QueryShapeComputer and ResultFingerprintBuilder.
    /// </summary>
    private static string ComputeStrictFingerprint(DatabaseDescriptor database, string cacheName)
    {
        NodeAst ast = SQLParserProcessor.Parse(
            $"SELECT * FROM orders{{cache={cacheName}, strict}}");
        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(ast);
        string? shapeId = QueryShapeComputer.Compute(selectQuery);
        TableSchema ordersSchema = database.Schema.Tables["orders"];
        List<(string, int)> schemaDeps = [(ordersSchema.Name!, ordersSchema.Version)];
        CacheHintOptions strictHint = new(cacheName, null, IsStrict: true);
        return ResultFingerprintBuilder.Build(database.Id, cacheName, shapeId, null, schemaDeps, strictHint);
    }

    [Test]
    public async Task StaleRevalidated_StrictEntryStale_StatusIsStaleRevalidated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "sr1", 10);

        const string cacheName = "sr_basic";
        string fingerprint = ComputeStrictFingerprint(database, cacheName);
        string tableId = database.Schema.Tables["orders"].Id!;
        string rowKeySpace = $"{database.Id}:{tableId}:r";

        // Inject a strict entry at CachedAt=Zero with a RangeDep over the real row keyspace.
        // The strict validator scans the range, finds rows with LastModified > Zero,
        // and returns false (stale) — triggering re-execution with wasStaleRevalidated=true.
        Cache.InjectEntryForTest(
            new CachedQueryResult(
                CacheName: cacheName, DatabaseId: database.Id,
                Rows: [], ResultFingerprint: fingerprint,
                CachedAt: HLCTimestamp.Zero, Status: QueryCacheStatus.Miss,
                HintIsStrict: true),
            new QueryDependencySet([rowKeySpace], [], []));

        // Use a non-Zero snapshot so canPublishStrict=true and the re-execution can publish.
        KvTransaction snapshotTx = KvTransaction.CreateSnapshotReadOnly(
            new HLCTimestamp(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0));
        var meta = new CacheMetadataHolder();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(snapshotTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}, strict}}",
                null), meta);
        await foreach (QueryResultRow _ in cursor) { }

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.StaleRevalidated),
            "A stale strict hit followed by successful re-execution must report StaleRevalidated");
        Assert.That(meta.CacheName, Is.EqualTo(cacheName));
        Assert.That(CacheMetadataHolder.ToStatusString(meta.Status), Is.EqualTo("stale-revalidated"));
    }

    [Test]
    public async Task StaleRevalidated_ReexecutionCannotRepublish_StatusIsStillStaleRevalidated()
    {
        // When a stale strict entry forces re-execution but the fresh result is too large
        // to re-publish, the status must still be StaleRevalidated — not EvictedBeforePublish.
        // The "stale strict entry triggered this" signal survives publish failure.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "sr2", 20);

        const string cacheName = "sr_ovrsz";
        string fingerprint = ComputeStrictFingerprint(database, cacheName);
        string tableId2 = database.Schema.Tables["orders"].Id!;
        string rowKeySpace2 = $"{database.Id}:{tableId2}:r";

        Cache.InjectEntryForTest(
            new CachedQueryResult(
                CacheName: cacheName, DatabaseId: database.Id,
                Rows: [], ResultFingerprint: fingerprint,
                CachedAt: HLCTimestamp.Zero, Status: QueryCacheStatus.Miss,
                HintIsStrict: true),
            new QueryDependencySet([rowKeySpace2], [], []));

        KvTransaction snapshotTx = KvTransaction.CreateSnapshotReadOnly(
            new HLCTimestamp(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0));
        var meta = new CacheMetadataHolder();

        int origMax = CamusDBConfig.QueryResultCacheMaxEntryRows;
        try
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = 0;  // force EvictedBeforePublish on re-execution

            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(snapshotTx, dbname,
                    $"SELECT * FROM orders{{cache={cacheName}, strict}}",
                    null), meta);
            await foreach (QueryResultRow _ in cursor) { }
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = origMax;
        }

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.StaleRevalidated),
            "StaleRevalidated must survive EvictedBeforePublish on the re-execution publish step");
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.OversizedResult),
            "BypassReason still carries the underlying publish-failure cause");
        Assert.That(meta.CacheName, Is.EqualTo(cacheName));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 9. ToStatusString and ToBypassReasonString cover all enum values.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void ToStatusString_AllValues_ReturnExpectedStrings()
    {
        Assert.That(CacheMetadataHolder.ToStatusString(QueryCacheStatus.Hit), Is.EqualTo("hit"));
        Assert.That(CacheMetadataHolder.ToStatusString(QueryCacheStatus.Miss), Is.EqualTo("miss"));
        Assert.That(CacheMetadataHolder.ToStatusString(QueryCacheStatus.Bypass), Is.EqualTo("bypass"));
        Assert.That(CacheMetadataHolder.ToStatusString(QueryCacheStatus.StaleRevalidated), Is.EqualTo("stale-revalidated"));
        Assert.That(CacheMetadataHolder.ToStatusString(QueryCacheStatus.EvictedBeforePublish), Is.EqualTo("evicted-before-publish"));
    }

    [Test]
    public void ToBypassReasonString_AllValues_ReturnExpectedStrings()
    {
        Assert.That(CacheMetadataHolder.ToBypassReasonString(QueryCacheBypassReason.None), Is.Null);
        Assert.That(CacheMetadataHolder.ToBypassReasonString(QueryCacheBypassReason.InTransaction), Is.EqualTo("in-transaction"));
        Assert.That(CacheMetadataHolder.ToBypassReasonString(QueryCacheBypassReason.InFlightWrite), Is.EqualTo("in-flight-write"));
        Assert.That(CacheMetadataHolder.ToBypassReasonString(QueryCacheBypassReason.OversizedResult), Is.EqualTo("oversized-result"));
        Assert.That(CacheMetadataHolder.ToBypassReasonString(QueryCacheBypassReason.StrictValidationLimit), Is.EqualTo("strict-validation-limit"));
    }
}
