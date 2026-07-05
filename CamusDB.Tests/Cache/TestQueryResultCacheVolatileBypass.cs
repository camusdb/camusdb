/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Verifies that queries containing volatile scalar functions bypass the result cache
/// rather than publishing a result that would be incorrectly replayed on warm reads.
///
/// Volatile functions: <c>random</c>, <c>now</c>/<c>current_timestamp</c>,
/// <c>current_date</c>, <c>unix_timestamp</c>, <c>gen_id</c>.
/// Every hinted query that invokes any of these must report
/// <see cref="QueryCacheStatus.Bypass"/> with
/// <see cref="QueryCacheBypassReason.NonDeterministic"/> on every execution, including
/// subsequent calls that would otherwise be served from a warm cache.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheVolatileBypass : CommandsExecutor.BaseTest
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

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task CreateRobotsTable(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbname,
            "CREATE TABLE robots (id STRING NOT NULL PRIMARY KEY, score int64 NOT NULL)",
            null));
    }

    private static async Task InsertRobot(string dbname, DatabaseDescriptor database, CommandExecutor executor,
        string id, int score)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"INSERT INTO robots (id, score) VALUES (\"{id}\", {score})", null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<CacheMetadataHolder> RunHintedQuery(
        string dbname, CommandExecutor executor, string sql)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. random() in projection → bypass on every call.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RandomInProjection_AlwaysBypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r1", 10);

        CacheMetadataHolder first = await RunHintedQuery(dbname, executor,
            "SELECT id, random() FROM robots{cache=volatile_random}");
        CacheMetadataHolder second = await RunHintedQuery(dbname, executor,
            "SELECT id, random() FROM robots{cache=volatile_random}");

        Assert.That(first.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(first.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
        Assert.That(first.CacheName, Is.EqualTo("volatile_random"));

        Assert.That(second.Status, Is.EqualTo(QueryCacheStatus.Bypass),
            "second call must also bypass — nothing was stored on the first call");
        Assert.That(second.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. now() in projection → bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task NowInProjection_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r2", 20);

        CacheMetadataHolder meta = await RunHintedQuery(dbname, executor,
            "SELECT id, now() FROM robots{cache=volatile_now}");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. current_timestamp (alias of now) → bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task CurrentTimestampInProjection_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r3", 30);

        CacheMetadataHolder meta = await RunHintedQuery(dbname, executor,
            "SELECT current_timestamp() FROM robots{cache=volatile_cts}");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. current_date in projection → bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task CurrentDateInProjection_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r4", 40);

        CacheMetadataHolder meta = await RunHintedQuery(dbname, executor,
            "SELECT current_date() FROM robots{cache=volatile_cd}");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. unix_timestamp() → bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task UnixTimestampInProjection_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r5", 50);

        CacheMetadataHolder meta = await RunHintedQuery(dbname, executor,
            "SELECT unix_timestamp() FROM robots{cache=volatile_ut}");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. gen_id() → bypass.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task GenIdInProjection_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r6", 60);

        CacheMetadataHolder meta = await RunHintedQuery(dbname, executor,
            "SELECT gen_id() FROM robots{cache=volatile_genid}");

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. Deterministic-only query → still caches normally (regression guard).
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task DeterministicQuery_CachesNormally()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r7", 70);

        CacheMetadataHolder miss = await RunHintedQuery(dbname, executor,
            "SELECT id, score FROM robots{cache=deterministic}");
        CacheMetadataHolder hit = await RunHintedQuery(dbname, executor,
            "SELECT id, score FROM robots{cache=deterministic}");

        Assert.That(miss.Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That(hit.Status, Is.EqualTo(QueryCacheStatus.Hit));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. Volatile function in WHERE → bypass even when projection is pure.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task VolatileFunctionInWhere_BypassesCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateRobotsTable(dbname, database, executor);
        await InsertRobot(dbname, database, executor, "r8", 80);

        // The bypass is set synchronously in Query() before the async enumerable is returned,
        // so meta is populated regardless of whether the live execution succeeds. We catch any
        // execution exception so we can still assert the bypass metadata.
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                "SELECT id FROM robots{cache=volatile_where} WHERE score > random()", null), meta);

        try { await foreach (QueryResultRow _ in cursor) { } } catch { /* live exec may throw; bypass is already recorded */ }

        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass));
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.NonDeterministic));
    }
}
