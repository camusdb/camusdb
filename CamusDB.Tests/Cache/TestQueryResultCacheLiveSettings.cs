/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Cache;

/// <summary>
/// A live <c>SET CLUSTER SETTING</c> of the result cache's <c>Runtime</c>-class settings must reach
/// the cache itself, not only <c>SHOW VARIABLES</c>. The host builds the cache once, from the boot
/// options and before the engine, so the engine forwards every published snapshot to it.
///
/// <para>Each test drives the real path: the SQL statement, the settings service, the options
/// holder, the engine's fan-out, and hinted <c>SELECT</c>s that publish through the cache. The cache
/// reads its sweep interval from configuration (<c>sweepIntervalMs: 0</c>), as the host builds it.
/// The boot values are far from the live values (60 s against 100 ms), so a cache that kept its boot
/// configuration fails every wait here by a wide margin.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestQueryResultCacheLiveSettings : BaseTest
{
    private const int BootTtlMs = 60_000;

    private const int BootSweepMs = 60_000;

    private CamusDBOptionsHolder holder = null!;

    private ClusterSettingsService settings = null!;

    private QueryResultCache cache = null!;

    private CamusDBOptions savedAmbient = null!;

    [SetUp]
    public async Task SetUpLiveSettings()
    {
        savedAmbient = CamusDBConfig.Ambient;

        CamusDBOptions boot = Options with
        {
            QueryResultCacheDefaultTtlMs = BootTtlMs,
            QueryResultCacheSweepIntervalMs = BootSweepMs,
        };

        holder = new CamusDBOptionsHolder(boot);

        // Built from the boot snapshot, before the engine, as the host builds it.
        cache = new QueryResultCache(boot, sweepIntervalMs: 0);

        // The base definition carries the same boot values, so re-resolution after a SET changes
        // only the key the test names.
        string dataDir = Options.DataDirectory;
        settings = new ClusterSettingsService(
            TestNode!,
            holder,
            () => new ConfigDefinition
            {
                DataDir = dataDir,
                QueryResultCacheDefaultTtlMs = BootTtlMs,
                QueryResultCacheSweepIntervalMs = BootSweepMs,
            },
            postResolve: null,
            isClusterMode: false,
            forwarder: null,
            logger);

        await settings.StartAsync();
    }

    [TearDown]
    public async Task TearDownLiveSettings()
    {
        if (settings is not null)
            await settings.DisposeAsync();

        cache?.Dispose();

        CamusDBConfig.SetAmbient(savedAmbient);
    }

    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> CreateEngineWithTable()
    {
        CommandExecutor executor = CreateCommandExecutor(holder.Current, holder, settings, cache);
        (string db, DatabaseDescriptor database, _) = await CreateDatabaseWith(executor);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE robots (id int64 PRIMARY KEY, name string(32))", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, db,
            "INSERT INTO robots (id, name) VALUES (1, 'r2'), (2, 'c3po'), (3, 'bb8')", null));
        await database.Transactions.CommitAsync(insert);

        return (db, database, executor);
    }

    private static async Task NonQueryAsync(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    /// <summary>Autocommit snapshot read: an explicit transaction bypasses the cache by definition.</summary>
    private static async Task<QueryCacheStatus> CachedQueryAsync(CommandExecutor executor, string db, string cacheName)
    {
        CacheMetadataHolder meta = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db,
                $"SELECT id FROM robots {{cache={cacheName}}}", null), meta);

        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(3, rows.Count);
        return meta.Status;
    }

    /// <summary>Publishes one entry under <paramref name="cacheName"/> and confirms it is served.</summary>
    private async Task<string> WarmAsync(CommandExecutor executor, string db, string databaseId, string cacheName)
    {
        await CachedQueryAsync(executor, db, cacheName);

        string? fingerprint = cache.GetFirstFingerprintByCacheName(databaseId, cacheName);
        Assert.IsNotNull(fingerprint, $"the '{cacheName}' query must publish an entry");
        return fingerprint!;
    }

    /// <summary>Polls until <paramref name="condition"/> holds or the deadline passes.</summary>
    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, int deadlineMs)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < deadlineMs)
        {
            if (await condition())
                return true;

            await Task.Delay(20);
        }

        return await condition();
    }

    [Test]
    public async Task ALiveTtlChangeGovernsTheNextEntry()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateEngineWithTable();

        // Published under the boot TTL, before the change.
        string before = await WarmAsync(executor, db, database.Id, "before");

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING query_result_cache_default_ttl_ms = 100");
        Assert.AreEqual(100, executor.Options.QueryResultCacheDefaultTtlMs);

        string after = await WarmAsync(executor, db, database.Id, "after");

        bool expired = await WaitUntilAsync(
            async () => await cache.TryGetAsync(database.Id, "after", after) is null,
            deadlineMs: 10_000);

        Assert.IsTrue(expired,
            "an entry published after the SET must expire after about 100 ms, not after the 60 s boot TTL");

        // An entry keeps the expiry it was stamped with: the lower TTL shortens new entries only.
        Assert.IsNotNull(await cache.TryGetAsync(database.Id, "before", before),
            "an entry published under the boot TTL keeps its 60 s expiry");
    }

    [Test]
    public async Task ShrinkingMaxEntriesTrimsTheCacheAtOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateEngineWithTable();

        await WarmAsync(executor, db, database.Id, "first");
        await WarmAsync(executor, db, database.Id, "second");
        string newest = await WarmAsync(executor, db, database.Id, "third");
        Assert.AreEqual(3, cache.EntryCount);

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING query_result_cache_max_entries = 1");

        // The trim runs inside the publish, so no wait is needed.
        Assert.AreEqual(1, cache.EntryCount, "a lower cap must trim the resident population at once");
        Assert.IsNotNull(await cache.TryGetAsync(database.Id, "third", newest),
            "the trim evicts from the LRU tail, so the most recent entry survives");

        // The new cap also governs the next publish.
        await CachedQueryAsync(executor, db, "fourth");
        Assert.AreEqual(1, cache.EntryCount);
    }

    [Test]
    public async Task ShrinkingMaxBytesTrimsTheCacheAtOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateEngineWithTable();

        await WarmAsync(executor, db, database.Id, "first");
        await WarmAsync(executor, db, database.Id, "second");
        long oneEntry = cache.TotalBytes / 2;
        Assert.Greater(oneEntry, 0);

        await NonQueryAsync(executor, db, $"SET CLUSTER SETTING query_result_cache_max_bytes = {oneEntry}");

        Assert.AreEqual(1, cache.EntryCount, "a lower byte cap must trim the resident population at once");
        Assert.LessOrEqual(cache.TotalBytes, oneEntry);
    }

    [Test]
    public async Task ALiveSweepIntervalChangeRetunesTheTimer()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateEngineWithTable();

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING query_result_cache_default_ttl_ms = 50");
        await NonQueryAsync(executor, db, "SET CLUSTER SETTING query_result_cache_sweep_interval_ms = 50");

        await WarmAsync(executor, db, database.Id, "swept");

        // EntryCount does not probe, so only the background sweep can remove the expired entry. With
        // the boot interval of 60 s it stays resident for the whole wait.
        bool swept = await WaitUntilAsync(() => Task.FromResult(cache.EntryCount == 0), deadlineMs: 10_000);

        Assert.IsTrue(swept, "the sweep must run at the new 50 ms interval, not at the 60 s boot interval");
    }

    [Test]
    public async Task AnEngineBuiltAfterAPublishBringsTheCacheLevel()
    {
        // The host builds the cache before the persisted overlay is published: model that publish
        // here, after the cache exists and before the engine is built.
        holder.Publish(holder.Current with { QueryResultCacheMaxEntries = 1 });

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateEngineWithTable();

        await WarmAsync(executor, db, database.Id, "first");
        await WarmAsync(executor, db, database.Id, "second");

        Assert.AreEqual(1, cache.EntryCount,
            "the cache must follow the snapshot the engine starts from, not the boot snapshot it was built from");
    }
}
