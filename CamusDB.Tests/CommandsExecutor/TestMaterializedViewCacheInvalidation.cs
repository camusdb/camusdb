
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Proves that a materialized-view refresh evicts results computed from the contents it retired.
///
/// <para>The hazard this covers is specific and quiet. A refresh deliberately keeps the relation's id
/// (so grants and dependencies survive) and does not move its schema version (the row encoding is
/// unchanged) — so <em>every</em> field a cached result is keyed and validated on looks exactly as it
/// did before, while the rows underneath are different ones in a different key-space. Nothing evicts
/// such an entry unless something evicts it deliberately.</para>
///
/// <para>It also pins <b>where</b> that eviction lives. Doing it in the statement that proposed the
/// swap covers only the node that ran it; it belongs in the replicated schema-apply callback, which
/// every node runs. This fixture is single-node and so cannot observe a follower directly, but the
/// refresher performs no invalidation of its own — so a stale hit here would mean the apply path is
/// not doing it either.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestMaterializedViewCacheInvalidation : BaseTest
{
    private QueryResultCache? cache;

    protected override CommandExecutor CreateCommandExecutor(CamusDBOptions options)
    {
        cache = new QueryResultCache(options, sweepIntervalMs: -1);
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: cache);
    }

    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    /// <summary>Autocommit snapshot read: an explicit transaction bypasses the cache by definition.</summary>
    private static async Task<(List<long> Ids, CacheMetadataHolder Meta)> ExecCachedQuery(
        CommandExecutor executor, string dbname, string sql)
    {
        CacheMetadataHolder meta = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), dbname, sql, null), meta);

        List<QueryResultRow> rows = await cursor.ToListAsync();
        return (rows.Select(r => r.Row["id"].LongValue).ToList(), meta);
    }

    [Test]
    public async Task ARefreshEvictsResultsComputedFromTheRetiredContents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, total int64, status string(16))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, total, status) VALUES (1, 10, 'open'), (2, 20, 'open'), (3, 30, 'closed')");

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // Warm, then confirm the cache is genuinely serving — otherwise the assertion after the
        // refresh would pass for the wrong reason.
        _ = await ExecCachedQuery(executor, dbname, "SELECT id FROM open_orders {cache=mv}");
        (List<long> hitIds, CacheMetadataHolder hit) = await ExecCachedQuery(executor, dbname, "SELECT id FROM open_orders {cache=mv}");

        Assert.AreEqual(QueryCacheStatus.Hit, hit.Status,
            $"the cache must be serving before the refresh (was {hit.Status}/{hit.BypassReason})");
        CollectionAssert.AreEqual(new List<long> { 1, 2 }, hitIds);

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, total, status) VALUES (4, 40, 'open')");
        Assert.AreEqual(3, await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

        (List<long> afterIds, CacheMetadataHolder after) = await ExecCachedQuery(executor, dbname, "SELECT id FROM open_orders {cache=mv}");

        Assert.AreNotEqual(QueryCacheStatus.Hit, after.Status,
            "an entry computed from the contents the swap retired must not survive it");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 4 }, afterIds,
            "and the rows served afterwards must be the rebuilt ones");
    }

    /// <summary>
    /// The publish-window race, which eviction alone cannot close: a query that began before the swap
    /// can publish its entry <em>after</em> the eviction has already fired, so it is absent from the
    /// dependency index and no later invalidation can find it.
    ///
    /// <para>Simulated by publishing an entry directly with the contents generation the query would
    /// have captured before the refresh. The on-hit re-check is what has to catch it, and it can only
    /// do so because the dependency carries the contents generation — the relation id and the schema
    /// version are both unchanged by a refresh, so every other field still matches.</para>
    /// </summary>
    [Test]
    public async Task AnEntryPublishedAfterTheSwapIsCaughtByTheOnHitRecheck()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE orders (id int64 PRIMARY KEY, total int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO orders (id, total) VALUES (1, 10), (2, 20)");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW mv AS SELECT id, total FROM orders");

        TableSchema view = database.Schema.Tables["mv"];
        string relationId = view.Id!;
        long generationBeforeRefresh = view.ContentsGeneration;

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW mv");

        Assert.AreNotEqual(generationBeforeRefresh, database.Schema.Tables["mv"].ContentsGeneration,
            "the refresh must have advanced the generation, or this test proves nothing");

        // Stand in for the racing publisher: an entry whose deps name the pre-refresh generation,
        // stored after the swap's eviction already ran.
        const string fingerprint = "raced-entry";
        QueryDependencySet racedDeps = new([], [], [(relationId, view.Version, generationBeforeRefresh)]);
        CacheGenerationToken token = cache!.PublishGate.SnapshotGenerations([]);

        await cache.TryPublishAsync(
            new CachedQueryResult(
                CacheName: "mv", DatabaseId: database.Id, Rows: [],
                ResultFingerprint: fingerprint, CachedAt: default, Status: QueryCacheStatus.Miss),
            token,
            racedDeps);

        // It is in the cache, and it must still not be served.
        Assert.IsFalse(
            StrictValidator.SchemaDepsCurrent(racedDeps, database),
            "an entry carrying a superseded contents generation must not validate as current");
    }
}
