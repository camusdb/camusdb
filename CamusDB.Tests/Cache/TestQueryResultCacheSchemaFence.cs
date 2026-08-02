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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Verifies the schema-identity fence in the query result cache:
///
/// <list type="bullet">
///   <item><description>
///     The fingerprint encodes the immutable table id (not the mutable name) so a
///     DROP TABLE + CREATE TABLE with the same name cannot produce a false hit even when
///     the new table happens to start at the same schema version as the old one.
///   </description></item>
///   <item><description>
///     <c>SchemaDepsCurrent</c> re-checks schema dep versions on every hit, non-strict
///     included. This guards against the publish-window race: an in-flight miss that
///     publishes after <c>InvalidateByTableId</c> has already fired bypasses the
///     dependency index, so it would not be found by key-based eviction. The on-hit
///     re-check catches it on the very next probe.
///   </description></item>
/// </list>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
public sealed class TestQueryResultCacheSchemaFence : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    /// <summary>
    /// Every engine this fixture builds gets its own cache, and honours <paramref name="options"/>.
    /// The options-taking overload is the one to override: the parameterless factory routes through it,
    /// so a test that asks for particular cache limits still gets the injected cache.
    /// </summary>
    protected override CommandExecutor CreateCommandExecutor(CamusDBOptions options)
    {
        _cache = new QueryResultCache(options, sweepIntervalMs: -1);
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    private QueryResultCache Cache => _cache!;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task CreateItems(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbname,
            "CREATE TABLE items (id STRING NOT NULL PRIMARY KEY, val int64 NOT NULL)",
            null));
    }

    private static async Task DropItems(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "DROP TABLE items", null));
    }

    private static async Task InsertItem(string dbname, DatabaseDescriptor database, CommandExecutor executor,
        string id, int val)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"INSERT INTO items (id, val) VALUES (\"{id}\", {val})", null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<CacheMetadataHolder> SelectItems(string dbname, CommandExecutor executor,
        string cacheName = "items_cache")
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM items{{cache={cacheName}}}", null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }

    /// <summary>
    /// Computes the fingerprint that <c>QueryExecutor.QueryWithCache</c> will compute for
    /// <c>SELECT * FROM items{{cache=<paramref name="cacheName"/>}}</c> against the
    /// <em>current</em> schema version of the <c>items</c> table.
    /// </summary>
    private static string ComputeItemsFingerprint(
        DatabaseDescriptor database, TableSchema itemsSchema, string cacheName)
    {
        NodeAst ast = SQLParserProcessor.Parse($"SELECT * FROM items{{cache={cacheName}}}");
        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(ast);
        string? shapeId = QueryShapeComputer.Compute(selectQuery);
        List<(string, int)> schemaDeps = [(itemsSchema.Id!, itemsSchema.Version)];
        CacheHintOptions hint = new(cacheName, null, IsStrict: false);
        return ResultFingerprintBuilder.Build(database.Id, cacheName, shapeId, null, schemaDeps, hint);
    }

    // ── 1. Drop+recreate same name at same version → miss (different table id) ──

    [Test]
    public async Task DropAndRecreate_SameName_ProducesMissNotHit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "a", 1);

        // Warm the cache.
        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Hit));

        // Drop and recreate the same table name. The new table gets a fresh id.
        await DropItems(dbname, database, executor);
        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "b", 2);

        // Must miss — new table id → different fingerprint, regardless of schema version.
        CacheMetadataHolder afterRecreate = await SelectItems(dbname, executor);
        Assert.That(afterRecreate.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "DROP+recreate with the same table name must miss: the new table id differs");
    }

    // ── 2. SchemaDepsCurrent is the guard against the publish-window race ─────
    //
    // The publish-window race: an in-flight miss starts executing (schema version N),
    // DDL fires and commits (version → N+1), InvalidateByTableId evicts existing entries
    // (there are none), then the in-flight miss publishes — after the invalidation has
    // already fired. The published entry has fingerprint F(tableId, N) and deps (tableId, N).
    //
    // Because deps are registered in _depIndex at publish time (after InvalidateByTableId),
    // the next DDL would evict it, but until then SchemaDepsCurrent is the only guard.
    //
    // However, a post-DDL probe computes fingerprint F(tableId, N+1) ≠ F(tableId, N) and
    // therefore never hits the stale F(tableId, N) entry. SchemaDepsCurrent would never
    // be called on it. The truly independent value of SchemaDepsCurrent is as defense-in-
    // depth: inject an entry where fingerprint and deps disagree (fingerprint looks current
    // but deps are stale) and confirm the re-check evicts it rather than serving it.
    //
    // This test does exactly that: the injected entry's fingerprint matches what the next
    // probe computes (current schema version N+1) but its schema dep carries the old version
    // N. Without SchemaDepsCurrent the probe would return a hit serving stale rows;
    // with it the entry is evicted and the probe returns a miss.
    [Test]
    public async Task SchemaDepsCurrent_StaleDepVersion_EvictsEntryOnHit()
    {
        const string cacheName = "fence_test";

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "x", 10);

        // ALTER TABLE bumps the schema version from N to N+1.
        int oldVersion = database.Schema.Tables["items"].Version;
        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "ALTER TABLE items ADD COLUMN tag string DEFAULT('')", null));

        TableSchema currentSchema = database.Schema.Tables["items"];
        int newVersion = currentSchema.Version;
        Assert.That(newVersion, Is.GreaterThan(oldVersion), "DDL must bump schema version");

        // Compute the fingerprint the next probe will use (current schema version N+1).
        string freshFingerprint = ComputeItemsFingerprint(database, currentSchema, cacheName);

        // Inject an entry at that fingerprint but with the OLD schema dep version.
        // This simulates a stale publish that somehow bypassed the dependency index
        // (e.g. published after InvalidateByTableId already fired, so it is not in
        // _depIndex and will not be found by a future InvalidateByTableId call).
        Cache.InjectEntryForTest(
            new CachedQueryResult(
                CacheName: cacheName, DatabaseId: database.Id,
                Rows: [], ResultFingerprint: freshFingerprint,
                CachedAt: HLCTimestamp.Zero, Status: QueryCacheStatus.Miss,
                HintIsStrict: false),
            new QueryDependencySet([], [], [(currentSchema.Id!, oldVersion)]));

        // Probe: the fingerprint matches — the entry IS found.
        // SchemaDepsCurrent then checks dep version oldVersion ≠ currentVersion → evict →
        // fall through to live execution → status StaleRevalidated.
        // Without SchemaDepsCurrent the entry would be returned as a Hit (stale rows).
        // A Miss here would mean the entry was never found (fingerprint mismatch), which
        // would not prove SchemaDepsCurrent ran. StaleRevalidated is the definitive signal.
        CacheMetadataHolder meta = await SelectItems(dbname, executor, cacheName);
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.StaleRevalidated),
            "SchemaDepsCurrent must detect the stale dep version and evict the entry, " +
            "yielding StaleRevalidated (found + evicted + re-executed) not Hit (served stale) " +
            "and not Miss (fingerprint mismatch — would not prove the dep check ran)");

        // A subsequent probe should populate a fresh entry (normal miss → hit cycle).
        CacheMetadataHolder fresh = await SelectItems(dbname, executor, cacheName);
        Assert.That(fresh.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "after eviction the next execution should repopulate and hit");
    }

    // ── 3. Non-strict hit detects schema version change and evicts ─────────────
    //      (dependency-index path: after a committed ALTER the entry is already evicted
    //       by InvalidateByTableId; this test documents that the probe sees a miss,
    //       regardless of which guard fires first)
    [Test]
    public async Task AlterTable_NonStrictHit_EvictsStaleEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "y", 20);

        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Hit));

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "ALTER TABLE items ADD COLUMN extra string DEFAULT('')", null));

        CacheMetadataHolder afterAlter = await SelectItems(dbname, executor);
        Assert.That(afterAlter.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "after DDL the entry must not be served (evicted by InvalidateByTableId)");
    }

    // ── 4. No schema change → non-strict hit still succeeds (regression guard) ─
    [Test]
    public async Task NoSchemaChange_NonStrictHit_Succeeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "z", 30);

        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Miss));
        Assert.That((await SelectItems(dbname, executor)).Status, Is.EqualTo(QueryCacheStatus.Hit),
            "non-strict hit must succeed when schema has not changed");
    }

    // ── 5. Promoted autocommit read (key-range sharding) is cache-eligible ──────
    //
    // When KeyRangeShardingEnabled is true, BeginReadOnlyAsync(promote: true) mints a
    // real Kahuna transaction identity so a shared range lock can be held, but
    // ReadTimestamp stays Zero (no pinned snapshot). The cache gate must admit this
    // transaction because it is semantically equivalent to a zero-snapshot autocommit
    // read for caching purposes. Without the fix, the gate's TransactionId == Zero check
    // rejects the promoted read and every HTTP autocommit SELECT under key-range sharding
    // silently bypasses the cache.
    [Test]
    public async Task PromotedAutocommitRead_KeyRangeSharding_HitMissCycle()
    {
        const string cacheName = "promoted_cache";

        // Promotion only happens under key-range sharding, so this engine is built with it enabled.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { KeyRangeShardingEnabled = true });

        await CreateItems(dbname, database, executor);
        await InsertItem(dbname, database, executor, "p1", 100);

        CacheMetadataHolder miss = await SelectItemsPromoted(dbname, database, executor, cacheName);
        Assert.That(miss.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "first promoted-read query must execute live and populate the cache");

        CacheMetadataHolder hit = await SelectItemsPromoted(dbname, database, executor, cacheName);
        Assert.That(hit.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "second promoted-read query must be served from cache: " +
            "the promoted read has ReadTimestamp == Zero so the gate admits it");
    }

    /// <summary>
    /// Runs <c>SELECT * FROM items{cache=<paramref name="cacheName"/>}</c> under a
    /// promoted autocommit read transaction and returns cache metadata.
    /// The promoted transaction is committed after the cursor is fully drained so its
    /// shared range locks are released (matching the HTTP-path single-statement lifecycle).
    /// </summary>
    private static async Task<CacheMetadataHolder> SelectItemsPromoted(
        string dbname,
        DatabaseDescriptor database,
        CommandExecutor executor,
        string cacheName)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote: true);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, $"SELECT * FROM items{{cache={cacheName}}}", null), meta);
            await foreach (QueryResultRow _ in cursor) { }
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
        return meta;
    }

    // ── 6. Fingerprint fence (deterministic): table id + schema version are folded
    //      into the digest ────────────────────────────────────────────────────────
    //
    // These isolate the mechanism that actually makes a stale entry unhittable in
    // single-threaded flow. The result fingerprint includes both the immutable table
    // id and the schema version (ResultFingerprintBuilder.Build), and QueryPlanner
    // always stamps the fingerprint from the CURRENT live schema version. So any
    // schema change produces a different fingerprint — a natural miss — before the
    // on-hit SchemaDepsCurrent re-check is ever consulted. (That re-check cannot fire on
    // the normal publish path — an entry's fingerprint and its stored deps always share the
    // same version — so it is defense-in-depth for a concurrent DDL race or a dep-index-
    // bypassing entry; its eviction logic is covered by SchemaDepsCurrent_StaleDepVersion_
    // EvictsEntryOnHit via a test injection seam.) Unlike the DROP/ALTER end-to-end tests
    // above — where InvalidateByTableId also evicts the entry and masks which guard
    // fires — these assert the fingerprint fence in isolation.

    private static string BuildFingerprint(string tableId, int schemaVersion)
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM items{cache=items_cache}");
        SelectQuery select = new SelectQueryCreator().CreateSelectQuery(ast);
        string? shape = QueryShapeComputer.Compute(select);
        CacheHintOptions hint = new("items_cache", null, IsStrict: false);
        List<(string, int)> deps = [(tableId, schemaVersion)];
        return ResultFingerprintBuilder.Build("db1", "items_cache", shape, null, deps, hint);
    }

    [Test]
    public void Fingerprint_SchemaVersionBump_ChangesDigest()
    {
        string tableId = ObjectIdGenerator.Generate().ToString();
        string fpV1 = BuildFingerprint(tableId, 1);
        string fpV2 = BuildFingerprint(tableId, 2);

        Assert.That(fpV2, Is.Not.EqualTo(fpV1),
            "a schema version bump must change the fingerprint so an entry cached at the " +
            "old version can no longer be hit (the fence behind acceptance: schema change → miss)");
    }

    [Test]
    public void Fingerprint_SameNameNewTableId_ChangesDigest()
    {
        // Same table name and same schema version, but a fresh table id — a DROP + CREATE
        // of the same name. The immutable id in the digest must still force a different
        // fingerprint, so the new incarnation cannot collide with the dropped table's entry.
        string oldId = ObjectIdGenerator.Generate().ToString();
        string newId = ObjectIdGenerator.Generate().ToString();
        string fpOld = BuildFingerprint(oldId, 1);
        string fpNew = BuildFingerprint(newId, 1);

        Assert.That(fpNew, Is.Not.EqualTo(fpOld),
            "drop+recreate at the same name and schema version must still differ by table id");
    }
}
