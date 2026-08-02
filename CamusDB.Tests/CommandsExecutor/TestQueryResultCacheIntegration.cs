
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Integration tests that prove the cache invalidation hooks are live: cache entries published
/// before a DML commit or a DDL operation are evicted by the real wiring inside
/// <see cref="KvTransactionsManager.CommitAsync"/> and <see cref="CommandExecutor"/>.
///
/// The fixture overrides <see cref="CreateCommandExecutor"/> to inject a real
/// <see cref="QueryResultCache"/>. Each test:
///   1. Creates a database and a table.
///   2. Publishes a cache entry with explicit range or schema dependencies.
///   3. Runs a DML or DDL command through <see cref="CommandExecutor"/>.
///   4. Asserts the entry was evicted by the invalidation hook.
///
/// These are the end-to-end paths left unexercised by the unit tests in
/// <c>TestQueryResultCacheInvalidation.cs</c>, which only exercise cache/gate primitives directly.
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheIntegration : BaseTest
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

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private QueryResultCache Cache => _cache!;

    private static CachedQueryResult MakeResult(string dbId, string fp) =>
        new(CacheName: "test", DatabaseId: dbId, Rows: [],
            ResultFingerprint: fp, CachedAt: default(HLCTimestamp),
            Status: QueryCacheStatus.Miss);

    private async Task<string> PublishRangeDep(DatabaseDescriptor db, TableDescriptor table, string fp)
    {
        string bucket = table.Store.RowKeySpace;
        QueryDependencySet deps = new([bucket], [], []);
        CacheGenerationToken token = Cache.PublishGate.SnapshotGenerations([bucket]);
        await Cache.TryPublishAsync(MakeResult(db.Id, fp), token, deps);
        return fp;
    }

    private async Task<string> PublishSchemaDep(DatabaseDescriptor db, string tableId, int version, string fp)
    {
        QueryDependencySet deps = new([], [], [(tableId, version)]);
        CacheGenerationToken token = Cache.PublishGate.SnapshotGenerations([]);
        await Cache.TryPublishAsync(MakeResult(db.Id, fp), token, deps);
        return fp;
    }

    private static async Task CreateOrdersTable(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbname,
            "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL)",
            null));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. INSERT commits → CommitAsync hook → InvalidateByModifiedKeys → eviction
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Insert_EvictsRangeDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "orders"));
        string fp = await PublishRangeDep(database, table, "fp-insert");

        // The entry must be live before the insert.
        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Not.Null,
            "Entry must be cached before the insert");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"order-001\", 100)", null));
        await database.Transactions.CommitAsync(tx);

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "INSERT commit must evict the range-dep cache entry via CommitAsync hook");
    }

    [Test]
    public async Task Update_EvictsRangeDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        // Seed one row.
        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txIns, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"order-upd\", 50)", null));
        await database.Transactions.CommitAsync(txIns);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "orders"));
        string fp = await PublishRangeDep(database, table, "fp-update");

        KvTransaction txUpd = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txUpd, dbname,
            "UPDATE orders SET amount = 99 WHERE id = \"order-upd\"", null));
        await database.Transactions.CommitAsync(txUpd);

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "UPDATE commit must evict the range-dep cache entry via CommitAsync hook");
    }

    [Test]
    public async Task Delete_EvictsRangeDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txIns, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"order-del\", 77)", null));
        await database.Transactions.CommitAsync(txIns);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "orders"));
        string fp = await PublishRangeDep(database, table, "fp-delete");

        KvTransaction txDel = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txDel, dbname,
            "DELETE FROM orders WHERE id = \"order-del\"", null));
        await database.Transactions.CommitAsync(txDel);

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "DELETE commit must evict the range-dep cache entry via CommitAsync hook");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Rolled-back write must NOT evict
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RolledBackInsert_PreservesRangeDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "orders"));
        string fp = await PublishRangeDep(database, table, "fp-rollback");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"order-rb\", 1)", null));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Not.Null,
            "A rolled-back write must not evict cached entries");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. DDL — postCommitInvalidate hook → InvalidateByTableId → eviction
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task AlterTable_AddColumn_EvictsSchemaDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        string tableId = database.Schema.Tables["orders"].Id!;
        int version = database.Schema.Tables["orders"].Version;
        string fp = await PublishSchemaDep(database, tableId, version, "fp-alter-add");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "ALTER TABLE orders ADD COLUMN notes STRING NULL", null));

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "ALTER TABLE ADD COLUMN must evict schema-dep entries via postCommitInvalidate");
    }

    [Test]
    public async Task AlterTable_DropColumn_EvictsSchemaDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txDdl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txDdl, dbname,
            "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL, extra STRING NULL)",
            null));

        string tableId = database.Schema.Tables["orders"].Id!;
        int version = database.Schema.Tables["orders"].Version;
        string fp = await PublishSchemaDep(database, tableId, version, "fp-alter-drop");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "ALTER TABLE orders DROP COLUMN extra", null));

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "ALTER TABLE DROP COLUMN must evict schema-dep entries via postCommitInvalidate");
    }

    [Test]
    public async Task DropTable_EvictsSchemaDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        string tableId = database.Schema.Tables["orders"].Id!;
        int version = database.Schema.Tables["orders"].Version;
        string fp = await PublishSchemaDep(database, tableId, version, "fp-drop-table");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "DROP TABLE orders", null));

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "DROP TABLE must evict schema-dep entries via postCommitInvalidate");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Add/Drop index — postCommitInvalidate → eviction
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task AlterIndex_AddIndex_EvictsSchemaDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        string tableId = database.Schema.Tables["orders"].Id!;
        int version = database.Schema.Tables["orders"].Version;
        string fp = await PublishSchemaDep(database, tableId, version, "fp-add-index");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE INDEX idx_amount ON orders (amount)", null));

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "CREATE INDEX must evict schema-dep entries");
    }

    [Test]
    public async Task AlterIndex_DropIndex_EvictsSchemaDepEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        KvTransaction txIdx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txIdx, dbname,
            "CREATE INDEX idx_amount ON orders (amount)", null));

        string tableId = database.Schema.Tables["orders"].Id!;
        int version = database.Schema.Tables["orders"].Version;
        string fp = await PublishSchemaDep(database, tableId, version, "fp-drop-index");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "ALTER TABLE orders DROP INDEX idx_amount", null));

        Assert.That(await Cache.TryGetAsync(database.Id, "test", fp), Is.Null,
            "DROP INDEX must evict schema-dep entries");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. DROP DATABASE → InvalidateDatabase → evicts all entries for that db
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task DropDatabase_EvictsAllEntriesForDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "orders"));
        string tableId = table.Id;
        int version = table.Schema.Version;

        // Publish two entries: one range dep, one schema dep.
        string fp1 = await PublishRangeDep(database, table, "fp-db-drop-range");
        string fp2 = await PublishSchemaDep(database, tableId, version, "fp-db-drop-schema");

        string dbId = database.Id;
        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        Assert.That(await Cache.TryGetAsync(dbId, "test", fp1), Is.Null,
            "DROP DATABASE must evict range-dep entries for the database");
        Assert.That(await Cache.TryGetAsync(dbId, "test", fp2), Is.Null,
            "DROP DATABASE must evict schema-dep entries for the database");
    }
}
