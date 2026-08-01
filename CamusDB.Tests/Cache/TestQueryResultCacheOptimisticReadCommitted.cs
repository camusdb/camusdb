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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cache;

/// <summary>
/// The result cache under <b>Optimistic locking with Read Committed isolation</b>.
///
/// The rest of the cache suite runs on the configured defaults (Serializable / Pessimistic), so
/// none of it exercises this combination. It behaves differently on both axes that matter here:
/// an optimistic transaction takes no locks while it runs and folds its writes at commit, and a
/// Read Committed autocommit read pins no snapshot. Cache correctness must not depend on either —
/// invalidation consumes the committed modified-key set, not the lock set.
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheOptimisticReadCommitted : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    protected override CommandExecutor CreateCommandExecutor()
    {
        _cache = new QueryResultCache(CamusDBConfig.Ambient, sweepIntervalMs: -1);
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, CamusDBConfig.Ambient,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    private static async Task SetupRobotsTable(string dbname, DatabaseDescriptor db, CommandExecutor exec)
    {
        KvTransaction ddl = await db.Transactions.BeginAsync();
        await exec.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE robots (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)", null));

        KvTransaction ins = await db.Transactions.BeginAsync();
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO robots (id, name) VALUES (\"r1\", \"alpha\")", null));
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO robots (id, name) VALUES (\"r2\", \"beta\")", null));
        await db.Transactions.CommitAsync(ins);
    }

    /// <summary>Opens the explicit transaction under test: Read Committed isolation, optimistic locking.</summary>
    private static Task<KvTransaction> BeginOptimisticReadCommittedAsync(DatabaseDescriptor db) =>
        db.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted,
            CamusTransactionMode.ReadWrite,
            locking: KeyValueTransactionLocking.Optimistic);

    /// <summary>Runs the hinted SELECT as an autocommit read and returns its metadata plus rows.</summary>
    private static async Task<(CacheMetadataHolder Meta, List<QueryResultRow> Rows)> RunHintedSelect(
        string dbname, CommandExecutor exec, string cacheName, KvTransaction? tx = null)
    {
        var meta = new CacheMetadataHolder();
        var rows = new List<QueryResultRow>();
        KvTransaction readTx = tx ?? KvTransaction.CreateReadOnly();
        string sql = $"SELECT * FROM robots{{cache={cacheName}}}";
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await exec.ExecuteSQLQuery(new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return (meta, rows);
    }

    private static string NameOf(List<QueryResultRow> rows, string id) =>
        rows.First(r => r.Row["id"].StrValue == id).Row["name"].StrValue!;

    [Test]
    public async Task CommittedUpdate_EvictsHintedEntry_AndTheNextSelectSeesTheNewValue()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "opt_rc_commit";

        (CacheMetadataHolder cold, _) = await RunHintedSelect(dbname, exec, cacheName);
        Assert.That(cold.Status, Is.EqualTo(QueryCacheStatus.Miss), "First hinted SELECT must publish");

        (CacheMetadataHolder warm, List<QueryResultRow> warmRows) = await RunHintedSelect(dbname, exec, cacheName);
        Assert.That(warm.Status, Is.EqualTo(QueryCacheStatus.Hit), "Second hinted SELECT must be served from cache");
        Assert.That(NameOf(warmRows, "r1"), Is.EqualTo("alpha"));

        KvTransaction tx = await BeginOptimisticReadCommittedAsync(db);
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "UPDATE robots SET name = \"gamma\" WHERE id = \"r1\"", null));
        await db.Transactions.CommitAsync(tx);

        (CacheMetadataHolder afterCommit, List<QueryResultRow> afterRows) =
            await RunHintedSelect(dbname, exec, cacheName);

        Assert.That(afterCommit.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "The committed write must have evicted the entry, so this SELECT re-executes");
        Assert.That(NameOf(afterRows, "r1"), Is.EqualTo("gamma"),
            "The re-executed SELECT must return the committed value, not the cached one");

        (CacheMetadataHolder rewarmed, List<QueryResultRow> rewarmedRows) =
            await RunHintedSelect(dbname, exec, cacheName);
        Assert.That(rewarmed.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "The post-write result must be cacheable again");
        Assert.That(NameOf(rewarmedRows, "r1"), Is.EqualTo("gamma"));
    }

    [Test]
    public async Task HintedSelectInsideTheTransaction_BypassesCacheAndSeesItsOwnWrite()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "opt_rc_ryow";

        await RunHintedSelect(dbname, exec, cacheName);          // publish "alpha"
        (CacheMetadataHolder warm, _) = await RunHintedSelect(dbname, exec, cacheName);
        Assert.That(warm.Status, Is.EqualTo(QueryCacheStatus.Hit));

        KvTransaction tx = await BeginOptimisticReadCommittedAsync(db);
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "UPDATE robots SET name = \"delta\" WHERE id = \"r1\"", null));

        (CacheMetadataHolder inTx, List<QueryResultRow> inTxRows) =
            await RunHintedSelect(dbname, exec, cacheName, tx);

        Assert.That(inTx.Status, Is.EqualTo(QueryCacheStatus.Bypass),
            "A hinted SELECT inside a write transaction must not be served from the cache");
        Assert.That(inTx.BypassReason, Is.EqualTo(QueryCacheBypassReason.InTransaction));
        Assert.That(NameOf(inTxRows, "r1"), Is.EqualTo("delta"),
            "Read-your-writes: the transaction must see its own uncommitted update, never the cached rows");

        await db.Transactions.RollbackAsync(tx);
    }

    [Test]
    public async Task RolledBackUpdate_PreservesTheHintedEntry()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "opt_rc_rollback";

        await RunHintedSelect(dbname, exec, cacheName);          // publish "alpha"

        KvTransaction tx = await BeginOptimisticReadCommittedAsync(db);
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "UPDATE robots SET name = \"epsilon\" WHERE id = \"r1\"", null));
        await db.Transactions.RollbackAsync(tx);

        (CacheMetadataHolder afterRollback, List<QueryResultRow> rows) =
            await RunHintedSelect(dbname, exec, cacheName);

        Assert.That(afterRollback.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "A rolled-back write changed nothing, so the entry must survive");
        Assert.That(NameOf(rows, "r1"), Is.EqualTo("alpha"),
            "The preserved entry must still hold the pre-transaction value");
    }
}
