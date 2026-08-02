/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Reproduction for the reported duplicate-primary-key issue, run against a MULTI-PARTITION node.
/// Production logs show the row key on one partition (persistent-keyvalue-1) and the ~pk / code
/// index keys on another (persistent-keyvalue-7). The default test fixtures use InitialPartitions=1,
/// so row + indexes share a partition and the bug is masked. This fixture spins up its own node with
/// many partitions to exercise cross-partition unique-index lookups and 2PC.
/// </summary>
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestMultiPartitionUniqueIndex : BaseTest
{
    private const string Id = "1e8921c8-58ed-483e-b4f2-c0f43cbc6c22";
    private const int Partitions = 8;

    private EmbeddedKahuna? node;
    private string storageRoot = "";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        // Match production: SQLite-backed, 8 partitions (so row vs index keys land on different
        // partitions/shards, exactly as in the reported wcbets node).
        storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-mp-{System.Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);

        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"test-multipart-{System.Guid.NewGuid():N}",
            Storage = "sqlite",
            StoragePath = storageRoot,
            WalStorage = "sqlite",
            WalPath = storageRoot,
            InitialPartitions = Partitions
        }.WithTestNodeDefaults());

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);
        await node.FlushAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (node is not null)
            await node.DisposeAsync();

        try { if (storageRoot.Length > 0) System.IO.Directory.Delete(storageRoot, recursive: true); }
        catch { /* best-effort */ }
    }

    /// <summary>
    /// A cluster-mode engine over this fixture's own node. The options-taking overload is the one to
    /// override, so the parameterless factory and <c>CreateDatabase(options)</c> both route through it.
    /// </summary>
    protected override CommandExecutor CreateCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options, sharedNode: node!, isClusterMode: true);
    }

    private static async Task CreateTeams(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL, name_es STRING NULL)",
            null));
        Assert.IsTrue(ddl.Success);
    }

    private static async Task<int> CountAll(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx, dbname, "SELECT id FROM teams", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }

    private static async Task<int> SelectByIdCount(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx, dbname, $"SELECT id FROM teams WHERE id = \"{Id}\"", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionLookupByPrimaryKeyAfterCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(txIns, dbname,
            $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", null)", null));
        await database.Transactions.CommitAsync(txIns);

        int byId = await SelectByIdCount(dbname, database, executor);
        Assert.AreEqual(1, byId, $"PK lookup after commit must find the row, found {byId}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionDuplicatePrimaryKeyRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        const string sql = "INSERT INTO teams (id, code, name, name_es) VALUES (\"" + Id + "\", \"BEL\", \"Belgium\", null)";

        KvTransaction tx1 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(tx1, dbname, sql, null));
        await database.Transactions.CommitAsync(tx1);

        KvTransaction tx2 = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new(tx2, dbname, sql, null)));
            Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code);
        }
        finally
        {
            // Duplicate detection happens after the multi-partition write set is registered.
            // Release those locks before CountAll opens its verification transaction.
            await database.Transactions.RollbackIfNotCompletedAsync(tx2);
        }

        int total = await CountAll(dbname, database, executor);
        Assert.AreEqual(1, total, $"Expected 1 row after duplicate insert attempt, found {total}");
    }

    /// <summary>
    /// Direct repro of the orphaning mechanism: insert, UPDATE in place (PK unchanged), then look
    /// the row up BY ID (index path). RowUpdater.UpdateUniqueIndexes deletes+re-puts the ~pk entry
    /// even though id didn't change; if that leaves the entry tombstoned, the by-id lookup returns 0.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionLookupByIdAfterInPlaceUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(txIns, dbname,
            $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", null)", null));
        await database.Transactions.CommitAsync(txIns);

        Assert.AreEqual(1, await SelectByIdCount(dbname, database, executor), "lookup by id should find the row right after insert");

        // In-place update — PK (id) unchanged; only name_es changes.
        KvTransaction txUpd = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult upd = await executor.ExecuteNonSQLQuery(new(txUpd, dbname,
            $"UPDATE teams SET name_es = \"Belgica\" WHERE id = \"{Id}\"", null));
        await database.Transactions.CommitAsync(txUpd);
        Assert.AreEqual(1, upd.ModifiedRows, "update should match the row");

        // The bug: after the in-place update, the ~pk entry is tombstoned, so by-id lookup misses.
        int afterUpdate = await SelectByIdCount(dbname, database, executor);
        Assert.AreEqual(1, afterUpdate, $"lookup by id must still find the row after an in-place update, found {afterUpdate}");
    }

    /// <summary>
    /// Reproduces the production orphaning: repeatedly delete+re-insert the SAME primary key. The
    /// revision number recurs across delete→re-set cycles; TryCommitMutationsHandler used to throw
    /// (Dictionary.Add duplicate revision), aborting the index entry's commit while the row commit
    /// succeeded — leaving the row alive but its ~pk entry tombstoned (orphan). After enough cycles
    /// that desync makes by-id lookups miss and duplicates accumulate. With the fix, every cycle
    /// leaves exactly one row reachable by id.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionRepeatedDeleteReinsertKeepsIndexConsistent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(txIns, dbname,
            $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", null)", null));
        await database.Transactions.CommitAsync(txIns);

        for (int cycle = 0; cycle < 8; cycle++)
        {
            KvTransaction txDel = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new(txDel, dbname, $"DELETE FROM teams WHERE id = \"{Id}\"", null));
            await database.Transactions.CommitAsync(txDel);

            KvTransaction txReins = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new(txReins, dbname,
                $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", \"v{cycle}\")", null));
            await database.Transactions.CommitAsync(txReins);

            int byId = await SelectByIdCount(dbname, database, executor);
            int total = await CountAll(dbname, database, executor);
            Assert.AreEqual(1, byId, $"cycle {cycle}: lookup by id must find exactly 1 row (orphaned index), found {byId}");
            Assert.AreEqual(1, total, $"cycle {cycle}: table must hold exactly 1 row (no duplicates), found {total}");
        }
    }

    /// <summary>
    /// App-reported scenario (row-by-row clear then re-import): delete a row, then insert a NEW row
    /// (fresh id) that reuses the same unique non-PK value (code). The DELETE must remove the unique
    /// `code` index entry too, otherwise the re-insert is wrongly rejected as a duplicate. Before the
    /// TryCommitMutations fix, the index entry's delete-commit threw and was skipped, leaving a stale
    /// `code` entry that blocked the new insert.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionDeleteFreesUniqueNonPkIndexForReinsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        const string code = "SEN";

        // Several import/clear cycles, each using a brand-new id but the SAME unique code.
        for (int cycle = 0; cycle < 5; cycle++)
        {
            string id = $"sen-{cycle}-{System.Guid.NewGuid():n}";

            KvTransaction txIns = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new(txIns, dbname,
                $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{id}\", \"{code}\", \"Senegal\", null)", null));
            await database.Transactions.CommitAsync(txIns);   // must not be rejected as duplicate code

            int total = await CountAll(dbname, database, executor);
            Assert.AreEqual(1, total, $"cycle {cycle}: exactly 1 row expected after insert, found {total}");

            // Clear by deleting the row, then loop to re-import with a new id + same code.
            KvTransaction txDel = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new(txDel, dbname, $"DELETE FROM teams WHERE id = \"{id}\"", null));
            await database.Transactions.CommitAsync(txDel);

            Assert.AreEqual(0, await CountAll(dbname, database, executor), $"cycle {cycle}: row should be gone after delete");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiPartitionUpsertTwiceLeavesSingleRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        async Task Upsert(string nameEs)
        {
            KvTransaction txU = await database.Transactions.BeginAsync();
            ExecuteNonSQLResult upd = await executor.ExecuteNonSQLQuery(new(txU, dbname,
                $"UPDATE teams SET name_es = \"{nameEs}\" WHERE id = \"{Id}\"", null));
            await database.Transactions.CommitAsync(txU);

            if (upd.ModifiedRows == 0)
            {
                KvTransaction txD = await database.Transactions.BeginAsync();
                await executor.ExecuteNonSQLQuery(new(txD, dbname, $"DELETE FROM teams WHERE id = \"{Id}\"", null));
                await database.Transactions.CommitAsync(txD);

                KvTransaction txI = await database.Transactions.BeginAsync();
                await executor.ExecuteNonSQLQuery(new(txI, dbname,
                    $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", \"{nameEs}\")", null));
                await database.Transactions.CommitAsync(txI);
            }
        }

        await Upsert("Belgium");
        await Upsert("Belgica");

        int total = await CountAll(dbname, database, executor);
        Assert.AreEqual(1, total, $"Upsert run twice must leave exactly 1 row, found {total}");
    }
}
