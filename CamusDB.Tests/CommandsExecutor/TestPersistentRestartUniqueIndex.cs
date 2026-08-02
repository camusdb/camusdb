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

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Reproduction targeting a node RESTART between operations — the most likely real-world trigger
/// for the reported duplicate keys (the wcbets node accumulated 50 rows / 45 ~pk entries over
/// time, very plausibly across restarts). Single-node, SQLite-backed, 8 partitions, on a fixed
/// storage path that survives the simulated restart (dispose + recreate the node).
/// </summary>
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestPersistentRestartUniqueIndex : BaseTest
{
    private const string Id = "1e8921c8-58ed-483e-b4f2-c0f43cbc6c22";
    private const string Insert =
        "INSERT INTO teams (id, code, name, name_es) VALUES (\"" + Id + "\", \"BEL\", \"Belgium\", null)";

    private static EmbeddedKahuna NewNode(string storageRoot) =>
        new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "test-restart",
            Storage = "sqlite",
            StoragePath = storageRoot,
            WalStorage = "sqlite",
            WalPath = storageRoot,
            InitialPartitions = 8
        }.WithTestNodeDefaults());

    private CommandExecutor NewExecutor(EmbeddedKahuna node) =>
        new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options, sharedNode: node, isClusterMode: true);

    private static async Task<int> CountAll(string dbname, DatabaseDescriptor db, CommandExecutor ex)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await ex.ExecuteSQLQuery(new(tx, dbname, "SELECT id FROM teams", null));
        int n = (await cursor.ToListAsync()).Count;
        await db.Transactions.CommitAsync(tx);
        return n;
    }

    [Test]
    [NonParallelizable]
    [Ignore("In-progress repro for the wcbets duplicate-PK issue. Currently blocked: after dispose+recreate " +
            "of the embedded SQLite node on the same path, OpenDatabase reports 'Table teams doesn't exist' — " +
            "the standalone embedded restart does not reload the catalog the way the CamusDb host does. " +
            "Needs faithful catalog bootstrap on restart before it can probe the duplicate-after-restart path.")]
    public async Task TestDuplicatePrimaryKeyRejectedAfterRestart()
    {
        string storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-restart-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);

        string dbname = "restartdb_" + Guid.NewGuid().ToString("n");
        int afterRestartCount;
        bool secondInsertRejected = false;

        try
        {
            // --- Boot 1: create table + insert the row, then commit and shut down. ---
            EmbeddedKahuna node1 = NewNode(storageRoot);
            await node1.StartAsync(CancellationToken.None);
            await node1.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node1.FlushAsync();

            CommandExecutor ex1 = NewExecutor(node1);
            await ex1.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
            DatabaseDescriptor db1 = await ex1.OpenDatabase(dbname);

            await ex1.ExecuteDDLSQL(new(await db1.Transactions.BeginAsync(), dbname,
                "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL, name_es STRING NULL)", null));

            KvTransaction txIns = await db1.Transactions.BeginAsync();
            ExecuteNonSQLResult r1 = await ex1.ExecuteNonSQLQuery(new(txIns, dbname, Insert, null));
            Assert.AreEqual(1, r1.ModifiedRows);
            await db1.Transactions.CommitAsync(txIns);

            await ex1.CloseDatabase(new CloseDatabaseTicket(dbname));
            await node1.FlushAsync();      // ensure catalog + data are durable before shutdown
            await node1.DisposeAsync();    // simulate process restart

            // --- Boot 2: reopen the same storage and try to insert the same primary key. ---
            EmbeddedKahuna node2 = NewNode(storageRoot);
            await node2.StartAsync(CancellationToken.None);
            await node2.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node2.FlushAsync();

            CommandExecutor ex2 = NewExecutor(node2);
            DatabaseDescriptor db2 = await ex2.OpenDatabase(dbname);

            KvTransaction tx2 = await db2.Transactions.BeginAsync();
            try
            {
                await ex2.ExecuteNonSQLQuery(new(tx2, dbname, Insert, null));
                await db2.Transactions.CommitAsync(tx2);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.DuplicateUniqueKeyValue)
            {
                secondInsertRejected = true;
                await db2.Transactions.RollbackAsync(tx2);
            }

            afterRestartCount = await CountAll(dbname, db2, ex2);

            await ex2.CloseDatabase(new CloseDatabaseTicket(dbname));
            await node2.DisposeAsync();
        }
        finally
        {
            try { System.IO.Directory.Delete(storageRoot, recursive: true); } catch { }
        }

        Assert.IsTrue(secondInsertRejected, "Duplicate primary key after restart must be rejected");
        Assert.AreEqual(1, afterRestartCount, $"Expected exactly 1 row after restart + duplicate insert, found {afterRestartCount}");
    }
}
