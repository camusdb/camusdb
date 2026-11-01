
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
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The cluster-mode arm of <see cref="TestFailedStatementAbortsTransaction"/>: the schema is a
/// replicated log, and the row keys and index keys of a table are owned by different Kahuna actors, so
/// the batch of a multi-row INSERT runs Kahuna's check round and stage round across actors. A refused
/// batch stages nothing on any actor, and the failed statement aborts the transaction as it does on a
/// single node.
/// </summary>
// Serial: boots an embedded Kahuna node for the fixture, as every node-booting fixture does.
[NonParallelizable]
internal sealed class TestFailedStatementAbortsTransactionCluster : BaseTest
{
    private EmbeddedKahuna? node;
    private string storageRoot = "";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-abort-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"test-abort-{Guid.NewGuid():N}",
            Storage = "sqlite",
            StoragePath = storageRoot,
            WalStorage = "sqlite",
            WalPath = storageRoot,
            InitialPartitions = 4
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

    /// <summary>A cluster-mode engine over this fixture's own node.</summary>
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options, sharedNode: node!, isClusterMode: true);
    }

    /// <summary>The committed (id, v) pairs of <c>x</c>, in key order.</summary>
    private static async Task<List<(long, long)>> Pairs(CommandExecutor executor, DatabaseDescriptor database, string db)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SELECT id, v FROM x", null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return rows.Select(r => (r.Row["id"].LongValue, r.Row["v"].LongValue)).OrderBy(p => p.Item1).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task Insert_PrimaryKeyConflictOnALaterRow_InClusterMode_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE TABLE x (id INT64 PRIMARY KEY, v INT64)", null));
        await database.Transactions.RollbackIfNotCompletedAsync(ddl);

        KvTransaction seed = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(seed, db, "INSERT INTO x (id, v) VALUES (1, 1)", null));
        await database.Transactions.CommitAsync(seed);

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (21, 21), (1, 1)", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, failure.Code, failure.Message);

        // Aborted: the next statement is refused before it runs, and the commit rolls the transaction back.
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (21, 21)", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, refused.Code, refused.Message);

        CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => database.Transactions.CommitAsync(tx))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, commit.Code, commit.Message);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);

        CollectionAssert.AreEqual(new List<(long, long)> { (1, 1) }, await Pairs(executor, database, db));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }
}
