/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
/// The cluster-mode arm of <see cref="TestDeclaredIntegerRange"/>: a schema change reaches the columns
/// through the replicated schema log, so the width must survive the log payload and its apply, and an
/// added column goes through the schema-change coordinator. A width that a payload forgets would leave
/// the check off in cluster mode only, which no standalone test can see.
/// </summary>
// Serial: boots an embedded Kahuna node for the fixture, as every node-booting fixture does.
[NonParallelizable]
internal sealed class TestDeclaredIntegerRangeCluster : BaseTest
{
    private EmbeddedKahuna? node;
    private string storageRoot = "";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-width-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"test-width-{Guid.NewGuid():N}",
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

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertOutOfRange(AsyncTestDelegate action)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(action)!;
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, exception.Code, exception.Message);
    }

    [Test]
    public async Task CreateTable_KeepsTheWidthThroughTheSchemaLog()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint, i integer, a array(smallint), b bigint)");

        await Dml(executor, database, db, "INSERT INTO t (id, s, i, b) VALUES (1, 32767, 2147483647, 2147483648)");

        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET s = 32768 WHERE id = 1"));
        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET i = 2147483648 WHERE id = 1"));
        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET a = ARRAY[32768] WHERE id = 1"));
    }

    [Test]
    public async Task AddColumn_KeepsTheWidthThroughTheCoordinator()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY)");
        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (1)");
        await Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN s smallint DEFAULT(7)");

        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET s = 32768 WHERE id = 1"));
    }

    [Test]
    public async Task RenameColumn_KeepsTheWidthInTheAppliedDelta()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint)");
        await Dml(executor, database, db, "INSERT INTO t (id, s) VALUES (1, 1)");
        await Ddl(executor, database, db, "ALTER TABLE t RENAME COLUMN s TO renamed");

        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET renamed = 32768 WHERE id = 1"));
    }

    /// <summary>In cluster mode a column comment is a delta with its own apply, not the single-node writer.</summary>
    [Test]
    public async Task CommentOnColumn_KeepsTheWidthInTheAppliedDelta()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint)");
        await Ddl(executor, database, db, "COMMENT ON COLUMN t.s IS 'a comment'");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, s) VALUES (1, 32768)"));
    }

    [Test]
    public async Task Relink_KeepsTheWidthThroughTheSchemaLog()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint)");
        string tableId = database.Schema.Tables["t"].Id!;

        Assert.IsTrue(await executor.DropTable(new DropTableTicket(db, "t", ifExists: false)));
        await Ddl(executor, database, db, $"CREATE TABLE back RELINK TO \"{tableId}\"");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO back (id, s) VALUES (1, 32768)"));
    }

    [Test]
    public async Task Reopen_KeepsTheWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint)");
        await Dml(executor, database, db, "INSERT INTO t (id, s) VALUES (1, 1)");

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        DatabaseDescriptor reopened = await executor.OpenDatabase(db);

        AssertOutOfRange(() => Dml(executor, reopened, db, "UPDATE t SET s = 32768 WHERE id = 1"));
    }
}
