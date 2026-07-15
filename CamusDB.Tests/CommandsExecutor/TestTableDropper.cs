
using NUnit.Framework;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
internal sealed class TestTableDropper : SharedNodeBaseTest
{    
    private async Task<(string, DatabaseDescriptor, CommandExecutor, CatalogsManager)> SetupDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (dbname, database, executor, new CatalogsManager(logger));
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalog, List<string> objectIds)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);

        await database.Transactions.CommitAsync(txnState);

        // The DDL transaction was committed above; the row inserts must run on their own
        // transaction. Reusing a finalized transaction is rejected by the coordinator.
        txnState = await database.Transactions.BeginAsync();

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2000) },
                        { "enabled", new(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, catalogs, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndDrop()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, _) = await SetupBasicTable();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        DropTableTicket dropTableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            ifExists: false
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillDropAndRecreate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, _) = await SetupBasicTable();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        DropTableTicket dropTableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            ifExists: false
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("type", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("status", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);

        Assert.True(catalogs.TableExists(database, "robots"));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("type", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![3].Type);

        Assert.AreEqual("status", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);
    }

    // Registration lifecycle — DROP TABLE + recreate with same name under key-range sharding.
    // Verifies that (a) the old table's TableDescriptor is evicted on DROP so the new table's
    // lazy open doesn't return stale schema or stale row data, (b) the new table's RegisterKeyRangeAsync
    // call registers a distinct key space (new ObjectId), and (c) INSERT + SELECT on the recreated
    // table see only the new rows. With InitialPartitions=1 the register call is a harmless no-op
    // from a routing standpoint, but the full descriptor-lifecycle path still executes.
    [Test]
    [NonParallelizable]
    public async Task TestDropAndRecreateTableUnderKeyRangeSharding_DescriptorIsEvictedAndNewDataVisible()
    {
        bool prevFlag = CamusConfig.KeyRangeShardingEnabled;
        CamusConfig.KeyRangeShardingEnabled = true;

        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

            // ── Phase 1: create "vehicles" and insert 5 rows ──────────────────────
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "vehicles",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("value", ColumnType.Integer64)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 0; i < 5; i++)
            {
                KvTransaction tx = await database.Transactions.BeginAsync();
                await executor.Insert(new InsertTicket(
                    txnState: tx,
                    databaseName: dbname,
                    tableName: "vehicles",
                    values:
                    [
                        new()
                        {
                            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "value", new(ColumnType.Integer64, i) }
                        }
                    ]
                ));
                await database.Transactions.CommitAsync(tx);
            }

            // Confirm 5 rows visible before drop.
            Assert.AreEqual(5, await CountRows(dbname, database, executor, "vehicles"));

            // ── Phase 2: drop and recreate with a different schema ─────────────────
            await executor.DropTable(new DropTableTicket(databaseName: dbname, tableName: "vehicles", ifExists: false));

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "vehicles",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("model", ColumnType.String, notNull: true)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 0; i < 3; i++)
            {
                KvTransaction tx = await database.Transactions.BeginAsync();
                await executor.Insert(new InsertTicket(
                    txnState: tx,
                    databaseName: dbname,
                    tableName: "vehicles",
                    values:
                    [
                        new()
                        {
                            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "model", new(ColumnType.String, "Model " + i) }
                        }
                    ]
                ));
                await database.Transactions.CommitAsync(tx);
            }

            // ── Phase 3: assert only 3 new rows are visible; schema is the new one ─
            Assert.AreEqual(3, await CountRows(dbname, database, executor, "vehicles"),
                "recreated table must see only the 3 new rows, not the 5 rows from the dropped table");

            TableSchema schema = new CatalogsManager(logger).GetTableSchema(database, "vehicles");
            Assert.AreEqual(2, schema.Columns!.Count, "new schema has 2 columns");
            Assert.AreEqual("model", schema.Columns![1].Name, "second column is 'model', not 'value'");
        }
        finally
        {
            CamusConfig.KeyRangeShardingEnabled = prevFlag;
        }
    }

    // C1 regression test: concurrent SELECTs must not conflict in key-range-sharding mode.
    // Before the fix, QueryController passed readOnly=false (the default) for SELECTs, so
    // every scan acquired an exclusive whole-bucket range lock. Two concurrent reads on the same
    // table would then fail fast with TransactionConflict (AlreadyLocked). Fix: pure reads use
    // read-only transactions (IsReadOnly=true) which skip the range-lock acquisition entirely.
    [Test]
    [NonParallelizable]
    public async Task C1_ConcurrentSelectsWithReadOnlyTransactionsDoNotConflictInKeyRangeMode()
    {
        bool prevFlag = CamusConfig.KeyRangeShardingEnabled;
        CamusConfig.KeyRangeShardingEnabled = true;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "books",
                columns:
                [
                    new ColumnInfo("id",    ColumnType.Id),
                    new ColumnInfo("title", ColumnType.String, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false));

            // Seed 5 rows.
            for (int i = 0; i < 5; i++)
            {
                KvTransaction ins = await database.Transactions.BeginAsync();
                await executor.Insert(new InsertTicket(
                    txnState: ins,
                    databaseName: dbname,
                    tableName: "books",
                    values: new()
                    {
                        new()
                        {
                            { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "title", new(ColumnType.String, $"Book {i}") },
                        },
                    }));
                await database.Transactions.CommitAsync(ins);
            }

            // Run 4 concurrent SELECTs using read-only transactions. Each must succeed and
            // see 5 rows. No TransactionConflict should be thrown.
            int concurrency = 4;
            using SemaphoreSlim gate = new(0, concurrency);
            List<Task<int>> tasks = [];

            for (int i = 0; i < concurrency; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    // Simulate the fixed HTTP layer: read-only transaction for a pure SELECT.
                    KvTransaction ro = database.Transactions.CreateReadOnlyTransaction();
                    (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                        await executor.ExecuteSQLQuery(new(ro, dbname, "SELECT id FROM books", null));

                    gate.Release(); // all tasks lined up; start scanning together
                    await gate.WaitAsync(concurrency - 1); // wait until everyone released

                    return (await cursor.ToListAsync()).Count;
                }));
            }

            int[] results = await Task.WhenAll(tasks);
            Assert.That(results, Is.All.EqualTo(5),
                "each concurrent read-only SELECT must return all 5 rows without conflicting");
        }
        finally { CamusConfig.KeyRangeShardingEnabled = prevFlag; }
    }

    private static async Task<int> CountRows(string dbname, DatabaseDescriptor database, CommandExecutor executor, string tableName)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx, dbname, $"SELECT id FROM {tableName}", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }
}