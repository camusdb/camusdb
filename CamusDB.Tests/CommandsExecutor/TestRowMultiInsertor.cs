
using NUnit.Framework;

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
internal sealed class TestRowMultiInsertor : SharedNodeBaseTest
{
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupDatabase(CamusDBOptions? options = null)
    {
        return await CreateDatabase(options ?? Options);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupMultiIndexTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "user_robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("robots_id", ColumnType.Id, notNull: true),
                new ColumnInfo("amount", ColumnType.Integer64)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new ConstraintInfo(ConstraintType.IndexMulti, "robots_id_idx", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    [Test]
    [Order(1)]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupMultiIndexTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        InsertTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            values: new()
            {
                new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, "5bc30818bc6a4e7b6c441308") },
                    { "robots_id", new ColumnValue(ColumnType.Id, "5e1aac86542f77367452d9b3") },
                    { "amount", new ColumnValue(ColumnType.Integer64, 100) }
                }
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [Order(2)]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsertWithQueryIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupMultiIndexTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();
        
        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "user_robots",
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "robots_id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "amount", new ColumnValue(ColumnType.Integer64, i * 1000) }
                    }
                }
            );

            await executor.Insert(insertTicket);
        }

        await database.Transactions.CommitAsync(txnState);

        KvTransaction txnState2 = await database.Transactions.BeginAsync();

        QueryTicket queryTicket = new(
            txnState: txnState2,
            databaseName: dbname,
            tableName: "user_robots",
            index: "robots_id_idx",
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        for (int i = 0; i < 10; i++)
        {
            IReadOnlyDictionary<string, ColumnValue> row = result[i].Row;
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].StrValue!.Length);

            Assert.AreEqual(ColumnType.Id, row["robots_id"].Type);
            Assert.AreEqual(24, row["robots_id"].StrValue!.Length);

            Assert.AreEqual(ColumnType.Integer64, row["amount"].Type);
            Assert.AreEqual(i * 1000, row["amount"].LongValue);
        }
    }

    // -----------------------------------------------------------------------
    // Chunked INSERT tests
    // -----------------------------------------------------------------------

    private static async Task<List<QueryResultRow>> QueryAllAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        QueryTicket qt = new(
            txnState: tx, databaseName: dbname, tableName: table,
            index: null, projection: null, filters: null, where: null,
            orderBy: null, limit: null, offset: null, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(qt);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupUniqueIndexTable(
        CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase(options);

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("code", ColumnType.String, notNull: true),
                new("value", ColumnType.Integer64)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "code_idx", new ColumnIndexInfo[] { new("code", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task ChunkedInsert_ManyRows_AllInserted()
    {
        // Force small chunks so 15 rows span multiple batches — verifies chunking produces
        // correct results with no missed or double-applied mutations.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupUniqueIndexTable(Options with { ForceSpillThresholdRows = 4 });

        List<Dictionary<string, ColumnValue>> rows = new();
        for (int i = 0; i < 15; i++)
        rows.Add(new()
        {
            { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "code", new ColumnValue(ColumnType.String, $"C{i:D4}") },
            { "value", new ColumnValue(ColumnType.Integer64, i) }
        });

        KvTransaction tx = await database.Transactions.BeginAsync();
        InsertTicket ticket = new(txnState: tx, databaseName: dbname, tableName: "items", values: rows);
        await executor.Insert(ticket);
        await database.Transactions.CommitAsync(tx);

        List<QueryResultRow> result = await QueryAllAsync(executor, database, dbname, "items");
        Assert.AreEqual(15, result.Count, "All rows must be present after chunked insert");
    }

    [Test]
    [NonParallelizable]
    public async Task ChunkedInsert_DuplicateUniqueKeyInLaterChunk_RollsBackAllChunks()
    {
        // With chunkSize=4 and 13 rows, chunks land at rows [0..3], [4..7], [8..11], [12].
        // Row 12 carries the same unique 'code' as row 0, which was already committed in the
        // first chunk. The insert must throw DuplicateUniqueKeyValue and no row from any chunk
        // must survive — the shared KvTransaction rolls back all staged writes on abort.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupUniqueIndexTable(Options with { ForceSpillThresholdRows = 4 });

        List<Dictionary<string, ColumnValue>> rows = new();
        for (int i = 0; i < 13; i++)
            rows.Add(new()
            {
                { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "code", new ColumnValue(ColumnType.String, i == 12 ? "C0000" : $"C{i:D4}") },
                { "value", new ColumnValue(ColumnType.Integer64, i) }
            });

        KvTransaction tx = await database.Transactions.BeginAsync();
        InsertTicket ticket = new(txnState: tx, databaseName: dbname, tableName: "items", values: rows);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code,
            "A duplicate unique key in a later chunk must throw DuplicateUniqueKeyValue");

        await database.Transactions.RollbackAsync(tx);

        List<QueryResultRow> result = await QueryAllAsync(executor, database, dbname, "items");
        Assert.AreEqual(0, result.Count,
            "No rows from any chunk must survive after a rolled-back chunked insert");
    }

    [Test]
    [Order(3)]
    [NonParallelizable]
    public async Task TestSameKeyMultiInsertWithQueryIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupMultiIndexTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "user_robots",
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "robots_id", new ColumnValue(ColumnType.Id, "5e1aac86542f77367452d9b3") },
                        { "amount", new ColumnValue(ColumnType.Integer64, i * 1000) }
                    }
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            index: "robots_id_idx",
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.AreEqual(10, result.Count);

        for (int i = 0; i < 10; i++)
        {
            IReadOnlyDictionary<string, ColumnValue> row = result[i].Row;
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(row["id"].Type, ColumnType.Id);
            Assert.AreEqual(row["id"].StrValue!.Length, 24);

            Assert.AreEqual(row["robots_id"].Type, ColumnType.Id);
            Assert.AreEqual(row["robots_id"].StrValue, "5e1aac86542f77367452d9b3");

            Assert.AreEqual(row["amount"].Type, ColumnType.Integer64);
            Assert.AreEqual(row["amount"].LongValue, i * 1000);
        }
    }
}