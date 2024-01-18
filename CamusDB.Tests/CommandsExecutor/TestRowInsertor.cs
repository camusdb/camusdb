
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Linq;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestRowInsertor : BaseTest
{    
    private async Task<(string, CommandExecutor)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        await executor.CreateDatabase(databaseTicket);

        return (dbname, executor);
    }

    private async Task<(string, CommandExecutor)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        return (dbname, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTypeAssigned()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Integer64, 1) },
                    { "name", new ColumnValue(ColumnType.String, "some name") },
                    { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(ticket);
        });
        Assert.AreEqual("Type Integer64 cannot be assigned to id (Id)", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidIntegerType()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Integer64, 1) },
                    { "name", new ColumnValue(ColumnType.String, "some name") },
                    { "year", new ColumnValue(ColumnType.Integer64, "invalid int value") },
                    { "enabled", new ColumnValue(ColumnType.Bool, 1234) },
                }
            );

            await executor.Insert(ticket);
        });

        Assert.AreEqual("Only type ColumnType.String to string value", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidBoolType()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Integer64, 1) },
                    { "name", new ColumnValue(ColumnType.String, "some name") },
                    { "year", new ColumnValue(ColumnType.Integer64, 1000) },
                    { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
                }
            );

            await executor.Insert(ticket);
        });

        Assert.AreEqual("Only type ColumnType.String to string value", e!.Message);
    }

    /*[Test]
    [NonParallelizable]
    public async Task TestInvalidDatabase()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "another_factory",
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "FALSE") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Database doesn't exist", e!.Message);
    }*/

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTable()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "unknown_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, true) },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInsertUnknownColum()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "unknownColumn", new ColumnValue(ColumnType.Bool, true) },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Unknown column 'unknownColumn' in column list", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInsertNotNullColumWithNull()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.Null, "") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Column 'name' cannot be null", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestTwoInserts()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        await executor.Insert(ticket);

        ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f191e810c19729de860ea") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, true) },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestTwoInsertsParallel()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        InsertTicket ticket2 = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f191e810c19729de860ea") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, true) },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulInsert()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket insertTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        await executor.Insert(insertTicket);

        QueryByIdTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: "507f1f77bcf86cd799439011"
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f1f77bcf86cd799439011");

        Assert.AreEqual(row["name"].Type, ColumnType.String);
        Assert.AreEqual(row["name"].StrValue, "some name");

        Assert.AreEqual(row["year"].Type, ColumnType.Integer64);
        Assert.AreEqual(row["year"].LongValue, 1234);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulInsertWithNulls()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket insertTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Null, "") },
                { "enabled", new ColumnValue(ColumnType.Null, "") },
            }
        );

        await executor.Insert(insertTicket);

        QueryByIdTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: "507f1f77bcf86cd799439011"
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f1f77bcf86cd799439011");

        Assert.AreEqual(row["name"].Type, ColumnType.String);
        Assert.AreEqual(row["name"].StrValue, "some name");

        Assert.AreEqual(row["year"].Type, ColumnType.Null);
        Assert.AreEqual(row["enabled"].Type, ColumnType.Null);
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulTwoParallelInserts()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "name", new ColumnValue(ColumnType.String, "some name 1") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        InsertTicket ticket2 = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f191e810c19729de860ea") },
                { "name", new ColumnValue(ColumnType.String, "some name 2") },
                { "year", new ColumnValue(ColumnType.Integer64, 4567) },
                { "enabled", new ColumnValue(ColumnType.Bool, true) },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        QueryByIdTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: "507f191e810c19729de860ea"
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f191e810c19729de860ea");

        Assert.AreEqual(row["name"].Type, ColumnType.String);
        Assert.AreEqual(row["name"].StrValue, "some name 2");

        Assert.AreEqual(row["year"].Type, ColumnType.Integer64);
        Assert.AreEqual(row["year"].LongValue, 4567);

        Assert.AreEqual(row["enabled"].Type, ColumnType.Bool);
        Assert.AreEqual(row["enabled"].BoolValue, true);

        QueryByIdTicket queryTicket2 = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: "507f1f77bcf86cd799439011"
        );

        result = await (await executor.QueryById(queryTicket2)).ToListAsync();

        row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f1f77bcf86cd799439011");

        Assert.AreEqual(row["name"].Type, ColumnType.String);
        Assert.AreEqual(row["name"].StrValue, "some name 1");

        Assert.AreEqual(row["year"].Type, ColumnType.Integer64);
        Assert.AreEqual(row["year"].LongValue, 1234);

        Assert.AreEqual(row["enabled"].Type, ColumnType.Bool);
        Assert.AreEqual(row["enabled"].BoolValue, false);
    }

    private static async Task DoInsert(string dbname, CommandExecutor executor)
    {
        InsertTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer64, 1234) },
                { "enabled", new ColumnValue(ColumnType.Bool, false) },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulMultipleParallelInserts()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        List<Task> tasks = new();

        for (int i = 0; i < 100; i++)
            tasks.Add(DoInsert(dbname, executor));

        await Task.WhenAll(tasks);

        Stopwatch stopwatch = Stopwatch.StartNew();

        tasks = new();

        for (int i = 0; i < 100; i++)
            tasks.Add(DoInsert(dbname, executor));

        await Task.WhenAll(tasks);

        System.Console.WriteLine(stopwatch.ElapsedMilliseconds);

        QueryTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["id"].Type, ColumnType.Id);
            Assert.AreEqual(row["id"].StrValue!.Length, 24);

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreEqual(row["name"].StrValue, "some name");

            Assert.AreEqual(row["year"].Type, ColumnType.Integer64);
            Assert.AreEqual(row["year"].LongValue, 1234);

            Assert.AreEqual(row["enabled"].Type, ColumnType.Bool);
            Assert.AreEqual(row["enabled"].BoolValue, false);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsert()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        int i;
        List<string> objectIds = new();

        for (i = 0; i < 50; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();
            objectIds.Add(objectId);

            InsertTicket insertTicket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer64, i * 1000) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(insertTicket);
        }

        i = 0;

        foreach (string objectId in objectIds)
        {
            QueryByIdTicket queryTicket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

            Dictionary<string, ColumnValue> row = result[0];

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].StrValue!.Length);

            Assert.AreEqual(ColumnType.String, row["name"].Type);
            Assert.AreEqual("some name " + i, row["name"].StrValue);

            Assert.AreEqual(ColumnType.Integer64, row["year"].Type);
            Assert.AreEqual(i * 1000, row["year"].LongValue);

            i++;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsertWithQuery()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        for (int i = 0; i < 50; i++)
        {
            InsertTicket insertTicket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer64, i * 1000) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();

        for (int i = 0; i < 50; i++)
        {
            Dictionary<string, ColumnValue> row = result[i].Row;

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].StrValue!.Length);

            Assert.AreEqual(ColumnType.String, row["name"].Type);
            Assert.AreEqual("some name " + i, row["name"].StrValue);

            Assert.AreEqual(ColumnType.Integer64, row["year"].Type);
            Assert.AreEqual(i * 1000, row["year"].LongValue);
        }
    }
}
