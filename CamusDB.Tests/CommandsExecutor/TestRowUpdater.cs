﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowUpdater : BaseTest
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

    private async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
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

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                        { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    /*[Test]
    [NonParallelizable]
    public async Task TestInvalidDatabase()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "another_factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
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
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "unknown_table",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.UpdateById(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNotNullColumWithNull()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.Null, "") }
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.UpdateById(ticket));
        Assert.AreEqual("Column 'name' cannot be null", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNotNullColumWithNull2()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.Null, null!) }
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.UpdateById(ticket));
        Assert.AreEqual("Column 'name' cannot be null", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateByIdSingleRow()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        Assert.AreEqual(1, await executor.UpdateById(ticket));

        QueryByIdTicket queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateUnknownRow()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: "---",
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        Assert.AreEqual(0, await executor.UpdateById(ticket));
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateByIdSingleRowTwice()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        Assert.AreEqual(1, await executor.UpdateById(ticket));

        QueryByIdTicket queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);

        ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0],
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value 2") }
            }
        );

        Assert.AreEqual(1, await executor.UpdateById(ticket));

        queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value 2", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdate()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        foreach (string objectId in objectsId)
        {
            UpdateByIdTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                id: objectId,
                values: new Dictionary<string, ColumnValue>()
                {
                    { "name", new ColumnValue(ColumnType.String, "updated value") }
                }
            );

            Assert.AreEqual(1, await executor.UpdateById(ticket));
        }

        foreach (string objectId in objectsId)
        {
            QueryByIdTicket queryByIdTicket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
            Assert.IsNotEmpty(result);

            Assert.AreEqual(objectId, result[0]["id"].StrValue);
            Assert.AreEqual("updated value", result[0]["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdateParallel()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        List<Task> tasks = new();

        foreach (string objectId in objectsId)
        {
            UpdateByIdTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                id: objectId,
                values: new Dictionary<string, ColumnValue>()
                {
                    { "name", new ColumnValue(ColumnType.String, "updated value") }
                }
            );

            tasks.Add(executor.UpdateById(ticket));
        }

        await Task.WhenAll(tasks);

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
        Assert.IsNotEmpty(result);

        queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("name", "=", new ColumnValue(ColumnType.String, "updated value"))
            },
            limit: null,
            offset: null,
            orderBy: null,
            parameters: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("name", "=", new ColumnValue(ColumnType.String, "another updated value"))
            },
            limit: null,
            offset: null,
            orderBy: null,
            parameters: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicUpdate()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            plainValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        Assert.AreEqual(1, await executor.Update(ticket));

        QueryByIdTicket queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateMany()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        UpdateTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            plainValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, 2010))
            },
            parameters: null
        );

        Assert.AreEqual(14, await executor.Update(ticket));

        QueryTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, 2010))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(14, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreEqual(row["name"].StrValue, "updated value");
        }

        queryTicket = new(
            txnId: await executor.NextTxnId(),
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("year", "<=", new ColumnValue(ColumnType.Integer64, 2010))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(11, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreNotEqual(row["name"].StrValue, "updated value");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNone()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        UpdateTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            where: null,
            plainValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            },
            exprValues: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, 200010))
            },
            parameters: null
        );

        Assert.AreEqual(0, await executor.Update(ticket));
    }
}
