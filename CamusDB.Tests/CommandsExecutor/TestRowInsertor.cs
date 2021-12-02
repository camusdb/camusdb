﻿
using System.IO;
using CamusDB.Core;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowInsertor
{
    [SetUp]
    public void Setup()
    {
        string path = Config.DataDirectory + "/factory";
        if (Directory.Exists(path))
        {
            File.Delete(path + "/tablespace0");
            File.Delete(path + "/schema");
            File.Delete(path + "/system");
            Directory.Delete(path);
        }
    }

    private async Task<CommandExecutor> SetupDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: "factory"
        );

        await executor.CreateDatabase(databaseTicket);

        return executor;
    }

    private async Task<CommandExecutor> SetupBasicTable()
    {
        var executor = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            database: "factory",
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(tableTicket);

        return executor;
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTypeAssigned()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Type Integer cannot be assigned to id (Id)", e!.Message);
    }

    [Test]
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
                { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Database doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTable()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "unknown_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInsertUnknownColum()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "unknownColumn", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Unknown column 'unknownColumn' in column list", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestTwoInserts()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        await executor.Insert(ticket);

        ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "true") },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestTwoInsertsParallel()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        InsertTicket ticket2 = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "true") },
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
        var executor = await SetupBasicTable();

        InsertTicket insertTicket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        await executor.Insert(insertTicket);

        QueryByIdTicket queryTicket = new(
            database: "factory",
            name: "robots",
            id: 1
        );

        List<List<ColumnValue>> result = await executor.QueryById(queryTicket);

        List<ColumnValue> row = result[0];

        Assert.AreEqual(row[0].Type, ColumnType.Id);
        Assert.AreEqual(row[0].Value, "1");

        Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "1234");
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulTwoParallelInserts()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name 1") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        InsertTicket ticket2 = new(
            database: "factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name 2") },
                { "year", new ColumnValue(ColumnType.Integer, "4567") },
                { "enabled", new ColumnValue(ColumnType.Bool, "true") },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        QueryByIdTicket queryTicket = new(
            database: "factory",
            name: "robots",
            id: 2
        );

        List<List<ColumnValue>> result = await executor.QueryById(queryTicket);

        List<ColumnValue> row = result[0];

        Assert.AreEqual(row[0].Type, ColumnType.Id);
        Assert.AreEqual(row[0].Value, "2");

        Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name 2");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "4567");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "true");

        QueryByIdTicket queryTicket2 = new(
            database: "factory",
            name: "robots",
            id: 1
        );

        result = await executor.QueryById(queryTicket2);

        row = result[0];

        Assert.AreEqual(row[0].Type, ColumnType.Id);
        Assert.AreEqual(row[0].Value, "1");

        Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name 1");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "1234");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "false");
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsert()
    {
        var executor = await SetupBasicTable();

        for (int i = 0; i < 50; i++)
        {
            InsertTicket insertTicket = new(
                database: "factory",
                name: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, i.ToString()) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer, (i * 1000).ToString()) },
                    { "enabled", new ColumnValue(ColumnType.Bool, "false") },
                }
            );

            await executor.Insert(insertTicket);
        }

        for (int i = 0; i < 50; i++)
        {
            QueryByIdTicket queryTicket = new(
                database: "factory",
                name: "robots",
                id: i
            );

            List<List<ColumnValue>> result = await executor.QueryById(queryTicket);

            List<ColumnValue> row = result[0];

            Assert.AreEqual(row[0].Type, ColumnType.Id);
            Assert.AreEqual(row[0].Value, i.ToString());

            Assert.AreEqual(row[1].Type, ColumnType.String);
            Assert.AreEqual(row[1].Value, "some name " + i);

            Assert.AreEqual(row[2].Type, ColumnType.Integer);
            Assert.AreEqual(row[2].Value, (i * 1000).ToString());
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsertWithQuery()
    {
        var executor = await SetupBasicTable();

        for (int i = 0; i < 50; i++)
        {
            InsertTicket insertTicket = new(
                database: "factory",
                name: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, i.ToString()) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer, (i * 1000).ToString()) },
                    { "enabled", new ColumnValue(ColumnType.Bool, "false") },
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            database: "factory",
            name: "robots"
        );

        List<List<ColumnValue>> result = await executor.Query(queryTicket);

        for (int i = 0; i < 50; i++)
        {
            List<ColumnValue> row = result[i];

            Assert.AreEqual(row[0].Type, ColumnType.Id);
            Assert.AreEqual(row[0].Value, i.ToString());

            Assert.AreEqual(row[1].Type, ColumnType.String);
            Assert.AreEqual(row[1].Value, "some name " + i);

            Assert.AreEqual(row[2].Type, ColumnType.Integer);
            Assert.AreEqual(row[2].Value, (i * 1000).ToString());
        }
    }
}
