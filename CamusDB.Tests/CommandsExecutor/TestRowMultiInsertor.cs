﻿
using System.IO;
using System.Text;
using CamusDB.Core;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowMultiInsertor
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

    private async Task<CommandExecutor> SetupMultiIndexTable()
    {
        var executor = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            database: "factory",
            name: "user_robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("robots_id", ColumnType.Id, notNull: true, index: IndexType.Multi),
                new ColumnInfo("amount", ColumnType.Integer)
            }
        );

        await executor.CreateTable(tableTicket);

        return executor;
    }

    [Test]
    [Order(1)]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        var executor = await SetupMultiIndexTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "robots_id", new ColumnValue(ColumnType.Id, "5") },
                { "amount", new ColumnValue(ColumnType.Integer, "100") }
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [Order(2)]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsertWithQueryIndex()
    {
        var executor = await SetupMultiIndexTable();

        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                database: "factory",
                name: "user_robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, i.ToString()) },
                    { "robots_id", new ColumnValue(ColumnType.Id, (i * 100).ToString()) },
                    { "amount", new ColumnValue(ColumnType.Integer, (i * 1000).ToString()) }
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            database: "factory",
            name: "user_robots",
            index: "robots_id"
        );

        List<List<ColumnValue>> result = await executor.Query(queryTicket);

        for (int i = 0; i < 10; i++)
        {
            List<ColumnValue> row = result[i];
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(row[0].Type, ColumnType.Id);
            Assert.AreEqual(row[0].Value, i.ToString());

            Assert.AreEqual(row[1].Type, ColumnType.Id);
            Assert.AreEqual(row[1].Value, (i * 100).ToString());

            Assert.AreEqual(row[2].Type, ColumnType.Integer);
            Assert.AreEqual(row[2].Value, (i * 1000).ToString());
        }
    }

    [Test]
    [Order(3)]
    [NonParallelizable]
    public async Task TestSameKeyMultiInsertWithQueryIndex()
    {
        var executor = await SetupMultiIndexTable();

        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                database: "factory",
                name: "user_robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, i.ToString()) },
                    { "robots_id", new ColumnValue(ColumnType.Id, "100") },
                    { "amount", new ColumnValue(ColumnType.Integer, (i * 1000).ToString()) }
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            database: "factory",
            name: "user_robots",
            index: "robots_id"
        );

        List<List<ColumnValue>> result = await executor.Query(queryTicket);

        for (int i = 0; i < 10; i++)
        {
            List<ColumnValue> row = result[i];
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(row[0].Type, ColumnType.Id);
            Assert.AreEqual(row[0].Value, i.ToString());

            Assert.AreEqual(row[1].Type, ColumnType.Id);
            Assert.AreEqual(row[1].Value, "100");

            Assert.AreEqual(row[2].Type, ColumnType.Integer);
            Assert.AreEqual(row[2].Value, (i * 1000).ToString());
        }
    }
}