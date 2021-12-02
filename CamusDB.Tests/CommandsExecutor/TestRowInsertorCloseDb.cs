
using System.IO;
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

public class TestRowInsertorCloseDb
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
                new ColumnInfo("usersId", ColumnType.Id, notNull: true, index: IndexType.Multi),
                new ColumnInfo("amount", ColumnType.Integer)
            }
        );

        await executor.CreateTable(tableTicket);

        return executor;
    }

    [Test]
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
                { "usersId", new ColumnValue(ColumnType.Id, "5") },
                { "amount", new ColumnValue(ColumnType.Integer, "100") }
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulTwoMultiParallelInserts()
    {
        var executor = await SetupMultiIndexTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "usersId", new ColumnValue(ColumnType.Id, "1234") },
                { "amount", new ColumnValue(ColumnType.Integer, "50") },
            }
        );

        InsertTicket ticket2 = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "usersId", new ColumnValue(ColumnType.Id, "1234") },
                { "amount", new ColumnValue(ColumnType.Integer, "50") },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        QueryByIdTicket queryTicket = new(
            database: "factory",
            name: "user_robots",
            id: 2
        );

        List<List<ColumnValue>> result = await executor.QueryById(queryTicket);

        List<ColumnValue> row = result[0];

        Assert.AreEqual(row[0].Type, ColumnType.Id);
        Assert.AreEqual(row[0].Value, "2");

        Assert.AreEqual(row[1].Type, ColumnType.Id);
        Assert.AreEqual(row[1].Value, "1234");

        /*Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "4567");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "true");*/

        QueryByIdTicket queryTicket2 = new(
            database: "factory",
            name: "user_robots",
            id: 1
        );

        result = await executor.QueryById(queryTicket2);

        row = result[0];

        Assert.AreEqual(row[0].Type, ColumnType.Id);
        Assert.AreEqual(row[0].Value, "1");

        /*Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name 1");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "1234");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "false");*/
    }
}