
using System.IO;
using System.Text;
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

public class TestRowInsertor
{
    [SetUp]
    public void Setup()
    {
        string path = Config.DataDirectory + "/test";
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

        await executor.CreateDatabase("test");

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("age", ColumnType.Integer),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        return executor;
    }

    [Test]
    public async Task TestBasicInsert()
    {
        var executor = await SetupDatabase();

        InsertTicket ticket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "1234") },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    public async Task TestTwoInserts()
    {
        var executor = await SetupDatabase();

        InsertTicket ticket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        await executor.Insert(ticket);

        ticket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "true") },
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    public async Task TestTwoInsertsParallel()
    {
        var executor = await SetupDatabase();

        InsertTicket ticket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );        

        InsertTicket ticket2 = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
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
    public async Task TestCheckSuccessfulInsert()
    {
        var executor = await SetupDatabase();

        InsertTicket insertTicket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        await executor.Insert(insertTicket);

        QueryByIdTicket queryTicket = new(
            database: "test",
            name: "my_table",
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
    public async Task TestSuccessfulTwoParallelInserts()
    {
        var executor = await SetupDatabase();

        InsertTicket ticket = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name 1") },
                { "age", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        InsertTicket ticket2 = new(
            database: "test",
            name: "my_table",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "2") },
                { "name", new ColumnValue(ColumnType.String, "some name 2") },
                { "age", new ColumnValue(ColumnType.Integer, "4567") },
                { "enabled", new ColumnValue(ColumnType.Bool, "true") },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        QueryByIdTicket queryTicket = new(
            database: "test",
            name: "my_table",
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
            database: "test",
            name: "my_table",
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
}
