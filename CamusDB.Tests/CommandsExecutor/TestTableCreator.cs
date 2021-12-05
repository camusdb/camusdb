
using System.IO;
using CamusDB.Core;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

public class TestTableCreator
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
            File.Delete(path + "/journal");
            Directory.Delete(path);
        }
    }

    private async Task<CommandExecutor> SetupDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: "test"
        );

        await executor.CreateDatabase(databaseTicket);

        return executor;
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTable()
    {
        CommandExecutor executor = await SetupDatabase();

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
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoColumns()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[] { }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table requires at least one column", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoDatabase()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "",
            name: "my_table",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Database name is required", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoTableName()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: "",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name is required", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableDuplicateColumn()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("id", ColumnType.String, notNull: true),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Duplicate column name: id", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableDuplicatePrimaryKey()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, primary: true),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Multiple primary key defined", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableInvalidTableName()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: new string('a', 300),
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name is too long", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableInvalidTableNameCharacters()
    {
        CommandExecutor executor = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_täble",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String),
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name has invalid characters", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableTwice()
    {
        CommandExecutor executor = await SetupDatabase();

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

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table 'my_table' already exists", e!.Message);
    }
}
