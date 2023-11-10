
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableCreator
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("test");
    }

    private async Task<(string, CommandExecutor)> SetupDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname
        );

        await executor.CreateDatabase(databaseTicket);

        return (dbname, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
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
