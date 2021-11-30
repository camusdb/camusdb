
using System.IO;
using System.Text;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core;

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
            Directory.Delete(path);
        }
    }

    [Test]
    public async Task TestCreateTable()
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
    }

    [Test]
    public async Task TestCreateTableNoColumns()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        await executor.CreateDatabase("test");

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[] { }
        );

        try
        {
            await executor.CreateTable(ticket);
        }
        catch (CamusDBException e)
        {
            Assert.AreEqual("Table requires at least one column", e.Message);
        }
    }

    [Test]
    public async Task TestCreateTableNoDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        await executor.CreateDatabase("test");

        CreateTableTicket ticket = new(
            database: "",
            name: "my_table",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
            }
        );

        try
        {
            await executor.CreateTable(ticket);
        }
        catch (CamusDBException e)
        {
            Assert.AreEqual("Database name is required", e.Message);
        }
    }

    [Test]
    public async Task TestCreateTableNoTableName()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        await executor.CreateDatabase("test");

        CreateTableTicket ticket = new(
            database: "test",
            name: "",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
            }
        );

        try
        {
            await executor.CreateTable(ticket);
        }
        catch (CamusDBException e)
        {
            Assert.AreEqual("Table name is required", e.Message);
        }
    }

    [Test]
    public async Task TestCreateTableDuplicateColumn()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        await executor.CreateDatabase("test");

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[] {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("id", ColumnType.String, notNull: true),
            }
        );

        try
        {
            await executor.CreateTable(ticket);
        }
        catch (CamusDBException e)
        {
            Assert.AreEqual("uplicate column name: id", e.Message);
        }
    }
}
