
using NUnit.Framework;

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

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableCreator
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("test");
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor descriptor = await executor.CreateDatabase(databaseTicket);

        return (dbname, executor, catalogsManager, descriptor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
            name: "my_table",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("age", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "my_table");

        Assert.AreEqual("my_table", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("age", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoColumns()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
            name: "my_table",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("age", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table 'my_table' already exists", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableWithSQL()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL)",
            parameters: null
        );

        Assert.IsTrue(await executor.ExecuteDDLSQL(createTableTicket));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }
}
