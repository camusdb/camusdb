﻿
using NUnit.Framework;

using System.Threading.Tasks;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using System.Collections.Generic;
using CamusDB.Core.Util.ObjectIds;
using System.Linq;
using CamusDB.Core;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableAlterer
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

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket createTicket = new(
            database: dbname,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(createTicket);

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                database: dbname,
                name: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer64, (2000 + i).ToString()) },
                    { "enabled", new ColumnValue(ColumnType.Bool, "false") },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, catalogs, database, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndDropColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        AlterTableTicket alterTableTicket = new(
            database: dbname,
            name: "robots",
            operation: AlterTableOperation.DropColumn,
            new ColumnInfo("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndDropColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        AlterTableTicket alterTableTicket = new(
            database: dbname,
            name: "robots",
            operation: AlterTableOperation.DropColumn,
            new ColumnInfo("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        QueryTicket queryTicket = new(
           database: dbname,
           name: "robots",
           index: null,
           where: null,
           filters: null,
           orderBy: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
            Assert.AreEqual(3, resultRow.Row.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddExistingColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        AlterTableTicket alterTableTicket = new(
            database: dbname,
            name: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("name", ColumnType.Integer64)
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterTable(alterTableTicket));
        Assert.AreEqual("Duplicate column 'name'", e!.Message);        
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddNewColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            database: dbname,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(ticket);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        AlterTableTicket alterTableTicket = new(
            database: dbname,
            name: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("type", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndAddColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        AlterTableTicket alterTableTicket = new(
            database: dbname,
            name: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("type", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);

        QueryTicket queryTicket = new(
           database: dbname,
           name: "robots",
           index: null,
           where: null,
           filters: null,
           orderBy: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
        {
            Assert.AreEqual(5, resultRow.Row.Count);
            Assert.AreEqual(ColumnType.Null, resultRow.Row["type"].Type);
        }

        UpdateTicket updateTicket = new(
            database: dbname,
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "type", new ColumnValue(ColumnType.Integer64, "100") }
            },
            where: null,
            filters: null
        );

        Assert.AreEqual(25, await executor.Update(updateTicket));

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
        {
            Assert.AreEqual(5, resultRow.Row.Count);
            Assert.AreEqual(ColumnType.Integer64, resultRow.Row["type"].Type);
            Assert.AreEqual("100", resultRow.Row["type"].Value);
        }
    }
}