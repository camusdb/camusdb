
using NUnit.Framework;

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableDropper : BaseTest
{    
    private async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupDatabase()
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

        DatabaseDescriptor descriptor = await executor.CreateDatabase(databaseTicket);

        return (dbname, executor, catalogsManager, descriptor);
    }

    private async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket createTicket = new(
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

        await executor.CreateTable(createTicket);

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
                        { "year", new ColumnValue(ColumnType.Integer64, 2000) },
                        { "enabled", new ColumnValue(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, catalogs, database, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndDrop()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        DropTableTicket dropTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots"
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillDropAndRecreate()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        DropTableTicket dropTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots"
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));

        CreateTableTicket createTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("type", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("status", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);

        Assert.True(catalogs.TableExists(database, "robots"));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("type", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![3].Type);

        Assert.AreEqual("status", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);
    }
}