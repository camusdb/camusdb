
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableDropper : BaseTest
{    
    private async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager, CatalogsManager)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        TransactionsManager transactions = new(hlc);
        CommandExecutor executor = new(hlc, validator, catalogs, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor database = await executor.CreateDatabase(databaseTicket);

        return (dbname, database, executor, transactions, catalogs);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalog, List<string> objectIds)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket createTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2000) },
                        { "enabled", new(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, database, executor, transactions, catalogs, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndDrop()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        DropTableTicket dropTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            ifExists: false
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillDropAndRecreate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        DropTableTicket dropTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            ifExists: false
        );

        Assert.True(await executor.DropTable(dropTableTicket));
        Assert.False(catalogs.TableExists(database, "robots"));

        CreateTableTicket createTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("type", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("status", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
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