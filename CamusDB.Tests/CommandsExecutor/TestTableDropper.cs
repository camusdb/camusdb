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
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableDropper
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("test");
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(hlc, validator, catalogsManager);

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
            databaseName: dbname,
            tableName: "robots",
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
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
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
    public async Task TestCreateTableFillAndDropColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        DropTableTicket dropTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots"
        );

        await executor.DropTable(dropTableTicket);
    }
}