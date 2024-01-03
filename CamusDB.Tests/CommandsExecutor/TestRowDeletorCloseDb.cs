
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Linq;
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

public class TestRowDeletorCloseDb
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("factory");
    }

    private static async Task<(string, CommandExecutor)> SetupDatabase()
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

        await executor.CreateDatabase(databaseTicket);

        return (dbname, executor);
    }

    private static async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
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

        await executor.CreateTable(tableTicket);

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
                    { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    private static async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupLargeDataTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots2",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(tableTicket);

        List<string> objectsId = new(25);
        string largeData = string.Join("", Enumerable.Repeat("a", 100000));

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots2",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "name", new ColumnValue(ColumnType.String, largeData) },
                    { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicDelete()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        DeleteByIdTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        Assert.AreEqual(1, await executor.DeleteById(ticket));

        QueryByIdTicket queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsEmpty(result);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        queryByIdTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteCriteria()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        DeleteTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
            }
        );

        Assert.AreEqual(1, await executor.Delete(ticket));

        QueryTicket queryTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
            },
            orderBy: null,
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        queryTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",            
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
            },
            orderBy: null,
            parameters: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }
}