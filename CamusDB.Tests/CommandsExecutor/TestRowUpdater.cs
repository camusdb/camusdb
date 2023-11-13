
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

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowUpdater
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("factory");
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

    private async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
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

        await executor.CreateTable(tableTicket);

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
                    { "enabled", new ColumnValue(ColumnType.Bool, "FALSE") },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    /*[Test]
    [NonParallelizable]
    public async Task TestInvalidDatabase()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "another_factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "FALSE") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Database doesn't exist", e!.Message);
    }*/

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTable()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            database: dbname,
            name: "unknown_table",
            id: objectsId[0],
            columnValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.UpdateById(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicUpdate()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            database: dbname,
            name: "robots",
            id: objectsId[0],
            columnValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        Assert.AreEqual(1, await executor.UpdateById(ticket));

        QueryByIdTicket queryByIdTicket = new(
            database: dbname,
            name: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].Value);
        Assert.AreEqual("updated value", result[0]["name"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDeleteUnknownRow()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        UpdateByIdTicket ticket = new(
            database: dbname,
            name: "robots",
            id: "---",
            columnValues: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            }
        );

        Assert.AreEqual(0, await executor.UpdateById(ticket));
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdate()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        foreach (string objectId in objectsId)
        {
            UpdateByIdTicket ticket = new(
                database: dbname,
                name: "robots",
                id: objectId,
                columnValues: new Dictionary<string, ColumnValue>()
                {
                    { "name", new ColumnValue(ColumnType.String, "updated value") }
                }
            );

            Assert.AreEqual(1, await executor.UpdateById(ticket));
        }

        foreach (string objectId in objectsId)
        {
            QueryByIdTicket queryByIdTicket = new(
                database: dbname,
                name: "robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
            Assert.IsNotEmpty(result);

            Assert.AreEqual(objectId, result[0]["id"].Value);
            Assert.AreEqual("updated value", result[0]["name"].Value);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdateParallel()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        List<Task> tasks = new();

        foreach (string objectId in objectsId)
        {
            UpdateByIdTicket ticket = new(
                database: dbname,
                name: "robots",
                id: objectId,
                columnValues: new Dictionary<string, ColumnValue>()
                {
                    { "name", new ColumnValue(ColumnType.String, "updated value") }
                }
            );

            tasks.Add(executor.UpdateById(ticket));
        }

        await Task.WhenAll(tasks);

        QueryTicket queryTicket = new(
           database: dbname,
           name: "robots",
           index: null,
           filters: null
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (Dictionary<string, ColumnValue> x in result)
            Assert.AreEqual("updated value", x["name"].Value);

        queryTicket = new(
           database: dbname,
           name: "robots",
           index: null,
           filters: new()
           {
               new("name", "=", new ColumnValue(ColumnType.String, "updated value")) 
           }
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (Dictionary<string, ColumnValue> x in result)
            Assert.AreEqual("updated value", x["name"].Value);

        queryTicket = new(
           database: dbname,
           name: "robots",
           index: null,
           filters: new()
           {
               new("name", "=", new ColumnValue(ColumnType.String, "another updated value"))
           }
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }
}
