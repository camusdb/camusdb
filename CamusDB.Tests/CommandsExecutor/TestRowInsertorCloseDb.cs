
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;
using CamusDB.Tests.Utils;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.Util.ObjectIds;
using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestRowInsertorCloseDb
{
    [SetUp]
    public void Setup()
    {
        SetupDb.Remove("factory");
    }

    private async Task<CommandExecutor> SetupDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: "factory"
        );

        await executor.CreateDatabase(databaseTicket);

        return executor;
    }

    private async Task<CommandExecutor> SetupMultiIndexTable()
    {
        var executor = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            database: "factory",
            name: "user_robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("usersId", ColumnType.Id, notNull: true, index: IndexType.Multi),
                new ColumnInfo("amount", ColumnType.Integer)
            }
        );

        await executor.CreateTable(tableTicket);

        return executor;
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        var executor = await SetupMultiIndexTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                { "amount", new ColumnValue(ColumnType.Integer, "100") }
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulTwoMultiParallelInserts()
    {
        var executor = await SetupMultiIndexTable();

        InsertTicket ticket = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                { "amount", new ColumnValue(ColumnType.Integer, "50") },
            }
        );

        InsertTicket ticket2 = new(
            database: "factory",
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "507f191e810c19729de860ea") },
                { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                { "amount", new ColumnValue(ColumnType.Integer, "50") },
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        CloseDatabaseTicket closeTicket = new("factory");
        await executor.CloseDatabase(closeTicket);

        QueryByIdTicket queryTicket = new(
            database: "factory",
            name: "user_robots",
            id: "507f191e810c19729de860ea"
        );

        List<Dictionary<string, ColumnValue>> result = await executor.QueryById(queryTicket);

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].Value, "507f191e810c19729de860ea");

        Assert.AreEqual(row["usersId"].Type, ColumnType.Id);
        Assert.AreEqual(row["usersId"].Value, "5e353cf5e95f1e3a432e49aa");

        /*Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "4567");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "true");*/

        QueryByIdTicket queryTicket2 = new(
            database: "factory",
            name: "user_robots",
            id: "507f1f77bcf86cd799439011"
        );

        result = await executor.QueryById(queryTicket2);

        row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].Value, "507f1f77bcf86cd799439011");

        /*Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name 1");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "1234");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "false");*/
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsert()
    {
        int i;
        var executor = await SetupMultiIndexTable();

        string[] userIds = new string[5];
        for (i = 0; i < 5; i++)
            userIds[i] = ObjectIdGenerator.Generate().ToString();
            
        List<string> objectIds = new();

        for (i = 0; i < 50; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();
            objectIds.Add(objectId);

            InsertTicket insertTicket = new(
                database: "factory",
                name: "user_robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "usersId", new ColumnValue(ColumnType.Id, userIds[i % 5]) },
                    { "amount", new ColumnValue(ColumnType.Integer, "50") },
                }
            );

            await executor.Insert(insertTicket);

            if ((i + 1) % 5 == 0)
            {
                CloseDatabaseTicket closeTicket = new("factory");
                await executor.CloseDatabase(closeTicket);
            }
        }

        /*i = 0;

        foreach (string objectId in objectIds)
        {
            QueryByIdTicket queryTicket = new(
                database: "factory",
                name: "robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await executor.QueryById(queryTicket);

            Dictionary<string, ColumnValue> row = result[0];

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].Value.Length);

            Assert.AreEqual(ColumnType.String, row["name"].Type);
            Assert.AreEqual("some name " + i, row["name"].Value);

            Assert.AreEqual(ColumnType.Integer, row["year"].Type);
            Assert.AreEqual((i * 1000).ToString(), row["year"].Value);

            i++;
        }*/
    }
}