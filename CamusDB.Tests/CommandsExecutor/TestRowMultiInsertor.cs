
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

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestRowMultiInsertor
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

    private async Task<(string, CommandExecutor)> SetupMultiIndexTable()
    {        
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            database: dbname,
            name: "user_robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("robots_id", ColumnType.Id, notNull: true, index: IndexType.Multi),
                new ColumnInfo("amount", ColumnType.Integer64)
            }
        );

        await executor.CreateTable(tableTicket);

        return (dbname, executor);
    }

    [Test]
    [Order(1)]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        (string dbname, CommandExecutor executor) = await SetupMultiIndexTable();

        InsertTicket ticket = new(
            database: dbname,
            name: "user_robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "5bc30818bc6a4e7b6c441308") },
                { "robots_id", new ColumnValue(ColumnType.Id, "5e1aac86542f77367452d9b3") },
                { "amount", new ColumnValue(ColumnType.Integer64, "100") }
            }
        );

        await executor.Insert(ticket);
    }

    [Test]
    [Order(2)]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsertWithQueryIndex()
    {
        (string dbname, CommandExecutor executor) = await SetupMultiIndexTable();

        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                database: dbname,
                name: "user_robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "robots_id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "amount", new ColumnValue(ColumnType.Integer64, (i * 1000).ToString()) }
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            database: dbname,
            name: "user_robots",
            index: "robots_id",
            filters: null,
            orderBy: null
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.Query(queryTicket)).ToListAsync();

        for (int i = 0; i < 10; i++)
        {
            Dictionary<string, ColumnValue> row = result[i];
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].Value.Length);

            Assert.AreEqual(ColumnType.Id, row["robots_id"].Type);
            Assert.AreEqual(24, row["robots_id"].Value.Length);

            Assert.AreEqual(ColumnType.Integer64, row["amount"].Type);
            Assert.AreEqual((i * 1000).ToString(), row["amount"].Value);
        }
    }

    [Test]
    [Order(3)]
    [NonParallelizable]
    public async Task TestSameKeyMultiInsertWithQueryIndex()
    {
        (string dbname, CommandExecutor executor) = await SetupMultiIndexTable();

        for (int i = 0; i < 10; i++)
        {
            InsertTicket insertTicket = new(
                database: dbname,
                name: "user_robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "robots_id", new ColumnValue(ColumnType.Id, "5e1aac86542f77367452d9b3") },
                    { "amount", new ColumnValue(ColumnType.Integer64, (i * 1000).ToString()) }
                }
            );

            await executor.Insert(insertTicket);
        }

        QueryTicket queryTicket = new(
            database: dbname,
            name: "user_robots",
            index: "robots_id",
            filters: null,
            orderBy: null
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(10, result.Count);

        for (int i = 0; i < 10; i++)
        {
            Dictionary<string, ColumnValue> row = result[i];
            Assert.AreEqual(3, row.Count);

            Assert.AreEqual(row["id"].Type, ColumnType.Id);
            Assert.AreEqual(row["id"].Value.Length, 24);

            Assert.AreEqual(row["robots_id"].Type, ColumnType.Id);
            Assert.AreEqual(row["robots_id"].Value, "5e1aac86542f77367452d9b3");

            Assert.AreEqual(row["amount"].Type, ColumnType.Integer64);
            Assert.AreEqual(row["amount"].Value, (i * 1000).ToString());
        }
    }
}