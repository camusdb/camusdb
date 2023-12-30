﻿
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

public class TestRowUpdaterUnique
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("factory");
    }

    private static async Task<(string, CommandExecutor)> SetupDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

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
            database: dbname,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true, index: IndexType.Unique),
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
                    { "enabled", new ColumnValue(ColumnType.Bool, "false") },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateMany()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        UpdateTicket ticket = new(
            database: dbname,
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "name", new ColumnValue(ColumnType.String, "updated value") }
            },
            where: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, "2010"))
            }
        );

        Assert.AreEqual(14, await executor.Update(ticket));

        /*QueryTicket queryTicket = new(
            database: dbname,
            name: "robots",
            index: null,
            where: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, "2010"))
            },
            orderBy: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(14, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreEqual(row["name"].Value, "updated value");
        }

        queryTicket = new(
            database: dbname,
            name: "robots",
            index: null,
            where: null,
            filters: new()
            {
                new("year", "<=", new ColumnValue(ColumnType.Integer64, "2010"))
            },
            orderBy: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(11, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreNotEqual(row["name"].Value, "updated value");
        }*/
    }
}