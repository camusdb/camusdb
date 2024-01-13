
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
using CamusDB.Core;

namespace CamusDB.Tests.CommandsExecutor;

public class TestExecuteSqlInsert
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
            },
            ifNotExists: false
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
                    { "year", new ColumnValue(ColumnType.Integer64, 2024 - i) },
                    { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    private static async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithDefaults()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64, defaultValue: new ColumnValue(ColumnType.Integer64, 1999)),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            ifNotExists: false
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
                    { "year", new ColumnValue(ColumnType.Integer64, 2024 - i) },
                    { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsertDiffFieldsAndValues()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (GEN_ID(), \"astro boy\", 3000)",
            parameters: null
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket));
        Assert.AreEqual("The number of fields is not equal to the number of values.", exception!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (GEN_ID(), \"astro boy\", 3000, false)",
            parameters: null
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(26, result.Count);

        foreach (QueryResultRow row in result)
        {
            if (row.Row["year"].LongValue == 3000)
                Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
            parameters: null
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(26, result.Count);

        foreach (QueryResultRow row in result)
        {
            if (row.Row["year"].LongValue == 3000)
            {
                Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
                Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            }
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
            parameters: null
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (@id, @name, @year, @enabled)",
            parameters: new()
            {
                { "@id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new ColumnValue(ColumnType.String, "astro boy") },
                { "@year", new ColumnValue(ColumnType.Integer64, 3000) } ,
                { "@enabled", new ColumnValue(ColumnType.Bool, false) }
            }
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(3000, row.Row["year"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert5()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
            parameters: null
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert6()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots VALUES (STR_ID(@id), @name, @year, @enabled)",
            parameters: new()
            {
                { "@id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new ColumnValue(ColumnType.String, "astro boy") },
                { "@year", new ColumnValue(ColumnType.Integer64, 2010) },
                { "@enabled", new ColumnValue(ColumnType.Bool, false) }
            }
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert7()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTableWithDefaults();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, enabled) VALUES (STR_ID(@id), @name, @enabled)",
            parameters: new()
            {
                { "@id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new ColumnValue(ColumnType.String, "astro boy") },
                { "@enabled", new ColumnValue(ColumnType.Bool, false) }
            }
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert8()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTableWithDefaults();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(@id), @name, DEFAULT, @enabled)",
            parameters: new()
            {
                { "@id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new ColumnValue(ColumnType.String, "astro boy") },
                { "@enabled", new ColumnValue(ColumnType.Bool, false) }
            }
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
           parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }
    }
}