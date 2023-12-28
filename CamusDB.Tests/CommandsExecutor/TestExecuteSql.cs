
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

namespace CamusDB.Tests.CommandsExecutor;

public class TestExecuteSql
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
                    { "year", new ColumnValue(ColumnType.Integer64, (2024 - i).ToString()) },
                    { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0 ? "true" : "false") },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGenericWhere()
    {
        (string dbname, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE 1=1",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE enabled=enabled",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE enabled",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual("true", row.Row["enabled"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE enabled=TRUE",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual("true", row.Row["enabled"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE enabled=FALSE",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual("false", row.Row["enabled"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year=2000",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual("2000", result[0].Row["year"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE 2000=year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual("2000", result[0].Row["year"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsString()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE name = \"some name 10\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsString2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE \"some name 10\"=name",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnNotEqualsInteger()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year!=2000",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(24, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnNotEqualsInteger2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE 2000!=year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(24, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsIntegerOr()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year=2000 OR year=2001",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(2, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsIntegerOr2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year=2000 OR year=2001 OR year=2002",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(3, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnGreaterInteger()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year>2020",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(4, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnLessInteger()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots ORDER BY year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("2000", result[0].Row["year"].Value);
        Assert.AreEqual("2001", result[1].Row["year"].Value);
        Assert.AreEqual("2024", result[24].Row["year"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots ORDER BY name",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("some name 0", result[0].Row["name"].Value);
        Assert.AreEqual("some name 1", result[1].Row["name"].Value);
        Assert.AreEqual("some name 9", result[24].Row["name"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots ORDER BY enabled",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("false", result[0].Row["enabled"].Value);
        Assert.AreEqual("false", result[1].Row["enabled"].Value);
        Assert.AreEqual("true", result[24].Row["enabled"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT x FROM robots ORDER BY enabled, year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("false", result[0].Row["enabled"].Value);
        Assert.AreEqual("false", result[1].Row["enabled"].Value);
        Assert.AreEqual("true", result[24].Row["enabled"].Value);        
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoConditions()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket updateTicket = new(
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE 1=1",
            parameters: null
        );

        Assert.AreEqual(25, await executor.ExecuteNonSQLQuery(updateTicket));        

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT x FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("1000", result[0].Row["year"].Value);
        Assert.AreEqual("1000", result[1].Row["year"].Value);
        Assert.AreEqual("1000", result[24].Row["year"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOne()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE year = 2024",
            parameters: null
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT x FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("1000", result[0].Row["year"].Value);
        Assert.AreEqual("2023", result[1].Row["year"].Value);        
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoMatches()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE year = 3000",
            parameters: null
        );

        Assert.AreEqual(0, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT x FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.AreNotEqual("3000", row.Row["year"].Value);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteNoConditions()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket deleteTicket = new(
            database: dbname,
            sql: "DELETE FROM robots WHERE 1=1",
            parameters: null
        );

        Assert.AreEqual(25, await executor.ExecuteNonSQLQuery(deleteTicket));

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT x FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);        
    }
}