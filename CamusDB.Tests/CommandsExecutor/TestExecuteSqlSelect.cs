
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
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

public class TestExecuteSqlSelect
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
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
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
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64, defaultValue: new ColumnValue(ColumnType.Integer64, 1999)),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
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
    public async Task TestExecuteSelectGenericWhere()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id FROM robots WHERE 1=1",
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
            sql: "SELECT id FROM robots WHERE enabled=enabled",
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
            sql: "SELECT enabled FROM robots WHERE enabled",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=TRUE",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=FALSE",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(false, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT year FROM robots WHERE year=2000",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT year FROM robots WHERE 2000=year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsString()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = \"some name 10\"",
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
            sql: "SELECT * FROM robots WHERE \"some name 10\"=name",
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
            sql: "SELECT * FROM robots WHERE year!=2000",
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
            sql: "SELECT * FROM robots WHERE 2000!=year",
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
            sql: "SELECT * FROM robots WHERE year=2000 OR year=2001",
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
            sql: "SELECT * FROM robots WHERE year=2000 OR year=2001 OR year=2002",
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
            sql: "SELECT * FROM robots WHERE year>2020",
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
            sql: "SELECT * FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsNull()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = null",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsNull2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = @null",
            parameters: new() { { "@null", new ColumnValue(ColumnType.Null, 0) } }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsId()
    {
        (string dbname, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, enabled FROM robots WHERE id = @id",
            parameters: new() { { "@id", new ColumnValue(ColumnType.Id, objectIds[0]) } }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(objectIds[0], row.Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsId2()
    {
        (string dbname, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, enabled FROM robots WHERE id = str_id(@id)",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectIds[0]) } }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(objectIds[0], row.Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some%\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some name 0\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some%0\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"%name%0\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereILike()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name ILIKE \"SOME%\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereILike2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name ILIKE \"%NAME%\"",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2001, result[1].Row["year"].LongValue);
        Assert.AreEqual(2024, result[24].Row["year"].LongValue);
    }



    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY name",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual("some name 0", result[0].Row["name"].StrValue);
        Assert.AreEqual("some name 1", result[1].Row["name"].StrValue);
        Assert.AreEqual("some name 9", result[24].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(false, result[0].Row["enabled"].BoolValue);
        Assert.AreEqual(false, result[1].Row["enabled"].BoolValue);
        Assert.AreEqual(true, result[24].Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled, year",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(false, result[0].Row["enabled"].BoolValue);
        Assert.AreEqual(false, result[1].Row["enabled"].BoolValue);
        Assert.AreEqual(true, result[24].Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy5()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY year DESC",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(2024, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);
        Assert.AreEqual(2000, result[24].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy6()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled DESC",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(true, result[0].Row["enabled"].BoolValue);
        Assert.AreEqual(true, result[1].Row["enabled"].BoolValue);
        Assert.AreEqual(false, result[24].Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectBoundParameters1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=@enabled",
            parameters: new() { { "@enabled", new ColumnValue(ColumnType.Bool, true) } }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregate1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT COUNT(*) FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(25, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregate2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT COUNT(id) FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(25, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateWithConditions()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT COUNT(id) FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(5, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjection1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.True(row.Row.ContainsKey("id"));
            Assert.AreEqual(24, row.Row["id"].StrValue!.Length);

            Assert.True(row.Row.ContainsKey("name"));
            Assert.False(row.Row.ContainsKey("year"));
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjection2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT year + year FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.False(row.Row.ContainsKey("id"));
            Assert.False(row.Row.ContainsKey("name"));
            Assert.False(row.Row.ContainsKey("year"));

            Assert.True(row.Row.ContainsKey("0"));
            Assert.True(row.Row["0"].LongValue >= 4000);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjection3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT year * 2 - year, year FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.False(row.Row.ContainsKey("id"));
            Assert.False(row.Row.ContainsKey("name"));
            Assert.True(row.Row.ContainsKey("year"));

            Assert.True(row.Row.ContainsKey("0"));
            Assert.AreEqual(row.Row["year"].LongValue, row.Row["0"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjectionAlias1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT year + year AS sumYear FROM robots WHERE year<2005",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.False(row.Row.ContainsKey("id"));
            Assert.False(row.Row.ContainsKey("name"));
            Assert.False(row.Row.ContainsKey("year"));

            Assert.True(row.Row.ContainsKey("sumYear"));
            Assert.True(row.Row["sumYear"].LongValue >= 4000);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 1",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit2()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 5",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit3()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE year >= 2020 LIMIT 5",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit4()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);

        Assert.AreEqual(2020, result[0].Row["year"].LongValue);
        Assert.AreEqual(2021, result[1].Row["year"].LongValue);
        Assert.AreEqual(2022, result[2].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit5()
    {
        (string dbname, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 1 OFFSET 5",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(objectIds[5], result[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit6()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots LIMIT @limit",
            parameters: new()
            {
                { "@limit", new ColumnValue(ColumnType.Integer64, 1)  }
            }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit7()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT * FROM robots LIMIT @limit OFFSET @offset",
            parameters: new()
            {
                { "@limit", new ColumnValue(ColumnType.Integer64, 1)  },
                { "@offset", new ColumnValue(ColumnType.Integer64, 1)  }
            }
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectForceIndex()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "SELECT id FROM robots@{FORCE_INDEX=pk}",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(ticket)).ToListAsync();
        Assert.IsNotEmpty(result);
    }
}