
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

public class TestExecuteSql : BaseTest
{    
    private async Task<(string, CommandExecutor)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        await executor.CreateDatabase(databaseTicket);

        return (dbname, executor);
    }

    private async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTable()
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
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                        { "year", new ColumnValue(ColumnType.Integer64, 2024 - i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
    }

    private async Task<(string dbname, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithDefaults()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
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
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                        { "year", new ColumnValue(ColumnType.Integer64, 2024 - i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, objectsId);
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
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(1000, result[1].Row["year"].LongValue);
        Assert.AreEqual(1000, result[24].Row["year"].LongValue);
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
           sql: "SELECT * FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOnePlaceholders()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "UPDATE robots SET year = @new_year WHERE year = @expected_year",
            parameters: new()
            {
               { "@new_year", new ColumnValue(ColumnType.Integer64, 1000) },
               { "@expected_year", new ColumnValue(ColumnType.Integer64, 2024) }
            }
        );

        Assert.AreEqual(1, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);
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
           sql: "SELECT * FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.AreNotEqual(3000, row.Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateIncrement()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket ticket = new(
            database: dbname,
            sql: "UPDATE robots SET year = year + 1000 WHERE true",
            parameters: null
        );

        Assert.AreEqual(25, await executor.ExecuteNonSQLQuery(ticket));

        ExecuteSQLTicket queryTicket = new(
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
       );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["year"].LongValue >= 3000);
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
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatchesAll()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket deleteTicket = new(
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 0",
            parameters: null
        );

        Assert.AreEqual(25, await executor.ExecuteNonSQLQuery(deleteTicket));

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatche1()
    {
        (string dbname, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        ExecuteSQLTicket deleteTicket = new(
            database: dbname,
            sql: "DELETE FROM robots WHERE year = 2000 OR year = 2001",
            parameters: null
        );

        Assert.AreEqual(2, await executor.ExecuteNonSQLQuery(deleteTicket));

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 2000 OR year = 2001",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }
}