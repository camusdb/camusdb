
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestExecuteSql : SharedNodeBaseTest
{    
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupDatabase()
    {
        return await CreateDatabase();
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2024 - i) },
                        { "enabled", new(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithDefaults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64, defaultValue: new ColumnValue(ColumnType.Integer64, 1999)),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2024 - i) },
                        { "enabled", new(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoConditions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE 1=1",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(25, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        
        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(1000, result[1].Row["year"].LongValue);
        Assert.AreEqual(1000, result[24].Row["year"].LongValue);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE year = 2024",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
           txnState: txnState,
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
       );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOnePlaceholders()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = @new_year WHERE year = @expected_year",
            parameters: new()
            {
               { "@new_year", new(ColumnType.Integer64, 1000) },
               { "@expected_year", new(ColumnType.Integer64, 2024) }
            }
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
           txnState: txnState,
           database: dbname,
           sql: "SELECT * FROM robots",
           parameters: null
       );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 1000 WHERE year = 3000",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(0, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
       );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.AreNotEqual(3000, row.Row["year"].LongValue);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateIncrement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = year + 1000 WHERE true",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(25, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
       );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["year"].LongValue >= 3000);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteNoConditions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE 1=1",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(25, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatchesAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 0",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(25, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatche1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year = 2000 OR year = 2001",
            parameters: null
        );
        
        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(2, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 2000 OR year = 2001",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWithMixedCaseIdentifiers()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT Name, Year FROM Robots WHERE Enabled = true",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.AreEqual(12, result.Count);
        Assert.IsTrue(result.All(row =>
            row.Row.Count == 2 &&
            row.Row.ContainsKey("name") &&
            row.Row.ContainsKey("year")));

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableWithMixedCaseIdentifiers()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE Users (UserId OID PRIMARY KEY NOT NULL, UserName STRING NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTicket);
        Assert.IsTrue(ddlResult.Success);

        ExecuteSQLTicket insertTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO USERS (UserId, UserName) VALUES (GEN_ID(), \"alice\")",
            parameters: null
        );

        await executor.ExecuteNonSQLQuery(insertTicket);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT userName FROM users WHERE userId IS NOT NULL",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("alice", result[0].Row["username"].StrValue);

        await database.Transactions.CommitAsync(txnState);
    }

    // DELETE LIMIT tests

    [Test]
    [NonParallelizable]
    public async Task TestDeleteLimit1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 2000 LIMIT 1",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year > 2000",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> remaining = await cursor.ToListAsync();
        Assert.AreEqual(23, remaining.Count);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDeleteLimitZeroDeletesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 2000 LIMIT 0",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(0, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> remaining = await cursor.ToListAsync();
        Assert.AreEqual(25, remaining.Count);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDeleteLimitExceedsMatchCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        // Only year=2000 matches, but LIMIT is 100 — all matches deleted
        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year = 2000 LIMIT 100",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 2000",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> remaining = await cursor.ToListAsync();
        Assert.IsEmpty(remaining);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDeleteLimitParameterized()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 2000 LIMIT @max",
            parameters: new() { { "@max", new(ColumnType.Integer64, 3) } }
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(3, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> remaining = await cursor.ToListAsync();
        Assert.AreEqual(22, remaining.Count);

        await database.Transactions.CommitAsync(txnState);
    }

    // UPDATE LIMIT tests

    [Test]
    [NonParallelizable]
    public async Task TestUpdateLimit1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 9999 WHERE year > 2000 LIMIT 1",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 9999",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> updated = await cursor.ToListAsync();
        Assert.AreEqual(1, updated.Count);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateLimitZeroUpdatesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 9999 WHERE year > 2000 LIMIT 0",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(0, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 9999",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        List<QueryResultRow> updated = await cursor.ToListAsync();
        Assert.IsEmpty(updated);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateLimitLeavesOtherRowsIntact()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 9999 WHERE year > 2000 LIMIT 5",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(5, execResult.ModifiedRows);

        // 25 total, 5 updated to 9999, 20 untouched (year > 2000 has 24 rows originally)
        ExecuteSQLTicket queryAll = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursorAll) = await executor.ExecuteSQLQuery(queryAll);
        Assert.AreEqual(25, (await cursorAll.ToListAsync()).Count);

        ExecuteSQLTicket queryUpdated = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = 9999",
            parameters: null
        );

        (var _, IAsyncEnumerable<QueryResultRow> cursorUpdated) = await executor.ExecuteSQLQuery(queryUpdated);
        Assert.AreEqual(5, (await cursorUpdated.ToListAsync()).Count);

        await database.Transactions.CommitAsync(txnState);
    }

    // Index integrity after limited delete/update

    [Test]
    [NonParallelizable]
    public async Task TestDeleteLimitIndexIntegrity()
    {
        // Table with a unique index on 'name'; limited delete must leave untouched rows queryable
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        for (int i = 0; i < 5; i++)
        {
            InsertTicket insert = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new() { new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "robot-" + i) },
                    { "year", new(ColumnType.Integer64, 2000 + i) },
                    { "enabled", new(ColumnType.Bool, false) },
                }}
            );
            await executor.Insert(insert);
        }

        await database.Transactions.CommitAsync(txnState);

        txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 1999 LIMIT 2",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(2, execResult.ModifiedRows);

        // Remaining 3 rows must still be findable via the unique index (by name)
        for (int i = 2; i < 5; i++)
        {
            ExecuteSQLTicket queryTicket = new(
                txnState: txnState,
                database: dbname,
                sql: $"SELECT * FROM robots WHERE name = 'robot-{i}'",
                parameters: null
            );

            (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(1, rows.Count, $"robot-{i} should still exist after limited delete");
        }

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateLimitIndexIntegrity()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        for (int i = 0; i < 5; i++)
        {
            InsertTicket insert = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new() { new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "robot-" + i) },
                    { "year", new(ColumnType.Integer64, 2000 + i) },
                    { "enabled", new(ColumnType.Bool, false) },
                }}
            );
            await executor.Insert(insert);
        }

        await database.Transactions.CommitAsync(txnState);

        txnState = await database.Transactions.BeginAsync();

        // Update only 1 row; the other 4 must remain reachable via unique index
        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 9999 WHERE year > 1999 LIMIT 1",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        // All 5 names still unique and findable
        for (int i = 0; i < 5; i++)
        {
            ExecuteSQLTicket queryTicket = new(
                txnState: txnState,
                database: dbname,
                sql: $"SELECT * FROM robots WHERE name = 'robot-{i}'",
                parameters: null
            );

            (var _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(1, rows.Count, $"robot-{i} must still be findable via unique index after limited update");
        }

        await database.Transactions.CommitAsync(txnState);
    }

    // No-limit regression — existing DELETE/UPDATE behavior unchanged

    [Test]
    [NonParallelizable]
    public async Task TestDeleteWithoutLimitStillDeletesAllMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket deleteTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DELETE FROM robots WHERE year > 2000",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(deleteTicket);
        Assert.AreEqual(24, execResult.ModifiedRows);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateWithoutLimitStillUpdatesAllMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket updateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "UPDATE robots SET year = 1 WHERE year > 2000",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(updateTicket);
        Assert.AreEqual(24, execResult.ModifiedRows);

        await database.Transactions.CommitAsync(txnState);
    }
}