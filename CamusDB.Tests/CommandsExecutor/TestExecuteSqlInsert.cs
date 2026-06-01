
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
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
public sealed class TestExecuteSqlInsert : SharedNodeBaseTest
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
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64, defaultValue: new(ColumnType.Integer64, 1999)),
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
    public async Task TestExecuteInsertDiffFieldsAndValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (GEN_ID(), \"astro boy\", 3000)",
            parameters: null
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket));
        Assert.AreEqual("The number of fields is not equal to the number of values. Fields=4 != Values=3 Position=0", exception!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (GEN_ID(), \"astro boy\", 3000, false)",
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

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(26, result.Count);

        foreach (QueryResultRow row in result)
        {
            if (row.Row["year"].LongValue == 3000)
                Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert2()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
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

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert3()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert4()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (@id, @name, @year, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@year", new(ColumnType.Integer64, 3000) } ,
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(3000, row.Row["year"].LongValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert5()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,        
            database: dbname,
            sql: "INSERT INTO robots VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"astro boy\", 3000, false)",
            parameters: null
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert6()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots VALUES (STR_ID(@id), @name, @year, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@year", new(ColumnType.Integer64, 2010) },
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert7()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithDefaults();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, enabled) VALUES (STR_ID(@id), @name, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult execResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, execResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert8()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithDefaults();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(@id), @name, DEFAULT, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult executeResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(1, executeResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsert9()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithDefaults();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (STR_ID(@id), @name, DEFAULT, @enabled), (STR_ID(@id2), @name, DEFAULT, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@id2", new(ColumnType.Id, "507f1f77bcf86cd799439012") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult sqlResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(2, sqlResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }

        queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439012\")",
            parameters: null
        );

        (_, cursor) = await executor.ExecuteSQLQuery(queryTicket);

        result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439012", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
            Assert.AreEqual(1999, row.Row["year"].LongValue);
        }
        
        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    //[NonParallelizable]
    public async Task TestExecuteInsert10()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots VALUES (STR_ID(@id), @name, @year, @enabled), (STR_ID(@id2), @name, @year, @enabled)",
            parameters: new()
            {
                { "@id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                { "@id2", new(ColumnType.Id, "507f1f77bcf86cd799439012") },
                { "@name", new(ColumnType.String, "astro boy") },
                { "@year", new(ColumnType.Integer64, 2010) },
                { "@enabled", new(ColumnType.Bool, false) }
            }
        );

        ExecuteNonSQLResult sqlResult = await executor.ExecuteNonSQLQuery(ticket);
        Assert.AreEqual(2, sqlResult.ModifiedRows);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439011\")",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439011", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }

        queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE id = STR_ID(\"507f1f77bcf86cd799439012\")",
            parameters: null
        );

        (_, cursor) = await executor.ExecuteSQLQuery(queryTicket);

        result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual("507f1f77bcf86cd799439012", row.Row["id"].StrValue);
            Assert.AreEqual("astro boy", row.Row["name"].StrValue);
        }

        await database.Transactions.CommitAsync(txnState);
    }

    /// <summary>
    /// Regression: a STRING primary key (and a STRING UNIQUE key) must reject a duplicate inserted
    /// in a separate, already-committed transaction. Mirrors the reported `teams` table:
    ///   CREATE TABLE teams (id STRING NOT NULL, code STRING NOT NULL, ..., PRIMARY KEY (id), UNIQUE KEY (code))
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsertDuplicateStringPrimaryKeyAcrossTransactions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        ExecuteSQLTicket createTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL)",
            parameters: null
        );
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(createTicket);
        Assert.IsTrue(ddl.Success);

        const string sql = "INSERT INTO teams (id, code, name) VALUES (\"1e8921c8-58ed-483e-b4f2-c0f43cbc6c22\", \"BEL\", \"Belgium\")";

        // First insert (own transaction) — succeeds.
        KvTransaction tx1 = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult r1 = await executor.ExecuteNonSQLQuery(new(tx1, dbname, sql, null));
        Assert.AreEqual(1, r1.ModifiedRows);
        await database.Transactions.CommitAsync(tx1);

        // Second insert with the SAME primary key (separate transaction) — must be rejected.
        KvTransaction tx2 = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new(tx2, dbname, sql, null)));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code);

        // And the table must still contain exactly one row.
        KvTransaction tx3 = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx3, dbname, "SELECT id FROM teams", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count);
        await database.Transactions.CommitAsync(tx3);
    }

    /// <summary>
    /// Regression: two CONCURRENT transactions inserting the same primary key must not both
    /// commit. Exactly one must win; the table must end with a single row. This reproduces the
    /// realistic web-app race (e.g. two simultaneous requests with the same id).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsertDuplicatePrimaryKeyConcurrent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL)",
            null));
        Assert.IsTrue(ddl.Success);

        const string sql = "INSERT INTO teams (id, code, name) VALUES (\"1e8921c8-58ed-483e-b4f2-c0f43cbc6c22\", \"BEL\", \"Belgium\")";

        async Task<bool> TryInsert()
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                await executor.ExecuteNonSQLQuery(new(tx, dbname, sql, null));
                await database.Transactions.CommitAsync(tx);
                return true;
            }
            catch (CamusDBException)
            {
                return false;
            }
        }

        bool[] results = await Task.WhenAll(TryInsert(), TryInsert());
        int succeeded = results.Count(ok => ok);

        // Verify how many rows actually landed.
        KvTransaction txq = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id FROM teams", null));
        int rowCount = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txq);

        Assert.AreEqual(1, rowCount, $"Expected exactly 1 row after concurrent duplicate inserts, found {rowCount}");
        Assert.AreEqual(1, succeeded, $"Expected exactly 1 insert to succeed, {succeeded} did");
    }

    /// <summary>
    /// Regression: after an in-place UPDATE, a subsequent full scan must return the row exactly
    /// once with the NEW value — not the pre-update version plus the post-update version.
    /// Mirrors the reported `teams` symptom (one physical row shown twice: name_es null + Belgica).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestFullScanAfterUpdateReturnsRowOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL, name_es STRING NULL)",
            null));
        Assert.IsTrue(ddl.Success);

        const string id = "1e8921c8-58ed-483e-b4f2-c0f43cbc6c22";

        // Insert (name_es null), each statement in its own committed transaction (like the CLI).
        KvTransaction txIns = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(txIns, dbname,
            $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{id}\", \"BEL\", \"Belgium\", null)", null));
        await database.Transactions.CommitAsync(txIns);

        // Update name_es in place.
        KvTransaction txUpd = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(txUpd, dbname,
            $"UPDATE teams SET name_es = \"Belgica\" WHERE id = \"{id}\"", null));
        await database.Transactions.CommitAsync(txUpd);

        // Full scan — must be exactly one row, carrying the updated value.
        KvTransaction txScan = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txScan, dbname, "SELECT id, name_es FROM teams", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txScan);

        Assert.AreEqual(1, rows.Count, $"Full scan after update must return 1 row, got {rows.Count}");
        Assert.AreEqual("Belgica", rows[0].Row["name_es"].StrValue);
    }
}