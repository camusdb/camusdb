
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
    public async Task TestExecuteInsertMoreValuesThanFields()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        // 5 values for 4 fields: the single-pass slot filler rejects the overflow value up front.
        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "INSERT INTO robots (id, name, year, enabled) VALUES (GEN_ID(), \"astro boy\", 3000, false, 42)",
            parameters: null
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception!.Code);
        Assert.That(exception!.Message, Does.Contain("Too many values in VALUES row"),
            "Supplying more values than fields must be rejected");
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
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new(tx2, dbname, sql, null)));
            Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code);
        }
        finally
        {
            // A rejected write still owns its registered lock set until the caller finalizes the
            // transaction. Release it before the verification transaction scans the same table.
            await database.Transactions.RollbackIfNotCompletedAsync(tx2);
        }

        // And the table must still contain exactly one row.
        KvTransaction tx3 = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx3, dbname, "SELECT id FROM teams", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count);
        await database.Transactions.CommitAsync(tx3);
    }

    /// <summary>
    /// A transaction begun with <see cref="global::Kahuna.Shared.KeyValue.DecisionDurability.Durable"/>
    /// must commit its writes and expose them to a later reader exactly like the best-effort default.
    /// Every CamusDB row/index/meta write is persistent, so durable-decision mode (which rejects a
    /// transaction that confirmed any ephemeral modification) accepts the commit; the coordinator
    /// assigns the record anchor from the first confirmed persistent write. On this happy path the
    /// commit resolves in one round, so anchor-based recovery is never exercised — but the anchor is
    /// still folded onto the handle (<see cref="KvTransaction.RecordAnchorKey"/>) so a finalize retried
    /// after coordinator loss can reach the durable decision.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestExecuteInsertWithDurableDecision()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE flags (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)",
            null));

        KvTransaction tx = await database.Transactions.BeginAsync(
            decisionDurability: global::Kahuna.Shared.KeyValue.DecisionDurability.Durable);
        await executor.ExecuteNonSQLQuery(new(tx, dbname,
            "INSERT INTO flags (id, name) VALUES (\"ca\", \"Canada\")", null));
        await database.Transactions.CommitAsync(tx);

        KvTransaction txq = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id, name FROM flags", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txq);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("ca", rows[0].Row["id"].StrValue);
        Assert.AreEqual("Canada", rows[0].Row["name"].StrValue);
    }

    /// <summary>
    /// An optimistic transaction takes no explicit exclusive locks; its writes fold implicit point
    /// locks into the coordinator working set, and the commit validates + finalizes them. This proves
    /// an optimistic transaction can commit at all: without the coordinator folding a batch write's
    /// implicit point lock, prepare would abort (empty lock set) and the row would never persist.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptimisticInsertCommitsAndIsReadable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE cities (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)",
            null));

        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await executor.ExecuteNonSQLQuery(new(tx, dbname,
            "INSERT INTO cities (id, name) VALUES (\"lis\", \"Lisbon\")", null));
        await database.Transactions.CommitAsync(tx);

        KvTransaction txq = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id, name FROM cities", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txq);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("lis", rows[0].Row["id"].StrValue);
        Assert.AreEqual("Lisbon", rows[0].Row["name"].StrValue);
    }

    /// <summary>
    /// Two CONCURRENT optimistic transactions inserting the same primary key must not both commit.
    /// Optimistic transactions do not block on a lock; the conflict is detected at commit (competing
    /// write intents on the same key), so exactly one wins and the table ends with a single row —
    /// the same guarantee the pessimistic path gives, reached by validation instead of blocking.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptimisticConcurrentDuplicatePrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE ports (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)",
            null));

        const string sql = "INSERT INTO ports (id, name) VALUES (\"rot\", \"Rotterdam\")";

        async Task<bool> TryInsert()
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                isolationLevel: CamusIsolationLevel.ReadCommitted,
                locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
            try
            {
                await executor.ExecuteNonSQLQuery(new(tx, dbname, sql, null));
                await database.Transactions.CommitAsync(tx);
                return true;
            }
            catch (CamusDBException)
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
                return false;
            }
        }

        bool[] results = await Task.WhenAll(TryInsert(), TryInsert());
        int succeeded = results.Count(ok => ok);

        KvTransaction txq = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id FROM ports", null));
        int rowCount = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txq);

        Assert.AreEqual(1, rowCount, $"Expected exactly 1 row after concurrent optimistic duplicate inserts, found {rowCount}");
        Assert.AreEqual(1, succeeded, $"Expected exactly 1 optimistic insert to succeed, {succeeded} did");
    }

    /// <summary>
    /// Optimistic read-set validation: a transaction that point-read a row which a concurrent
    /// transaction then modified and committed must ABORT at commit, even though the two
    /// transactions wrote disjoint keys (a read-write / write-skew conflict). This exercises the
    /// read-folding path — the optimistic transaction's read is registered as an observation and
    /// validated against the current committed revision at commit.
    ///
    /// The read here is a primary-key lookup; a full-table SELECT folds and aborts the same way —
    /// see <see cref="TestOptimisticQueryScanFoldsIntoReadSet"/> — so isolation does not depend on
    /// which plan shape answered the read.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptimisticReadWriteConflictAbortsAtCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE accounts (id STRING NOT NULL PRIMARY KEY, balance STRING NOT NULL)",
            null));

        KvTransaction seed = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO accounts (id, balance) VALUES (\"a\", \"100\")", null));
        await database.Transactions.CommitAsync(seed);

        // T1 (optimistic) reads account "a" — folds a read observation at its current revision.
        KvTransaction t1 = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        (_, IAsyncEnumerable<QueryResultRow> t1cursor) =
            await executor.ExecuteSQLQuery(new(t1, dbname, "SELECT id, balance FROM accounts WHERE id = \"a\"", null));
        List<QueryResultRow> t1rows = await t1cursor.ToListAsync();
        Assert.AreEqual("100", t1rows.Single().Row["balance"].StrValue);

        // T2 concurrently updates the row T1 read, and commits — invalidating T1's observation.
        KvTransaction t2 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(t2, dbname,
            "UPDATE accounts SET balance = \"200\" WHERE id = \"a\"", null));
        await database.Transactions.CommitAsync(t2);

        // T1 now writes a disjoint key and tries to commit. Its read set is stale → commit must abort.
        await executor.ExecuteNonSQLQuery(new(t1, dbname,
            "INSERT INTO accounts (id, balance) VALUES (\"b\", \"50\")", null));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await database.Transactions.CommitAsync(t1),
            "Optimistic commit must abort when a read-set entry was modified by a committed peer");

        // The aborted transaction's write must not have landed.
        KvTransaction txq = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id FROM accounts", null));
        int rowCount = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txq);
        Assert.AreEqual(1, rowCount, "The aborted optimistic transaction's insert must not persist");
    }

    /// <summary>
    /// A plain query scan IS folded into the commit-time read set. Same shape as
    /// <see cref="TestOptimisticReadWriteConflictAbortsAtCommit"/> but the read is a full-table
    /// SELECT instead of a primary-key lookup, and the outcome must be the same: a concurrent
    /// update to a row the query scanned aborts the optimistic commit.
    ///
    /// This is deliberate: read-set folding follows the transaction
    /// (<c>KvTransaction.FoldReads</c>), never the plan shape. If only point reads folded, the same
    /// predicate answered by a PK lookup would be validated at commit while a table scan of it
    /// would not — isolation would silently depend on the planner's choice. The shared range lock
    /// is not a substitute: it fires only for Serializable read-write transactions or under
    /// key-range sharding, so a Read Committed optimistic scan would otherwise carry no read
    /// protection at all. The price — commit cost scaling with rows scanned — is the optimistic
    /// contract; a transaction that cannot afford it should use pessimistic locking, whose scans
    /// register nothing.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptimisticQueryScanFoldsIntoReadSet()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE balances (id STRING NOT NULL PRIMARY KEY, balance STRING NOT NULL)",
            null));

        KvTransaction seed = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO balances (id, balance) VALUES (\"a\", \"100\")", null));
        await database.Transactions.CommitAsync(seed);

        // T1 (optimistic) scans the table. Every scanned row registers a commit-time read dependency.
        KvTransaction t1 = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        (_, IAsyncEnumerable<QueryResultRow> t1cursor) =
            await executor.ExecuteSQLQuery(new(t1, dbname, "SELECT id, balance FROM balances", null));
        Assert.AreEqual("100", (await t1cursor.ToListAsync()).Single().Row["balance"].StrValue);

        // T2 modifies the row T1 scanned, and commits.
        KvTransaction t2 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(t2, dbname,
            "UPDATE balances SET balance = \"200\" WHERE id = \"a\"", null));
        await database.Transactions.CommitAsync(t2);

        // T1 writes a disjoint key — the write itself is conflict-free, but the scanned row's
        // observation no longer validates, so the optimistic commit must abort.
        await executor.ExecuteNonSQLQuery(new(t1, dbname,
            "INSERT INTO balances (id, balance) VALUES (\"b\", \"50\")", null));
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await database.Transactions.CommitAsync(t1))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex.Code,
            "a scanned row modified by a concurrent commit must abort the optimistic transaction");

        // The aborted transaction's insert must not persist.
        KvTransaction txq = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id FROM balances", null));
        int rowCount = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txq);
        Assert.AreEqual(1, rowCount, "the aborted optimistic transaction's insert must not persist");
    }

    /// <summary>
    /// The UPDATE locate scan still folds every row it scanned, including rows the WHERE rejected
    /// and which the statement therefore never wrote. Here a concurrent transaction edits such a
    /// row so that it would now match the UPDATE's predicate; because the row was in the locate
    /// scan's read set, the optimistic UPDATE must abort rather than silently commit a result that
    /// skipped a matching row.
    ///
    /// Write-intent conflict detection alone cannot catch this — the two transactions wrote
    /// disjoint rows — which is why the locate scan's read-set folding is load-bearing here.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestUpdateLocateScanStillFoldsScannedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE tiers (id STRING NOT NULL PRIMARY KEY, tier STRING NOT NULL, note STRING NOT NULL)",
            null));

        KvTransaction seed = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO tiers (id, tier, note) VALUES (\"a\", \"gold\", \"-\")", null));
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO tiers (id, tier, note) VALUES (\"b\", \"silver\", \"-\")", null));
        await database.Transactions.CommitAsync(seed);

        // T1 (optimistic) updates every gold row. "tier" is not indexed, so the locate scan reads
        // BOTH rows and writes only "a" — row "b" is read-only for this transaction.
        KvTransaction t1 = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await executor.ExecuteNonSQLQuery(new(t1, dbname,
            "UPDATE tiers SET note = \"touched\" WHERE tier = \"gold\"", null));

        // T2 promotes row "b" to gold and commits: T1's result is now missing a matching row.
        KvTransaction t2 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(t2, dbname,
            "UPDATE tiers SET tier = \"gold\" WHERE id = \"b\"", null));
        await database.Transactions.CommitAsync(t2);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await database.Transactions.CommitAsync(t1),
            "An UPDATE must abort when a row its locate scan read was modified by a committed peer");

        // The aborted UPDATE's write must not have landed.
        KvTransaction txq = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id, note FROM tiers WHERE id = \"a\"", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txq);
        Assert.AreEqual("-", rows.Single().Row["note"].StrValue, "The aborted UPDATE must not persist its write");
    }

    /// <summary>
    /// Control for <see cref="TestOptimisticReadWriteConflictAbortsAtCommit"/>: with no concurrent
    /// modification of the read row, the optimistic transaction's read-set validates and it commits.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptimisticReadThenWriteCommitsWhenNoConflict()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE ledgers (id STRING NOT NULL PRIMARY KEY, balance STRING NOT NULL)",
            null));

        KvTransaction seed = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO ledgers (id, balance) VALUES (\"a\", \"100\")", null));
        await database.Transactions.CommitAsync(seed);

        KvTransaction t1 = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        (_, IAsyncEnumerable<QueryResultRow> t1cursor) =
            await executor.ExecuteSQLQuery(new(t1, dbname, "SELECT id, balance FROM ledgers", null));
        _ = await t1cursor.ToListAsync();

        await executor.ExecuteNonSQLQuery(new(t1, dbname,
            "INSERT INTO ledgers (id, balance) VALUES (\"b\", \"50\")", null));
        await database.Transactions.CommitAsync(t1);

        KvTransaction txq = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id FROM ledgers", null));
        int rowCount = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txq);
        Assert.AreEqual(2, rowCount, "A conflict-free optimistic transaction must commit its write");
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
                // The conflict may surface either at insert time (tx still Active) or at commit time
                // (the coordinator aborts the loser and the tx is already finalized). Use the
                // outcome-agnostic rollback so cleanup never throws "already RolledBack".
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
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
    /// An INSERT that names the same column twice in the explicit target list must be rejected
    /// before any row is written. Using StringComparer.Ordinal: (name, name) is a duplicate,
    /// but (name, NAME) is not — they are distinct column references.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestInsertDuplicateTargetColumn_ThrowsAndInsertsNoRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txDDL = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txDDL, dbname,
            "CREATE TABLE items (id OID NOT NULL PRIMARY KEY, name STRING NOT NULL, value INT NOT NULL)",
            null));
        await database.Transactions.CommitAsync(txDDL);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new(txIns, dbname,
                "INSERT INTO items (id, name, name) VALUES (gen_id(), \"foo\", \"bar\")", null)));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code,
            "A duplicate target column must throw InvalidInput");
        Assert.IsTrue(ex.Message.Contains("name"), "Error message should identify the repeated column");

        await database.Transactions.RollbackAsync(txIns);

        // No row must have been inserted.
        KvTransaction txQ = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txQ, dbname, "SELECT id FROM items", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(txQ);

        Assert.AreEqual(0, count, "No row must survive after a rejected duplicate-column insert");
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
