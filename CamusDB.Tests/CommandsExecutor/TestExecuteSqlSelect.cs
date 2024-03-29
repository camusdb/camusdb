
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.CommandsExecutor;

public class TestExecuteSqlSelect : BaseTest
{
    private async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        TransactionsManager transactions = new(hlc);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor database = await executor.CreateDatabase(databaseTicket);

        return (dbname, database, executor, transactions);
    }

    private async Task<(string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket tableTicket = new(
            txnState: txnState,
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

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions, objectsId);
    }    

    private async Task<(string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId)> SetupBasicTableWithNulls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket tableTicket = new(
            txnState: txnState,
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
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                        { "year", new ColumnValue(ColumnType.Null, "") },
                        { "enabled", new ColumnValue(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGenericWhere()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots WHERE 1=1",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots WHERE enabled=enabled",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool3()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=TRUE",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereBool4()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=FALSE",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(false, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots WHERE year=2000",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsInteger2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots WHERE 2000=year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsString()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = \"some name 10\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsString2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE \"some name 10\"=name",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnNotEqualsInteger()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year!=2000",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(24, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnNotEqualsInteger2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE 2000!=year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(24, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsIntegerOr()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year=2000 OR year=2001",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(2, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnEqualsIntegerOr2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year=2000 OR year=2001 OR year=2002",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(3, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnGreaterInteger()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year>2020",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(4, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereColumnLessInteger()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsNull()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = null",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsNull2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE name = @null",
            parameters: new() { { "@null", new ColumnValue(ColumnType.Null, 0) } }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsId()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectIds) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, enabled FROM robots WHERE id = @id",
            parameters: new() { { "@id", new ColumnValue(ColumnType.Id, objectIds[0]) } }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(objectIds[0], row.Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereEqualsId2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectIds) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, enabled FROM robots WHERE id = str_id(@id)",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectIds[0]) } }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(objectIds[0], row.Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some%\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some name 0\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike3()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"some%0\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereLike4()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name LIKE \"%name%0\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereILike()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name ILIKE \"SOME%\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectWhereILike2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE name ILIKE \"%NAME%\"",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["name"].StrValue!.StartsWith("some"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY name",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled, year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY year DESC",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots ORDER BY enabled DESC",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled FROM robots WHERE enabled=@enabled",
            parameters: new() { { "@enabled", new ColumnValue(ColumnType.Bool, true) } }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow row in result)
            Assert.AreEqual(true, row.Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregate1()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(*) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(25, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregate2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(id) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(25, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateWithConditions()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(id) FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(5, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjection1()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year + year FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year * 2 - year, year FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year + year AS sumYear FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 1",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 5",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit3()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year >= 2020 LIMIT 5",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(5, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit4()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
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
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectIds) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots LIMIT 1 OFFSET 5",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);

        Assert.AreEqual(objectIds[5], result[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit6()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots LIMIT @limit",
            parameters: new()
            {
                { "@limit", new ColumnValue(ColumnType.Integer64, 1)  }
            }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit7()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots LIMIT @limit OFFSET @offset",
            parameters: new()
            {
                { "@limit", new ColumnValue(ColumnType.Integer64, 1)  },
                { "@offset", new ColumnValue(ColumnType.Integer64, 1)  }
            }
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(1, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectForceIndex()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots@{FORCE_INDEX=pk}",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectIsNull()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year IS NULL",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectIsNotNull()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year IS NOT NULL",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectIsNullAll()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTableWithNulls();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year IS NULL",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectIsNotNullNone()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTableWithNulls();

        TransactionState txnState = await transactions.Start();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year IS NOT NULL",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }
}