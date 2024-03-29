
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
using CamusDB.Core.Util.Time;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestExecuteSql : BaseTest
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

    private async Task<(string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId)> SetupBasicTableWithDefaults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket tableTicket = new(
            txnState: txnState,
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

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoConditions()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);
        
        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(1000, result[1].Row["year"].LongValue);
        Assert.AreEqual(1000, result[24].Row["year"].LongValue);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOne()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateMatchOnePlaceholders()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        Assert.AreEqual(1000, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateNoMatches()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.AreNotEqual(3000, row.Row["year"].LongValue);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteUpdateIncrement()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

        foreach (QueryResultRow row in result)
            Assert.True(row.Row["year"].LongValue >= 3000);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteNoConditions()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatchesAll()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await transactions.Commit(database, txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteDeleteMatche1()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

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

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await transactions.Commit(database, txnState);
    }
}