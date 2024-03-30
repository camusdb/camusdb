
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

using CamusDB.Core;
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
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowDeletor : BaseTest
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
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions, objectsId);
    }

    private async Task<(string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId)> SetupLargeDataTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket tableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots2",
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
        string largeData = string.Join("", Enumerable.Repeat("a", 100000));

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots2",
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "name", new ColumnValue(ColumnType.String, largeData) },
                        { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions, objectsId);
    }

    /*[Test]
    [NonParallelizable]
    public async Task TestInvalidDatabase()
    {
        var executor = await SetupBasicTable();

        InsertTicket ticket = new(
            database: "another_factory",
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Integer, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "FALSE") },
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.AreEqual("Database doesn't exist", e!.Message);
    }*/

    [Test]
    [NonParallelizable]
    public async Task TestInvalidTable()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "unknown_table",
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            }
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Delete(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicDelete()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            }
        );

        DeleteResult execResult = await executor.Delete(ticket);
        Assert.AreEqual(1, execResult.DeletedRows);

        QueryByIdTicket queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDeleteUnknownRow()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, "---"))
            }
        );

        DeleteResult execResult = await executor.Delete(ticket);
        Assert.AreEqual(0, execResult.DeletedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDelete()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        foreach (string objectId in objectsId)
        {
            DeleteTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                where: null,
                filters: new()
                {
                    new("id", "=", new(ColumnType.Id, objectId))
                }
            );

            DeleteResult execResult = await executor.Delete(ticket);
            Assert.AreEqual(1, execResult.DeletedRows);
        }

        QueryTicket queryTicket = new(
           txnState: txnState,
           txnType: TransactionType.ReadOnly,
           databaseName: dbname,
           tableName: "robots",
           index: null,
           projection: null,
           where: null,
           filters: null,
           orderBy: null,
           limit: null,
           offset: null,
           parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDelete2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupLargeDataTable();

        TransactionState txnState = await transactions.Start();

        foreach (string objectId in objectsId)
        {
            DeleteTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots2",
                where: null,
                filters: new()
                {
                    new("id", "=", new(ColumnType.Id, objectId))
                }
            );

            DeleteResult execResult = await executor.Delete(ticket);
            Assert.AreEqual(1, execResult.DeletedRows);
        }

        QueryTicket queryTicket = new(
           txnState: txnState,
           txnType: TransactionType.ReadOnly,
           databaseName: dbname,
           tableName: "robots2",
           index: null,
           projection: null,
           where: null,
           filters: null,
           orderBy: null,
           limit: null,
           offset: null,
           parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteParallel()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        List<Task> tasks = new();

        foreach (string objectId in objectsId)
        {
            DeleteTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                where: null,
                filters: new()
                {
                    new("id", "=", new(ColumnType.Id, objectId))
                }
            );

            tasks.Add(executor.Delete(ticket));
        }

        await Task.WhenAll(tasks);

        QueryTicket queryTicket = new(
           txnState: txnState,
           txnType: TransactionType.ReadOnly,
           databaseName: dbname,
           tableName: "robots",
           index: null,
           projection: null,
           where: null,
           filters: null,
           orderBy: null,
           limit: null,
           offset: null,
           parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteCriteria()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            }
        );

        DeleteResult execResult = await executor.Delete(ticket);
        Assert.AreEqual(1, execResult.DeletedRows);

        QueryTicket queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteCriteria2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        QueryTicket queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("year", ">", new(ColumnType.Integer64, 2010))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
       );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("year", ">", new(ColumnType.Integer64, 2010))
            }
        );

        DeleteResult execResult = await executor.Delete(ticket);
        Assert.AreEqual(14, execResult.DeletedRows);

        queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: new()
            {
                new("year", ">", new(ColumnType.Integer64, 2010))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (_, cursor) = await executor.Query(queryTicket);

        result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteCriteriaNoRows()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("year", "<", new(ColumnType.Integer64, -1))
            }
        );

        DeleteResult execResult = await executor.Delete(ticket);
        Assert.AreEqual(0, execResult.DeletedRows);

        QueryTicket queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);
    }
}
