
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestRowUpdater : BaseTest
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
        
        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "unknown_table",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Update(ticket));
        Assert.AreEqual("Table 'unknown_table' doesn't exist", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNotNullColumWithNull()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.Null, "") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Update(ticket));
        Assert.AreEqual("Column 'name' cannot be null", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNotNullColumWithNull2()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.Null, null!) }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Update(ticket));
        Assert.AreEqual("Column 'name' cannot be null", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateByIdSingleRow()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        UpdateResult execResult = await executor.Update(ticket);
        Assert.AreEqual(1, execResult.UpdatedRows);

        QueryByIdTicket queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateUnknownRow()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();
        
        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, "---"))
            },
            parameters: null
        );

        UpdateResult execResult = await executor.Update(ticket);
        Assert.AreEqual(0, execResult.UpdatedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateByIdSingleRowTwice()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();
        
        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        UpdateResult execResult = await executor.Update(ticket);
        Assert.AreEqual(1, execResult.UpdatedRows);

        QueryByIdTicket queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
        
        ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value 2") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        execResult = await executor.Update(ticket);
        Assert.AreEqual(1, execResult.UpdatedRows);

        queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value 2", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdate()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        foreach (string objectId in objectsId)
        {
            UpdateTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                plainValues: new()
                {
                    { "name", new(ColumnType.String, "updated value") }
                },
                exprValues: null,
                where: null,
                filters: new()
                {
                    new("id", "=", new(ColumnType.Id, objectId))
                },
                parameters: null
            );

            UpdateResult execResult = await executor.Update(ticket);
            Assert.AreEqual(1, execResult.UpdatedRows);
        }

        foreach (string objectId in objectsId)
        {
            QueryByIdTicket queryByIdTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
            Assert.IsNotEmpty(result);

            Assert.AreEqual(objectId, result[0]["id"].StrValue);
            Assert.AreEqual("updated value", result[0]["name"].StrValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdateParallel()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        List<Task> tasks = new(objectsId.Count);

        foreach (string objectId in objectsId)
        {
            UpdateTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                plainValues: new()
                {
                    { "name", new(ColumnType.String, "updated value") }
                },
                exprValues: null,
                where: null,
                filters: new()
                {
                    new("id", "=", new(ColumnType.Id, objectId))
                },
                parameters: null
            );

            tasks.Add(executor.Update(ticket));
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
        Assert.IsNotEmpty(result);

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
                new("name", "=", new(ColumnType.String, "updated value"))
            },
            limit: null,
            offset: null,
            orderBy: null,
            parameters: null
        );
        
        (DatabaseDescriptor _, cursor) = await executor.Query(queryTicket);

        result = await cursor.ToListAsync();
        Assert.AreEqual(25, result.Count);

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
                new("name", "=", new(ColumnType.String, "another updated value"))
            },
            limit: null,
            offset: null,
            orderBy: null,
            parameters: null
        );

        (DatabaseDescriptor _, cursor) = await executor.Query(queryTicket);

        result = await cursor.ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicUpdate()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        UpdateResult updateResult = await executor.Update(ticket);
        Assert.AreEqual(1, updateResult.UpdatedRows);

        QueryByIdTicket queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateMany()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("year", ">", new(ColumnType.Integer64, 2010))
            },
            parameters: null
        );

        UpdateResult updateResult = await executor.Update(ticket);
        Assert.AreEqual(14, updateResult.UpdatedRows);

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
        Assert.AreEqual(14, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreEqual(row["name"].StrValue, "updated value");
        }

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
                new("year", "<=", new(ColumnType.Integer64, 2010))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, cursor) = await executor.Query(queryTicket);
        
        result = await cursor.ToListAsync();
        Assert.AreEqual(11, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreNotEqual(row["name"].StrValue, "updated value");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateNone()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions, List<string> _) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        UpdateTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            plainValues: new()
            {
                { "name", new(ColumnType.String, "updated value") }
            },
            exprValues: null,
            filters: new()
            {
                new("year", ">", new(ColumnType.Integer64, 200010))
            },
            parameters: null
        );

        UpdateResult updateResult = await executor.Update(ticket);
        Assert.AreEqual(0, updateResult.UpdatedRows);
    }
}
