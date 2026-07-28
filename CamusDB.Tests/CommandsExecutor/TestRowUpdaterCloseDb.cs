
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestRowUpdaterCloseDb : BaseTest
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
            new ColumnInfo[]
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
        
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicUpdateById()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();
        
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
        
        await database.Transactions.CommitAsync(txnState);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        txnState = await database.Transactions.BeginAsync();

        queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        
        await database.Transactions.CommitAsync(txnState);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);
        
        txnState = await database.Transactions.BeginAsync();

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
    public async Task TestBasicUpdateByIdTwice()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();
        
        UpdateTicket updateTicket = new(
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

        UpdateResult execResult = await executor.Update(updateTicket);
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

        await database.Transactions.CommitAsync(txnState);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        // The previous transaction was committed above; a further write must run on a fresh
        // transaction. Reusing a finalized transaction is rejected by the coordinator.
        txnState = await database.Transactions.BeginAsync();

        queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(objectsId[0], result[0]["id"].StrValue);
        Assert.AreEqual("updated value", result[0]["name"].StrValue);
        
        updateTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "name", new(ColumnType.String, "new updated value") }
            },
            exprValues: null,
            where: null,
            filters: new()
            {
                new("id", "=", new(ColumnType.Id, objectsId[0]))
            },
            parameters: null
        );

        execResult = await executor.Update(updateTicket);
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
        Assert.AreEqual("new updated value", result[0]["name"].StrValue);

        await database.Transactions.CommitAsync(txnState);
    }
}