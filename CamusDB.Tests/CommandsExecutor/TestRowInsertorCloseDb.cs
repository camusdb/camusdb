﻿
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
using System.Collections.Concurrent;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.ObjectIds;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestRowInsertorCloseDb : BaseTest
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

    private async Task<(string dbname, CommandExecutor executor, TransactionsManager transactions)> SetupMultiIndexTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket tableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("usersId", ColumnType.Id, notNull: true),
                new ColumnInfo("amount", ColumnType.Integer64)
            },            
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new ConstraintInfo(ConstraintType.IndexMulti, "usersId", new ColumnIndexInfo[] { new("usersId", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        await transactions.Commit(database, txnState);

        return (dbname, executor, transactions);
    }

    [Test]
    [NonParallelizable]
    public async Task TestBasicInsert()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions) = await SetupMultiIndexTable();

        TransactionState txnState = await transactions.Start();

        InsertTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            values: new()
            {
                new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                    { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                    { "amount", new ColumnValue(ColumnType.Integer64, 100) }
                }
            }
        );

        await executor.Insert(ticket);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        QueryByIdTicket queryTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            id: "507f1f77bcf86cd799439011"
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();
        Assert.AreEqual(1, result.Count);

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f1f77bcf86cd799439011");

        Assert.AreEqual(row["usersId"].Type, ColumnType.Id);
        Assert.AreEqual(row["usersId"].StrValue, "5e353cf5e95f1e3a432e49aa");
    }

    [Test]
    [NonParallelizable]
    public async Task TestSuccessfulTwoMultiParallelInserts()
    {
        (string dbname, CommandExecutor executor, TransactionsManager transactions) = await SetupMultiIndexTable();

        TransactionState txnState = await transactions.Start();

        InsertTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            values: new()
            {
                new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011") },
                    { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                    { "amount", new ColumnValue(ColumnType.Integer64, 50) },
                }
            }
        );

        InsertTicket ticket2 = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            values: new()
            {
                new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, "507f191e810c19729de860ea") },
                    { "usersId", new ColumnValue(ColumnType.Id, "5e353cf5e95f1e3a432e49aa") },
                    { "amount", new ColumnValue(ColumnType.Integer64, 50) },
                }
            }
        );

        await Task.WhenAll(new Task[]
        {
            executor.Insert(ticket),
            executor.Insert(ticket2)
        });

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        QueryByIdTicket queryTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            id: "507f191e810c19729de860ea"
        );

        List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();
        Assert.AreEqual(1, result.Count);

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f191e810c19729de860ea");

        Assert.AreEqual(row["usersId"].Type, ColumnType.Id);
        Assert.AreEqual(row["usersId"].StrValue, "5e353cf5e95f1e3a432e49aa");

        /*Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "4567");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "true");*/

        QueryByIdTicket queryTicket2 = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            id: "507f1f77bcf86cd799439011"
        );

        result = await (await executor.QueryById(queryTicket2)).ToListAsync();

        row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].StrValue, "507f1f77bcf86cd799439011");

        /*Assert.AreEqual(row[1].Type, ColumnType.String);
        Assert.AreEqual(row[1].Value, "some name 1");

        Assert.AreEqual(row[2].Type, ColumnType.Integer);
        Assert.AreEqual(row[2].Value, "1234");

        Assert.AreEqual(row[3].Type, ColumnType.Bool);
        Assert.AreEqual(row[3].Value, "false");*/
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsert()
    {
        int i;
        (string dbname, CommandExecutor executor, TransactionsManager transactions) = await SetupMultiIndexTable();

        TransactionState txnState = await transactions.Start();

        string[] userIds = new string[5];
        for (i = 0; i < 5; i++)
            userIds[i] = ObjectIdGenerator.Generate().ToString();

        List<string> objectIds = new();

        for (i = 0; i < 50; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();
            objectIds.Add(objectId);

            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "user_robots",
                values: new()
                {
                    new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, objectId) },
                        { "usersId", new ColumnValue(ColumnType.Id, userIds[i % 5]) },
                        { "amount", new ColumnValue(ColumnType.Integer64, 50) },
                    }
                }
            );

            await executor.Insert(insertTicket);

            if ((i + 1) % 5 == 0)
            {
                CloseDatabaseTicket closeTicket = new(dbname);
                await executor.CloseDatabase(closeTicket);
            }
        }

        i = 0;

        foreach (string objectId in objectIds)
        {
            QueryByIdTicket queryTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "user_robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

            Dictionary<string, ColumnValue> row = result[0];

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].StrValue!.Length);            

            i++;
        }

        Assert.AreEqual(50, i);
    }

    private static async Task InsertRow(TransactionState txnState, string dbname, CommandExecutor executor, ConcurrentBag<string> objectIds, string[] userIds, int i)
    {
        string objectId = ObjectIdGenerator.Generate().ToString();
        objectIds.Add(objectId);

        InsertTicket insertTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "user_robots",
            values: new()
            {
                new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "usersId", new ColumnValue(ColumnType.Id, userIds[i % 5]) },
                    { "amount", new ColumnValue(ColumnType.Integer64, 50) },
                }
            }
        );

        await executor.Insert(insertTicket);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCheckSuccessfulMultiInsert2()
    {
        int i;
        (string dbname, CommandExecutor executor, TransactionsManager transactions) = await SetupMultiIndexTable();

        TransactionState txnState = await transactions.Start();

        string[] userIds = new string[5];
        for (i = 0; i < 5; i++)
            userIds[i] = ObjectIdGenerator.Generate().ToString();

        List<Task> tasks = new();
        ConcurrentBag<string> objectIds = new();

        for (i = 0; i < 50; i++)        
            tasks.Add(InsertRow(txnState, dbname, executor, objectIds, userIds, i));
                        
        await Task.WhenAll(tasks);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        i = 0;

        foreach (string objectId in objectIds)
        {
            QueryByIdTicket queryTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "user_robots",
                id: objectId
            );

            List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

            Dictionary<string, ColumnValue> row = result[0];

            Assert.AreEqual(ColumnType.Id, row["id"].Type);
            Assert.AreEqual(24, row["id"].StrValue!.Length);

            i++;
        }

        Assert.AreEqual(50, i);
    }
}