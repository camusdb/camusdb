
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
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.Tests.CommandsExecutor;

public class TestRowDeletorCloseDb : BaseTest
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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId)> SetupBasicTable()
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
                        { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        await transactions.Commit(database, txnState);

        return (dbname, database, executor, transactions, objectsId);
    }    

    [Test]
    [NonParallelizable]
    public async Task TestBasicDelete()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

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

        await transactions.Commit(database, txnState);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

        queryByIdTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            id: objectsId[0]
        );

        result = await (await executor.QueryById(queryByIdTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiDeleteCriteria()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, List<string> objectsId) = await SetupBasicTable();

        TransactionState txnState = await transactions.Start();

        DeleteTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new()
            {
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
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
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
            },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await transactions.Commit(database, txnState);

        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);

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
                new("id", "=", new ColumnValue(ColumnType.Id, objectsId[0]))
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
}