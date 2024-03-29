
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
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
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestRowUpdaterUnique : BaseTest
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
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) })
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

        UpdateResult execResult = await executor.Update(ticket);
        Assert.AreEqual(14, execResult.UpdatedRows);

        /*QueryTicket queryTicket = new(
            database: dbname,
            name: "robots",
            index: null,
            where: null,
            filters: new()
            {
                new("year", ">", new ColumnValue(ColumnType.Integer64, "2010"))
            },
            orderBy: null
        );

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(14, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreEqual(row["name"].Value, "updated value");
        }

        queryTicket = new(
            database: dbname,
            name: "robots",
            index: null,
            where: null,
            filters: new()
            {
                new("year", "<=", new ColumnValue(ColumnType.Integer64, "2010"))
            },
            orderBy: null
        );

        result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.AreEqual(11, result.Count);

        foreach (QueryResultRow resultRow in result)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreNotEqual(row["name"].Value, "updated value");
        }*/
    }
}