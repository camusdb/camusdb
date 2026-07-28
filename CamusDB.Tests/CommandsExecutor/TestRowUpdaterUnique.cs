
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

using CamusDB.Core;
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

public sealed class TestRowUpdaterUnique : SharedNodeBaseTest
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
        
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUpdateMany()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

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
                new("year", ">", new(ColumnType.Integer64, 2010))
            },
            parameters: null
        );

        // Updating multiple rows to the same value violates the unique index on 'name'.
        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Update(ticket));
        Assert.AreEqual("Duplicate entry for key 'robots.name_idx'", exception!.Message);

        await database.Transactions.RollbackIfNotCompletedAsync(txnState);

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
            IReadOnlyDictionary<string, ColumnValue> row = resultRow.Row;

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
            IReadOnlyDictionary<string, ColumnValue> row = resultRow.Row;

            Assert.AreEqual(row["name"].Type, ColumnType.String);
            Assert.AreNotEqual(row["name"].Value, "updated value");
        }*/
    }
}