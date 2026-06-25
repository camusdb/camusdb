
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
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
public class TestExecuteSqlSelect : SharedNodeBaseTest
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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithYearIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        AlterIndexTicket alterIndexTicket = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        await executor.AlterIndex(alterIndexTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithCompositeIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTableWithYearIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        AlterIndexTicket alterIndexTicket = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_enabled_idx",
            columns: new ColumnIndexInfo[]
            {
                new("year", OrderType.Ascending),
                new("enabled", OrderType.Ascending)
            },
            operation: AlterIndexOperation.AddUniqueIndex
        );

        await executor.AlterIndex(alterIndexTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupNamedRobotsWithNameIndex()
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

        string[] names = ["alice", "bob", "boba", "carl"];

        foreach (string name in names)
        {
            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new ColumnValue(ColumnType.String, name) },
                        { "year", new ColumnValue(ColumnType.Integer64, 2000) },
                        { "enabled", new ColumnValue(ColumnType.Bool, true) },
                    }
                }
            );

            await executor.Insert(ticket);
        }

        await database.Transactions.CommitAsync(txnState);

        AlterIndexTicket alterIndexTicket = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        await executor.AlterIndex(alterIndexTicket);

        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId)> SetupBasicTableWithNulls()
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

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGenericWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
    public async Task TestExecuteSelectOrderByThreeColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled, year, name FROM robots ORDER BY enabled ASC, year DESC, name ASC",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(25, result.Count);

        for (int i = 1; i < result.Count; i++)
        {
            QueryResultRow previous = result[i - 1];
            QueryResultRow current = result[i];

            int enabledCompare = previous.Row["enabled"].CompareTo(current.Row["enabled"]);
            Assert.LessOrEqual(enabledCompare, 0);

            if (enabledCompare != 0)
                continue;

            int yearCompare = previous.Row["year"].CompareTo(current.Row["year"]);
            Assert.GreaterOrEqual(yearCompare, 0);

            if (yearCompare != 0)
                continue;

            Assert.LessOrEqual(
                string.CompareOrdinal(previous.Row["name"].StrValue, current.Row["name"].StrValue),
                0);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderBy5()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
    public async Task TestExecuteSelectAggregateSum()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT SUM(year) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(50300, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateSumWithConditions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT SUM(year) FROM robots WHERE year<2005",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(10010, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateAverage()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT AVG(year) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.AreEqual(2012.0, result[0].Row["0"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateMinMax()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket minTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT MIN(year) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> minCursor) = await executor.ExecuteSQLQuery(minTicket);
        List<QueryResultRow> minResult = await minCursor.ToListAsync();

        Assert.AreEqual(1, minResult.Count);
        Assert.AreEqual(ColumnType.Integer64, minResult[0].Row["0"].Type);
        Assert.AreEqual(2000, minResult[0].Row["0"].LongValue);

        ExecuteSQLTicket maxTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT MAX(year) FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> maxCursor) = await executor.ExecuteSQLQuery(maxTicket);
        List<QueryResultRow> maxResult = await maxCursor.ToListAsync();

        Assert.AreEqual(1, maxResult.Count);
        Assert.AreEqual(ColumnType.Integer64, maxResult[0].Row["0"].Type);
        Assert.AreEqual(2024, maxResult[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateWithAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT SUM(year) AS totalYear FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["totalyear"].Type);
        Assert.AreEqual(50300, result[0].Row["totalyear"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectProjection1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
    public async Task TestExecuteSelectProjectionWithSingleTableAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT r.id, r.name FROM robots r",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        Assert.AreEqual(25, result.Count);

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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

            Assert.True(row.Row.ContainsKey("sumyear"));
            Assert.True(row.Row["sumyear"].LongValue >= 4000);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectLimit1()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots@{FORCE_INDEX=pk}",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);
        Assert.AreEqual(25, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectSecondaryIndexEqualityScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithYearIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, year FROM robots WHERE year = 2000",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectRangePredicateExactRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots WHERE year >= 2001 AND year < 2005 ORDER BY year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual(2001, result[0].Row["year"].LongValue);
        Assert.AreEqual(2002, result[1].Row["year"].LongValue);
        Assert.AreEqual(2003, result[2].Row["year"].LongValue);
        Assert.AreEqual(2004, result[3].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectBetweenPredicateExactRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots WHERE year BETWEEN 2001 AND 2004 ORDER BY year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual(2001, result[0].Row["year"].LongValue);
        Assert.AreEqual(2002, result[1].Row["year"].LongValue);
        Assert.AreEqual(2003, result[2].Row["year"].LongValue);
        Assert.AreEqual(2004, result[3].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectSecondaryIndexRangeScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithYearIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots WHERE year >= 2001 AND year < 2005 ORDER BY year",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual(2001, result[0].Row["year"].LongValue);
        Assert.AreEqual(2002, result[1].Row["year"].LongValue);
        Assert.AreEqual(2003, result[2].Row["year"].LongValue);
        Assert.AreEqual(2004, result[3].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCompositeIndexEqualityScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithCompositeIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, year, enabled FROM robots WHERE year = 2000 AND enabled = false",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        ExecuteSQLTicket fullScanTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, year, enabled FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> fullScanCursor) = await executor.ExecuteSQLQuery(fullScanTicket);
        List<QueryResultRow> expected = await fullScanCursor
            .Where(row => row.Row["year"].LongValue == 2000 && !row.Row["enabled"].BoolValue)
            .ToListAsync();

        Assert.AreEqual(expected.Count, result.Count);
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(expected[0].Row["id"].StrValue, result[0].Row["id"].StrValue);
        Assert.AreEqual(2000, result[0].Row["year"].LongValue);
        Assert.IsFalse(result[0].Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCompositeIndexPrefixRangeDoesNotLeakLaterPrefixValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithCompositeIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year, enabled FROM robots WHERE year = 2023 AND enabled > false",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        ExecuteSQLTicket fullScanTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year, enabled FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> fullScanCursor) = await executor.ExecuteSQLQuery(fullScanTicket);
        List<QueryResultRow> expected = await fullScanCursor
            .Where(row => row.Row["year"].LongValue == 2023 && row.Row["enabled"].BoolValue)
            .ToListAsync();

        Assert.AreEqual(expected.Count, result.Count);
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(2023, result[0].Row["year"].LongValue);
        Assert.IsTrue(result[0].Row["enabled"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNonUniqueStringIndexEqualityDoesNotScanSuffixKeys()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupNamedRobotsWithNameIndex();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT name FROM robots WHERE name = 'bob'",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        ExecuteSQLTicket fullScanTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT name FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> fullScanCursor) = await executor.ExecuteSQLQuery(fullScanTicket);
        List<QueryResultRow> expected = await fullScanCursor
            .Where(row => row.Row["name"].StrValue == "bob")
            .ToListAsync();

        Assert.AreEqual(expected.Count, result.Count);
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("bob", result[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateCountWithAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(*) AS total FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["total"].Type);
        Assert.AreEqual(25, result[0].Row["total"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectOrderByLimitExactRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT year FROM robots ORDER BY year DESC LIMIT 3",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual(2024, result[0].Row["year"].LongValue);
        Assert.AreEqual(2023, result[1].Row["year"].LongValue);
        Assert.AreEqual(2022, result[2].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectIsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithNulls();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithNulls();

        KvTransaction txnState = await database.Transactions.BeginAsync();

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

    #region Pending query feature acceptance fixtures

    private sealed record AppUsersPostsFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor,
        string UserAId,
        string UserBId,
        string UserCId,
        string UserDId);

    private async Task<AppUsersPostsFixture> SetupAppUsersAndPosts(bool indexPostsUserId = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket usersTicket = new(
            databaseName: dbname,
            tableName: "app_users",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
                new("role", ColumnType.String, notNull: true)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(usersTicket);

        CreateTableTicket postsTicket = new(
            databaseName: dbname,
            tableName: "posts",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("user_id", ColumnType.Id),
                new("title", ColumnType.String, notNull: true),
                new("published", ColumnType.Bool)
            },
            constraints: indexPostsUserId
                ?
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                    new(ConstraintType.IndexMulti, "posts_user_id_idx", new ColumnIndexInfo[] { new("user_id", OrderType.Ascending) }),
                ]
                :
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                ],
            ifNotExists: false
        );

        await executor.CreateTable(postsTicket);

        string userAId = ObjectIdGenerator.Generate().ToString();
        string userBId = ObjectIdGenerator.Generate().ToString();
        string userCId = ObjectIdGenerator.Generate().ToString();
        string userDId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "app_users",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, userAId) },
                    { "email", new(ColumnType.String, "a@example.com") },
                    { "role", new(ColumnType.String, "admin") },
                },
                new()
                {
                    { "id", new(ColumnType.Id, userBId) },
                    { "email", new(ColumnType.String, "b@example.com") },
                    { "role", new(ColumnType.String, "admin") },
                },
                new()
                {
                    { "id", new(ColumnType.Id, userCId) },
                    { "email", new(ColumnType.String, "c@example.com") },
                    { "role", new(ColumnType.String, "member") },
                },
                new()
                {
                    { "id", new(ColumnType.Id, userDId) },
                    { "email", new(ColumnType.String, "d@example.com") },
                    { "role", new(ColumnType.String, "member") },
                },
            }));

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "posts",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "user_id", new(ColumnType.Id, userAId) },
                    { "title", new(ColumnType.String, "Post A") },
                    { "published", new(ColumnType.Bool, true) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "user_id", new(ColumnType.Id, userAId) },
                    { "title", new(ColumnType.String, "Draft") },
                    { "published", new(ColumnType.Bool, false) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "user_id", new(ColumnType.Id, userBId) },
                    { "title", new(ColumnType.String, "Post B") },
                    { "published", new(ColumnType.Bool, true) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "user_id", new(ColumnType.Id, userCId) },
                    { "title", new(ColumnType.String, "Post C") },
                    { "published", new(ColumnType.Bool, false) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "user_id", new(ColumnType.Id, userDId) },
                    { "title", new(ColumnType.String, "Post D") },
                    { "published", new(ColumnType.Bool, true) },
                },
            }));

        await database.Transactions.CommitAsync(txnState);

        return new AppUsersPostsFixture(dbname, database, executor, userAId, userBId, userCId, userDId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByOrderByGroupColumnNotInSelect()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT COUNT(*) AS cnt FROM app_users GROUP BY role ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.AreEqual(1, row.Row.Count);
            Assert.IsTrue(row.Row.ContainsKey("cnt"));
            Assert.IsFalse(row.Row.ContainsKey("role"));
            Assert.AreEqual(2, row.Row["cnt"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByOrderByAggregateAlias()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users GROUP BY role ORDER BY cnt, role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);
        Assert.AreEqual("member", result[1].Row["role"].StrValue);
        Assert.AreEqual(2, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByOrderByAggregateExpression()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users GROUP BY role ORDER BY COUNT(*), role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual("member", result[1].Row["role"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByRoleOnly()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role FROM app_users GROUP BY role ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual("member", result[1].Row["role"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByRoleCount()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users GROUP BY role ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);

        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["cnt"].Type);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);

        Assert.AreEqual("member", result[1].Row["role"].StrValue);
        Assert.AreEqual(ColumnType.Integer64, result[1].Row["cnt"].Type);
        Assert.AreEqual(2, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByAndOrderByOrdinals()
    {
        RobotsUserRobotsFixture fixture = await SetupRobotsAndUserRobots();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT robots_id, COUNT(*) AS cnt FROM user_robots GROUP BY 1 ORDER BY 2 DESC",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);
        Assert.AreEqual(1, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByHavingAggregateAlias()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users GROUP BY role HAVING cnt > 1 ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);
        Assert.AreEqual("member", result[1].Row["role"].StrValue);
        Assert.AreEqual(2, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByHavingAggregateExpression()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users GROUP BY role HAVING COUNT(*) > 1 ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);
        Assert.AreEqual(2, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByHavingGroupKey()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role FROM app_users GROUP BY role HAVING role = 'admin' ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByHavingRunsAfterWhere()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT role, COUNT(*) AS cnt FROM app_users WHERE role = 'admin' GROUP BY role HAVING cnt > 0 ORDER BY role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual(2, result[0].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateOnlyHavingAlias()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: database.Name,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState,
            database.Name,
            "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "R2D2") },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "C3PO") },
                },
            }));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: database.Name,
            sql: "SELECT COUNT(*) AS x FROM robots HAVING x > 0",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(2, result[0].Row["x"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectAggregateOnlyHavingExpression()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: database.Name,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState,
            database.Name,
            "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "R2D2") },
                },
            }));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: database.Name,
            sql: "SELECT COUNT(*) AS x FROM robots HAVING COUNT(*) > 0",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(1, result[0].Row["x"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByEnabledSumAvg()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled, SUM(year) AS total, AVG(year) AS average FROM robots GROUP BY enabled ORDER BY enabled",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);

        Assert.AreEqual(false, result[0].Row["enabled"].BoolValue);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["total"].Type);
        Assert.AreEqual(ColumnType.Float64, result[0].Row["average"].Type);
        Assert.Greater(result[0].Row["total"].LongValue, 0);
        Assert.Greater(result[0].Row["average"].FloatValue, 0);

        Assert.AreEqual(true, result[1].Row["enabled"].BoolValue);
        Assert.Greater(result[1].Row["total"].LongValue, 0);
        Assert.Greater(result[1].Row["average"].FloatValue, 0);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectGroupByCountNullHandling()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> _) = await SetupBasicTableWithNulls();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT enabled, COUNT(*) AS all_rows, COUNT(year) AS non_null_years FROM robots GROUP BY enabled ORDER BY enabled",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);

        foreach (QueryResultRow row in result)
        {
            Assert.Greater(row.Row["all_rows"].LongValue, 0);
            Assert.AreEqual(0, row.Row["non_null_years"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinPending()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id ORDER BY u.email, p.title",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(5, result.Count);

        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual("Draft", result[0].Row["title"].StrValue);

        Assert.AreEqual("a@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual("Post A", result[1].Row["title"].StrValue);

        Assert.AreEqual("b@example.com", result[2].Row["email"].StrValue);
        Assert.AreEqual("Post B", result[2].Row["title"].StrValue);

        Assert.AreEqual("c@example.com", result[3].Row["email"].StrValue);
        Assert.AreEqual("Post C", result[3].Row["title"].StrValue);

        Assert.AreEqual("d@example.com", result[4].Row["email"].StrValue);
        Assert.AreEqual("Post D", result[4].Row["title"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinWithWherePushdown()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.role = \"admin\" AND p.published = true ORDER BY u.email, p.title",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual("Post A", result[0].Row["title"].StrValue);
        Assert.AreEqual("b@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual("Post B", result[1].Row["title"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinWithMixedOnAndWherePredicates()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.role = \"member\" ORDER BY u.email, p.title",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("c@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual("Post C", result[0].Row["title"].StrValue);
        Assert.AreEqual("d@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual("Post D", result[1].Row["title"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinCountStar()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT COUNT(*) AS cnt FROM app_users u JOIN posts p ON p.user_id = u.id",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(5, result[0].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinGroupByRoleCount()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT u.role, COUNT(*) AS cnt FROM app_users u JOIN posts p ON p.user_id = u.id GROUP BY u.role ORDER BY u.role",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("admin", result[0].Row["role"].StrValue);
        Assert.AreEqual(3, result[0].Row["cnt"].LongValue);
        Assert.AreEqual("member", result[1].Row["role"].StrValue);
        Assert.AreEqual(2, result[1].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInnerJoinIndexedMatchesNestedLoop()
    {
        const string sql =
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id ORDER BY u.email, p.title";

        AppUsersPostsFixture nestedLoopFixture = await SetupAppUsersAndPosts(indexPostsUserId: false);
        List<QueryResultRow> nestedLoopRows = await ExecuteJoinSelect(nestedLoopFixture, sql);

        AppUsersPostsFixture indexedFixture = await SetupAppUsersAndPosts(indexPostsUserId: true);
        List<QueryResultRow> indexedRows = await ExecuteJoinSelect(indexedFixture, sql);

        Assert.AreEqual(5, nestedLoopRows.Count);
        Assert.AreEqual(5, indexedRows.Count);

        for (int i = 0; i < nestedLoopRows.Count; i++)
        {
            Assert.AreEqual(nestedLoopRows[i].Row["email"].StrValue, indexedRows[i].Row["email"].StrValue);
            Assert.AreEqual(nestedLoopRows[i].Row["title"].StrValue, indexedRows[i].Row["title"].StrValue);
        }
    }

    private static async Task<List<QueryResultRow>> ExecuteJoinSelect(AppUsersPostsFixture fixture, string sql)
    {
        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: sql,
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectScalarSubqueryInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id, name FROM robots WHERE year = (SELECT MAX(year) FROM robots) ORDER BY name",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(objectsId[0], result[0].Row["id"].StrValue);
        Assert.AreEqual("some name 0", result[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectScalarSubqueryZeroRowsReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(*) AS cnt FROM robots WHERE year = (SELECT year FROM robots WHERE year = 9999)",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(0, result[0].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectScalarSubqueryMultipleRowsThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year = (SELECT year FROM robots)",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            _ = await executor.ExecuteSQLQuery(ticket);
        })!;
        Assert.That(ex.Message, Does.Contain("more than one row"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInSubquery()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT email FROM app_users WHERE id IN (SELECT user_id FROM posts WHERE published = true) ORDER BY email");

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual("b@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual("d@example.com", result[2].Row["email"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectInSubqueryMultipleColumnsThrows()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT email FROM app_users WHERE id IN (SELECT user_id, title FROM posts)",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            _ = await fixture.Executor.ExecuteSQLQuery(ticket);
        })!;

        Assert.That(ex.Message, Does.Contain("exactly one column"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNotInSubqueryExcludesMatchingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocked_robots",
            columns: new ColumnInfo[]
            {
                new("robots_id", ColumnType.Id),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "blocked_robots",
            values: new()
            {
                new() { { "robots_id", new(ColumnType.Id, objectsId[0]) } },
                new() { { "robots_id", new(ColumnType.Id, objectsId[1]) } },
            }));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots WHERE id NOT IN (SELECT robots_id FROM blocked_robots) ORDER BY id",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(23, result.Count);
        Assert.IsFalse(result.Any(row => row.Row["id"].StrValue == objectsId[0]));
        Assert.IsFalse(result.Any(row => row.Row["id"].StrValue == objectsId[1]));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNotInSubqueryEmptyReturnsAllRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocked_robots",
            columns: new ColumnInfo[]
            {
                new("robots_id", ColumnType.Id),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(*) AS cnt FROM robots WHERE id NOT IN (SELECT robots_id FROM blocked_robots)",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(objectsId.Count, result[0].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNotInSubqueryNullInSubqueryFiltersUnknownRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocked_robots",
            columns: new ColumnInfo[]
            {
                new("robots_id", ColumnType.Id),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "blocked_robots",
            values: new()
            {
                new() { { "robots_id", new(ColumnType.Id, objectsId[0]) } },
                new() { { "robots_id", new(ColumnType.Null, 0) } },
            }));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT COUNT(*) AS cnt FROM robots WHERE id NOT IN (SELECT robots_id FROM blocked_robots)",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(0, result[0].Row["cnt"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNotInSubqueryMatchingValueStillFilteredWhenSubqueryContainsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectsId) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocked_robots",
            columns: new ColumnInfo[]
            {
                new("robots_id", ColumnType.Id),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "blocked_robots",
            values: new()
            {
                new() { { "robots_id", new(ColumnType.Id, objectsId[0]) } },
                new() { { "robots_id", new(ColumnType.Null, 0) } },
            }));

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT id FROM robots WHERE id NOT IN (SELECT robots_id FROM blocked_robots) AND id = @id",
            parameters: new() { { "id", new(ColumnType.Id, objectsId[0]) } });

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectNotInSubqueryMultipleColumnsThrows()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: "SELECT email FROM app_users WHERE id NOT IN (SELECT user_id, title FROM posts)",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            _ = await fixture.Executor.ExecuteSQLQuery(ticket);
        })!;

        Assert.That(ex.Message, Does.Contain("exactly one column"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectExistsSubquery()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT email FROM app_users WHERE EXISTS (SELECT * FROM posts WHERE posts.user_id = app_users.id) ORDER BY email");

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual("b@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual("c@example.com", result[2].Row["email"].StrValue);
        Assert.AreEqual("d@example.com", result[3].Row["email"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectExistsSubqueryCorrelatedToOuterDerivedTable()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT d.user_id FROM (SELECT user_id FROM posts GROUP BY user_id) d "
            + "WHERE EXISTS (SELECT * FROM app_users u WHERE u.id = d.user_id) "
            + "ORDER BY user_id");

        Assert.AreEqual(4, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectUncorrelatedExistsSelectStar()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT email FROM app_users WHERE EXISTS (SELECT * FROM posts) ORDER BY email");

        Assert.AreEqual(4, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectUncorrelatedExistsMultiColumnProjection()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT email FROM app_users WHERE EXISTS (SELECT user_id, title FROM posts) ORDER BY email");

        Assert.AreEqual(4, result.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectJoinAggregatedDerivedTable()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT u.email, d.post_count FROM app_users u "
            + "JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "ON d.user_id = u.id ORDER BY u.email");

        Assert.AreEqual(4, result.Count);

        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual(2, result[0].Row["post_count"].LongValue);

        Assert.AreEqual("b@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual(1, result[1].Row["post_count"].LongValue);

        Assert.AreEqual("c@example.com", result[2].Row["email"].StrValue);
        Assert.AreEqual(1, result[2].Row["post_count"].LongValue);

        Assert.AreEqual("d@example.com", result[3].Row["email"].StrValue);
        Assert.AreEqual(1, result[3].Row["post_count"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectFromDerivedTableOnly()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "ORDER BY post_count");

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual(1, result[0].Row["post_count"].LongValue);
        Assert.AreEqual(1, result[1].Row["post_count"].LongValue);
        Assert.AreEqual(1, result[2].Row["post_count"].LongValue);
        Assert.AreEqual(2, result[3].Row["post_count"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectFromDerivedTableOnlyWithUnqualifiedWhere()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "WHERE post_count = 2 ORDER BY post_count");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(2, result[0].Row["post_count"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectFromDerivedTableOnlyWithQualifiedWhere()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "WHERE d.post_count = 2 ORDER BY post_count");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(2, result[0].Row["post_count"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDerivedTableJoinBaseTable()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT u.email, d.post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "JOIN app_users u ON d.user_id = u.id ORDER BY u.email");

        Assert.AreEqual(4, result.Count);

        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual(2, result[0].Row["post_count"].LongValue);

        Assert.AreEqual("b@example.com", result[1].Row["email"].StrValue);
        Assert.AreEqual(1, result[1].Row["post_count"].LongValue);

        Assert.AreEqual("c@example.com", result[2].Row["email"].StrValue);
        Assert.AreEqual(1, result[2].Row["post_count"].LongValue);

        Assert.AreEqual("d@example.com", result[3].Row["email"].StrValue);
        Assert.AreEqual(1, result[3].Row["post_count"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectJoinDerivedTableWithDerivedWherePushdown()
    {
        AppUsersPostsFixture fixture = await SetupAppUsersAndPosts();

        List<QueryResultRow> result = await ExecuteJoinSelect(
            fixture,
            "SELECT u.email, d.post_count FROM app_users u "
            + "JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "ON d.user_id = u.id WHERE d.post_count = 2 ORDER BY u.email");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("a@example.com", result[0].Row["email"].StrValue);
        Assert.AreEqual(2, result[0].Row["post_count"].LongValue);
    }

    private sealed record RobotsUserRobotsFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    private async Task<RobotsUserRobotsFixture> SetupRobotsAndUserRobots(bool indexRobotsId = true)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "user_robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("robots_id", ColumnType.Id, notNull: true),
                new("amount", ColumnType.Integer64),
            },
            constraints: indexRobotsId
                ?
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                    new(ConstraintType.IndexMulti, "robots_id_idx", new ColumnIndexInfo[] { new("robots_id", OrderType.Ascending) }),
                ]
                :
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                ],
            ifNotExists: false));

        string robotAId = ObjectIdGenerator.Generate().ToString();
        string robotBId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, robotAId) },
                    { "name", new(ColumnType.String, "Alpha") },
                    { "enabled", new(ColumnType.Bool, true) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, robotBId) },
                    { "name", new(ColumnType.String, "Beta") },
                    { "enabled", new(ColumnType.Bool, false) },
                },
            }));

        await executor.Insert(new InsertTicket(
            txnState,
            dbname,
            "user_robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "robots_id", new(ColumnType.Id, robotAId) },
                    { "amount", new(ColumnType.Integer64, 100) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "robots_id", new(ColumnType.Id, robotAId) },
                    { "amount", new(ColumnType.Integer64, 200) },
                },
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "robots_id", new(ColumnType.Id, robotBId) },
                    { "amount", new(ColumnType.Integer64, 50) },
                },
            }));

        await database.Transactions.CommitAsync(txnState);

        return new RobotsUserRobotsFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteRobotsJoinSelect(RobotsUserRobotsFixture fixture, string sql)
    {
        KvTransaction txnState = await fixture.Database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: fixture.DbName,
            sql: sql,
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCommaJoinMatchesExplicitJoin()
    {
        RobotsUserRobotsFixture fixture = await SetupRobotsAndUserRobots();

        List<QueryResultRow> commaRows = await ExecuteRobotsJoinSelect(
            fixture,
            "SELECT r.name, u.amount FROM robots r, user_robots u "
            + "WHERE r.id = u.robots_id ORDER BY r.name, u.amount");

        List<QueryResultRow> explicitRows = await ExecuteRobotsJoinSelect(
            fixture,
            "SELECT r.name, u.amount FROM robots r JOIN user_robots u ON r.id = u.robots_id "
            + "ORDER BY r.name, u.amount");

        Assert.AreEqual(explicitRows.Count, commaRows.Count);

        for (int i = 0; i < commaRows.Count; i++)
        {
            Assert.AreEqual(explicitRows[i].Row["name"].StrValue, commaRows[i].Row["name"].StrValue);
            Assert.AreEqual(explicitRows[i].Row["amount"].LongValue, commaRows[i].Row["amount"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCommaJoinWithSingleSourceFilter()
    {
        RobotsUserRobotsFixture fixture = await SetupRobotsAndUserRobots();

        List<QueryResultRow> result = await ExecuteRobotsJoinSelect(
            fixture,
            "SELECT r.name, u.amount FROM robots r, user_robots u "
            + "WHERE r.id = u.robots_id AND r.enabled = true ORDER BY u.amount");

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("Alpha", result[0].Row["name"].StrValue);
        Assert.AreEqual(100, result[0].Row["amount"].LongValue);
        Assert.AreEqual("Alpha", result[1].Row["name"].StrValue);
        Assert.AreEqual(200, result[1].Row["amount"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCommaJoinIndexedMatchesNestedLoop()
    {
        RobotsUserRobotsFixture nestedLoopFixture = await SetupRobotsAndUserRobots(indexRobotsId: false);
        RobotsUserRobotsFixture indexedFixture = await SetupRobotsAndUserRobots(indexRobotsId: true);

        const string sql =
            "SELECT r.name, u.amount FROM robots r, user_robots u "
            + "WHERE r.id = u.robots_id ORDER BY r.name, u.amount";

        List<QueryResultRow> nestedLoopRows = await ExecuteRobotsJoinSelect(nestedLoopFixture, sql);
        List<QueryResultRow> indexedRows = await ExecuteRobotsJoinSelect(indexedFixture, sql);

        Assert.AreEqual(nestedLoopRows.Count, indexedRows.Count);

        for (int i = 0; i < nestedLoopRows.Count; i++)
        {
            Assert.AreEqual(nestedLoopRows[i].Row["name"].StrValue, indexedRows[i].Row["name"].StrValue);
            Assert.AreEqual(nestedLoopRows[i].Row["amount"].LongValue, indexedRows[i].Row["amount"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectCommaJoinWithAliasedProjection()
    {
        RobotsUserRobotsFixture fixture = await SetupRobotsAndUserRobots();

        List<QueryResultRow> result = await ExecuteRobotsJoinSelect(
            fixture,
            "SELECT r.id, u.id AS uid, r.name, u.amount FROM robots r, user_robots u "
            + "WHERE r.id = u.robots_id ORDER BY u.amount DESC");

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual(200, result[0].Row["amount"].LongValue);
        Assert.AreEqual(100, result[1].Row["amount"].LongValue);
        Assert.AreEqual(50, result[2].Row["amount"].LongValue);
        Assert.IsTrue(result[0].Row.ContainsKey("uid"));
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupDistinctDupItems()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "dup_items",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("code", ColumnType.String, notNull: true),
                new("note", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        (string id, string code, string? note)[] rows =
        [
            (ObjectIdGenerator.Generate().ToString(), "A", null),
            (ObjectIdGenerator.Generate().ToString(), "A", null),
            (ObjectIdGenerator.Generate().ToString(), "A", "x"),
            (ObjectIdGenerator.Generate().ToString(), "B", null),
            (ObjectIdGenerator.Generate().ToString(), "C", null),
            (ObjectIdGenerator.Generate().ToString(), "C", null),
        ];

        foreach ((string id, string code, string? note) row in rows)
        {
            Dictionary<string, ColumnValue> values = new()
            {
                { "id", new(ColumnType.Id, row.id) },
                { "code", new(ColumnType.String, row.code) },
                {
                    "note",
                    row.note is not null
                        ? new(ColumnType.String, row.note)
                        : new ColumnValue(ColumnType.Null, 0)
                },
            };

            await executor.Insert(new InsertTicket(txnState, dbname, "dup_items", new List<Dictionary<string, ColumnValue>> { values }));
        }

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctSingleColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code FROM dup_items ORDER BY code",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual("A", result[0].Row["code"].StrValue);
        Assert.AreEqual("B", result[1].Row["code"].StrValue);
        Assert.AreEqual("C", result[2].Row["code"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctMultiColumnTuple()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code, note FROM dup_items ORDER BY code, note",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(4, result.Count);
        Assert.AreEqual("A", result[0].Row["code"].StrValue);
        Assert.AreEqual(ColumnType.Null, result[0].Row["note"].Type);
        Assert.AreEqual("A", result[1].Row["code"].StrValue);
        Assert.AreEqual("x", result[1].Row["note"].StrValue);
        Assert.AreEqual("B", result[2].Row["code"].StrValue);
        Assert.AreEqual(ColumnType.Null, result[2].Row["note"].Type);
        Assert.AreEqual("C", result[3].Row["code"].StrValue);
        Assert.AreEqual(ColumnType.Null, result[3].Row["note"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctNullDuplicates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code, note FROM dup_items WHERE code = \"A\" ORDER BY note",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual(ColumnType.Null, result[0].Row["note"].Type);
        Assert.AreEqual("x", result[1].Row["note"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctOrderByLimitAfterDedup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code FROM dup_items ORDER BY code LIMIT 2",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
        Assert.AreEqual("A", result[0].Row["code"].StrValue);
        Assert.AreEqual("B", result[1].Row["code"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctOrderByNonProjectedColumn_throwsInvalidInput()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code FROM dup_items ORDER BY note",
            parameters: null);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.ExecuteSQLQuery(ticket))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("ORDER BY", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctOrderByProjectedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDistinctDupItems();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT code FROM dup_items ORDER BY code",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual("A", result[0].Row["code"].StrValue);
        Assert.AreEqual("B", result[1].Row["code"].StrValue);
        Assert.AreEqual("C", result[2].Row["code"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteSelectDistinctStarRemovesExactDuplicates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT DISTINCT enabled FROM robots",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> result = await cursor.ToListAsync();

        Assert.AreEqual(2, result.Count);
    }

    #endregion
}
