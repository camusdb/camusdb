
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
using CamusDB.Core;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
public class TestExecuteSqlCreateTable : SharedNodeBaseTest
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("factory");
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor, CatalogsManager)> SetupDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (dbname, database, executor, new CatalogsManager(logger));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTable2()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(2, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.AreEqual(0, (new ColumnValue(ColumnType.String, "hello")).CompareTo(tableSchema.Columns![1].DefaultValue));
        Assert.False(tableSchema.Columns![1].NotNull);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableIfNotExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE IF NOT EXISTS robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(2, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.AreEqual(0, (new ColumnValue(ColumnType.String, "hello")).CompareTo(tableSchema.Columns![1].DefaultValue));
        Assert.False(tableSchema.Columns![1].NotNull);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE IF NOT EXISTS robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsFalse(ddlResult.Success);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableConstraints()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE robots (id OID NOT NULL, name STRING NOT NULL, year INT64 NOT NULL) PRIMARY KEY (id)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        ExecuteSQLTicket queryTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsEmpty(result);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableInlineUnique()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE app_users (id STRING PRIMARY KEY NOT NULL, email STRING UNIQUE NOT NULL, display_name STRING NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "app_users"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("email", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Unique, index!.Type);
        Assert.AreEqual(new[] { "email" }, index.Columns);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateUniqueIndexIfNotExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE app_users (id STRING PRIMARY KEY NOT NULL, email STRING NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        ExecuteSQLTicket createIndexTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE UNIQUE INDEX IF NOT EXISTS app_users_email_uq ON app_users (email)",
            parameters: null
        );

        ddlResult = await executor.ExecuteDDLSQL(createIndexTicket);
        Assert.IsTrue(ddlResult.Success);

        ddlResult = await executor.ExecuteDDLSQL(createIndexTicket);
        Assert.IsFalse(ddlResult.Success);

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "app_users"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("app_users_email_uq", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Unique, index!.Type);

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableDoublePk()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE robots (id OID NOT NULL PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL) PRIMARY KEY (id)",
            parameters: null
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteDDLSQL(createTableTicket));
        Assert.AreEqual("Primary key already exists on table 'robots'", exception!.Message);
    }
}
