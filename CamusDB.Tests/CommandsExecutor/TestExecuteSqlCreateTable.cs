
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

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableInlinePrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE t (`UsersId` OID NOT NULL, `Id` OID NOT NULL, PRIMARY KEY (`UsersId`, `Id`))",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        OpenTableTicket openTableTicket = new(databaseName: dbname, tableName: "t");
        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("~pk", out TableIndexSchema? pkIndex),
            "Inline PRIMARY KEY (UsersId, Id) must create the ~pk index");
        Assert.AreEqual(IndexType.Unique, pkIndex!.Type);
        CollectionAssert.AreEquivalent(new[] { "usersid", "id" }, pkIndex.Columns);

        await database.Transactions.CommitAsync(txnState);
    }

    /// <summary>
    /// Regression test for the bug where inline PRIMARY KEY and UNIQUE constraints were applied
    /// only in-memory on the leader and never written to the Raft schema log, so they were
    /// invisible after a descriptor cache eviction (which happens on every schema replication).
    ///
    /// After the fix, AddConstraints calls ReplicateIndexChangeAsync, which puts an AddIndex
    /// entry in the Raft log. The fix is verified by evicting the cached TableDescriptor
    /// (reproducing what InvalidateAppliedTableDescriptor does on every cluster node) and
    /// confirming the index is rebuilt correctly from tableSchema.Indexes.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestInlineConstraintsPersistedAfterDescriptorEviction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        // Case 1: PRIMARY KEY (cols) at end of column list
        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE orders (order_id OID NOT NULL, line_id OID NOT NULL, PRIMARY KEY (order_id, line_id))",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        // Evict the cached TableDescriptor — this is exactly what InvalidateAppliedTableDescriptor
        // does when an AddIndex Raft entry is applied on any node.
        database.TableDescriptors.TryRemove("orders", out _);

        // Rebuild from tableSchema.Indexes (the persisted B1 source of truth).
        OpenTableTicket openTableTicket = new(databaseName: dbname, tableName: "orders");
        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("~pk", out TableIndexSchema? pkIndex),
            "~pk must survive descriptor cache eviction — it must be in tableSchema.Indexes");
        Assert.AreEqual(IndexType.Unique, pkIndex!.Type);
        CollectionAssert.AreEquivalent(new[] { "order_id", "line_id" }, pkIndex.Columns);

        // Verify SHOW CREATE TABLE includes PRIMARY KEY
        ExecuteSQLTicket showCreateTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "SHOW CREATE TABLE orders",
            parameters: null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> showCursor) = await executor.ExecuteSQLQuery(showCreateTicket);
        List<QueryResultRow> showRows = await showCursor.ToListAsync();
        Assert.AreEqual(1, showRows.Count);
        string createSql = showRows[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("PRIMARY KEY", createSql, "SHOW CREATE TABLE must include PRIMARY KEY clause");

        // Verify DESC shows PRI for the pk columns
        ExecuteSQLTicket descTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DESC orders",
            parameters: null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> descCursor) = await executor.ExecuteSQLQuery(descTicket);
        List<QueryResultRow> descRows = await descCursor.ToListAsync();
        Assert.AreEqual(2, descRows.Count);
        Assert.AreEqual("PRI", descRows[0].Row["Key"].StrValue, "order_id must be Key=PRI");
        Assert.AreEqual("PRI", descRows[1].Row["Key"].StrValue, "line_id must be Key=PRI");

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInlineColumnPrimaryKeyPersistedAfterDescriptorEviction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        // Case 2: id OID PRIMARY KEY NOT NULL (inline column constraint form)
        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE products (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        // Evict the cached TableDescriptor
        database.TableDescriptors.TryRemove("products", out _);

        OpenTableTicket openTableTicket = new(databaseName: dbname, tableName: "products");
        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("~pk", out TableIndexSchema? pkIndex),
            "~pk must survive descriptor cache eviction for inline column PRIMARY KEY form");
        Assert.AreEqual(IndexType.Unique, pkIndex!.Type);
        CollectionAssert.AreEquivalent(new[] { "id" }, pkIndex.Columns);

        // Verify DESC shows PRI
        ExecuteSQLTicket descTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "DESC products",
            parameters: null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> descCursor) = await executor.ExecuteSQLQuery(descTicket);
        List<QueryResultRow> descRows = await descCursor.ToListAsync();
        Assert.AreEqual(2, descRows.Count);
        Assert.AreEqual("PRI", descRows.First(r => r.Row["Field"].StrValue == "id").Row["Key"].StrValue,
            "id column must be Key=PRI after descriptor eviction");

        await database.Transactions.CommitAsync(txnState);
    }

    [Test]
    [NonParallelizable]
    public async Task TestInlineUniqueIndexPersistedAfterDescriptorEviction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        // Case 3: UNIQUE inline column constraint
        ExecuteSQLTicket createTableTicket = new(
            txnState: txnState,
            database: dbname,
            sql: "CREATE TABLE app_users2 (id STRING PRIMARY KEY NOT NULL, email STRING UNIQUE NOT NULL)",
            parameters: null
        );

        ExecuteDDLSQLResult ddlResult = await executor.ExecuteDDLSQL(createTableTicket);
        Assert.IsTrue(ddlResult.Success);

        // Evict cached descriptor
        database.TableDescriptors.TryRemove("app_users2", out _);

        OpenTableTicket openTableTicket = new(databaseName: dbname, tableName: "app_users2");
        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("~pk", out TableIndexSchema? pkIndex),
            "~pk must survive eviction");
        Assert.AreEqual(IndexType.Unique, pkIndex!.Type);

        Assert.True(table.Indexes.TryGetValue("email", out TableIndexSchema? emailIndex),
            "UNIQUE email index must survive descriptor cache eviction");
        Assert.AreEqual(IndexType.Unique, emailIndex!.Type);
        CollectionAssert.AreEquivalent(new[] { "email" }, emailIndex.Columns);

        await database.Transactions.CommitAsync(txnState);
    }
}
